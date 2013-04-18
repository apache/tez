/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.tez.dag.app.dag.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.MRVertexOutputCommitter;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.tez.common.InputSpec;
import org.apache.tez.common.OutputSpec;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAGConfiguration;
import org.apache.tez.dag.api.EdgeProperty.ConnectionPattern;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.impl.NullVertexOutputCommitter;
import org.apache.tez.dag.api.impl.VertexContext;
import org.apache.tez.dag.api.impl.VertexOutputCommitter;
import org.apache.tez.dag.api.records.TaskState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexScheduler;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.event.DAGEventVertexCompleted;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventSourceTaskAttemptCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventSourceVertexStarted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskAttemptCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskAttemptFetchFailure;
import org.apache.tez.dag.app.dag.event.VertexEventTaskCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.engine.common.security.JobTokenIdentifier;
import org.apache.tez.engine.common.security.JobTokenSecretManager;
import org.apache.tez.engine.records.TezDAGID;
import org.apache.tez.engine.records.TezDependentTaskCompletionEvent;
import org.apache.tez.engine.records.TezTaskAttemptID;
import org.apache.tez.engine.records.TezTaskID;
import org.apache.tez.engine.records.TezVertexID;


/** Implementation of Vertex interface. Maintains the state machines of Vertex.
 * The read and write calls use ReadWriteLock for concurrency.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class VertexImpl implements org.apache.tez.dag.app.dag.Vertex,
  EventHandler<VertexEvent>, VertexContext {

  private static final TezDependentTaskCompletionEvent[]
    EMPTY_TASK_ATTEMPT_COMPLETION_EVENTS = new TezDependentTaskCompletionEvent[0];

  private static final Log LOG = LogFactory.getLog(VertexImpl.class);

  //The maximum fraction of fetch failures allowed for a map
  private static final double MAX_ALLOWED_FETCH_FAILURES_FRACTION = 0.5;

  // Maximum no. of fetch-failure notifications after which map task is failed
  private static final int MAX_FETCH_FAILURES_NOTIFICATIONS = 3;

  //final fields
  private final Clock clock;

  // TODO: Recovery
  //private final Map<TaskId, TaskInfo> completedTasksFromPreviousRun;

  private final Lock readLock;
  private final Lock writeLock;
  private final TaskAttemptListener taskAttemptListener;
  private final TaskHeartbeatHandler taskHeartbeatHandler;
  private final Object tasksSyncHandle = new Object();

  private final EventHandler eventHandler;
  // TODO Metrics
  //private final MRAppMetrics metrics;
  private final AppContext appContext;
  
  private boolean lazyTasksCopyNeeded = false;
  volatile Map<TezTaskID, Task> tasks = new LinkedHashMap<TezTaskID, Task>();
  private Object fullCountersLock = new Object();
  private TezCounters fullCounters = null;
  private Resource taskResource;

  public DAGConfiguration conf;

  //fields initialized in init

  private int numStartedSourceVertices = 0;
  private int distanceFromRoot = 0;
  
  private List<TezDependentTaskCompletionEvent> sourceTaskAttemptCompletionEvents;
  private final List<String> diagnostics = new ArrayList<String>();

  //task/attempt related datastructures
  private final Map<TezTaskID, Integer> successSourceAttemptCompletionEventNoMap =
    new HashMap<TezTaskID, Integer>();
  private final Map<TezTaskAttemptID, Integer> fetchFailuresMapping =
    new HashMap<TezTaskAttemptID, Integer>();

  List<InputSpec> inputSpecList;
  List<OutputSpec> outputSpecList;
  
  private static final InternalErrorTransition
      INTERNAL_ERROR_TRANSITION = new InternalErrorTransition();
  private static final TaskAttemptCompletedEventTransition
      TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION =
          new TaskAttemptCompletedEventTransition();
  private static final SourceTaskAttemptCompletedEventTransition
      SOURCE_TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION =
          new SourceTaskAttemptCompletedEventTransition();

  protected static final
    StateMachineFactory<VertexImpl, VertexState, VertexEventType, VertexEvent>
       stateMachineFactory
     = new StateMachineFactory<VertexImpl, VertexState, VertexEventType, VertexEvent>
              (VertexState.NEW)

          // Transitions from NEW state
          .addTransition
              (VertexState.NEW,
              EnumSet.of(VertexState.INITED, VertexState.FAILED),
              VertexEventType.V_INIT,
              new InitTransition())
          .addTransition(VertexState.NEW, VertexState.KILLED,
              VertexEventType.V_KILL,
              new KillNewVertexTransition())
          .addTransition(VertexState.NEW, VertexState.ERROR,
              VertexEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from INITED state
          .addTransition(VertexState.INITED, VertexState.INITED,
              VertexEventType.V_SOURCE_VERTEX_STARTED,
              new SourceVertexStartedTransition())
          .addTransition(VertexState.INITED, VertexState.RUNNING, 
              VertexEventType.V_START,
              new StartTransition())

          .addTransition(VertexState.INITED, VertexState.KILLED,
              VertexEventType.V_KILL,
              new KillInitedVertexTransition())
          .addTransition(VertexState.INITED, VertexState.ERROR,
              VertexEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from RUNNING state
          .addTransition(VertexState.RUNNING, VertexState.RUNNING,
              VertexEventType.V_TASK_ATTEMPT_COMPLETED,
              TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition(VertexState.RUNNING, VertexState.RUNNING,
              VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
              SOURCE_TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition
              (VertexState.RUNNING,
              EnumSet.of(VertexState.RUNNING, VertexState.SUCCEEDED, VertexState.FAILED),
              VertexEventType.V_TASK_COMPLETED,
              new TaskCompletedTransition())
          .addTransition
              (VertexState.RUNNING,
              EnumSet.of(VertexState.RUNNING, VertexState.SUCCEEDED, VertexState.FAILED),
              VertexEventType.V_COMPLETED,
              new VertexNoTasksCompletedTransition())
          .addTransition(VertexState.RUNNING, VertexState.KILL_WAIT,
              VertexEventType.V_KILL, new KillTasksTransition())
          .addTransition(VertexState.RUNNING, VertexState.RUNNING,
              VertexEventType.V_TASK_RESCHEDULED,
              new TaskRescheduledTransition())
          .addTransition(VertexState.RUNNING, VertexState.RUNNING,
              VertexEventType.V_TASK_ATTEMPT_FETCH_FAILURE,
              new TaskAttemptFetchFailureTransition())
          .addTransition(
              VertexState.RUNNING,
              VertexState.ERROR, VertexEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from KILL_WAIT state.
          .addTransition
              (VertexState.KILL_WAIT,
              EnumSet.of(VertexState.KILL_WAIT, VertexState.KILLED),
              VertexEventType.V_TASK_COMPLETED,
              new KillWaitTaskCompletedTransition())
          .addTransition(VertexState.KILL_WAIT, VertexState.KILL_WAIT,
              VertexEventType.V_TASK_ATTEMPT_COMPLETED,
              TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition(VertexState.KILL_WAIT, VertexState.KILL_WAIT,
              VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
              SOURCE_TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition(
              VertexState.KILL_WAIT,
              VertexState.ERROR, VertexEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(VertexState.KILL_WAIT, VertexState.KILL_WAIT,
              EnumSet.of(VertexEventType.V_KILL,
                  VertexEventType.V_TASK_RESCHEDULED,
                  VertexEventType.V_TASK_ATTEMPT_FETCH_FAILURE))

          // Transitions from SUCCEEDED state
          .addTransition(
              VertexState.SUCCEEDED,
              VertexState.ERROR, VertexEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(VertexState.SUCCEEDED, VertexState.SUCCEEDED,
              EnumSet.of(VertexEventType.V_KILL,
                  VertexEventType.V_TASK_ATTEMPT_FETCH_FAILURE,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_COMPLETED))

          // Transitions from FAILED state
          .addTransition(
              VertexState.FAILED,
              VertexState.ERROR, VertexEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(VertexState.FAILED, VertexState.FAILED,
              EnumSet.of(VertexEventType.V_KILL,
                  VertexEventType.V_TASK_ATTEMPT_FETCH_FAILURE,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_COMPLETED))

          // Transitions from KILLED state
          .addTransition(
              VertexState.KILLED,
              VertexState.ERROR, VertexEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(VertexState.KILLED, VertexState.KILLED,
              EnumSet.of(VertexEventType.V_KILL,
                  VertexEventType.V_TASK_ATTEMPT_FETCH_FAILURE,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_COMPLETED))

          // No transitions from INTERNAL_ERROR state. Ignore all.
          .addTransition(
              VertexState.ERROR,
              VertexState.ERROR,
              EnumSet.of(VertexEventType.V_INIT,
                  VertexEventType.V_KILL,
                  VertexEventType.V_TASK_COMPLETED,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_RESCHEDULED,
                  VertexEventType.V_DIAGNOSTIC_UPDATE,
                  VertexEventType.V_TASK_ATTEMPT_FETCH_FAILURE,
                  VertexEventType.INTERNAL_ERROR))
          // create the topology tables
          .installTopology();

  private final StateMachine<VertexState, VertexEventType, VertexEvent> stateMachine;

  //changing fields while the vertex is running
  private int numTasks;
  private int completedTaskCount = 0;
  private int succeededTaskCount = 0;
  private int failedTaskCount = 0;
  private int killedTaskCount = 0;

  private long initTime;
  private long startTime;
  private long finishTime;
  private float progress;

  private Credentials fsTokens;
  private Token<JobTokenIdentifier> jobToken;
  private JobTokenSecretManager jobTokenSecretManager;

  private final TezVertexID vertexId;

  private final String vertexName;
  private final String processorName;

  private Map<Vertex, EdgeProperty> sourceVertices;
  private int sourcePhysicalEdges = 0;
  private Map<Vertex, EdgeProperty> targetVertices;
  
  private VertexScheduler vertexScheduler;

  private VertexOutputCommitter committer;
  private AtomicBoolean committed = new AtomicBoolean(false);
  private VertexLocationHint vertexLocationHint;
  private Map<String, LocalResource> localResources;
  private Map<String, String> environment;

  public VertexImpl(TezVertexID vertexId, String vertexName,
      DAGConfiguration conf, EventHandler eventHandler,
      TaskAttemptListener taskAttemptListener,
      JobTokenSecretManager jobTokenSecretManager,
      Token<JobTokenIdentifier> jobToken,
      Credentials fsTokenCredentials, Clock clock,
      // TODO: Recovery
      //Map<TaskId, TaskInfo> completedTasksFromPreviousRun,
      // TODO Metrics
      //MRAppMetrics metrics,
      TaskHeartbeatHandler thh,
      AppContext appContext, VertexLocationHint vertexLocationHint) {
    this.vertexId = vertexId;
    this.vertexName = vertexName;
    this.conf = conf;
    //this.metrics = metrics;
    this.clock = clock;
    // TODO: Recovery
    //this.completedTasksFromPreviousRun = completedTasksFromPreviousRun;
    this.appContext = appContext;

    this.taskAttemptListener = taskAttemptListener;
    this.taskHeartbeatHandler = thh;
    this.eventHandler = eventHandler;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    this.fsTokens = fsTokenCredentials;
    this.jobTokenSecretManager = jobTokenSecretManager;
    this.jobToken = jobToken;
    this.committer = new NullVertexOutputCommitter();
    this.vertexLocationHint = vertexLocationHint;
    
    this.taskResource = conf.getVertexResource(getName());
    this.processorName = conf.getVertexTaskModuleClassName(getName());
    this.localResources = conf.getVertexLocalResources(getName());
    this.environment = conf.getVertexEnv(getName());

    // This "this leak" is okay because the retained pointer is in an
    //  instance variable.
    stateMachine = stateMachineFactory.make(this);
  }

  protected StateMachine<VertexState, VertexEventType, VertexEvent> getStateMachine() {
    return stateMachine;
  }

  @Override
  public TezVertexID getVertexId() {
    return vertexId;
  }
  
  @Override
  public int getDistanceFromRoot() {
    return distanceFromRoot;
  }

  @Override
  public DAGConfiguration getConf() {
    // FIXME this should be renamed as it is giving global DAG conf
    // we need a function to give user-land configuration for this vertex
    return conf;
  }

  @Override
  public String getName() {
    return vertexName;
  }

  EventHandler getEventHandler() {
    return this.eventHandler;
  }

  @Override
  public Task getTask(TezTaskID taskID) {
    readLock.lock();
    try {
      return tasks.get(taskID);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public int getTotalTasks() {
    return numTasks;
  }

  @Override
  public int getCompletedTasks() {
    readLock.lock();
    try {
      return succeededTaskCount + failedTaskCount + killedTaskCount;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TezCounters getAllCounters() {

    readLock.lock();

    try {
      VertexState state = getInternalState();
      if (state == VertexState.ERROR || state == VertexState.FAILED
          || state == VertexState.KILLED || state == VertexState.SUCCEEDED) {
        this.mayBeConstructFinalFullCounters();
        return fullCounters;
      }

      TezCounters counters = new TezCounters();
      return incrTaskCounters(counters, tasks.values());

    } finally {
      readLock.unlock();
    }
  }

  public static TezCounters incrTaskCounters(
      TezCounters counters, Collection<Task> tasks) {
    for (Task task : tasks) {
      counters.incrAllCounters(task.getCounters());
    }
    return counters;
  }

  @Override
  public TezDependentTaskCompletionEvent[] getTaskAttemptCompletionEvents(
      int fromEventId, int maxEvents) {
    TezDependentTaskCompletionEvent[] events = EMPTY_TASK_ATTEMPT_COMPLETION_EVENTS;
    readLock.lock();
    try {
      if (sourceTaskAttemptCompletionEvents.size() > fromEventId) {
        int actualMax = Math.min(maxEvents,
            (sourceTaskAttemptCompletionEvents.size() - fromEventId));
        events = sourceTaskAttemptCompletionEvents.subList(fromEventId,
            actualMax + fromEventId).toArray(events);
      }
      return events;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<String> getDiagnostics() {
    readLock.lock();
    try {
      return diagnostics;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public float getProgress() {
    this.readLock.lock();
    try {
      computeProgress();
      return progress;
    } finally {
      this.readLock.unlock();
    }
  }

  private void computeProgress() {
    this.readLock.lock();
    try {
      float progress = 0f;
      for (Task task : this.tasks.values()) {
        progress += (task.isFinished() ? 1f : task.getProgress());
      }
      if (this.numTasks != 0) {
        progress /= this.numTasks;
      }
      this.progress = progress;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Map<TezTaskID, Task> getTasks() {
    synchronized (tasksSyncHandle) {
      lazyTasksCopyNeeded = true;
      return Collections.unmodifiableMap(tasks);
    }
  }

  @Override
  public VertexState getState() {
    readLock.lock();
    try {
      return getStateMachine().getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void scheduleTasks(Collection<TezTaskID> taskIDs) {
    for (TezTaskID taskID : taskIDs) {
      eventHandler.handle(new TaskEvent(taskID,
          TaskEventType.T_SCHEDULE));
    }
  }

  @Override
  /**
   * The only entry point to change the Vertex.
   */
  public void handle(VertexEvent event) {
    LOG.info("DEBUG: Processing VertexEvent " + event.getVertexId()
        + " of type " + event.getType() + " while in state "
        + getInternalState() + ". Event: " + event);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing VertexEvent " + event.getVertexId() + " of type "
          + event.getType() + " while in state " + getInternalState());
    }
    try {
      writeLock.lock();
      VertexState oldState = getInternalState();
      try {
         getStateMachine().doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        addDiagnostic("Invalid event " + event.getType() +
            " on Job " + this.vertexId);
        eventHandler.handle(new VertexEvent(this.vertexId,
            VertexEventType.INTERNAL_ERROR));
      }
      //notify the eventhandler of state change
      if (oldState != getInternalState()) {
        LOG.info(vertexId + " transitioned from " + oldState + " to "
                 + getInternalState());
      }
    }

    finally {
      writeLock.unlock();
    }
  }

  @Private
  public VertexState getInternalState() {
    readLock.lock();
    try {
     return getStateMachine().getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  //helpful in testing
  protected void addTask(Task task) {
    synchronized (tasksSyncHandle) {
      if (lazyTasksCopyNeeded) {
        Map<TezTaskID, Task> newTasks = new LinkedHashMap<TezTaskID, Task>();
        newTasks.putAll(tasks);
        tasks = newTasks;
        lazyTasksCopyNeeded = false;
      }
    }
    tasks.put(task.getTaskId(), task);
    // TODO Metrics
    //metrics.waitingTask(task);
  }

  void setFinishTime() {
    finishTime = clock.getTime();
  }

  void logJobHistoryVertexInitedEvent() {
    // TODO JobHistory
    /*
    JobSubmittedEvent jse = new JobSubmittedEvent(job.oldJobId,
        job.conf.get(MRJobConfig.JOB_NAME, "test"),
      job.conf.get(MRJobConfig.USER_NAME, "mapred"),
      job.appSubmitTime,
      job.remoteJobConfFile.toString(),
      job.jobACLs, job.queueName);
    job.eventHandler.handle(new JobHistoryEvent(job.jobId, jse));
    */
  }

  void logJobHistoryVertexStartedEvent() {
    // TODO JobHistory
    /*
      JobInitedEvent jie =
        new JobInitedEvent(job.oldJobId,
             job.startTime,
             job.numMapTasks, job.numReduceTasks,
             job.getState().toString(),
             job.isUber()); //Will transition to state running. Currently in INITED
      job.eventHandler.handle(new JobHistoryEvent(job.jobId, jie));
      JobInfoChangeEvent jice = new JobInfoChangeEvent(job.oldJobId,
          job.appSubmitTime, job.startTime);
      job.eventHandler.handle(new JobHistoryEvent(job.jobId, jice));
     */

  }


  void logJobHistoryVertexFinishedEvent() {
    this.setFinishTime();
    // TODO JobHistory
    //eventHandler.handle(new JobFinishEvent(jobId));
  }

  void logJobHistoryVertexAbortedEvent() {
    // TODO JobHistory
    /*
    JobUnsuccessfulCompletionEvent unsuccessfulJobEvent =
        new JobUnsuccessfulCompletionEvent(oldJobId,
            finishTime,
            succeededMapTaskCount,
            succeededReduceTaskCount,
            finalState.toString());
      eventHandler.handle(new JobHistoryEvent(jobId, unsuccessfulJobEvent));
      */
  }

  void logJobHistoryUnsuccessfulVertexCompletion() {
    // TODO JobHistory
    /*
    JobUnsuccessfulCompletionEvent failedEvent =
        new JobUnsuccessfulCompletionEvent(job.oldJobId,
            job.finishTime, 0, 0,
            VertexState.KILLED.toString());
    job.eventHandler.handle(new JobHistoryEvent(job.jobId, failedEvent));
    */
  }

  /**
   * Create the default file System for this job.
   * @param conf the conf object
   * @return the default filesystem for this job
   * @throws IOException
   */
  protected FileSystem getFileSystem(Configuration conf) throws IOException {
    return FileSystem.get(conf);
  }

  static VertexState checkVertexCompleteSuccess(VertexImpl vertex) {
    // FIXME this vertex is definitely buggy as completed includes killed/failed
    // check for vertex success
    if (vertex.completedTaskCount == vertex.tasks.size()) {
      if (vertex.failedTaskCount > 0) {
        try {
          vertex.committer.abortVertex(VertexStatus.State.FAILED);
        } catch (IOException e) {
          LOG.error("Failed to do abort on vertex, name=" + vertex.getName(),
              e);
        }
        vertex.eventHandler.handle(new DAGEventVertexCompleted(vertex
            .getVertexId(), VertexState.FAILED));
        return vertex.finished(VertexState.FAILED);
      } else {
        try {
          if (!vertex.committed.getAndSet(true)) {
            // commit only once
            vertex.committer.commitVertex();
          }
        } catch (IOException e) {
          LOG.error("Failed to do commit on vertex, name=" + vertex.getName(), e);
          vertex.eventHandler.handle(new DAGEventVertexCompleted(vertex
              .getVertexId(), VertexState.FAILED));
          return vertex.finished(VertexState.FAILED);
        }
        vertex.eventHandler.handle(new DAGEventVertexCompleted(vertex
            .getVertexId(), vertex.getState()));
        return vertex.finished(VertexState.SUCCEEDED);
      }
    }
    // TODO: what if one of the tasks failed?
    return null;
  }

  VertexState finished(VertexState finalState) {
    if (getInternalState() == VertexState.RUNNING) {
      // TODO: Metrics
      // metrics.endRunningJob(this);
    }
    if (finishTime == 0) setFinishTime();
    logJobHistoryVertexFinishedEvent();

    switch (finalState) {
      case KILLED:
        // TODO: Metrics
        //metrics.killedJob(this);
        break;
      case FAILED:
        // TODO: Metrics
        //metrics.failedJob(this);
        break;
      case SUCCEEDED:
        // TODO: Metrics
        //metrics.completedJob(this);
    }
    return finalState;
  }

  public static class InitTransition
      implements MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      try {
        //log to job history
        vertex.logJobHistoryVertexInitedEvent();

        // TODODAGAM
        // TODO: Splits?
        vertex.numTasks = vertex.conf.getNumVertexTasks(vertex.getName());
        /*
        TaskSplitMetaInfo[] taskSplitMetaInfo = createSplits(job, job.jobId);
        job.numMapTasks = taskSplitMetaInfo.length;
        */

        if (vertex.numTasks == 0) {
          vertex.addDiagnostic("No of tasks for vertex " + vertex.getVertexId());
        }

        checkTaskLimits();

        // FIXME should depend on source num tasks
        vertex.sourceTaskAttemptCompletionEvents =
            new ArrayList<TezDependentTaskCompletionEvent>(vertex.numTasks + 10);

        // create the Tasks but don't start them yet
        createTasks(vertex);
        
        

        // FIXME this only works if all edges are bipartite
        boolean hasBipartite = false;
        if (vertex.sourceVertices != null) {
          for (EdgeProperty edgeProperty : vertex.sourceVertices.values()) {
            // FIXME The init needs to be in
            // topo sort order of graph or else source may not be initialized.
            // Also should not depend on assumption of single-threaded dispatcher
            if(edgeProperty.getConnectionPattern() == ConnectionPattern.BIPARTITE) {
              hasBipartite = true;
              break;
            }
          }
        }
        
        if (hasBipartite) {
          // setup vertex scheduler
          // TODO this needs to consider data size and perhaps API. 
          // Currently implicitly BIPARTITE is the only edge type
          vertex.vertexScheduler = new 
              BipartiteSlowStartVertexScheduler(vertex,
                                                0.5f,  // FIXME get from config
                                                0.8f); // FIXME get from config
        } else {
          // schedule all tasks upon vertex start
          vertex.vertexScheduler = new ImmediateStartVertexScheduler(vertex);
        }

        // FIXME how do we decide vertex needs a committer?
        // for now, only for leaf vertices
        // FIXME make commmitter type configurable per vertex
        if (vertex.targetVertices.isEmpty()) {
          vertex.committer = new MRVertexOutputCommitter();
        }
        vertex.committer.init(vertex);
        vertex.committer.setupVertex();

        // TODO: Metrics
        //vertex.metrics.endPreparingJob(job);
        vertex.initTime = vertex.clock.getTime();
        return VertexState.INITED;

      } catch (IOException e) {
        LOG.warn("Vertex init failed", e);
        vertex.addDiagnostic("Job init failed : "
            + StringUtils.stringifyException(e));
        vertex.abortVertex(VertexStatus.State.FAILED);
        // TODO: Metrics
        //job.metrics.endPreparingJob(vertex);
        return vertex.finished(VertexState.FAILED);
      }
    }


    private void createTasks(VertexImpl vertex) {
      // TODO Fixme
      DAGConfiguration conf = vertex.getConf();
      boolean useNullLocationHint = true;
      if (vertex.vertexLocationHint != null
          && vertex.vertexLocationHint.getTaskLocationHints() != null
          && vertex.vertexLocationHint.getTaskLocationHints().length ==
              vertex.numTasks) {
        useNullLocationHint = false;
      }
      for (int i=0; i < vertex.numTasks; ++i) {
        TaskLocationHint locHint = null;
        if (!useNullLocationHint) {
          locHint = vertex.vertexLocationHint.getTaskLocationHints()[i];
        }
        TaskImpl task =
            new TaskImpl(vertex.getVertexId(), i,
                vertex.eventHandler,
                null,
                conf,
                vertex.taskAttemptListener,
                null,
                vertex.jobToken,
                vertex.fsTokens,
                vertex.clock,
                vertex.taskHeartbeatHandler,
                vertex.appContext,
                vertex.processorName, false,
                locHint, vertex.taskResource,
                vertex.localResources,
                vertex.environment);
        vertex.addTask(task);
        LOG.info("Created task for vertex " + vertex.getVertexId() + ": " +
            task.getTaskId());
      }

    }

    // TODO: Splits
    /*
    protected TaskSplitMetaInfo[] createSplits(VertexImpl job, JobId jobId) {
      TaskSplitMetaInfo[] allTaskSplitMetaInfo;
      try {
        allTaskSplitMetaInfo = SplitMetaInfoReader.readSplitMetaInfo(
            job.oldJobId, job.fs,
            job.conf,
            job.remoteJobSubmitDir);
      } catch (IOException e) {
        throw new YarnException(e);
      }
      return allTaskSplitMetaInfo;
    }
    */

    /**
     * If the number of tasks are greater than the configured value
     * throw an exception that will fail job initialization
     */
    private void checkTaskLimits() {
      // no code, for now
    }
  } // end of InitTransition

  // Temporary to maintain topological order while starting vertices. Not useful
  // since there's not much difference between the INIT and RUNNING states.
  public static class SourceVertexStartedTransition implements
      SingleArcTransition<VertexImpl, VertexEvent> {

    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      VertexEventSourceVertexStarted startEvent = 
                                      (VertexEventSourceVertexStarted) event;
      int distanceFromRoot = startEvent.getSourceDistanceFromRoot() + 1;
      if(vertex.distanceFromRoot < distanceFromRoot) {
        vertex.distanceFromRoot = distanceFromRoot;
      }
      vertex.numStartedSourceVertices++;
      if (vertex.numStartedSourceVertices == vertex.sourceVertices.size()) {
        // Consider inlining this.
        LOG.info("Starting vertex: " + vertex.getVertexId() + 
                 " with name: " + vertex.getName() + 
                 " with distanceFromRoot: " + vertex.distanceFromRoot );
        vertex.eventHandler.handle(new VertexEvent(vertex.vertexId,
            VertexEventType.V_START));
      }
    }
  }

  public static class StartTransition
  implements SingleArcTransition<VertexImpl, VertexEvent> {
    /**
     * This transition executes in the event-dispatcher thread, though it's
     * triggered in MRAppMaster's startJobs() method.
     */
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      vertex.startTime = vertex.clock.getTime();
      vertex.vertexScheduler.onVertexStarted();
      vertex.logJobHistoryVertexStartedEvent();

      // TODO: Metrics
      //job.metrics.runningJob(job);

      // default behavior is to start immediately. so send information about us
      // starting to downstream vertices. If the connections/structure of this
      // vertex is not fully defined yet then we could send this event later
      // when we are ready
      for (Vertex targetVertex : vertex.targetVertices.keySet()) {
        vertex.eventHandler.handle(
            new VertexEventSourceVertexStarted(targetVertex.getVertexId(),
                                               vertex.distanceFromRoot));
      }

      // If we have no tasks, just transition to vertex completed
      if (vertex.numTasks == 0) {
        vertex.eventHandler.handle(
            new VertexEvent(vertex.vertexId, VertexEventType.V_COMPLETED));
      }
    }
  }

  private void abortVertex(VertexStatus.State finalState) {
    //TODO: Committer?    /*
    try {
      committer.abortVertex(finalState);
    } catch (IOException e) {
      LOG.warn("Could not abort vertex, name=" + getName(), e);
    }

    if (finishTime == 0) {
      setFinishTime();
    }
    logJobHistoryVertexAbortedEvent();
  }

  private void mayBeConstructFinalFullCounters() {
    // Calculating full-counters. This should happen only once for the vertex.
    synchronized (this.fullCountersLock) {
      if (this.fullCounters != null) {
        // Already constructed. Just return.
        return;
      }
      this.constructFinalFullcounters();
    }
  }

  @Private
  public void constructFinalFullcounters() {
    this.fullCounters = new TezCounters();
    for (Task t : this.tasks.values()) {
      TezCounters counters = t.getCounters();
      this.fullCounters.incrAllCounters(counters);
    }
  }

  // Task-start has been moved out of InitTransition, so this arc simply
  // hardcodes 0 for both map and reduce finished tasks.
  private static class KillNewVertexTransition
  implements SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      vertex.setFinishTime();
      vertex.logJobHistoryUnsuccessfulVertexCompletion();
      vertex.finished(VertexState.KILLED);
    }
  }

  private static class KillInitedVertexTransition
  implements SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      vertex.abortVertex(VertexStatus.State.KILLED);
      vertex.addDiagnostic("Job received Kill in INITED state.");
      vertex.finished(VertexState.KILLED);
    }
  }

  private static class KillTasksTransition
      implements SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      vertex.addDiagnostic("Job received Kill while in RUNNING state.");
      for (Task task : vertex.tasks.values()) {
        vertex.eventHandler.handle(
            new TaskEvent(task.getTaskId(), TaskEventType.T_KILL));
      }
      // TODO: Metrics
      //job.metrics.endRunningJob(job);
    }
  }

  /**
   * Here, the Vertex is being told that one of his source task-attempts
   * completed.
   */
  private static class SourceTaskAttemptCompletedEventTransition implements
  SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      TezDependentTaskCompletionEvent tce =
          ((VertexEventSourceTaskAttemptCompleted) event).getCompletionEvent();
      // Add the TaskAttemptCompletionEvent
      //eventId is equal to index in the arraylist
      tce.setEventId(vertex.sourceTaskAttemptCompletionEvents.size());
      vertex.sourceTaskAttemptCompletionEvents.add(tce);
      // FIXME this needs to be ordered/grouped by source vertices or else 
      // my tasks will not know which events are for which vertices' tasks. This 
      // differentiation was not needed for MR because there was only 1 M stage.
      // if the tce is sent to the task then a solution could be to add vertex 
      // name to the tce
      // need to send vertex name and task index in that vertex

      TezTaskAttemptID attemptId = tce.getTaskAttemptID();
      TezTaskID taskId = attemptId.getTaskID();
      //make the previous completion event as obsolete if it exists
      if (TezDependentTaskCompletionEvent.Status.SUCCEEDED.equals(tce.getStatus())) {
        Object successEventNo =
            vertex.successSourceAttemptCompletionEventNoMap.remove(taskId);
        if (successEventNo != null) {
          TezDependentTaskCompletionEvent successEvent =
              vertex.sourceTaskAttemptCompletionEvents.get((Integer) successEventNo);
          successEvent.setTaskStatus(TezDependentTaskCompletionEvent.Status.OBSOLETE);
        }
        vertex.successSourceAttemptCompletionEventNoMap.put(taskId, tce.getEventId());
      }
      
      vertex.vertexScheduler.onSourceTaskCompleted(attemptId);
    }
  }

  private static class TaskAttemptCompletedEventTransition implements
      SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      TezDependentTaskCompletionEvent tce =
        ((VertexEventTaskAttemptCompleted) event).getCompletionEvent();

      // FIXME this should only be sent for successful events? looks like all 
      // need to be sent in the existing shuffle code
      // Notify all target vertices
      if (vertex.targetVertices != null) {
        for (Vertex targetVertex : vertex.targetVertices.keySet()) {
          vertex.eventHandler.handle(
              new VertexEventSourceTaskAttemptCompleted(
                  targetVertex.getVertexId(), tce)
              );
        }
      }
    }
  }

  private static class TaskAttemptFetchFailureTransition implements
      SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      VertexEventTaskAttemptFetchFailure fetchfailureEvent =
          (VertexEventTaskAttemptFetchFailure) event;
      for (TezTaskAttemptID mapId : fetchfailureEvent.getSources()) {
        Integer fetchFailures = vertex.fetchFailuresMapping.get(mapId);
        fetchFailures = (fetchFailures == null) ? 1 : (fetchFailures+1);
        vertex.fetchFailuresMapping.put(mapId, fetchFailures);

        //get number of running reduces
        int runningReduceTasks = 0;
        for (TezTaskID taskId : vertex.tasks.keySet()) {
          if (TaskState.RUNNING.equals(vertex.tasks.get(taskId).getState())) {
            runningReduceTasks++;
          }
        }

        float failureRate = runningReduceTasks == 0 ? 1.0f :
          (float) fetchFailures / runningReduceTasks;
        // declare faulty if fetch-failures >= max-allowed-failures
        boolean isMapFaulty =
            (failureRate >= MAX_ALLOWED_FETCH_FAILURES_FRACTION);
        if (fetchFailures >= MAX_FETCH_FAILURES_NOTIFICATIONS && isMapFaulty) {
          LOG.info("Too many fetch-failures for output of task attempt: " +
              mapId + " ... raising fetch failure to source");
          vertex.eventHandler.handle(new TaskAttemptEvent(mapId,
              TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURES));
          vertex.fetchFailuresMapping.remove(mapId);
        }
      }
    }
  }

  private static class TaskCompletedTransition implements
      MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      vertex.completedTaskCount++;//FIXME this is a bug
      LOG.info("Num completed Tasks: " + vertex.completedTaskCount);
      VertexEventTaskCompleted taskEvent = (VertexEventTaskCompleted) event;
      Task task = vertex.tasks.get(taskEvent.getTaskID());
      if (taskEvent.getState() == TaskState.SUCCEEDED) {
        taskSucceeded(vertex, task);
      } else if (taskEvent.getState() == TaskState.FAILED) {
        taskFailed(vertex, task);
      } else if (taskEvent.getState() == TaskState.KILLED) {
        taskKilled(vertex, task);
      }

      vertex.vertexScheduler.onVertexCompleted();
      VertexState state = checkVertexForCompletion(vertex);
      if(state == VertexState.SUCCEEDED) {
        vertex.vertexScheduler.onVertexCompleted();
      }
      return state;
    }

    protected VertexState checkVertexForCompletion(VertexImpl vertex) {
      //check for vertex failure
      if (vertex.failedTaskCount > 1) {
        vertex.setFinishTime();

        String diagnosticMsg = "Vertex failed as tasks failed. "
            + "failedTasks:"
            + vertex.failedTaskCount;
        LOG.info(diagnosticMsg);
        vertex.addDiagnostic(diagnosticMsg);
        vertex.abortVertex(VertexStatus.State.FAILED);
        vertex.eventHandler.handle(new DAGEventVertexCompleted(vertex
            .getVertexId(), VertexState.FAILED));
        return vertex.finished(VertexState.FAILED);
      }

      VertexState vertexCompleteSuccess =
          VertexImpl.checkVertexCompleteSuccess(vertex);
      if (vertexCompleteSuccess != null) {
        return vertexCompleteSuccess;
      }

      //return the current state, Vertex not finished yet
      return vertex.getInternalState();
    }

    private void taskSucceeded(VertexImpl vertex, Task task) {
      vertex.succeededTaskCount++;
      // TODO Metrics
      // job.metrics.completedTask(task);
    }

    private void taskFailed(VertexImpl vertex, Task task) {
      vertex.failedTaskCount++;
      vertex.addDiagnostic("Task failed " + task.getTaskId());
      // TODO Metrics
      //vertex.metrics.failedTask(task);
    }

    private void taskKilled(VertexImpl vertex, Task task) {
      vertex.killedTaskCount++;
      // TODO Metrics
      //job.metrics.killedTask(task);
    }
  }

  // Transition class for handling jobs with no tasks
  // TODODAGAM - is this allowed for a vertex?
  static class VertexNoTasksCompletedTransition implements
  MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      VertexState vertexCompleteSuccess =
          VertexImpl.checkVertexCompleteSuccess(vertex);
      if (vertexCompleteSuccess != null) {
        return vertexCompleteSuccess;
      }

      // Return the current state, Job not finished yet
      return vertex.getInternalState();
    }
  }

  private static class TaskRescheduledTransition implements
      SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      //succeeded map task is restarted back
      vertex.completedTaskCount--;
      vertex.succeededTaskCount--;
    }
  }

  private static class KillWaitTaskCompletedTransition extends
      TaskCompletedTransition {
    @Override
    protected VertexState checkVertexForCompletion(VertexImpl vertex) {
      if (vertex.completedTaskCount == vertex.tasks.size()) {
        vertex.setFinishTime();
        vertex.abortVertex(VertexStatus.State.KILLED);
        return vertex.finished(VertexState.KILLED);
      }
      //return the current state, Job not finished yet
      return vertex.getInternalState();
    }
  }

  private void addDiagnostic(String diag) {
    diagnostics.add(diag);
  }

  private static class InternalErrorTransition implements
      SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      //TODO Is this JH event required.
      vertex.setFinishTime();
      vertex.logJobHistoryUnsuccessfulVertexCompletion();
      vertex.finished(VertexState.ERROR);
    }
  }

  @Override
  public void setInputVertices(Map<Vertex, EdgeProperty> inVertices) {
    this.sourceVertices = inVertices;
  }

  @Override
  public void setOutputVertices(Map<Vertex, EdgeProperty> outVertices) {
    this.targetVertices = outVertices;
  }

  @Override
  public int compareTo(Vertex other) {
    return this.vertexId.compareTo(other.getVertexId());
  }

  @Override
  public Map<Vertex, EdgeProperty> getInputVertices() {
    return Collections.unmodifiableMap(this.sourceVertices);
  }

  @Override
  public Map<Vertex, EdgeProperty> getOutputVertices() {
    return Collections.unmodifiableMap(this.targetVertices);
  }

  @Override
  public int getInputVerticesCount() {
    return this.sourceVertices.size();
  }

  @Override
  public int getOutputVerticesCount() {
    return this.targetVertices.size();
  }

  @Override
  public TezDAGID getDAGId() {
    return appContext.getDAGID();
  }

  public Resource getTaskResource() {
    return taskResource;
  }

  @Override
  public DAG getDAG() {
    return appContext.getDAG();
  }

  // TODO Eventually remove synchronization.
  @Override
  public synchronized List<InputSpec> getInputSpecList() {
    inputSpecList = new ArrayList<InputSpec>(
        this.getInputVerticesCount());
    for (Vertex srcVertex : this.getInputVertices().keySet()) {
      InputSpec inputSpec = new InputSpec(srcVertex.getName(),
          srcVertex.getTotalTasks());
      LOG.info("DEBUG: For vertex : " + this.getName()
          + ", Using InputSpec : " + inputSpec);
      // TODO DAGAM This should be based on the edge type.
      inputSpecList.add(inputSpec);
    }
    return inputSpecList;
  }

  // TODO Eventually remove synchronization.
  @Override
  public synchronized List<OutputSpec> getOutputSpecList() {
    if (this.outputSpecList == null) {
      outputSpecList = new ArrayList<OutputSpec>(this.getOutputVerticesCount());
      for (Vertex targetVertex : this.getOutputVertices().keySet()) {
        OutputSpec outputSpec = new OutputSpec(targetVertex.getName(),
            targetVertex.getTotalTasks());
        LOG.info("DEBUG: For vertex : " + this.getName()
            + ", Using OutputSpec : " + outputSpec);
        // TODO DAGAM This should be based on the edge type.
        outputSpecList.add(outputSpec);
      }
    }
    return outputSpecList;
  }
}
