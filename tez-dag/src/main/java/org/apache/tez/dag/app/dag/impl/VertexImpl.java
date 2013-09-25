/* Licensed to the Apache Software Foundation (ASF) under one
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.MRVertexOutputCommitter;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.dag.api.client.ProgressBuilder;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.VertexStatusBuilder;
import org.apache.tez.dag.api.committer.NullVertexOutputCommitter;
import org.apache.tez.dag.api.committer.VertexContext;
import org.apache.tez.dag.api.committer.VertexOutputCommitter;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.EdgeManager;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.TaskTerminationCause;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexScheduler;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.VertexTerminationCause;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventDiagnosticsUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.DAGEventVertexCompleted;
import org.apache.tez.dag.app.dag.event.DAGEventVertexReRunning;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventAttemptFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStatusUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventTermination;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.dag.event.VertexEventSourceTaskAttemptCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventSourceVertexStarted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskAttemptCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskReschedule;
import org.apache.tez.dag.app.dag.event.VertexEventTermination;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.api.events.TaskAttemptFailedEvent;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TezEvent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;


/** Implementation of Vertex interface. Maintains the state machines of Vertex.
 * The read and write calls use ReadWriteLock for concurrency.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class VertexImpl implements org.apache.tez.dag.app.dag.Vertex,
  EventHandler<VertexEvent> {

  private static final String LINE_SEPARATOR = System
      .getProperty("line.separator");

  private static final Log LOG = LogFactory.getLog(VertexImpl.class);

  //final fields
  private final Clock clock;


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
  // must be a linked map for ordering
  volatile LinkedHashMap<TezTaskID, Task> tasks = new LinkedHashMap<TezTaskID, Task>();
  private Object fullCountersLock = new Object();
  private TezCounters fullCounters = null;
  private Resource taskResource;

  private Configuration conf;

  //fields initialized in init

  private int numStartedSourceVertices = 0;
  private int distanceFromRoot = 0;

  private final List<String> diagnostics = new ArrayList<String>();

  //task/attempt related datastructures
  @VisibleForTesting
  int numSuccessSourceAttemptCompletions = 0;

  List<InputSpec> inputSpecList;
  List<OutputSpec> outputSpecList;

  private static final InternalErrorTransition
      INTERNAL_ERROR_TRANSITION = new InternalErrorTransition();
  private static final RouteEventTransition
      ROUTE_EVENT_TRANSITION = new RouteEventTransition();
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
              VertexEventType.V_TERMINATE,
              new TerminateNewVertexTransition())
          .addTransition(VertexState.NEW, VertexState.ERROR,
              VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from INITED state
          .addTransition(VertexState.INITED, VertexState.INITED,
              VertexEventType.V_SOURCE_VERTEX_STARTED,
              new SourceVertexStartedTransition())
          .addTransition(VertexState.INITED, VertexState.RUNNING,
              VertexEventType.V_START,
              new StartTransition())

          .addTransition(VertexState.INITED, VertexState.KILLED,
              VertexEventType.V_TERMINATE,
              new TerminateInitedVertexTransition())
          .addTransition(VertexState.INITED, VertexState.ERROR,
              VertexEventType.V_INTERNAL_ERROR,
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
              EnumSet.of(VertexState.RUNNING,
                  VertexState.SUCCEEDED, VertexState.TERMINATING, VertexState.FAILED),
              VertexEventType.V_TASK_COMPLETED,
              new TaskCompletedTransition())
          .addTransition(VertexState.RUNNING, VertexState.TERMINATING,
              VertexEventType.V_TERMINATE,
              new VertexKilledTransition())
          .addTransition(VertexState.RUNNING, VertexState.RUNNING,
              VertexEventType.V_TASK_RESCHEDULED,
              new TaskRescheduledTransition())
          .addTransition(
              VertexState.RUNNING,
              VertexState.ERROR, VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(
              VertexState.RUNNING,
              VertexState.RUNNING, VertexEventType.V_ROUTE_EVENT,
              ROUTE_EVENT_TRANSITION)

          // Transitions from TERMINATING state.
          .addTransition
              (VertexState.TERMINATING,
              EnumSet.of(VertexState.TERMINATING, VertexState.KILLED, VertexState.FAILED),
              VertexEventType.V_TASK_COMPLETED,
              new TaskCompletedTransition())
          .addTransition(VertexState.TERMINATING, VertexState.TERMINATING,
              VertexEventType.V_TASK_ATTEMPT_COMPLETED,
              TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION) // TODO shouldnt be done for KILL_WAIT vertex
          .addTransition(VertexState.TERMINATING, VertexState.TERMINATING,
              VertexEventType.V_SOURCE_TASK_ATTEMPT_COMPLETED,
              SOURCE_TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition(
              VertexState.TERMINATING,
              VertexState.ERROR, VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(VertexState.TERMINATING, VertexState.TERMINATING,
              EnumSet.of(VertexEventType.V_TERMINATE,
                  VertexEventType.V_TASK_RESCHEDULED))

          // Transitions from SUCCEEDED state
          .addTransition(
              VertexState.SUCCEEDED,
              VertexState.ERROR, VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(VertexState.SUCCEEDED, 
              EnumSet.of(VertexState.RUNNING, VertexState.FAILED), 
              VertexEventType.V_TASK_RESCHEDULED,
              new TaskRescheduledAfterVertexSuccessTransition())

          // Ignore-able events
          .addTransition(VertexState.SUCCEEDED, VertexState.SUCCEEDED,
              EnumSet.of(VertexEventType.V_TERMINATE,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_COMPLETED))

          // Transitions from FAILED state
          .addTransition(
              VertexState.FAILED,
              VertexState.ERROR, VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(VertexState.FAILED, VertexState.FAILED,
              EnumSet.of(VertexEventType.V_TERMINATE,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_COMPLETED))

          // Transitions from KILLED state
          .addTransition(
              VertexState.KILLED,
              VertexState.ERROR, VertexEventType.V_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(VertexState.KILLED, VertexState.KILLED,
              EnumSet.of(VertexEventType.V_TERMINATE,
                  VertexEventType.V_START,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_COMPLETED))

          // No transitions from INTERNAL_ERROR state. Ignore all.
          .addTransition(
              VertexState.ERROR,
              VertexState.ERROR,
              EnumSet.of(VertexEventType.V_INIT,
                  VertexEventType.V_TERMINATE,
                  VertexEventType.V_TASK_COMPLETED,
                  VertexEventType.V_TASK_ATTEMPT_COMPLETED,
                  VertexEventType.V_TASK_RESCHEDULED,
                  VertexEventType.V_DIAGNOSTIC_UPDATE,
                  VertexEventType.V_INTERNAL_ERROR))
          // create the topology tables
          .installTopology();

  private final StateMachine<VertexState, VertexEventType, VertexEvent>
      stateMachine;

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

  private Credentials credentials;

  private final TezVertexID vertexId;  //runtime assigned id.
  private final VertexPlan vertexPlan;

  private final String vertexName;
  private final ProcessorDescriptor processorDescriptor;

  // For committer
  private final VertexContext vertexContext;

  @VisibleForTesting
  Map<Vertex, Edge> sourceVertices;
  private Map<Vertex, Edge> targetVertices;

  private VertexScheduler vertexScheduler;

  private VertexOutputCommitter committer;
  private AtomicBoolean committed = new AtomicBoolean(false);
  private VertexLocationHint vertexLocationHint;
  private Map<String, LocalResource> localResources;
  private Map<String, String> environment;
  private final String javaOpts;
  private final ContainerContext containerContext;
  private VertexTerminationCause terminationCause;

  public VertexImpl(TezVertexID vertexId, VertexPlan vertexPlan,
      String vertexName, Configuration conf, EventHandler eventHandler,
      TaskAttemptListener taskAttemptListener,
      Credentials credentials, Clock clock,
      // TODO: Recovery
      //Map<TaskId, TaskInfo> completedTasksFromPreviousRun,
      // TODO Metrics
      //MRAppMetrics metrics,
      TaskHeartbeatHandler thh,
      AppContext appContext, VertexLocationHint vertexLocationHint) {
    this.vertexId = vertexId;
    this.vertexPlan = vertexPlan;
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

    this.credentials = credentials;
    this.committer = new NullVertexOutputCommitter();
    this.vertexLocationHint = vertexLocationHint;
    if (LOG.isDebugEnabled()) {
      logLocationHints(this.vertexLocationHint);
    }

    this.taskResource = DagTypeConverters
        .createResourceRequestFromTaskConfig(vertexPlan.getTaskConfig());
    this.processorDescriptor = DagTypeConverters
        .convertProcessorDescriptorFromDAGPlan(vertexPlan
            .getProcessorDescriptor());
    this.localResources = DagTypeConverters
        .createLocalResourceMapFromDAGPlan(vertexPlan.getTaskConfig()
            .getLocalResourceList());
    this.environment = DagTypeConverters
        .createEnvironmentMapFromDAGPlan(vertexPlan.getTaskConfig()
            .getEnvironmentSettingList());
    this.javaOpts = vertexPlan.getTaskConfig().hasJavaOpts() ? vertexPlan
        .getTaskConfig().getJavaOpts() : null;

    this.vertexContext = new VertexContext(getDAGId(),
        this.processorDescriptor.getUserPayload(), this.vertexId,
        getApplicationAttemptId());

    this.containerContext = new ContainerContext(this.localResources,
        this.credentials, this.environment, this.javaOpts);
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
  public VertexPlan getVertexPlan() {
    return vertexPlan;
  }

  @Override
  public int getDistanceFromRoot() {
    return distanceFromRoot;
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
  public Task getTask(int taskIndex) {
    readLock.lock();
    try {
      // does it matter to create a duplicate list for efficiency
      // instead of traversing the map
      // local assign to LinkedHashMap to ensure that sequential traversal 
      // assumption is satisfied
      LinkedHashMap<TezTaskID, Task> taskList = tasks;
      int i=0; 
      for(Map.Entry<TezTaskID, Task> entry : taskList.entrySet()) {
        if(taskIndex == i) {
          return entry.getValue();
        }
        ++i;
      }
      return null;
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
  public int getSucceededTasks() {
    readLock.lock();
    try {
      return succeededTaskCount;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public int getRunningTasks() {
    readLock.lock();
    try {
      int num=0;
      for (Task task : tasks.values()) {
        if(task.getState() == TaskState.RUNNING)
          num++;
      }
      return num;
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

  @Override
  public ProgressBuilder getVertexProgress() {
    this.readLock.lock();
    try {
      ProgressBuilder progress = new ProgressBuilder();
      progress.setTotalTaskCount(numTasks);
      progress.setSucceededTaskCount(succeededTaskCount);
      progress.setRunningTaskCount(getRunningTasks());
      progress.setFailedTaskCount(failedTaskCount);
      progress.setKilledTaskCount(killedTaskCount);
      return progress;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public VertexStatusBuilder getVertexStatus() {
    this.readLock.lock();
    try {
      VertexStatusBuilder status = new VertexStatusBuilder();
      status.setState(getInternalState());
      status.setDiagnostics(diagnostics);
      status.setProgress(getVertexProgress());
      return status;
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

  /**
   * Set the terminationCause if it had not yet been set.
   *
   * @param trigger The trigger
   * @return true if setting the value succeeded.
   */
  boolean trySetTerminationCause(VertexTerminationCause trigger) {
    if(terminationCause == null){
      terminationCause = trigger;
      return true;
    }
    return false;
  }

  public VertexTerminationCause getTerminationCause(){
    readLock.lock();
    try {
      return terminationCause;
    } finally {
      readLock.unlock();
    }
  }

  // TODO Create InputReadyVertexManager that schedules when there is something 
  // to read and use that as default instead of ImmediateStart.TEZ-480
  @Override
  public void scheduleTasks(Collection<TezTaskID> taskIDs) {
    readLock.lock();
    try {
      for (TezTaskID taskID : taskIDs) {
        eventHandler.handle(new TaskEvent(taskID,
            TaskEventType.T_SCHEDULE));
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void setParallelism(int parallelism,
      Map<Vertex, EdgeManager> sourceEdgeManagers) {
    writeLock.lock();
    try {
      if (parallelism >= numTasks) {
        // not that hard to support perhaps. but checking right now since there
        // is no use case for it and checking may catch other bugs.
        throw new TezUncheckedException(
            "Increasing parallelism is not supported");
      }
      if (parallelism == numTasks) {
        LOG.info("Ingoring setParallelism to current value: " + parallelism);
        return;
      }
      
      // start buffering incoming events so that we can re-route existing events
      for (Edge edge : sourceVertices.values()) {
        edge.startEventBuffering();
      }
      
      // Use a set since the same event may have been sent to multiple tasks
      // and we want to avoid duplicates
      Set<TezEvent> pendingEvents = new HashSet<TezEvent>();
      
      LOG.info("Vertex " + getVertexId() + " parallelism set to " + parallelism);
      // assign to local variable of LinkedHashMap to make sure that changing
      // type of task causes compile error. We depend on LinkedHashMap for order
      LinkedHashMap<TezTaskID, Task> currentTasks = this.tasks;
      Iterator<Map.Entry<TezTaskID, Task>> iter = currentTasks.entrySet()
          .iterator();
      int i = 0;
      while (iter.hasNext()) {
        i++;
        Map.Entry<TezTaskID, Task> entry = iter.next();
        Task task = entry.getValue();
        if (task.getState() != TaskState.NEW) {
          throw new TezUncheckedException(
              "All tasks must be in initial state when changing parallelism"
                  + " for vertex: " + getVertexId() + " name: " + getName());
        }
        pendingEvents.addAll(task.getAndClearTaskTezEvents());
        if (i <= parallelism) {
          continue;
        }
        LOG.info("Removing task: " + entry.getKey());
        iter.remove();
      }
      this.numTasks = parallelism;
      assert tasks.size() == numTasks;

      // set new edge managers
      if(sourceEdgeManagers != null) {
        for(Map.Entry<Vertex, EdgeManager> entry : sourceEdgeManagers.entrySet()) {
          Vertex sourceVertex = entry.getKey();
          EdgeManager edgeManager = entry.getValue();
          Edge edge = sourceVertices.get(sourceVertex);
          LOG.info("Replacing edge manager for source:" 
              + sourceVertex.getVertexId() + " destination: " + getVertexId());
          edge.setEdgeManager(edgeManager);
        }
      }
      
      // Re-route all existing TezEvents according to new routing table
      // At this point only events attributed to source task attempts can be 
      // re-routed. e.g. DataMovement or InputFailed events.  
      // This assumption is fine for now since these tasks haven't been started.
      // So they can only get events generated from source task attempts that 
      // have already been started.
      DAG dag = getDAG();
      for(TezEvent event : pendingEvents) {
        TezVertexID sourceVertexId = event.getSourceInfo().getTaskAttemptID()
            .getTaskID().getVertexID(); 
        Vertex sourceVertex = dag.getVertex(sourceVertexId);
        Edge sourceEdge = sourceVertices.get(sourceVertex);
        sourceEdge.sendTezEventToDestinationTasks(event);
      }
      
      // stop buffering events
      for (Edge edge : sourceVertices.values()) {
        edge.stopEventBuffering();
      }

    } finally {
      writeLock.unlock();
    }
    
  }

  @Override
  /**
   * The only entry point to change the Vertex.
   */
  public void handle(VertexEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing VertexEvent " + event.getVertexId()
          + " of type " + event.getType() + " while in state "
          + getInternalState() + ". Event: " + event);
    }
    try {
      writeLock.lock();
      VertexState oldState = getInternalState();
      try {
         getStateMachine().doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        String message = "Invalid event " + event.getType() +
            " on vertex " + this.vertexName +
            " with vertexId " + this.vertexId +
            " at current state " + oldState;
        LOG.error("Can't handle " + message, e);
        addDiagnostic(message);
        eventHandler.handle(new VertexEvent(this.vertexId,
            VertexEventType.V_INTERNAL_ERROR));
      }

      if (oldState != getInternalState()) {
        LOG.info(vertexId + " transitioned from " + oldState + " to "
                 + getInternalState());
      }
    }

    finally {
      writeLock.unlock();
    }
  }

  private VertexState getInternalState() {
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
        LinkedHashMap<TezTaskID, Task> newTasks = new LinkedHashMap<TezTaskID, Task>();
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


  void logJobHistoryVertexStartedEvent() {
    VertexStartedEvent startEvt = new VertexStartedEvent(vertexId,
        vertexName, initTime, startTime, numTasks, getProcessorName());
    this.eventHandler.handle(new DAGHistoryEvent(startEvt));
  }

  void logJobHistoryVertexFinishedEvent() {
    this.setFinishTime();
    VertexFinishedEvent finishEvt = new VertexFinishedEvent(vertexId,
        vertexName, startTime, finishTime, VertexStatus.State.SUCCEEDED, "",
        getAllCounters());
    this.eventHandler.handle(new DAGHistoryEvent(finishEvt));
  }

  void logJobHistoryVertexFailedEvent(VertexStatus.State state) {
    VertexFinishedEvent finishEvt = new VertexFinishedEvent(vertexId,
        vertexName, startTime, clock.getTime(), state,
        StringUtils.join(LINE_SEPARATOR, getDiagnostics()),
        getAllCounters());
    this.eventHandler.handle(new DAGHistoryEvent(finishEvt));
  }

  static VertexState checkVertexForCompletion(VertexImpl vertex) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking for vertex completion"
          + ", failedTaskCount=" + vertex.failedTaskCount
          + ", killedTaskCount=" + vertex.killedTaskCount
          + ", successfulTaskCount=" + vertex.succeededTaskCount
          + ", completedTaskCount=" + vertex.completedTaskCount
          + ", terminationCause=" + vertex.terminationCause);
    }

    //check for vertex failure first
    if (vertex.completedTaskCount > vertex.tasks.size()) {
      LOG.error("task completion accounting issue: completedTaskCount > nTasks:"
          + ", failedTaskCount=" + vertex.failedTaskCount
          + ", killedTaskCount=" + vertex.killedTaskCount
          + ", successfulTaskCount=" + vertex.succeededTaskCount
          + ", completedTaskCount=" + vertex.completedTaskCount
          + ", terminationCause=" + vertex.terminationCause);
    }

    if (vertex.completedTaskCount == vertex.tasks.size()) {
      //Only succeed if tasks complete successfully and no terminationCause is registered.
      if(vertex.succeededTaskCount == vertex.tasks.size() && vertex.terminationCause == null) {
        try {
          if (!vertex.committed.getAndSet(true)) {
            // commit only once
            vertex.committer.commitVertex();
          }
        } catch (IOException e) {
          LOG.error("Failed to do commit on vertex, name=" + vertex.getName(), e);
          vertex.trySetTerminationCause(VertexTerminationCause.COMMIT_FAILURE);
          return vertex.finished(VertexState.FAILED);
        }
        return vertex.finished(VertexState.SUCCEEDED);
      }
      else if(vertex.terminationCause == VertexTerminationCause.DAG_KILL ){
        vertex.setFinishTime();
        String diagnosticMsg = "Vertex killed due to user-initiated job kill. "
            + "failedTasks:"
            + vertex.failedTaskCount;
        LOG.info(diagnosticMsg);
        vertex.addDiagnostic(diagnosticMsg);
        vertex.abortVertex(VertexStatus.State.KILLED);
        return vertex.finished(VertexState.KILLED);
      }
      else if(vertex.terminationCause == VertexTerminationCause.OTHER_VERTEX_FAILURE ){
        vertex.setFinishTime();
        String diagnosticMsg = "Vertex killed as other vertex failed. "
            + "failedTasks:"
            + vertex.failedTaskCount;
        LOG.info(diagnosticMsg);
        vertex.addDiagnostic(diagnosticMsg);
        vertex.abortVertex(VertexStatus.State.KILLED);
        return vertex.finished(VertexState.KILLED);
      }
      else if(vertex.terminationCause == VertexTerminationCause.OWN_TASK_FAILURE ){
        if(vertex.failedTaskCount == 0){
          LOG.error("task failure accounting error.  terminationCause=TASK_FAILURE but vertex.failedTaskCount == 0");
        }
        vertex.setFinishTime();
        String diagnosticMsg = "Vertex killed as one or more tasks failed. "
            + "failedTasks:"
            + vertex.failedTaskCount;
        LOG.info(diagnosticMsg);
        vertex.addDiagnostic(diagnosticMsg);
        vertex.abortVertex(VertexStatus.State.FAILED);
        return vertex.finished(VertexState.FAILED);
      }
      else {
        //should never occur
        throw new TezUncheckedException("All tasks complete, but cannot determine final state of vertex"
            + ", failedTaskCount=" + vertex.failedTaskCount
            + ", killedTaskCount=" + vertex.killedTaskCount
            + ", successfulTaskCount=" + vertex.succeededTaskCount
            + ", completedTaskCount=" + vertex.completedTaskCount
            + ", terminationCause=" + vertex.terminationCause);
      }
    }

    //return the current state, Vertex not finished yet
    return vertex.getInternalState();
  }

  /**
   * Set the terminationCause and send a kill-message to all tasks.
   * The task-kill messages are only sent once.
   * @param the trigger that is causing the Vertex to transition to KILLED/FAILED
   * @param event The type of kill event to send to the vertices.
   */
  void enactKill(VertexTerminationCause trigger, TaskTerminationCause taskterminationCause) {
    if(trySetTerminationCause(trigger)){
      for (Task task : tasks.values()) {
        eventHandler.handle(
            new TaskEventTermination(task.getTaskId(), taskterminationCause));
      }
    }
  }

  VertexState finished(VertexState finalState) {
    if (finishTime == 0) setFinishTime();

    switch (finalState) {
      case KILLED:
        eventHandler.handle(new DAGEventVertexCompleted(getVertexId(),
            finalState));
        logJobHistoryVertexFailedEvent(VertexStatus.State.KILLED);
        break;
      case ERROR:
        eventHandler.handle(new DAGEvent(getDAGId(),
            DAGEventType.INTERNAL_ERROR));
        logJobHistoryVertexFailedEvent(VertexStatus.State.FAILED);
        break;
      case FAILED:
        eventHandler.handle(new DAGEventVertexCompleted(getVertexId(),
            finalState));
        logJobHistoryVertexFailedEvent(VertexStatus.State.FAILED);
        break;
      case SUCCEEDED:
        eventHandler.handle(new DAGEventVertexCompleted(getVertexId(),
            finalState));
        logJobHistoryVertexFinishedEvent();
        break;
      default:
        throw new TezUncheckedException("Unexpected VertexState: " + finalState);
    }
    return finalState;
  }

  public static class InitTransition
      implements MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      try {

        // TODODAGAM
        // TODO: Splits?

        vertex.numTasks = vertex.getVertexPlan().getTaskConfig().getNumTasks();

        /*
        TaskSplitMetaInfo[] taskSplitMetaInfo = createSplits(job, job.jobId);
        job.numMapTasks = taskSplitMetaInfo.length;
        */

        if (vertex.numTasks == 0) {
          vertex.addDiagnostic("No tasks for vertex " + vertex.getVertexId());
          vertex.trySetTerminationCause(VertexTerminationCause.ZERO_TASKS);
          vertex.abortVertex(VertexStatus.State.FAILED);
          return vertex.finished(VertexState.FAILED);
        }

        checkTaskLimits();

        // create the Tasks but don't start them yet
        createTasks(vertex);

        // TODO get this from API
        boolean hasBipartite = false;
        if (vertex.sourceVertices != null) {
          for (Edge edge : vertex.sourceVertices.values()) {
            if (edge.getEdgeProperty().getDataMovementType() == 
                      DataMovementType.SCATTER_GATHER) {
              hasBipartite = true;
              break;
            }
          }
        }

        if (hasBipartite) {
          // setup vertex scheduler
          // TODO this needs to consider data size and perhaps API.
          // Currently implicitly BIPARTITE is the only edge type
          vertex.vertexScheduler = new ShuffleVertexManager(vertex);
        } else {
          // schedule all tasks upon vertex start
          vertex.vertexScheduler = new ImmediateStartVertexScheduler(vertex);
        }

        vertex.vertexScheduler.initialize(vertex.conf);

        // FIXME how do we decide vertex needs a committer?
        // Answer: Do commit for every vertex
        // for now, only for leaf vertices
        // TODO TEZ-41 make commmitter type configurable per vertex
        if (vertex.targetVertices.isEmpty()) {
          vertex.committer = new MRVertexOutputCommitter();
        }
        vertex.committer.init(vertex.vertexContext);
        vertex.committer.setupVertex();

        // TODO: Metrics
        //vertex.metrics.endPreparingJob(job);
        vertex.initTime = vertex.clock.getTime();
        return VertexState.INITED;

      } catch (IOException e) {
        LOG.warn("Vertex init failed", e);
        vertex.addDiagnostic("Job init failed : "
            + StringUtils.stringifyException(e));
        vertex.trySetTerminationCause(VertexTerminationCause.INIT_FAILURE);
        vertex.abortVertex(VertexStatus.State.FAILED);
        // TODO: Metrics
        //job.metrics.endPreparingJob(vertex);
        return vertex.finished(VertexState.FAILED);
      }
    }


    private void createTasks(VertexImpl vertex) {
      Configuration conf = vertex.conf;
      boolean useNullLocationHint = true;
      if (vertex.vertexLocationHint != null
          && vertex.vertexLocationHint.getTaskLocationHints() != null
          && vertex.vertexLocationHint.getTaskLocationHints().size() ==
              vertex.numTasks) {
        useNullLocationHint = false;
      }
      for (int i=0; i < vertex.numTasks; ++i) {
        TaskLocationHint locHint = null;
        if (!useNullLocationHint) {
          locHint = vertex.vertexLocationHint.getTaskLocationHints().get(i);
        }
        TaskImpl task =
            new TaskImpl(vertex.getVertexId(), i,
                vertex.eventHandler,
                conf,
                vertex.taskAttemptListener,
                vertex.clock,
                vertex.taskHeartbeatHandler,
                vertex.appContext,
                vertex.targetVertices.isEmpty(),
                locHint, vertex.taskResource,
                vertex.containerContext);
        vertex.addTask(task);
        if(LOG.isDebugEnabled()) {
          LOG.debug("Created task for vertex " + vertex.getVertexId() + ": " +
              task.getTaskId());
        }
      }

    }

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

    }
  }

  private void abortVertex(VertexStatus.State finalState) {
    try {
      committer.abortVertex(finalState);
    } catch (IOException e) {
      LOG.warn("Could not abort vertex, name=" + getName(), e);
    }

    if (finishTime == 0) {
      setFinishTime();
    }
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
  private static class TerminateNewVertexTransition
  implements SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      VertexEventTermination vet = (VertexEventTermination) event;
      vertex.trySetTerminationCause(vet.getTerminationCause());
      vertex.setFinishTime();
      vertex.addDiagnostic("Vertex received Kill in NEW state.");
      vertex.finished(VertexState.KILLED);
    }
  }

  private static class TerminateInitedVertexTransition
  implements SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      VertexEventTermination vet = (VertexEventTermination) event;
      vertex.trySetTerminationCause(vet.getTerminationCause());
      vertex.abortVertex(VertexStatus.State.KILLED);
      vertex.addDiagnostic("Vertex received Kill in INITED state.");
      vertex.finished(VertexState.KILLED);
    }
  }

  private static class VertexKilledTransition
      implements SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      vertex.addDiagnostic("Vertex received Kill while in RUNNING state.");
      VertexEventTermination vet = (VertexEventTermination) event;
      VertexTerminationCause trigger = vet.getTerminationCause();
      switch(trigger){
        case DAG_KILL : vertex.enactKill(trigger, TaskTerminationCause.DAG_KILL); break;
        case OTHER_VERTEX_FAILURE: vertex.enactKill(trigger, TaskTerminationCause.OTHER_VERTEX_FAILURE); break;
        case OWN_TASK_FAILURE: vertex.enactKill(trigger, TaskTerminationCause.OTHER_TASK_FAILURE); break;
        default://should not occur
          throw new TezUncheckedException("VertexKilledTransition: event.terminationCause is unexpected: " + trigger);
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
      VertexEventTaskAttemptCompleted completionEvent =
          ((VertexEventSourceTaskAttemptCompleted) event).getCompletionEvent();
      LOG.info("Source task attempt completed for vertex: " + vertex.getVertexId()
            + " attempt: " + completionEvent.getTaskAttemptId()
            + " with state: " + completionEvent.getTaskAttemptState());
      
      if (TaskAttemptStateInternal.SUCCEEDED.equals(completionEvent
          .getTaskAttemptState())) {
        vertex.numSuccessSourceAttemptCompletions++;
        vertex.vertexScheduler.onSourceTaskCompleted(completionEvent
            .getTaskAttemptId());
      }

    }
  }

  private static class TaskAttemptCompletedEventTransition implements
      SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      VertexEventTaskAttemptCompleted completionEvent =
        ((VertexEventTaskAttemptCompleted) event);

      // If different tasks were connected to different destination vertices
      // then this would need to be sent via the edges
      // Notify all target vertices
      if (vertex.targetVertices != null) {
        for (Vertex targetVertex : vertex.targetVertices.keySet()) {
          vertex.eventHandler.handle(
              new VertexEventSourceTaskAttemptCompleted(
                  targetVertex.getVertexId(), completionEvent)
              );
        }
      }
    }
  }

  private static class TaskCompletedTransition implements
      MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      boolean forceTransitionToKillWait = false;
      vertex.completedTaskCount++;
      LOG.info("Num completed Tasks: " + vertex.completedTaskCount);
      VertexEventTaskCompleted taskEvent = (VertexEventTaskCompleted) event;
      Task task = vertex.tasks.get(taskEvent.getTaskID());
      if (taskEvent.getState() == TaskState.SUCCEEDED) {
        taskSucceeded(vertex, task);
      } else if (taskEvent.getState() == TaskState.FAILED) {
        vertex.enactKill(VertexTerminationCause.OWN_TASK_FAILURE, TaskTerminationCause.OTHER_TASK_FAILURE);
        forceTransitionToKillWait = true;
        taskFailed(vertex, task);
      } else if (taskEvent.getState() == TaskState.KILLED) {
        taskKilled(vertex, task);
      }

      VertexState state = VertexImpl.checkVertexForCompletion(vertex);
      if(state == VertexState.RUNNING && forceTransitionToKillWait){
        return VertexState.TERMINATING;
      }

      return state;
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

  private static class TaskRescheduledTransition implements
      SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      //succeeded task is restarted back
      vertex.completedTaskCount--;
      vertex.succeededTaskCount--;
    }
  }
  
  private static class TaskRescheduledAfterVertexSuccessTransition implements
    MultipleArcTransition<VertexImpl, VertexEvent, VertexState> {

    @Override
    public VertexState transition(VertexImpl vertex, VertexEvent event) {
      if (vertex.committer instanceof NullVertexOutputCommitter) {
        LOG.info(vertex.getVertexId() + " back to running due to rescheduling "
            + ((VertexEventTaskReschedule)event).getTaskID());
        (new TaskRescheduledTransition()).transition(vertex, event);
        // inform the DAG that we are re-running
        vertex.eventHandler.handle(new DAGEventVertexReRunning(vertex.getVertexId()));
        return VertexState.RUNNING;
      }
      
      LOG.info(vertex.getVertexId() + " failed due to post-commit rescheduling of "
          + ((VertexEventTaskReschedule)event).getTaskID());
      // terminate any running tasks
      vertex.enactKill(VertexTerminationCause.OWN_TASK_FAILURE,
          TaskTerminationCause.OWN_TASK_FAILURE);
      // since the DAG thinks this vertex is completed it must be notified of 
      // an error
      vertex.eventHandler.handle(new DAGEvent(vertex.getDAGId(),
          DAGEventType.INTERNAL_ERROR));
      return VertexState.FAILED;
    }
  }
  
  private void addDiagnostic(String diag) {
    diagnostics.add(diag);
  }
  
  private static void checkEventSourceMetadata(Vertex vertex, EventMetaData sourceMeta) {
    if (!sourceMeta.getTaskVertexName().equals(vertex.getName())) {
      throw new TezUncheckedException("Bad routing of event"
          + ", Event-vertex=" + sourceMeta.getTaskVertexName()
          + ", Expected=" + vertex.getName());
    }
  }

  private static class RouteEventTransition  implements
  SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      VertexEventRouteEvent rEvent = (VertexEventRouteEvent) event;
      List<TezEvent> tezEvents = rEvent.getEvents();
      for(TezEvent tezEvent : tezEvents) {
        LOG.info("Vertex: " + vertex.getName() + " routing event: "
            + tezEvent.getEventType());
        EventMetaData sourceMeta = tezEvent.getSourceInfo();
        checkEventSourceMetadata(vertex, sourceMeta);
        switch(tezEvent.getEventType()) {
        case DATA_MOVEMENT_EVENT:
          {
            TezTaskAttemptID srcTaId = sourceMeta.getTaskAttemptID();
            DataMovementEvent dmEvent = (DataMovementEvent) tezEvent.getEvent();
            dmEvent.setVersion(srcTaId.getId());
            Edge destEdge = vertex.targetVertices.get(vertex.getDAG().getVertex(
                sourceMeta.getEdgeVertexName()));
            destEdge.sendTezEventToDestinationTasks(tezEvent);
          }
          break;
        case INPUT_FAILED_EVENT:
        {
          TezTaskAttemptID srcTaId = sourceMeta.getTaskAttemptID();
          InputFailedEvent ifEvent = (InputFailedEvent) tezEvent.getEvent();
          ifEvent.setVersion(srcTaId.getId());
          Edge destEdge = vertex.targetVertices.get(vertex.getDAG().getVertex(
              sourceMeta.getEdgeVertexName()));
          destEdge.sendTezEventToDestinationTasks(tezEvent);
        }
        break;
        case INPUT_READ_ERROR_EVENT:
          {
            Edge srcEdge = vertex.sourceVertices.get(vertex.getDAG().getVertex(
                sourceMeta.getEdgeVertexName()));
            srcEdge.sendTezEventToSourceTasks(tezEvent);
          }
          break;
        case TASK_STATUS_UPDATE_EVENT:
          {
            TaskStatusUpdateEvent sEvent =
                (TaskStatusUpdateEvent) tezEvent.getEvent();
            vertex.getEventHandler().handle(
                new TaskAttemptEventStatusUpdate(sourceMeta.getTaskAttemptID(),
                    sEvent));
          }
          break;
        case TASK_ATTEMPT_COMPLETED_EVENT:
          {
            vertex.getEventHandler().handle(
                new TaskAttemptEvent(sourceMeta.getTaskAttemptID(),
                    TaskAttemptEventType.TA_DONE));
          }
          break;
        case TASK_ATTEMPT_FAILED_EVENT:
          {
            TaskAttemptFailedEvent taskFailedEvent =
                (TaskAttemptFailedEvent) tezEvent.getEvent();
            vertex.getEventHandler().handle(
                new TaskAttemptEventAttemptFailed(sourceMeta.getTaskAttemptID(),
                    TaskAttemptEventType.TA_FAILED,
                    "Error: " + taskFailedEvent.getDiagnostics()));
          }
          break;
        default:
          throw new TezUncheckedException("Unhandled tez event type: "
              + tezEvent.getEventType());
        }
      }
    }
  }
  
  private static class InternalErrorTransition implements
      SingleArcTransition<VertexImpl, VertexEvent> {
    @Override
    public void transition(VertexImpl vertex, VertexEvent event) {
      LOG.error("Invalid event " + event.getType() + " on Vertex "
          + vertex.getVertexId());
      vertex.eventHandler.handle(new DAGEventDiagnosticsUpdate(
          vertex.getDAGId(), "Invalid event " + event.getType()
          + " on Vertex " + vertex.getVertexId()));
      vertex.setFinishTime();
      vertex.finished(VertexState.ERROR);
    }
  }

  @Override
  public void setInputVertices(Map<Vertex, Edge> inVertices) {
    this.sourceVertices = inVertices;
  }

  @Override
  public void setOutputVertices(Map<Vertex, Edge> outVertices) {
    this.targetVertices = outVertices;
  }

  @Override
  public int compareTo(Vertex other) {
    return this.vertexId.compareTo(other.getVertexId());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Vertex other = (Vertex) obj;
    return this.vertexId.equals(other.getVertexId());
  }

  @Override
  public int hashCode() {
    final int prime = 11239;
    return prime + prime * this.vertexId.hashCode();
  }

  @Override
  public Map<Vertex, Edge> getInputVertices() {
    return Collections.unmodifiableMap(this.sourceVertices);
  }

  @Override
  public Map<Vertex, Edge> getOutputVertices() {
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
  public ProcessorDescriptor getProcessorDescriptor() {
    return processorDescriptor;
  }

  @Override
  public DAG getDAG() {
    return appContext.getCurrentDAG();
  }

  private TezDAGID getDAGId() {
    return getDAG().getID();
  }

  private ApplicationAttemptId getApplicationAttemptId() {
    return appContext.getApplicationAttemptId();
  }

  public Resource getTaskResource() {
    return taskResource;
  }

  @VisibleForTesting
  String getProcessorName() {
    return this.processorDescriptor.getClassName();
  }

  @VisibleForTesting
  String getJavaOpts() {
    return this.javaOpts;
  }

  // TODO Eventually remove synchronization.
  @Override
  public synchronized List<InputSpec> getInputSpecList(int taskIndex) {
    inputSpecList = new ArrayList<InputSpec>(
        this.getInputVerticesCount());
    for (Entry<Vertex, Edge> entry : this.getInputVertices().entrySet()) {
      InputSpec inputSpec = entry.getValue().getDestinationSpec(taskIndex);
      if (LOG.isDebugEnabled()) {
        LOG.debug("For vertex : " + this.getName()
            + ", Using InputSpec : " + inputSpec);
      }
      // TODO DAGAM This should be based on the edge type.
      inputSpecList.add(inputSpec);
    }
    return inputSpecList;
  }

  // TODO Eventually remove synchronization.
  @Override
  public synchronized List<OutputSpec> getOutputSpecList(int taskIndex) {
    if (this.outputSpecList == null) {
      outputSpecList = new ArrayList<OutputSpec>(this.getOutputVerticesCount());
      for (Entry<Vertex, Edge> entry : this.getOutputVertices().entrySet()) {
        OutputSpec outputSpec = entry.getValue().getSourceSpec(taskIndex);
        outputSpecList.add(outputSpec);
      }
    }
    return outputSpecList;
  }

  @VisibleForTesting
  VertexOutputCommitter getVertexOutputCommitter() {
    return this.committer;
  }

  @VisibleForTesting
  // Only to be used for testing
  void setVertexOutputCommitter(VertexOutputCommitter committer) {
    this.committer = committer;
  }

  @VisibleForTesting
  VertexScheduler getVertexScheduler() {
    return this.vertexScheduler;
  }

  private static void logLocationHints(VertexLocationHint locationHint) {
    Multiset<String> hosts = HashMultiset.create();
    Multiset<String> racks = HashMultiset.create();
    int counter = 0;
    for (TaskLocationHint taskLocationHint : locationHint
        .getTaskLocationHints()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Hosts: ");
      for (String host : taskLocationHint.getDataLocalHosts()) {
        hosts.add(host);
        sb.append(host).append(", ");
      }
      sb.append("Racks: ");
      for (String rack : taskLocationHint.getRacks()) {
        racks.add(rack);
        sb.append(rack).append(", ");
      }
      LOG.debug("Location: " + counter + " : " + sb.toString());
      counter++;
    }

    LOG.debug("Host Counts");
    for (Multiset.Entry<String> host : hosts.entrySet()) {
      LOG.debug("host: " + host.toString());
    }

    LOG.debug("Rack Counts");
    for (Multiset.Entry<String> rack : racks.entrySet()) {
      LOG.debug("rack: " + rack.toString());
    }
  }
}
