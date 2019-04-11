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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.dag.app.dag.event.TaskEventTAFailed;
import org.apache.tez.runtime.api.TaskFailureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskReport;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;
import org.apache.tez.dag.app.RecoveryParser.TaskRecoveryData;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.StateChangeNotifier;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.TaskStateInternal;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventDiagnosticsUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventAttemptKilled;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventKillRequest;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventOutputFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventTerminationCauseEvent;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventScheduleTask;
import org.apache.tez.dag.app.dag.event.TaskEventTAKilled;
import org.apache.tez.dag.app.dag.event.TaskEventTAUpdate;
import org.apache.tez.dag.app.dag.event.TaskEventTermination;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEventTaskAttemptCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskReschedule;
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.app.rm.node.AMNodeEventTaskAttemptEnded;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.utils.TezBuilderUtils;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TaskStatistics;
import org.apache.tez.runtime.api.impl.TezEvent;

import com.google.common.annotations.VisibleForTesting;

import org.apache.tez.state.OnStateChangedCallback;
import org.apache.tez.state.StateMachineTez;

/**
 * Implementation of Task interface.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TaskImpl implements Task, EventHandler<TaskEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(TaskImpl.class);

  private static final String LINE_SEPARATOR = System
    .getProperty("line.separator");

  protected final Configuration conf;
  protected final TaskCommunicatorManagerInterface taskCommunicatorManagerInterface;
  protected final TaskHeartbeatHandler taskHeartbeatHandler;
  protected final EventHandler eventHandler;
  private final TezTaskID taskId;
  private Map<TezTaskAttemptID, TaskAttempt> attempts;
  protected final int maxFailedAttempts;
  protected final Clock clock;
  private final Vertex vertex;
  private final Lock readLock;
  private final Lock writeLock;
  private final List<String> diagnostics = new ArrayList<String>();
  private TezCounters counters = new TezCounters();
  // TODO Metrics
  //private final MRAppMetrics metrics;
  protected final AppContext appContext;
  private final Resource taskResource;
  private TaskSpec baseTaskSpec;
  private TaskLocationHint locationHint;
  private final ContainerContext containerContext;
  @VisibleForTesting
  long scheduledTime;
  final StateChangeNotifier stateChangeNotifier;

  private final TaskRecoveryData recoveryData;

  private final List<TezEvent> tezEventsForTaskAttempts = new ArrayList<TezEvent>();
  static final ArrayList<TezEvent> EMPTY_TASK_ATTEMPT_TEZ_EVENTS =
      new ArrayList(0);

  // track the status of TaskAttempt (true mean completed, false mean uncompleted)
  private final Map<Integer, Boolean> taskAttemptStatus = new HashMap<Integer,Boolean>();

  // The set of nodes with active running attempts at the time of the latest attempt for
  // this task was scheduled. This set is empty when scheduling original task attempt, and
  // non-empty scheduling a speculative attempt, in which case scheduler should avoid
  // scheduling the speculative attempt onto node(s) recorded in this set.
  private final Set<NodeId> nodesWithRunningAttempts = Collections
      .newSetFromMap(new ConcurrentHashMap<NodeId, Boolean>());

  private static final SingleArcTransition<TaskImpl , TaskEvent>
     ATTEMPT_KILLED_TRANSITION = new AttemptKilledTransition();
  private static final SingleArcTransition<TaskImpl, TaskEvent>
     KILL_TRANSITION = new KillTransition();
  private static final SingleArcTransition<TaskImpl, TaskEvent>
      REDUNDANT_COMPLETED_TRANSITION = new AttemptRedundantCompletedTransition();


  private static final TaskStateChangedCallback STATE_CHANGED_CALLBACK = new TaskStateChangedCallback();
  
  private static final StateMachineFactory
               <TaskImpl, TaskStateInternal, TaskEventType, TaskEvent>
            stateMachineFactory
           = new StateMachineFactory<TaskImpl, TaskStateInternal, TaskEventType, TaskEvent>
               (TaskStateInternal.NEW)

    // define the state machine of Task

    // Transitions from NEW state
    // Stay in NEW in recovery when Task is killed in the previous AM           
    .addTransition(TaskStateInternal.NEW,
        EnumSet.of(TaskStateInternal.NEW, TaskStateInternal.SCHEDULED),
        TaskEventType.T_SCHEDULE, new InitialScheduleTransition())
    .addTransition(TaskStateInternal.NEW, TaskStateInternal.KILLED,
        TaskEventType.T_TERMINATE,
        new KillNewTransition())

    // Transitions from SCHEDULED state
      //when the first attempt is launched, the task state is set to RUNNING
     .addTransition(TaskStateInternal.SCHEDULED, TaskStateInternal.RUNNING,
         TaskEventType.T_ATTEMPT_LAUNCHED, new LaunchTransition())
     .addTransition(TaskStateInternal.SCHEDULED, TaskStateInternal.KILL_WAIT,
         TaskEventType.T_TERMINATE,
         KILL_TRANSITION)
     .addTransition(TaskStateInternal.SCHEDULED, TaskStateInternal.SCHEDULED,
         TaskEventType.T_ATTEMPT_KILLED, ATTEMPT_KILLED_TRANSITION)
     .addTransition(TaskStateInternal.SCHEDULED,
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.FAILED),
        TaskEventType.T_ATTEMPT_FAILED,
        new AttemptFailedTransition())
     // Happens in recovery   
     .addTransition(TaskStateInternal.SCHEDULED,
        EnumSet.of(TaskStateInternal.RUNNING, TaskStateInternal.SUCCEEDED),
        TaskEventType.T_ATTEMPT_SUCCEEDED,
        new AttemptSucceededTransition())

    // When current attempt fails/killed and new attempt launched then
    // TODO Task should go back to SCHEDULED state TEZ-495
    // Transitions from RUNNING state
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING,
        TaskEventType.T_ATTEMPT_LAUNCHED) //more attempts may start later
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING,
        TaskEventType.T_ADD_SPEC_ATTEMPT, new RedundantScheduleTransition())
    .addTransition(TaskStateInternal.RUNNING, 
        EnumSet.of(TaskStateInternal.SUCCEEDED),
        TaskEventType.T_ATTEMPT_SUCCEEDED,
        new AttemptSucceededTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING,
        TaskEventType.T_ATTEMPT_KILLED,
        ATTEMPT_KILLED_TRANSITION)
    .addTransition(TaskStateInternal.RUNNING,
        EnumSet.of(TaskStateInternal.RUNNING, TaskStateInternal.FAILED),
        TaskEventType.T_ATTEMPT_FAILED,
        new AttemptFailedTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.KILL_WAIT,
        TaskEventType.T_TERMINATE,
        KILL_TRANSITION)
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING,
        TaskEventType.T_SCHEDULE)

    // Transitions from KILL_WAIT state
    .addTransition(TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskStateInternal.KILL_WAIT, TaskStateInternal.KILLED),
        TaskEventType.T_ATTEMPT_KILLED,
        new KillWaitAttemptCompletedTransition())
    .addTransition(TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskStateInternal.KILL_WAIT, TaskStateInternal.KILLED),
        TaskEventType.T_ATTEMPT_FAILED,
        new KillWaitAttemptCompletedTransition())
    .addTransition(TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskStateInternal.KILL_WAIT, TaskStateInternal.KILLED),
        TaskEventType.T_ATTEMPT_SUCCEEDED,
        new KillWaitAttemptCompletedTransition())
    // Ignore-able transitions.
    .addTransition(
        TaskStateInternal.KILL_WAIT,
        TaskStateInternal.KILL_WAIT,
        EnumSet.of(
            TaskEventType.T_TERMINATE,
            TaskEventType.T_ATTEMPT_LAUNCHED,
            TaskEventType.T_ADD_SPEC_ATTEMPT))

    // Transitions from SUCCEEDED state
    .addTransition(TaskStateInternal.SUCCEEDED, //only possible for map tasks
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.SUCCEEDED, TaskStateInternal.FAILED),
        TaskEventType.T_ATTEMPT_FAILED, new TaskRetroactiveFailureTransition())
    .addTransition(TaskStateInternal.SUCCEEDED, //only possible for map tasks
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.SUCCEEDED),
        TaskEventType.T_ATTEMPT_KILLED, new TaskRetroactiveKilledTransition())
    .addTransition(
        TaskStateInternal.SUCCEEDED, TaskStateInternal.SUCCEEDED,
        TaskEventType.T_ATTEMPT_SUCCEEDED, REDUNDANT_COMPLETED_TRANSITION)
    // Ignore-able transitions.
    .addTransition(TaskStateInternal.SUCCEEDED, TaskStateInternal.SUCCEEDED,
        EnumSet.of(
            TaskEventType.T_ADD_SPEC_ATTEMPT,
            TaskEventType.T_TERMINATE,
            TaskEventType.T_ATTEMPT_LAUNCHED))
    .addTransition(TaskStateInternal.SUCCEEDED, TaskStateInternal.SUCCEEDED,
        TaskEventType.T_SCHEDULE)

    // Transitions from FAILED state
    .addTransition(TaskStateInternal.FAILED, TaskStateInternal.FAILED,
        EnumSet.of(
            TaskEventType.T_ATTEMPT_FAILED,
            TaskEventType.T_ATTEMPT_KILLED,
            TaskEventType.T_ATTEMPT_SUCCEEDED), REDUNDANT_COMPLETED_TRANSITION)
    .addTransition(TaskStateInternal.FAILED, TaskStateInternal.FAILED,
        EnumSet.of(
            TaskEventType.T_TERMINATE,
            TaskEventType.T_SCHEDULE,
            TaskEventType.T_ADD_SPEC_ATTEMPT,
            TaskEventType.T_ATTEMPT_LAUNCHED))

    // Transitions from KILLED state
    // Ignorable event: T_ATTEMPT_KILLED
    // Refer to TEZ-2379
    // T_ATTEMPT_KILLED can show up in KILLED state as
    // a SUCCEEDED attempt can still transition to KILLED after receiving
    // a KILL event.
    // This could happen when there is a race where the task receives a
    // kill event, it tries to kill all running attempts and a potential
    // running attempt succeeds before it receives the kill event.
    // The task will then receive both a SUCCEEDED and KILLED
    // event from the same attempt.
    // Duplicate events from a single attempt in KILL_WAIT are handled
    // properly. However, the subsequent T_ATTEMPT_KILLED event might
    // be received after the task reaches its terminal KILLED state.
    .addTransition(TaskStateInternal.KILLED, TaskStateInternal.KILLED,
        EnumSet.of(
            TaskEventType.T_TERMINATE,
            TaskEventType.T_SCHEDULE,
            TaskEventType.T_ADD_SPEC_ATTEMPT,
            TaskEventType.T_ATTEMPT_LAUNCHED,
            TaskEventType.T_ATTEMPT_SUCCEEDED,
            TaskEventType.T_ATTEMPT_FAILED,
            TaskEventType.T_ATTEMPT_KILLED))

    // create the topology tables
    .installTopology();

  private void augmentStateMachine() {
    stateMachine
        .registerStateEnteredCallback(TaskStateInternal.SUCCEEDED,
            STATE_CHANGED_CALLBACK);
  }

  private final StateMachineTez<TaskStateInternal, TaskEventType, TaskEvent, TaskImpl>
    stateMachine;

  // TODO: Recovery
  /*
  // By default, the next TaskAttempt number is zero. Changes during recovery
  protected int nextAttemptNumber = 0;
  private List<TaskAttemptInfo> taskAttemptsFromPreviousGeneration =
      new ArrayList<TaskAttemptInfo>();

  private static final class RecoverdAttemptsComparator implements
      Comparator<TaskAttemptInfo> {
    @Override
    public int compare(TaskAttemptInfo attempt1, TaskAttemptInfo attempt2) {
      long diff = attempt1.getStartTime() - attempt2.getStartTime();
      return diff == 0 ? 0 : (diff < 0 ? -1 : 1);
    }
  }

  private static final RecoverdAttemptsComparator RECOVERED_ATTEMPTS_COMPARATOR =
      new RecoverdAttemptsComparator();

   */

  //should be set to one which comes first
  //saying COMMIT_PENDING
  private TezTaskAttemptID commitAttempt;

  @VisibleForTesting
  TezTaskAttemptID successfulAttempt;

  @VisibleForTesting
  int failedAttempts;

  private final boolean leafVertex;

  @VisibleForTesting
  long finishTime = -1L;

  @Override
  public TaskState getState() {
    readLock.lock();
    try {
      return getExternalState(getInternalState());
    } finally {
      readLock.unlock();
    }
  }

  public TaskImpl(TezVertexID vertexId, int taskIndex,
      EventHandler eventHandler, Configuration conf,
      TaskCommunicatorManagerInterface taskCommunicatorManagerInterface,
      Clock clock, TaskHeartbeatHandler thh, AppContext appContext,
      boolean leafVertex, Resource resource,
      ContainerContext containerContext,
      StateChangeNotifier stateChangeNotifier,
      Vertex vertex) {
    this.conf = conf;
    this.clock = clock;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
    this.attempts = Collections.emptyMap();
    maxFailedAttempts = vertex.getVertexConfig().getMaxFailedTaskAttempts();
    taskId = TezTaskID.getInstance(vertexId, taskIndex);
    this.taskCommunicatorManagerInterface = taskCommunicatorManagerInterface;
    this.taskHeartbeatHandler = thh;
    this.eventHandler = eventHandler;
    this.appContext = appContext;
    this.stateChangeNotifier = stateChangeNotifier;
    this.vertex = vertex;
    this.leafVertex = leafVertex;
    this.taskResource = resource;
    this.containerContext = containerContext;
    this.recoveryData = appContext.getDAGRecoveryData() == null ?
        null : appContext.getDAGRecoveryData().getTaskRecoveryData(taskId);
    stateMachine = new StateMachineTez<TaskStateInternal, TaskEventType, TaskEvent, TaskImpl>(
        stateMachineFactory.make(this), this);
    augmentStateMachine();
  }
  
  @Override
  public Map<TezTaskAttemptID, TaskAttempt> getAttempts() {
    readLock.lock();

    try {
      if (attempts.size() <= 1) {
        return attempts;
      }

      Map<TezTaskAttemptID, TaskAttempt> result
          = new LinkedHashMap<TezTaskAttemptID, TaskAttempt>();
      result.putAll(attempts);

      return result;
    } finally {
      readLock.unlock();
    }
  }
  
  @Override
  public TaskAttempt getAttempt(TezTaskAttemptID attemptID) {
    readLock.lock();
    try {
      return attempts.get(attemptID);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Vertex getVertex() {
    return vertex;
  }

  @Override
  public TezTaskID getTaskId() {
    return taskId;
  }

  @Override
  public boolean isFinished() {
    TaskStateInternal internalState = getInternalState();
    return (internalState == TaskStateInternal.SUCCEEDED ||
        internalState == TaskStateInternal.FAILED ||
        internalState == TaskStateInternal.KILLED ||
        internalState == TaskStateInternal.KILL_WAIT);
  }

  @Override
  public TaskReport getReport() {
    TaskReport report = new TaskReportImpl();
    readLock.lock();
    try {
      report.setTaskId(taskId);
      report.setStartTime(getLaunchTime());
      report.setFinishTime(getLastTaskAttemptFinishTime());
      report.setTaskState(getState());
      report.setProgress(getProgress());
      return report;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TezCounters getCounters() {
    TezCounters counters = new TezCounters();
    counters.incrAllCounters(this.counters);
    readLock.lock();
    try {
      TaskAttempt bestAttempt = selectBestAttempt();
      if (bestAttempt != null) {
        counters.incrAllCounters(bestAttempt.getCounters());
      }
      return counters;
    } finally {
      readLock.unlock();
    }
  }
  
  TaskStatistics getStatistics() {
    // simply return the stats from the best attempt
    readLock.lock();
    try {
      TaskAttemptImpl bestAttempt = (TaskAttemptImpl) selectBestAttempt();
      if (bestAttempt == null) {
        return null;
      }
      return bestAttempt.getStatistics();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public float getProgress() {
    readLock.lock();
    try {
      final TaskStateInternal state = getInternalState();
      if (state == TaskStateInternal.RUNNING) {
        TaskAttempt bestAttempt = selectBestAttempt();
        if (bestAttempt == null) {
          return 0f;
        }
        return bestAttempt.getProgress();
      } else if (state == TaskStateInternal.SUCCEEDED) {
        return 1.0f;
      } else {
        return 0.0f;
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public ArrayList<TezEvent> getTaskAttemptTezEvents(TezTaskAttemptID attemptID,
      int fromEventId, int maxEvents) {
    ArrayList<TezEvent> events = EMPTY_TASK_ATTEMPT_TEZ_EVENTS;
    readLock.lock();

    try {
      if (!attempts.containsKey(attemptID)) {
        throw new TezUncheckedException("Unknown TA: " + attemptID
            + " asking for events from task:" + getTaskId());
      }

      if (tezEventsForTaskAttempts.size() > fromEventId) {
        int actualMax = Math.min(maxEvents,
            (tezEventsForTaskAttempts.size() - fromEventId));
        int toEventId = actualMax + fromEventId;
        events = new ArrayList<TezEvent>(tezEventsForTaskAttempts.subList(fromEventId, toEventId));
        LOG.info("TaskAttempt:" + attemptID + " sent events: (" + fromEventId
            + "-" + toEventId + ").");
        // currently not modifying the events so that we dont have to create
        // copies of events. e.g. if we have to set taskAttemptId into the TezEvent
        // destination metadata then we will need to create a copy of the TezEvent
        // and then modify the metadata and then send the copy on the RPC. This
        // is important because TezEvents are only routed in the AM and not copied
        // during routing. So e.g. a broadcast edge will send the same event to
        // all consumers (like it should). If copies were created then re-routing
        // the events on parallelism changes would be difficult. We would have to
        // buffer the events in the Vertex until the parallelism was set and then
        // route the events.
      }
      return events;
    } finally {
      readLock.unlock();
    }
  }
  
  @Override
  public TaskSpec getBaseTaskSpec() {
    readLock.lock();
    try {
      return baseTaskSpec;
    } finally {
      readLock.unlock();
    }
  }
  
  @Override
  public TaskLocationHint getTaskLocationHint() {
    readLock.lock();
    try {
      return locationHint;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<String> getDiagnostics() {
    readLock.lock();
    try {
      return this.diagnostics;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getFinishTime() {
    readLock.lock();
    try {
      return finishTime;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Set<NodeId> getNodesWithRunningAttempts() {
    return nodesWithRunningAttempts;
  }

  @VisibleForTesting
  public TaskStateInternal getInternalState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  private static TaskState getExternalState(TaskStateInternal smState) {
    if (smState == TaskStateInternal.KILL_WAIT) {
      return TaskState.KILLED;
    } else {
      return TaskState.valueOf(smState.name());
    }
  }

  //this is always called in read/write lock
  private long getLaunchTime() {
    long taskLaunchTime = 0;
    boolean launchTimeSet = false;
    for (TaskAttempt at : attempts.values()) {
      // select the least launch time of all attempts
      long attemptLaunchTime = at.getLaunchTime();
      if (attemptLaunchTime != 0 && !launchTimeSet) {
        // For the first non-zero launch time
        launchTimeSet = true;
        taskLaunchTime = attemptLaunchTime;
      } else if (attemptLaunchTime != 0 && taskLaunchTime > attemptLaunchTime) {
        taskLaunchTime = attemptLaunchTime;
      }
    }
    if (!launchTimeSet) {
      return this.scheduledTime;
    }
    return taskLaunchTime;
  }

  //this is always called in read/write lock
  //TODO Verify behaviour is Task is killed (no finished attempt)
  private long getLastTaskAttemptFinishTime() {
    if (!isFinished()) {
      return 0;
    }
    long finishTime = 0;
    for (TaskAttempt at : attempts.values()) {
      //select the max finish time of all attempts
      // FIXME shouldnt this not count attempts killed after an attempt succeeds
      if (finishTime < at.getFinishTime()) {
        finishTime = at.getFinishTime();
      }
    }
    return finishTime;
  }

  private TaskStateInternal finished(TaskStateInternal finalState) {
    if (getInternalState() == TaskStateInternal.RUNNING) {
      // TODO Metrics
      //metrics.endRunningTask(this);
    }
    return finalState;
  }

  //select the nextAttemptNumber with best progress
  // always called inside the Read Lock
  private TaskAttempt selectBestAttempt() {
    float progress = 0f;
    TaskAttempt result = null;
    for (TaskAttempt at : attempts.values()) {
      switch (at.getState()) {

      // ignore all failed task attempts
      case FAILED:
      case KILLED:
        continue;
      default:
      }
      if (result == null) {
        result = at; //The first time around
      }
      // calculate the best progress
      float attemptProgress = at.getProgress();
      if (attemptProgress > progress) {
        result = at;
        progress = attemptProgress;
      }
    }
    return result;
  }

  @Override
  public boolean canCommit(TezTaskAttemptID taskAttemptID) {
    writeLock.lock();
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Commit go/no-go request from " + taskAttemptID);
      }
      TaskState state = getState();
      if (state == TaskState.SCHEDULED) {
        // the actual running task ran and is done and asking for commit. we are still stuck 
        // in the scheduled state which indicates a backlog in event processing. lets wait for the
        // backlog to clear. returning false will make the attempt come back to us.
        LOG.info(
            "Event processing delay. "
            + "Attempt committing before state machine transitioned to running : Task {}", taskId);
        return false;
      }
      // at this point the attempt is no longer in scheduled state or else we would still 
      // have been in scheduled state in task impl.
      if (state != TaskState.RUNNING) {
        LOG.info("Task not running. Issuing kill to bad commit attempt " + taskAttemptID);
        eventHandler.handle(new TaskAttemptEventKillRequest(taskAttemptID
            , "Task not running. Bad attempt.", TaskAttemptTerminationCause.TERMINATED_ORPHANED));
        return false;
      }
      if (commitAttempt == null) {
        TaskAttempt ta = getAttempt(taskAttemptID);
        if (ta == null) {
          throw new TezUncheckedException("Unknown task for commit: " + taskAttemptID);
        }
        // Its ok to get a non-locked state snapshot since we handle changes of
        // state in the task attempt. Dont want to deadlock here.
        TaskAttemptState taState = ta.getStateNoLock();
        if (taState == TaskAttemptState.RUNNING) {
          commitAttempt = taskAttemptID;
          LOG.info(taskAttemptID + " given a go for committing the task output.");
          return true;
        } else {
          LOG.info(taskAttemptID + " with state: " + taState +
              " given a no-go for commit because its not running.");
          return false;
        }
      } else {
        if (commitAttempt.equals(taskAttemptID)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(taskAttemptID + " already given a go for committing the task output.");
          }
          return true;
        }
        // Don't think this can be a pluggable decision, so simply raise an
        // event for the TaskAttempt to delete its output.
        // Wait for commit attempt to succeed. Dont kill this. If commit
        // attempt fails then choose a different committer. When commit attempt
        // succeeds then this and others will be killed
        if (LOG.isDebugEnabled()) {
          LOG.debug(commitAttempt + " is current committer. Commit waiting for:  " + taskAttemptID);
        }
        return false;
      }

    } finally {
      writeLock.unlock();
    }
  }

  TaskAttemptImpl createAttempt(int attemptNumber, TezTaskAttemptID schedulingCausalTA) {
    TezTaskAttemptID attemptId = TezBuilderUtils.newTaskAttemptId(taskId, attemptNumber);
    TaskSpec taskSpec = new TaskSpec(attemptId,
        baseTaskSpec.getDAGName(), baseTaskSpec.getVertexName(),
        baseTaskSpec.getVertexParallelism(), baseTaskSpec.getProcessorDescriptor(),
        baseTaskSpec.getInputs(), baseTaskSpec.getOutputs(), baseTaskSpec.getGroupInputs(),
        baseTaskSpec.getTaskConf());
    return new TaskAttemptImpl(attemptId, eventHandler,
        taskCommunicatorManagerInterface, conf, clock, taskHeartbeatHandler, appContext,
        (failedAttempts > 0), taskResource, containerContext, leafVertex, this,
        locationHint, taskSpec, schedulingCausalTA);
  }

  @Override
  public TaskAttempt getSuccessfulAttempt() {
    readLock.lock();
    try {
      if (null == successfulAttempt) {
        return null;
      }
      return attempts.get(successfulAttempt);
    } finally {
      readLock.unlock();
    }
  }

  // This is always called in the Write Lock
  private void addAndScheduleAttempt(TezTaskAttemptID schedulingCausalTA) {
    TaskAttempt attempt = createAttempt(attempts.size(), schedulingCausalTA);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created attempt " + attempt.getID());
    }
    switch (attempts.size()) {
      case 0:
        attempts = Collections.singletonMap(attempt.getID(), attempt);
        break;

      case 1:
        Map<TezTaskAttemptID, TaskAttempt> newAttempts
            = new LinkedHashMap<TezTaskAttemptID, TaskAttempt>(maxFailedAttempts);
        newAttempts.putAll(attempts);
        attempts = newAttempts;
        Preconditions.checkArgument(attempts.put(attempt.getID(), attempt) == null,
            attempt.getID() + " already existed");
        break;

      default:
        Preconditions.checkArgument(attempts.put(attempt.getID(), attempt) == null,
            attempt.getID() + " already existed");
        break;
    }

    // TODO: Recovery
    /*
    // Update nextATtemptNumber
    if (taskAttemptsFromPreviousGeneration.isEmpty()) {
      ++nextAttemptNumber;
    } else {
      // There are still some TaskAttempts from previous generation, use them
      nextAttemptNumber =
          taskAttemptsFromPreviousGeneration.remove(0).getAttemptId().getId();
    }
    */

    this.taskAttemptStatus.put(attempt.getID().getId(), false);
    //schedule the nextAttemptNumber
    // send event to DAG to assign priority and schedule the attempt with global
    // picture in mind
    eventHandler.handle(new DAGEventSchedulerUpdate(
        DAGEventSchedulerUpdate.UpdateType.TA_SCHEDULE, attempt));

  }

  @Override
  public void handle(TaskEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing TaskEvent " + event.getTaskID() + " of type "
          + event.getType() + " while in state " + getInternalState()
          + ". Event: " + event);
    }
    try {
      writeLock.lock();
      TaskStateInternal oldState = getInternalState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event" + event.getType()
            + " at current state " + oldState + " for task " + this.taskId, e);
        internalError(event.getType());
      } catch (RuntimeException e) {
        LOG.error("Uncaught exception when trying handle event " + event.getType()
            + " at current state " + oldState + " for task " + this.taskId, e);
        internalErrorUncaughtException(event.getType(), e);
      }
      if (oldState != getInternalState()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(taskId + " Task Transitioned from " + oldState + " to "
              + getInternalState() + " due to event "
              + event.getType());
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  protected void internalError(TaskEventType type) {
    LOG.error("Invalid event " + type + " on Task " + this.taskId + " in state:"
        + getInternalState());
    eventHandler.handle(new DAGEventDiagnosticsUpdate(
        this.taskId.getVertexID().getDAGId(), "Invalid event " + type +
        " on Task " + this.taskId));
    eventHandler.handle(new DAGEvent(this.taskId.getVertexID().getDAGId(),
        DAGEventType.INTERNAL_ERROR));
  }

  protected void internalErrorUncaughtException(TaskEventType type, Exception e) {
    eventHandler.handle(new DAGEventDiagnosticsUpdate(
        this.taskId.getVertexID().getDAGId(), "Uncaught exception when handling  event " + type +
        " on Task " + this.taskId + ", error=" + e.getMessage()));
    eventHandler.handle(new DAGEvent(this.taskId.getVertexID().getDAGId(),
        DAGEventType.INTERNAL_ERROR));
  }


  private void sendTaskAttemptCompletionEvent(TezTaskAttemptID attemptId,
      TaskAttemptStateInternal attemptState) {
    eventHandler.handle(new VertexEventTaskAttemptCompleted(attemptId, attemptState));
  }

  // always called inside a transition, in turn inside the Write Lock
  private void handleTaskAttemptCompletion(TezTaskAttemptID attemptId,
      TaskAttemptStateInternal attemptState) {
    this.sendTaskAttemptCompletionEvent(attemptId, attemptState);
    sendDAGSchedulerFinishedEvent(attemptId);
  }

  private void sendDAGSchedulerFinishedEvent(TezTaskAttemptID taId) {
    // send notification to DAG scheduler
    eventHandler.handle(new DAGEventSchedulerUpdate(
        DAGEventSchedulerUpdate.UpdateType.TA_COMPLETED, attempts.get(taId)));
  }
  
  private static void unSucceed(TaskImpl task) {
    task.commitAttempt = null;
    task.successfulAttempt = null;
  }
  
  /**
  * @return a String representation of the splits.
  *
  * Subclasses can override this method to provide their own representations
  * of splits (if any).
  *
  */
  protected String getSplitsAsString(){
	  return "";
  }

  protected void logJobHistoryTaskStartedEvent() {
    TaskStartedEvent startEvt = new TaskStartedEvent(taskId,
        getVertex().getName(), scheduledTime, getLaunchTime());
    this.appContext.getHistoryHandler().handle(
        new DAGHistoryEvent(taskId.getVertexID().getDAGId(), startEvt));
  }

  protected void logJobHistoryTaskFinishedEvent() {
    // FIXME need to handle getting finish time as this function
    // is called from within a transition
    this.finishTime = clock.getTime();
    TaskFinishedEvent finishEvt = new TaskFinishedEvent(taskId,
        getVertex().getName(), getLaunchTime(), this.finishTime,
        successfulAttempt,
        TaskState.SUCCEEDED, "", getCounters(), failedAttempts);
    this.appContext.getHistoryHandler().handle(
        new DAGHistoryEvent(taskId.getVertexID().getDAGId(), finishEvt));
  }

  protected void logJobHistoryTaskFailedEvent(TaskState finalState) {
    this.finishTime = clock.getTime();
    TaskFinishedEvent finishEvt = new TaskFinishedEvent(taskId,
        getVertex().getName(), getLaunchTime(), this.finishTime, null,
        finalState, 
        StringUtils.join(getDiagnostics(), LINE_SEPARATOR),
        getCounters(), failedAttempts);
    this.appContext.getHistoryHandler().handle(
        new DAGHistoryEvent(taskId.getVertexID().getDAGId(), finishEvt));
  }

  private void addDiagnosticInfo(String diag) {
    if (diag != null && !diag.equals("")) {
      diagnostics.add(diag);
    }
  }
  
  @VisibleForTesting
  int getUncompletedAttemptsCount() {
    try {
      readLock.lock();
      return Maps.filterValues(taskAttemptStatus, new Predicate<Boolean>() {
        @Override
        public boolean apply(Boolean state) {
          return !state;
        }
      }).size();
    } finally {
      readLock.unlock();
    }
  }

  @VisibleForTesting
  int getFinishedAttemptsCount() {
    try {
      readLock.lock();
      return Maps.filterValues(taskAttemptStatus, new Predicate<Boolean>() {
        @Override
        public boolean apply(Boolean state) {
          return state;
        }
      }).size();
    } finally {
      readLock.unlock();
    }
  }

  private static class InitialScheduleTransition
    implements MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal> {

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      if (task.recoveryData != null) {
        TaskStartedEvent tStartedEvent = task.recoveryData.getTaskStartedEvent();
        TaskFinishedEvent tFinishedEvent = task.recoveryData.getTaskFinishedEvent();
        // If TaskStartedEvent is not seen but TaskFinishedEvent is seen, that means 
        // Task is killed before it is started. Just send T_TERMINATE to itself to move to KILLED
        if (tStartedEvent == null
            && tFinishedEvent != null) {
          Preconditions.checkArgument(tFinishedEvent.getState() == TaskState.KILLED,
              "TaskStartedEvent is not seen, but TaskFinishedEvent is seen and with invalid state="
                  + tFinishedEvent.getState() + ", taskId=" + task.getTaskId());
          // TODO (TEZ-2938)
          // use tFinishedEvent.getTerminationCause after adding TaskTerminationCause to TaskFinishedEvent
          task.eventHandler.handle(new TaskEventTermination(task.taskId,
              TaskAttemptTerminationCause.UNKNOWN_ERROR, tFinishedEvent.getDiagnostics(), true));
          return TaskStateInternal.NEW;
        }
      } else {
        task.scheduledTime = task.clock.getTime();
        task.logJobHistoryTaskStartedEvent();
        task.vertex.reportTaskStartTime(task.getLaunchTime());
      }
      // No matter whether it is in recovery or normal execution, always schedule new task attempt.
      // TaskAttempt will continue the recovery if necessary and send task attempt status
      // to this Task.
      TaskEventScheduleTask scheduleEvent = (TaskEventScheduleTask) event;
      task.locationHint = scheduleEvent.getTaskLocationHint();
      task.baseTaskSpec = scheduleEvent.getBaseTaskSpec();
      // For now, initial scheduling dependency is due to vertex manager scheduling
      task.addAndScheduleAttempt(null);
      return TaskStateInternal.SCHEDULED;
    }
  }

  // Used when creating a new attempt while one is already running.
  //  Currently we do this for speculation.  In the future we may do this
  //  for tasks that failed in a way that might indicate application code
  //  problems, so we can take later failures in parallel and flush the
  //  job quickly when this happens.
  private static class RedundantScheduleTransition
    implements SingleArcTransition<TaskImpl, TaskEvent> {

    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      LOG.info("Scheduling a redundant attempt for task " + task.taskId);
      task.counters.findCounter(TaskCounter.NUM_SPECULATIONS).increment(1);
      TaskAttempt earliestUnfinishedAttempt = null;
      for (TaskAttempt ta : task.attempts.values()) {
        // find the oldest running attempt
        if (!ta.isFinished()) {
          earliestUnfinishedAttempt = ta;
          task.nodesWithRunningAttempts.add(ta.getNodeId());
        }
      }
      task.addAndScheduleAttempt(earliestUnfinishedAttempt.getID());
    }
  }


  private static class AttemptSucceededTransition
      implements MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal> {

    private String recoverSuccessTaskAttempt(TaskImpl task) {
      // Found successful attempt
      // Recover data
      String errorMsg = null;
      if (task.getVertex().getOutputCommitters() != null
          && !task.getVertex().getOutputCommitters().isEmpty()) {
        for (Entry<String, OutputCommitter> entry
            : task.getVertex().getOutputCommitters().entrySet()) {
          LOG.info("Recovering data for task from previous DAG attempt"
              + ", taskId=" + task.getTaskId()
              + ", output=" + entry.getKey());
          OutputCommitter committer = entry.getValue();
          if (!committer.isTaskRecoverySupported()) {
            errorMsg = "Task recovery not supported by committer"
                + ", failing task attempt";
            LOG.info(errorMsg
                + ", taskId=" + task.getTaskId()
                + ", attemptId=" + task.successfulAttempt
                + ", output=" + entry.getKey());
            break;
          }
          try {
            committer.recoverTask(task.getTaskId().getId(),
                task.appContext.getApplicationAttemptId().getAttemptId()-1);
          } catch (Exception e) {
            errorMsg = "Task recovery failed by committer: "
                + ExceptionUtils.getStackTrace(e);
            LOG.warn("Task recovery failed by committer"
                + ", taskId=" + task.getTaskId()
                + ", attemptId=" + task.successfulAttempt
                + ", output=" + entry.getKey(), e);
            break;
          }
        }
      }
      return errorMsg;
    }

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      TezTaskAttemptID successTaId = ((TaskEventTAUpdate) event).getTaskAttemptID();
      // Try to recover the succeeded TaskAttempt. It may be not recoverable if has committer which don't support
      // recovery. In that case just reschedule new attempt if numFailedAttempts does not exceeded maxFailedAttempts.
      if (task.recoveryData!= null
          && task.recoveryData.isTaskAttemptSucceeded(successTaId)) {
        String errorMsg = recoverSuccessTaskAttempt(task);
        if (errorMsg != null) {
          LOG.info("Can not recover the successful task attempt, schedule new task attempt,"
              + "taskId=" + task.getTaskId());
          task.successfulAttempt = null;
          task.addAndScheduleAttempt(successTaId);
          task.eventHandler.handle(new TaskAttemptEventAttemptKilled(successTaId,
              errorMsg, TaskAttemptTerminationCause.TERMINATED_AT_RECOVERY, true));
          return TaskStateInternal.RUNNING;
        } else {
          task.successfulAttempt = successTaId;
          LOG.info("Recovered a successful attempt"
              + ", taskAttemptId=" + task.successfulAttempt.toString());
        }
      }
      // both recovery to succeeded and normal dag succeeded go here.
      if (task.commitAttempt != null &&
          !task.commitAttempt.equals(successTaId)) {
        // The succeeded attempt is not the one that was selected to commit
        // This is impossible and has to be a bug
        throw new TezUncheckedException("TA: " + successTaId
            + " succeeded but TA: " + task.commitAttempt
            + " was expected to commit and succeed");
      }

      task.handleTaskAttemptCompletion(successTaId,
          TaskAttemptStateInternal.SUCCEEDED);
      task.taskAttemptStatus.put(successTaId.getId(), true);
      task.successfulAttempt = successTaId;
      task.eventHandler.handle(new VertexEventTaskCompleted(
          task.taskId, TaskState.SUCCEEDED));
      LOG.info("Task succeeded with attempt " + task.successfulAttempt);
      task.logJobHistoryTaskFinishedEvent();
      TaskAttempt successfulAttempt = task.attempts.get(successTaId);

      // issue kill to all other attempts
      for (TaskAttempt attempt : task.attempts.values()) {
        if (!attempt.getID().equals(task.successfulAttempt) &&
            // This is okay because it can only talk us out of sending a
            //  TA_KILL message to an attempt that doesn't need one for
            //  other reasons.
            !attempt.isFinished()) {
          LOG.info("Issuing kill to other attempt " + attempt.getID() + " as attempt: " +
              task.successfulAttempt + " has succeeded");
          String diagnostics = null;
          TaskAttemptTerminationCause errCause = null;
          if (attempt.getLaunchTime() < successfulAttempt.getLaunchTime()) {
            diagnostics = "Killed this attempt as other speculative attempt : " + successTaId
                + " succeeded";
            errCause = TaskAttemptTerminationCause.TERMINATED_EFFECTIVE_SPECULATION;
          } else {
            diagnostics = "Killed this speculative attempt as original attempt: " + successTaId
                + " succeeded";
            errCause = TaskAttemptTerminationCause.TERMINATED_INEFFECTIVE_SPECULATION;
          }
          task.eventHandler.handle(new TaskAttemptEventKillRequest(attempt
              .getID(), diagnostics, errCause));
        }
      }
      return task.finished(TaskStateInternal.SUCCEEDED);
    }
  }

  private static class AttemptKilledTransition implements
      SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TaskEventTAUpdate castEvent = (TaskEventTAUpdate) event;
      task.addDiagnosticInfo("TaskAttempt " + castEvent.getTaskAttemptID().getId() + " killed");
      if (task.commitAttempt !=null &&
          castEvent.getTaskAttemptID().equals(task.commitAttempt)) {
        task.commitAttempt = null;
      }
      task.handleTaskAttemptCompletion(
          castEvent.getTaskAttemptID(),
          TaskAttemptStateInternal.KILLED);
      // we KillWaitAttemptCompletedTransitionready have a spare
      task.taskAttemptStatus.put(castEvent.getTaskAttemptID().getId(), true);

      boolean isRejection = false;
      if (event instanceof TaskEventTAKilled) {
        TaskEventTAKilled killEvent = (TaskEventTAKilled) event;
        if (killEvent.getCausalEvent() instanceof TaskAttemptEventTerminationCauseEvent) {
          TaskAttemptEventTerminationCauseEvent cause =
              (TaskAttemptEventTerminationCauseEvent)killEvent.getCausalEvent();
          isRejection = cause.getTerminationCause() == TaskAttemptTerminationCause.SERVICE_BUSY;
        }
      }
      if (isRejection) { // TODO: remove as part of TEZ-3881.
        task.getVertex().incrementRejectedTaskAttemptCount();
      } else {
        task.getVertex().incrementKilledTaskAttemptCount();
      }
      if (task.shouldScheduleNewAttempt()) {
        task.addAndScheduleAttempt(castEvent.getTaskAttemptID());
      }
    }
  }

  private static class KillWaitAttemptCompletedTransition implements
      MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal> {

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      TaskEventTAUpdate castEvent = (TaskEventTAUpdate)event;
      task.handleTaskAttemptCompletion(
          castEvent.getTaskAttemptID(),
          TaskAttemptStateInternal.KILLED);
      task.taskAttemptStatus.put(castEvent.getTaskAttemptID().getId(), true);
      // check whether all attempts are finished
      if (task.getFinishedAttemptsCount() == task.attempts.size()) {
        task.logJobHistoryTaskFailedEvent(getExternalState(TaskStateInternal.KILLED));
        task.eventHandler.handle(
            new VertexEventTaskCompleted(
                task.taskId, getExternalState(TaskStateInternal.KILLED)));
        return TaskStateInternal.KILLED;
      }
      return task.getInternalState();
    }
  }
  
  private boolean shouldScheduleNewAttempt() {
    return (getUncompletedAttemptsCount() == 0
            && successfulAttempt == null);
  }

  private static class AttemptFailedTransition implements
    MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal> {

    private TezTaskAttemptID schedulingCausalTA;
    
    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      task.failedAttempts++;
      task.getVertex().incrementFailedTaskAttemptCount();
      TaskEventTAFailed castEvent = (TaskEventTAFailed) event;
      schedulingCausalTA = castEvent.getTaskAttemptID();
      task.addDiagnosticInfo("TaskAttempt " + castEvent.getTaskAttemptID().getId() + " failed,"
          + " info=" + task.getAttempt(castEvent.getTaskAttemptID()).getDiagnostics());
      if (task.commitAttempt != null &&
          castEvent.getTaskAttemptID().equals(task.commitAttempt)) {
        task.commitAttempt = null;
      }
      // The attempt would have informed the scheduler about it's failure

      task.taskAttemptStatus.put(castEvent.getTaskAttemptID().getId(), true);
      if (task.failedAttempts < task.maxFailedAttempts &&
          castEvent.getTaskFailureType() == TaskFailureType.NON_FATAL) {
        task.handleTaskAttemptCompletion(
            ((TaskEventTAUpdate) event).getTaskAttemptID(),
            TaskAttemptStateInternal.FAILED);
        // we don't need a new event if we already have a spare
        if (task.shouldScheduleNewAttempt()) {
          LOG.info("Scheduling new attempt for task: " + task.getTaskId()
              + ", currentFailedAttempts: " + task.failedAttempts + ", maxFailedAttempts: "
              + task.maxFailedAttempts);
          task.addAndScheduleAttempt(getSchedulingCausalTA());
        }
      } else {
        if (castEvent.getTaskFailureType() == TaskFailureType.NON_FATAL) {
          LOG.info(
              "Failing task: {} due to too many failed attempts. currentFailedAttempts={}, maxFailedAttempts={}",
              task.getTaskId(), task.failedAttempts, task.maxFailedAttempts);
        } else {
          LOG.info(
              "Failing task: {} due to {} error reported by TaskAttempt. CurrentFailedAttempts={}",
              task.getTaskId(), TaskFailureType.FATAL, task.failedAttempts);
        }
        task.handleTaskAttemptCompletion(
            ((TaskEventTAUpdate) event).getTaskAttemptID(),
            TaskAttemptStateInternal.FAILED);
        task.logJobHistoryTaskFailedEvent(TaskState.FAILED);
        task.eventHandler.handle(
            new VertexEventTaskCompleted(task.taskId, TaskState.FAILED));
        return task.finished(TaskStateInternal.FAILED);
      }
      return getDefaultState(task);
    }

    protected TaskStateInternal getDefaultState(TaskImpl task) {
      return task.getInternalState();
    }
    
    protected TezTaskAttemptID getSchedulingCausalTA() {
      return schedulingCausalTA;
    }
  }

  private static class TaskRetroactiveFailureTransition
      extends AttemptFailedTransition {

    private TezTaskAttemptID schedulingCausalTA;

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      TaskEventTAFailed castEvent = (TaskEventTAFailed) event;
      TezTaskAttemptID failedAttemptId = castEvent.getTaskAttemptID();
      TaskAttempt failedAttempt = task.getAttempt(failedAttemptId);
      ContainerId containerId = failedAttempt.getAssignedContainerID();
      if (containerId != null) {
        AMContainer amContainer = task.appContext.getAllContainers().
            get(containerId);
        if (amContainer != null) {
          // inform the node about failure
          task.eventHandler.handle(
              new AMNodeEventTaskAttemptEnded(amContainer.getContainer().getNodeId(),
                  task.getVertex().getTaskSchedulerIdentifier(),
                  containerId, failedAttemptId, true));
        }
      }
      
      if (task.getInternalState() == TaskStateInternal.SUCCEEDED &&
          !failedAttemptId.equals(task.successfulAttempt)) {
        // don't allow a different task attempt to override a previous
        // succeeded state and mark the attempt status as done
        task.taskAttemptStatus.put(failedAttemptId.getId(), true);
        return TaskStateInternal.SUCCEEDED;
      }

      if (task.leafVertex) {
        LOG.error("Unexpected event for task of leaf vertex " + event.getType() + ", taskId: "
            + task.getTaskId());
        task.internalError(event.getType());
      }
      Preconditions.checkState(castEvent.getCausalEvent() != null);
      TaskAttemptEventOutputFailed destinationEvent = 
          (TaskAttemptEventOutputFailed) castEvent.getCausalEvent();
      schedulingCausalTA = destinationEvent.getInputFailedEvent().getSourceInfo().getTaskAttemptID();

      // super.transition is mostly coded for the case where an
      //  UNcompleted task failed.  When a COMPLETED task retroactively
      //  fails, we have to let AttemptFailedTransition.transition
      //  believe that there's no redundancy.
      unSucceed(task);

      TaskStateInternal returnState = super.transition(task, event);

      if (returnState == TaskStateInternal.SCHEDULED) {
        // tell the dag about the rescheduling
        task.eventHandler.handle(new VertexEventTaskReschedule(task.taskId));
      }

      return returnState;
    }
    
    @Override
    protected TezTaskAttemptID getSchedulingCausalTA() {
      return schedulingCausalTA;
    }

    @Override
    protected TaskStateInternal getDefaultState(TaskImpl task) {
      return TaskStateInternal.SCHEDULED;
    }
  }

  private static class TaskRetroactiveKilledTransition implements
    MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal> {

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {

      TaskEventTAUpdate attemptEvent = (TaskEventTAUpdate) event;
      TezTaskAttemptID attemptId = attemptEvent.getTaskAttemptID();
      TaskStateInternal resultState = TaskStateInternal.SUCCEEDED;
      if(task.successfulAttempt.equals(attemptId)) {
        // typically we are here because this map task was run on a bad node and
        // we want to reschedule it on a different node.
        // Depending on whether there are previous failed attempts or not this
        // can SCHEDULE or RESCHEDULE the container allocate request. If this
        // SCHEDULE's then the dataLocal hosts of this taskAttempt will be used
        // from the map splitInfo. So the bad node might be sent as a location
        // to the RM. But the RM would ignore that just like it would ignore
        // currently pending container requests affinitized to bad nodes.
        unSucceed(task);
        task.eventHandler.handle(new VertexEventTaskReschedule(task.taskId));
        resultState = TaskStateInternal.SCHEDULED;
      }
      ATTEMPT_KILLED_TRANSITION.transition(task, event);
      return resultState;
    }
  }

  private static class KillNewTransition
    implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TaskEventTermination terminateEvent = (TaskEventTermination)event;
      task.addDiagnosticInfo(terminateEvent.getDiagnosticInfo());
      if (terminateEvent.isFromRecovery()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Recovered to KILLED, taskId=" + task.getTaskId());
        }
      } else {
        task.logJobHistoryTaskFailedEvent(TaskState.KILLED);
      }
      task.eventHandler.handle(
          new VertexEventTaskCompleted(task.taskId, TaskState.KILLED));
      // TODO Metrics
      //task.metrics.endWaitingTask(task);
    }
  }

  private static class AttemptRedundantCompletedTransition
      implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TezTaskAttemptID successTaId = ((TaskEventTAUpdate)event).getTaskAttemptID();
      task.taskAttemptStatus.put(successTaId.getId(), true);
    }
  }

  private static class TaskStateChangedCallback
      implements OnStateChangedCallback<TaskStateInternal, TaskImpl> {

    @Override
    public void onStateChanged(TaskImpl task, TaskStateInternal taskStateInternal) {
      // Only registered for SUCCEEDED notifications at the moment
      Preconditions.checkState(taskStateInternal == TaskStateInternal.SUCCEEDED);
      TaskAttempt successfulAttempt = task.getSuccessfulAttempt();
      // TODO TEZ-1577.
      // This is a horrible hack to get around recovery issues. Without this, recovery would fail
      // for successful vertices.
      // With this, recovery will end up failing for DAGs making use of InputInitializerEvents
      int succesfulAttemptInt = -1;
      if (successfulAttempt != null) {
        succesfulAttemptInt = successfulAttempt.getID().getId();
      }
      task.stateChangeNotifier.taskSucceeded(task.getVertex().getName(), task.getTaskId(),
          succesfulAttemptInt);
    }
  }

  private void killUnfinishedAttempt(TaskAttempt attempt, String logMsg, TaskAttemptTerminationCause errorCause) {
    if (commitAttempt != null && commitAttempt.equals(attempt.getID())) {
      LOG.info("Unsetting commit attempt: " + commitAttempt + " since attempt is being killed");
      commitAttempt = null;
    }
    if (attempt != null && !attempt.isFinished()) {
      eventHandler.handle(new TaskAttemptEventKillRequest(attempt.getID(), logMsg, errorCause));
    }
  }

  @Override
  public void registerTezEvent(TezEvent tezEvent) {
    this.writeLock.lock();
    try {
      this.tezEventsForTaskAttempts.add(tezEvent);
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public long getFirstAttemptStartTime() {
    readLock.lock();
    try {
      // The first attempt will always have an index of 0.
      return getAttempt(TezTaskAttemptID.getInstance(getTaskId(), 0)).getScheduleTime();
    } finally {
      readLock.unlock();
    }
  }

  private static class KillTransition
    implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TaskEventTermination terminateEvent = (TaskEventTermination)event;
      task.addDiagnosticInfo(terminateEvent.getDiagnosticInfo());
      // issue kill to all non finished attempts
      for (TaskAttempt attempt : task.attempts.values()) {
        task.killUnfinishedAttempt(attempt, "Task KILL is received. Killing attempt. Diagnostics: "
            + terminateEvent.getDiagnosticInfo(), terminateEvent.getTerminationCause());
      }
    }
  }

  static class LaunchTransition
      implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      // TODO Metrics
      /*
      task.metrics.launchedTask(task);
      task.metrics.runningTask(task);
      */
    }
  }

  @Private
  @VisibleForTesting
  void setCounters(TezCounters counters) {
    try {
      writeLock.lock();
      this.counters = counters;
    } finally {
      writeLock.unlock();
    }
  }

}
