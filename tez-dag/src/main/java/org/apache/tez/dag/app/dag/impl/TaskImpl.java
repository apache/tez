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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskReport;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.TaskStateInternal;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventDiagnosticsUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventKillRequest;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventRecoverTask;
import org.apache.tez.dag.app.dag.event.TaskEventTAUpdate;
import org.apache.tez.dag.app.dag.event.TaskEventTermination;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEventTaskAttemptCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskReschedule;
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.app.rm.node.AMNodeEventTaskAttemptEnded;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.impl.TezEvent;

import com.google.common.annotations.VisibleForTesting;

/**
 * Implementation of Task interface.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TaskImpl implements Task, EventHandler<TaskEvent> {

  private static final Log LOG = LogFactory.getLog(TaskImpl.class);

  private static final String LINE_SEPARATOR = System
    .getProperty("line.separator");

  protected final Configuration conf;
  protected final TaskAttemptListener taskAttemptListener;
  protected final TaskHeartbeatHandler taskHeartbeatHandler;
  protected final EventHandler eventHandler;
  private final TezTaskID taskId;
  private Map<TezTaskAttemptID, TaskAttempt> attempts;
  private final int maxFailedAttempts;
  protected final Clock clock;
  private final Lock readLock;
  private final Lock writeLock;
  private final List<String> diagnostics = new ArrayList<String>();
  // TODO Metrics
  //private final MRAppMetrics metrics;
  protected final AppContext appContext;
  private final Resource taskResource;
  private final ContainerContext containerContext;
  @VisibleForTesting
  long scheduledTime;

  private final List<TezEvent> tezEventsForTaskAttempts = new ArrayList<TezEvent>();
  private static final List<TezEvent> EMPTY_TASK_ATTEMPT_TEZ_EVENTS =
      new ArrayList(0);

  // counts the number of attempts that are either running or in a state where
  //  they will come to be running when they get a Container
  @VisibleForTesting
  int numberUncompletedAttempts = 0;

  private boolean historyTaskStartGenerated = false;

  private static final SingleArcTransition<TaskImpl, TaskEvent>
     ATTEMPT_KILLED_TRANSITION = new AttemptKilledTransition();
  private static final SingleArcTransition<TaskImpl, TaskEvent>
     KILL_TRANSITION = new KillTransition();

  // Recovery related flags
  boolean recoveryStartEventSeen = false;

  private static final StateMachineFactory
               <TaskImpl, TaskStateInternal, TaskEventType, TaskEvent>
            stateMachineFactory
           = new StateMachineFactory<TaskImpl, TaskStateInternal, TaskEventType, TaskEvent>
               (TaskStateInternal.NEW)

    // define the state machine of Task

    // Transitions from NEW state
    .addTransition(TaskStateInternal.NEW, TaskStateInternal.SCHEDULED,
        TaskEventType.T_SCHEDULE, new InitialScheduleTransition())
    .addTransition(TaskStateInternal.NEW, TaskStateInternal.KILLED,
        TaskEventType.T_TERMINATE,
        new KillNewTransition())

    // Recover transition
    .addTransition(TaskStateInternal.NEW,
        EnumSet.of(TaskStateInternal.NEW,
            TaskStateInternal.SCHEDULED,
            TaskStateInternal.RUNNING, TaskStateInternal.SUCCEEDED,
            TaskStateInternal.FAILED, TaskStateInternal.KILLED),
        TaskEventType.T_RECOVER, new RecoverTransition())

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

    // When current attempt fails/killed and new attempt launched then
    // TODO Task should go back to SCHEDULED state TEZ-495
    // Transitions from RUNNING state
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING,
        TaskEventType.T_ATTEMPT_LAUNCHED) //more attempts may start later
    // This is an optional event.
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING,
        TaskEventType.T_ATTEMPT_OUTPUT_CONSUMABLE,
        new AttemptProcessingCompleteTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING,
        TaskEventType.T_ADD_SPEC_ATTEMPT, new RedundantScheduleTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.SUCCEEDED,
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
            TaskEventType.T_ATTEMPT_OUTPUT_CONSUMABLE,
            TaskEventType.T_ADD_SPEC_ATTEMPT))

    // Transitions from SUCCEEDED state
      // TODO May required different handling if OUTPUT_CONSUMABLE is one of
      // the stages. i.e. Task would only SUCCEED after all output consumed.
    .addTransition(TaskStateInternal.SUCCEEDED, //only possible for map tasks
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.SUCCEEDED, TaskStateInternal.FAILED),
        TaskEventType.T_ATTEMPT_FAILED, new TaskRetroactiveFailureTransition())
    .addTransition(TaskStateInternal.SUCCEEDED, //only possible for map tasks
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.SUCCEEDED),
        TaskEventType.T_ATTEMPT_KILLED, new TaskRetroactiveKilledTransition())
    // Ignore-able transitions.
    .addTransition(
        TaskStateInternal.SUCCEEDED, TaskStateInternal.SUCCEEDED,
        EnumSet.of(TaskEventType.T_ADD_SPEC_ATTEMPT,
            TaskEventType.T_TERMINATE,
            TaskEventType.T_ATTEMPT_SUCCEEDED, // Maybe track and reuse later
            TaskEventType.T_ATTEMPT_LAUNCHED))
    .addTransition(TaskStateInternal.SUCCEEDED, TaskStateInternal.SUCCEEDED,
        TaskEventType.T_SCHEDULE)

    // Transitions from FAILED state
    .addTransition(TaskStateInternal.FAILED, TaskStateInternal.FAILED,
        EnumSet.of(
            TaskEventType.T_TERMINATE,
            TaskEventType.T_ADD_SPEC_ATTEMPT))

    // Transitions from KILLED state
    .addTransition(TaskStateInternal.KILLED, TaskStateInternal.KILLED,
        EnumSet.of(
            TaskEventType.T_TERMINATE,
            TaskEventType.T_ADD_SPEC_ATTEMPT))
    .addTransition(TaskStateInternal.FAILED, TaskStateInternal.FAILED,
        TaskEventType.T_SCHEDULE)

    // create the topology tables
    .installTopology();

  private final StateMachine<TaskStateInternal, TaskEventType, TaskEvent>
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

  private TezTaskAttemptID outputConsumableAttempt;
  private boolean outputConsumableAttemptSuccessSent = false;

  //should be set to one which comes first
  //saying COMMIT_PENDING
  private TezTaskAttemptID commitAttempt;

  @VisibleForTesting
  TezTaskAttemptID successfulAttempt;

  @VisibleForTesting
  int failedAttempts;
  @VisibleForTesting
  int finishedAttempts;//finish are total of success, failed and killed

  private final boolean leafVertex;
  private TaskState recoveredState = TaskState.NEW;

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
      TaskAttemptListener taskAttemptListener,
      Clock clock, TaskHeartbeatHandler thh, AppContext appContext,
      boolean leafVertex, Resource resource,
      ContainerContext containerContext) {
    this.conf = conf;
    this.clock = clock;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
    this.attempts = Collections.emptyMap();
    // TODO Avoid reading this from configuration for each task.
    maxFailedAttempts = this.conf.getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS,
                              TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT);
    taskId = TezTaskID.getInstance(vertexId, taskIndex);
    this.taskAttemptListener = taskAttemptListener;
    this.taskHeartbeatHandler = thh;
    this.eventHandler = eventHandler;
    this.appContext = appContext;

    this.leafVertex = leafVertex;
    this.taskResource = resource;
    this.containerContext = containerContext;
    stateMachine = stateMachineFactory.make(this);
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
    return appContext.getCurrentDAG().getVertex(taskId.getVertexID());
  }

  @Override
  public TezTaskID getTaskId() {
    return taskId;
  }

  @Override
  public boolean isFinished() {
    readLock.lock();
    try {
      return (getInternalState() == TaskStateInternal.SUCCEEDED ||
              getInternalState() == TaskStateInternal.FAILED ||
              getInternalState() == TaskStateInternal.KILLED ||
              getInternalState() == TaskStateInternal.KILL_WAIT);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskReport getReport() {
    TaskReport report = new TaskReportImpl();
    readLock.lock();
    try {
      report.setTaskId(taskId);
      report.setStartTime(getLaunchTime());
      report.setFinishTime(getFinishTime());
      report.setTaskState(getState());
      report.setProgress(getProgress());
      return report;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TezCounters getCounters() {
    TezCounters counters = null;
    readLock.lock();
    try {
      TaskAttempt bestAttempt = selectBestAttempt();
      if (bestAttempt != null) {
        counters = bestAttempt.getCounters();
      } else {
        counters = TaskAttemptImpl.EMPTY_COUNTERS;
//        counters.groups = new HashMap<CharSequence, CounterGroup>();
      }
      return counters;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public float getProgress() {
    readLock.lock();
    try {
      TaskAttempt bestAttempt = selectBestAttempt();
      if (bestAttempt == null) {
        return 0f;
      }
      return bestAttempt.getProgress();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<TezEvent> getTaskAttemptTezEvents(TezTaskAttemptID attemptID,
      int fromEventId, int maxEvents) {
    List<TezEvent> events = EMPTY_TASK_ATTEMPT_TEZ_EVENTS;
    readLock.lock();

    if (!attempts.containsKey(attemptID)) {
      throw new TezUncheckedException("Unknown TA: " + attemptID
          + " asking for events from task:" + getTaskId());
    }

    try {
      if (tezEventsForTaskAttempts.size() > fromEventId) {
        int actualMax = Math.min(maxEvents,
            (tezEventsForTaskAttempts.size() - fromEventId));
        int toEventId = actualMax + fromEventId;
        events = Collections.unmodifiableList(new ArrayList<TezEvent>(
            tezEventsForTaskAttempts.subList(fromEventId, toEventId)));
        LOG.info("TaskAttempt:" + attemptID + " sent events: (" + fromEventId
            + "-" + toEventId + ")");
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
  public List<String> getDiagnostics() {
    readLock.lock();
    try {
      return this.diagnostics;
    } finally {
      readLock.unlock();
    }
  }

  private TaskAttempt createRecoveredEvent(TezTaskAttemptID tezTaskAttemptID) {
    TaskAttempt taskAttempt = createAttempt(tezTaskAttemptID.getId());
    return taskAttempt;
  }

  @Override
  public TaskState restoreFromEvent(HistoryEvent historyEvent) {
    switch (historyEvent.getEventType()) {
      case TASK_STARTED:
      {
        TaskStartedEvent tEvent = (TaskStartedEvent) historyEvent;
        recoveryStartEventSeen = true;
        this.scheduledTime = tEvent.getScheduledTime();
        if (this.attempts == null
            || this.attempts.isEmpty()) {
          this.attempts = new LinkedHashMap<TezTaskAttemptID, TaskAttempt>();
        }
        recoveredState = TaskState.SCHEDULED;
        finishedAttempts = 0;
        return recoveredState;
      }
      case TASK_FINISHED:
      {
        TaskFinishedEvent tEvent = (TaskFinishedEvent) historyEvent;
        if (!recoveryStartEventSeen
            && !tEvent.getState().equals(TaskState.KILLED)) {
          throw new TezUncheckedException("Finished Event seen but"
              + " no Started Event was encountered earlier"
              + ", taskId=" + taskId
              + ", finishState=" + tEvent.getState());
        }
        recoveredState = tEvent.getState();
        if (tEvent.getState() == TaskState.SUCCEEDED
            && tEvent.getSuccessfulAttemptID() != null) {
          successfulAttempt = tEvent.getSuccessfulAttemptID();
        }
        return recoveredState;
      }
      case TASK_ATTEMPT_STARTED:
      {
        TaskAttemptStartedEvent taskAttemptStartedEvent =
            (TaskAttemptStartedEvent) historyEvent;
        TaskAttempt recoveredAttempt = createRecoveredEvent(
            taskAttemptStartedEvent.getTaskAttemptID());
        recoveredAttempt.restoreFromEvent(taskAttemptStartedEvent);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding restored attempt into known attempts map"
              + ", taskAttemptId=" + taskAttemptStartedEvent.getTaskAttemptID());
        }
        this.attempts.put(taskAttemptStartedEvent.getTaskAttemptID(),
            recoveredAttempt);
        ++numberUncompletedAttempts;
        this.recoveredState = TaskState.RUNNING;
        return recoveredState;
      }
      case TASK_ATTEMPT_FINISHED:
      {
        TaskAttemptFinishedEvent taskAttemptFinishedEvent =
            (TaskAttemptFinishedEvent) historyEvent;
        TaskAttempt taskAttempt = this.attempts.get(
            taskAttemptFinishedEvent.getTaskAttemptID());
        finishedAttempts++;
        if (taskAttempt == null) {
          LOG.warn("Received an attempt finished event for an attempt that "
              + " never started or does not exist"
              + ", taskAttemptId=" + taskAttemptFinishedEvent.getTaskAttemptID()
              + ", taskAttemptFinishState=" + taskAttemptFinishedEvent.getState());
          TaskAttempt recoveredAttempt = createRecoveredEvent(
              taskAttemptFinishedEvent.getTaskAttemptID());
          this.attempts.put(taskAttemptFinishedEvent.getTaskAttemptID(),
              recoveredAttempt);
          if (!taskAttemptFinishedEvent.getState().equals(TaskAttemptState.KILLED)) {
            throw new TezUncheckedException("Could not find task attempt"
                + " when trying to recover"
                + ", taskAttemptId=" + taskAttemptFinishedEvent.getTaskAttemptID()
                + ", taskAttemptFinishState" + taskAttemptFinishedEvent.getState());
          }
          return recoveredState;
        }
        --numberUncompletedAttempts;
        if (numberUncompletedAttempts < 0) {
          throw new TezUncheckedException("Invalid recovery event for attempt finished"
              + ", more completions than starts encountered"
              + ", taskId=" + taskId
              + ", finishedAttempts=" + finishedAttempts
              + ", incompleteAttempts=" + numberUncompletedAttempts);
        }
        TaskAttemptState taskAttemptState = taskAttempt.restoreFromEvent(
            taskAttemptFinishedEvent);
        if (taskAttemptState.equals(TaskAttemptState.SUCCEEDED)) {
          recoveredState = TaskState.SUCCEEDED;
          successfulAttempt = taskAttempt.getID();
        } else if (taskAttemptState.equals(TaskAttemptState.FAILED)){
          failedAttempts++;
        }
        return recoveredState;
      }
      default:
        throw new RuntimeException("Unexpected event received for restoring"
            + " state, eventType=" + historyEvent.getEventType());
    }
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
  private long getFinishTime() {
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
      if (getState() != TaskState.RUNNING) {
        LOG.info("Task not running. Issuing kill to bad commit attempt " + taskAttemptID);
        eventHandler.handle(new TaskAttemptEventKillRequest(taskAttemptID
            , "Task not running. Bad attempt."));
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
          LOG.info(taskAttemptID + " given a go for committing the task output.");
          return true;
        }
        // Don't think this can be a pluggable decision, so simply raise an
        // event for the TaskAttempt to delete its output.
        // Wait for commit attempt to succeed. Dont kill this. If commit
        // attempt fails then choose a different committer. When commit attempt
        // succeeds then this and others will be killed
        LOG.info(commitAttempt
            + " is current committer. Commit waiting for:  "
            + taskAttemptID);
        return false;
      }

    } finally {
      writeLock.unlock();
    }
  }

  // TODO remove hacky name lookup
  @Override
  public boolean needsWaitAfterOutputConsumable() {
    Vertex vertex = getVertex();
    ProcessorDescriptor processorDescriptor = vertex.getProcessorDescriptor();
    if (processorDescriptor != null &&
        processorDescriptor.getClassName().contains("InitialTaskWithInMemSort")) {
      return true;
    } else {
      return false;
    }
  }


  @Override
  public TezTaskAttemptID getOutputConsumableAttempt() {
    readLock.lock();
    try {
      return this.outputConsumableAttempt;
    } finally {
      readLock.unlock();
    }
  }

  TaskAttemptImpl createAttempt(int attemptNumber) {
    return new TaskAttemptImpl(getTaskId(), attemptNumber, eventHandler,
        taskAttemptListener, conf, clock, taskHeartbeatHandler, appContext,
        (failedAttempts > 0), taskResource, containerContext, leafVertex);
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
  private void addAndScheduleAttempt() {
    TaskAttempt attempt = createAttempt(attempts.size());
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
        attempts.put(attempt.getID(), attempt);
        break;

      default:
        attempts.put(attempt.getID(), attempt);
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

    ++numberUncompletedAttempts;
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
        LOG.error("Can't handle this event at current state for "
            + this.taskId, e);
        internalError(event.getType());
      }
      if (oldState != getInternalState()) {
        LOG.info(taskId + " Task Transitioned from " + oldState + " to "
            + getInternalState());
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

  private void sendTaskAttemptCompletionEvent(TezTaskAttemptID attemptId,
      TaskAttemptStateInternal attemptState) {
    // raise the completion event only if the container is assigned
    // to nextAttemptNumber
    if (needsWaitAfterOutputConsumable()) {
      // An event may have been sent out during the OUTPUT_READY state itself.
      // Make sure the same event is not being sent out again.
      if (attemptId == outputConsumableAttempt
          && attemptState == TaskAttemptStateInternal.SUCCEEDED) {
        if (outputConsumableAttemptSuccessSent) {
          return;
        }
      }
    }
    eventHandler.handle(new VertexEventTaskAttemptCompleted(attemptId, attemptState));
  }

  // always called inside a transition, in turn inside the Write Lock
  private void handleTaskAttemptCompletion(TezTaskAttemptID attemptId,
      TaskAttemptStateInternal attemptState) {
    this.sendTaskAttemptCompletionEvent(attemptId, attemptState);
  }

  // TODO: Recovery
  /*
  private static TaskFinishedEvent createTaskFinishedEvent(TaskImpl task, TaskStateInternal taskState) {
    TaskFinishedEvent tfe =
      new TaskFinishedEvent(task.taskId,
        task.successfulAttempt,
        task.getFinishTime(task.successfulAttempt),
        task.taskId.getTaskType(),
        taskState.toString(),
        task.getCounters());
    return tfe;
  }

  private static TaskFailedEvent createTaskFailedEvent(TaskImpl task, List<String> diag, TaskStateInternal taskState, TezTaskAttemptID taId) {
    StringBuilder errorSb = new StringBuilder();
    if (diag != null) {
      for (String d : diag) {
        errorSb.append(", ").append(d);
      }
    }
    TaskFailedEvent taskFailedEvent = new TaskFailedEvent(
        TypeConverter.fromYarn(task.taskId),
     // Hack since getFinishTime needs isFinished to be true and that doesn't happen till after the transition.
        task.getFinishTime(taId),
        TypeConverter.fromYarn(task.getType()),
        errorSb.toString(),
        taskState.toString(),
        taId == null ? null : TypeConverter.fromYarn(taId));
    return taskFailedEvent;
  }
  */

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
    TaskFinishedEvent finishEvt = new TaskFinishedEvent(taskId,
        getVertex().getName(), getLaunchTime(), clock.getTime(),
        successfulAttempt,
        TaskState.SUCCEEDED, "", getCounters());
    this.appContext.getHistoryHandler().handle(
        new DAGHistoryEvent(taskId.getVertexID().getDAGId(), finishEvt));
  }

  protected void logJobHistoryTaskFailedEvent(TaskState finalState) {
    TaskFinishedEvent finishEvt = new TaskFinishedEvent(taskId,
        getVertex().getName(), getLaunchTime(), clock.getTime(), null,
        finalState, 
        StringUtils.join(getDiagnostics(), LINE_SEPARATOR),
        getCounters());
    this.appContext.getHistoryHandler().handle(
        new DAGHistoryEvent(taskId.getVertexID().getDAGId(), finishEvt));
  }

  private void addDiagnosticInfo(String diag) {
    if (diag != null && !diag.equals("")) {
      diagnostics.add(diag);
    }
  }
  
  private static class InitialScheduleTransition
    implements SingleArcTransition<TaskImpl, TaskEvent> {

    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      task.addAndScheduleAttempt();
      task.scheduledTime = task.clock.getTime();
      task.logJobHistoryTaskStartedEvent();
      task.historyTaskStartGenerated = true;
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
      task.addAndScheduleAttempt();
    }
  }

  private static class AttemptProcessingCompleteTransition implements
      SingleArcTransition<TaskImpl, TaskEvent> {

    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TaskEventTAUpdate taEvent = (TaskEventTAUpdate) event;
      TezTaskAttemptID attemptId = taEvent.getTaskAttemptID();

      if (task.outputConsumableAttempt == null) {
        task.sendTaskAttemptCompletionEvent(attemptId,
            TaskAttemptStateInternal.SUCCEEDED);
        task.outputConsumableAttempt = attemptId;
        task.outputConsumableAttemptSuccessSent = true;
        if (LOG.isDebugEnabled()) {
          LOG.debug("TezTaskAttemptID: " + attemptId
              + " set as the OUTPUT_READY attempt");
        }
      } else {
        // Nothing to do. This task will eventually be told to die, or will be
        // killed.
        if (LOG.isDebugEnabled()) {
          LOG.debug("TezTaskAttemptID: "
              + attemptId + " reporting OUTPUT_READY."
              + " Will be asked to die since another attempt "
              + task.outputConsumableAttempt + " already has output ready");
        }
        task.eventHandler.handle(new TaskAttemptEventKillRequest(attemptId,
            "Alternate attemptId already serving output"));
      }

    }
  }

  private static class AttemptSucceededTransition
      implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TezTaskAttemptID successTaId = ((TaskEventTAUpdate) event).getTaskAttemptID();
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
      task.finishedAttempts++;
      --task.numberUncompletedAttempts;
      task.successfulAttempt = successTaId;
      task.eventHandler.handle(new VertexEventTaskCompleted(
          task.taskId, TaskState.SUCCEEDED));
      LOG.info("Task succeeded with attempt " + task.successfulAttempt);
      if (task.historyTaskStartGenerated) {
        task.logJobHistoryTaskFinishedEvent();
      }

      // issue kill to all other attempts
      for (TaskAttempt attempt : task.attempts.values()) {
        if (attempt.getID() != task.successfulAttempt &&
            // This is okay because it can only talk us out of sending a
            //  TA_KILL message to an attempt that doesn't need one for
            //  other reasons.
            !attempt.isFinished()) {
          LOG.info("Issuing kill to other attempt " + attempt.getID());
          task.eventHandler.handle(new TaskAttemptEventKillRequest(attempt
              .getID(), "Alternate attempt succeeded"));
        }
      }
      // send notification to DAG scheduler
      task.eventHandler.handle(new DAGEventSchedulerUpdate(
          DAGEventSchedulerUpdate.UpdateType.TA_SUCCEEDED, task.attempts
              .get(task.successfulAttempt)));
      task.finished(TaskStateInternal.SUCCEEDED);
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
      task.finishedAttempts++;
      // we KillWaitAttemptCompletedTransitionready have a spare
      if (--task.numberUncompletedAttempts == 0
          && task.successfulAttempt == null) {
        task.addAndScheduleAttempt();
      }
    }
  }

  private static class RecoverTransition implements
      MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal> {

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent taskEvent) {
      if (taskEvent instanceof TaskEventRecoverTask) {
        TaskEventRecoverTask taskEventRecoverTask =
            (TaskEventRecoverTask) taskEvent;
        if (taskEventRecoverTask.getDesiredState() != null
            && !taskEventRecoverTask.recoverData()) {
          // TODO recover attempts if desired state is given?
          // History may not have all data.
          switch (taskEventRecoverTask.getDesiredState()) {
            case SUCCEEDED:
              return TaskStateInternal.SUCCEEDED;
            case FAILED:
              return TaskStateInternal.FAILED;
            case KILLED:
              return TaskStateInternal.KILLED;
          }
        }
      }

      TaskStateInternal endState = TaskStateInternal.NEW;
      if (task.attempts != null) {
        for (TaskAttempt taskAttempt : task.attempts.values()) {
          task.eventHandler.handle(new TaskAttemptEvent(
              taskAttempt.getID(), TaskAttemptEventType.TA_RECOVER));
        }
      }
      LOG.info("Trying to recover task"
          + ", taskId=" + task.getTaskId()
          + ", recoveredState=" + task.recoveredState);
      switch(task.recoveredState) {
        case NEW:
          // Nothing to do until the vertex schedules this task
          endState = TaskStateInternal.NEW;
          break;
        case SCHEDULED:
        case RUNNING:
        case SUCCEEDED:
          if (task.successfulAttempt != null) {
            //Found successful attempt
            //Recover data
            boolean recoveredData = true;
            if (task.getVertex().getOutputCommitters() != null
                && !task.getVertex().getOutputCommitters().isEmpty()) {
              for (Entry<String, OutputCommitter> entry
                  : task.getVertex().getOutputCommitters().entrySet()) {
                LOG.info("Recovering data for task from previous DAG attempt"
                    + ", taskId=" + task.getTaskId()
                    + ", output=" + entry.getKey());
                OutputCommitter committer = entry.getValue();
                if (!committer.isTaskRecoverySupported()) {
                  LOG.info("Task recovery not supported by committer"
                      + ", failing task attempt"
                      + ", taskId=" + task.getTaskId()
                      + ", attemptId=" + task.successfulAttempt
                      + ", output=" + entry.getKey());
                  recoveredData = false;
                  break;
                }
                try {
                  committer.recoverTask(task.getTaskId().getId(),
                      task.appContext.getApplicationAttemptId().getAttemptId()-1);
                } catch (Exception e) {
                  LOG.warn("Task recovery failed by committer"
                      + ", taskId=" + task.getTaskId()
                      + ", attemptId=" + task.successfulAttempt
                      + ", output=" + entry.getKey(), e);
                  recoveredData = false;
                  break;
                }
              }
            }
            if (!recoveredData) {
              task.successfulAttempt = null;
            } else {
              LOG.info("Recovered a successful attempt"
                  + ", taskAttemptId=" + task.successfulAttempt.toString());
              task.logJobHistoryTaskFinishedEvent();
              task.eventHandler.handle(
                  new VertexEventTaskCompleted(task.taskId,
                      getExternalState(TaskStateInternal.SUCCEEDED)));
              task.eventHandler.handle(
                  new VertexEventTaskAttemptCompleted(
                      task.successfulAttempt, TaskAttemptStateInternal.SUCCEEDED));
              endState = TaskStateInternal.SUCCEEDED;
              break;
            }
          }

          if (endState != TaskStateInternal.SUCCEEDED &&
              task.failedAttempts >= task.maxFailedAttempts) {
            // Exceeded max attempts
            task.finished(TaskStateInternal.FAILED);
            endState = TaskStateInternal.FAILED;
            break;
          }

          // no successful attempt and all attempts completed
          // schedule a new one
          // If any incomplete, the running attempt will moved to failed and its
          // update will trigger a new attempt if possible
          if (task.attempts.size() == task.finishedAttempts) {
            task.addAndScheduleAttempt();
          }
          endState = TaskStateInternal.RUNNING;
          break;
        case KILLED:
          // Nothing to do
          // Inform vertex
          task.eventHandler.handle(
              new VertexEventTaskCompleted(task.taskId,
                  getExternalState(TaskStateInternal.KILLED)));
          endState  = TaskStateInternal.KILLED;
          break;
        case FAILED:
          // Nothing to do
          // Inform vertex
          task.eventHandler.handle(
              new VertexEventTaskCompleted(task.taskId,
                  getExternalState(TaskStateInternal.FAILED)));

          endState = TaskStateInternal.FAILED;
          break;
      }

      return endState;
    }
  }


  private static class KillWaitAttemptCompletedTransition implements
      MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal> {

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      task.handleTaskAttemptCompletion(
          ((TaskEventTAUpdate) event).getTaskAttemptID(),
          TaskAttemptStateInternal.KILLED);
      task.finishedAttempts++;
      // check whether all attempts are finished
      if (task.finishedAttempts == task.attempts.size()) {
        if (task.historyTaskStartGenerated) {
          task.logJobHistoryTaskFailedEvent(getExternalState(TaskStateInternal.KILLED));
        } else {
          LOG.debug("Not generating HistoryFinish event since start event not" +
          		" generated for task: " + task.getTaskId());
        }

        task.eventHandler.handle(
            new VertexEventTaskCompleted(
                task.taskId, getExternalState(TaskStateInternal.KILLED)));
        return TaskStateInternal.KILLED;
      }
      return task.getInternalState();
    }
  }

  private static class AttemptFailedTransition implements
    MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal> {

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      task.failedAttempts++;
      TaskEventTAUpdate castEvent = (TaskEventTAUpdate) event;
      task.addDiagnosticInfo("TaskAttempt " + castEvent.getTaskAttemptID().getId() + " failed,"
          + " info=" + task.getAttempt(castEvent.getTaskAttemptID()).getDiagnostics());
      if (task.commitAttempt != null &&
          castEvent.getTaskAttemptID().equals(task.commitAttempt)) {
        task.commitAttempt = null;
      }
      if (castEvent.getTaskAttemptID().equals(task.outputConsumableAttempt)) {
        task.outputConsumableAttempt = null;
        task.handleTaskAttemptCompletion(castEvent.getTaskAttemptID(),
            TaskAttemptStateInternal.FAILED);
      }

      // The attempt would have informed the scheduler about it's failure

      task.finishedAttempts++;
      if (task.failedAttempts < task.maxFailedAttempts) {
        task.handleTaskAttemptCompletion(
            ((TaskEventTAUpdate) event).getTaskAttemptID(),
            TaskAttemptStateInternal.FAILED);
        // we don't need a new event if we already have a spare
        if (--task.numberUncompletedAttempts == 0
            && task.successfulAttempt == null) {
          LOG.info("Scheduling new attempt for task: " + task.getTaskId()
              + ", currentFailedAttempts: " + task.failedAttempts + ", maxFailedAttempts: "
              + task.maxFailedAttempts);
          task.addAndScheduleAttempt();
        }
      } else {
        LOG.info("Failing task: " + task.getTaskId()
            + ", currentFailedAttempts: " + task.failedAttempts + ", maxFailedAttempts: "
            + task.maxFailedAttempts);
        task.handleTaskAttemptCompletion(
            ((TaskEventTAUpdate) event).getTaskAttemptID(),
            TaskAttemptStateInternal.FAILED);

        if (task.historyTaskStartGenerated) {
          task.logJobHistoryTaskFailedEvent(TaskState.FAILED);
        } else {
          LOG.debug("Not generating HistoryFinish event since start event not" +
          		" generated for task: " + task.getTaskId());
        }
        task.eventHandler.handle(
            new VertexEventTaskCompleted(task.taskId, TaskState.FAILED));
        return task.finished(TaskStateInternal.FAILED);
      }
      return getDefaultState(task);
    }

    protected TaskStateInternal getDefaultState(TaskImpl task) {
      return task.getInternalState();
    }
  }

  private static class TaskRetroactiveFailureTransition
      extends AttemptFailedTransition {

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      if (task.leafVertex) {
        LOG.error("Unexpected event for task of leaf vertex " + event.getType() + ", taskId: "
            + task.getTaskId());
        task.internalError(event.getType());
      }

      TaskEventTAUpdate castEvent = (TaskEventTAUpdate) event;
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
                  containerId, failedAttemptId, true));
        }
      }
      
      if (task.getInternalState() == TaskStateInternal.SUCCEEDED &&
          !failedAttemptId.equals(task.successfulAttempt)) {
        // don't allow a different task attempt to override a previous
        // succeeded state
        return TaskStateInternal.SUCCEEDED;
      }

      // super.transition is mostly coded for the case where an
      //  UNcompleted task failed.  When a COMPLETED task retroactively
      //  fails, we have to let AttemptFailedTransition.transition
      //  believe that there's no redundancy.
      unSucceed(task);

      // fake values for code for super.transition
      ++task.numberUncompletedAttempts;
      task.finishedAttempts--;
      TaskStateInternal returnState = super.transition(task, event);

      if (returnState == TaskStateInternal.SCHEDULED) {
        // tell the dag about the rescheduling
        task.eventHandler.handle(new VertexEventTaskReschedule(task.taskId));
      }

      return returnState;
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
      // verify that this occurs only for map task
      // TODO: consider moving it to MapTaskImpl
      if (task.leafVertex) {
        LOG.error("Unexpected event for task of leaf vertex " + event.getType());
        task.internalError(event.getType());
      }

      TaskEventTAUpdate attemptEvent = (TaskEventTAUpdate) event;
      TezTaskAttemptID attemptId = attemptEvent.getTaskAttemptID();
      if(task.successfulAttempt == attemptId) {
        // successful attempt is now killed. reschedule
        // tell the job about the rescheduling
        unSucceed(task);
        task.handleTaskAttemptCompletion(
            attemptId,
            TaskAttemptStateInternal.KILLED);
        task.eventHandler.handle(new VertexEventTaskReschedule(task.taskId));
        // typically we are here because this map task was run on a bad node and
        // we want to reschedule it on a different node.
        // Depending on whether there are previous failed attempts or not this
        // can SCHEDULE or RESCHEDULE the container allocate request. If this
        // SCHEDULE's then the dataLocal hosts of this taskAttempt will be used
        // from the map splitInfo. So the bad node might be sent as a location
        // to the RM. But the RM would ignore that just like it would ignore
        // currently pending container requests affinitized to bad nodes.
        task.addAndScheduleAttempt();
        return TaskStateInternal.SCHEDULED;
      } else {
        // nothing to do
        return TaskStateInternal.SUCCEEDED;
      }
    }
  }

  private static class KillNewTransition
    implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TaskEventTermination terminateEvent = (TaskEventTermination)event;
      task.addDiagnosticInfo(terminateEvent.getDiagnosticInfo());
      if (task.historyTaskStartGenerated) {
        task.logJobHistoryTaskFailedEvent(TaskState.KILLED);
      } else {
        LOG.debug("Not generating HistoryFinish event since start event not" +
        		" generated for task: " + task.getTaskId());
      }

      task.eventHandler.handle(
          new VertexEventTaskCompleted(task.taskId, TaskState.KILLED));
      // TODO Metrics
      //task.metrics.endWaitingTask(task);
    }
  }

  private void killUnfinishedAttempt(TaskAttempt attempt, String logMsg) {
    if (commitAttempt != null && commitAttempt.equals(attempt)) {
      LOG.info("Removing commit attempt: " + commitAttempt);
      commitAttempt = null;
    }
    if (attempt != null && !attempt.isFinished()) {
      eventHandler.handle(new TaskAttemptEventKillRequest(attempt.getID(),
          logMsg));
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

  private static class KillTransition
    implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TaskEventTermination terminateEvent = (TaskEventTermination)event;
      task.addDiagnosticInfo(terminateEvent.getDiagnosticInfo());
      // issue kill to all non finished attempts
      for (TaskAttempt attempt : task.attempts.values()) {
        task.killUnfinishedAttempt
            (attempt, "Task KILL is received. Killing attempt!");
      }
      task.numberUncompletedAttempts = 0;
      
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
}
