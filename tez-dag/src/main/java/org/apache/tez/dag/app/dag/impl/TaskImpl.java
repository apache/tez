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

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskReport;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.TaskStateInternal;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventDiagnosticsUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventKillRequest;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventTAUpdate;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEventTaskAttemptCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskReschedule;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.engine.common.security.JobTokenIdentifier;
import org.apache.tez.engine.records.TezDependentTaskCompletionEvent;

import com.google.common.annotations.VisibleForTesting;

/**
 * Implementation of Task interface.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TaskImpl implements Task, EventHandler<TaskEvent> {

  private static final Log LOG = LogFactory.getLog(TaskImpl.class);

  protected final TezConfiguration conf;
  protected final int partition;
  protected final TaskAttemptListener taskAttemptListener;
  protected final TaskHeartbeatHandler taskHeartbeatHandler;
  protected final EventHandler eventHandler;
  private final TezTaskID taskId;
  private Map<TezTaskAttemptID, TaskAttempt> attempts;
  private final int maxAttempts;
  protected final Clock clock;
  private final Lock readLock;
  private final Lock writeLock;
  // TODO Metrics
  //private final MRAppMetrics metrics;
  protected final AppContext appContext;
  private long scheduledTime;

  protected boolean encryptedShuffle;
  protected Credentials credentials;
  protected Token<JobTokenIdentifier> jobToken;
  protected String processorName;
  protected TaskLocationHint locationHint;
  protected Resource taskResource;
  protected Map<String, LocalResource> localResources;
  protected Map<String, String> environment;

  // counts the number of attempts that are either running or in a state where
  //  they will come to be running when they get a Container
  private int numberUncompletedAttempts = 0;

  private boolean historyTaskStartGenerated = false;

  private static final SingleArcTransition<TaskImpl, TaskEvent>
     ATTEMPT_KILLED_TRANSITION = new AttemptKilledTransition();
  private static final SingleArcTransition<TaskImpl, TaskEvent>
     KILL_TRANSITION = new KillTransition();

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
        TaskEventType.T_KILL, new KillNewTransition())

    // Transitions from SCHEDULED state
      //when the first attempt is launched, the task state is set to RUNNING
     .addTransition(TaskStateInternal.SCHEDULED, TaskStateInternal.RUNNING,
         TaskEventType.T_ATTEMPT_LAUNCHED, new LaunchTransition())
     .addTransition(TaskStateInternal.SCHEDULED, TaskStateInternal.KILL_WAIT,
         TaskEventType.T_KILL, KILL_TRANSITION)
     .addTransition(TaskStateInternal.SCHEDULED, TaskStateInternal.SCHEDULED,
         TaskEventType.T_ATTEMPT_KILLED, ATTEMPT_KILLED_TRANSITION)
     .addTransition(TaskStateInternal.SCHEDULED,
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.FAILED),
        TaskEventType.T_ATTEMPT_FAILED,
        new AttemptFailedTransition())

    // Transitions from RUNNING state
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING,
        TaskEventType.T_ATTEMPT_LAUNCHED) //more attempts may start later
    // This is an optional event.
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING,
        TaskEventType.T_ATTEMPT_OUTPUT_CONSUMABLE,
        new AttemptProcessingCompleteTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING,
        TaskEventType.T_ATTEMPT_COMMIT_PENDING,
        new AttemptCommitPendingTransition())
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
        TaskEventType.T_KILL, KILL_TRANSITION)

    // Transitions from KILL_WAIT state
    .addTransition(TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskStateInternal.KILL_WAIT, TaskStateInternal.KILLED),
        TaskEventType.T_ATTEMPT_KILLED,
        new KillWaitAttemptKilledTransition())
    // Ignore-able transitions.
    .addTransition(
        TaskStateInternal.KILL_WAIT,
        TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskEventType.T_KILL,
            TaskEventType.T_ATTEMPT_LAUNCHED,
            TaskEventType.T_ATTEMPT_OUTPUT_CONSUMABLE,
            TaskEventType.T_ATTEMPT_COMMIT_PENDING,
            TaskEventType.T_ATTEMPT_FAILED,
            TaskEventType.T_ATTEMPT_SUCCEEDED,
            TaskEventType.T_ADD_SPEC_ATTEMPT))

    // Transitions from SUCCEEDED state
      // TODO May required different handling if OUTPUT_CONSUMABLE is one of
      // the stages. i.e. Task would only SUCCEED after all output consumed.
    .addTransition(TaskStateInternal.SUCCEEDED, //only possible for map tasks
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.SUCCEEDED, TaskStateInternal.FAILED),
        TaskEventType.T_ATTEMPT_FAILED, new MapRetroactiveFailureTransition())
    .addTransition(TaskStateInternal.SUCCEEDED, //only possible for map tasks
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.SUCCEEDED),
        TaskEventType.T_ATTEMPT_KILLED, new MapRetroactiveKilledTransition())
    // Ignore-able transitions.
    .addTransition(
        TaskStateInternal.SUCCEEDED, TaskStateInternal.SUCCEEDED,
        EnumSet.of(TaskEventType.T_ADD_SPEC_ATTEMPT,
            TaskEventType.T_ATTEMPT_LAUNCHED))

    // Transitions from FAILED state
    .addTransition(TaskStateInternal.FAILED, TaskStateInternal.FAILED,
        EnumSet.of(TaskEventType.T_KILL,
                   TaskEventType.T_ADD_SPEC_ATTEMPT))

    // Transitions from KILLED state
    .addTransition(TaskStateInternal.KILLED, TaskStateInternal.KILLED,
        EnumSet.of(TaskEventType.T_KILL,
                   TaskEventType.T_ADD_SPEC_ATTEMPT))

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

  private TezTaskAttemptID successfulAttempt;

  private int failedAttempts;
  private int finishedAttempts;//finish are total of success, failed and killed

  private final boolean leafVertex;

  protected String javaOpts;

  @Override
  public TaskState getState() {
    readLock.lock();
    try {
      return getExternalState(getInternalState());
    } finally {
      readLock.unlock();
    }
  }

  public TaskImpl(TezVertexID vertexId, int partition,
      EventHandler eventHandler, TezConfiguration conf,
      TaskAttemptListener taskAttemptListener,
      Token<JobTokenIdentifier> jobToken,
      Credentials credentials, Clock clock,
      // TODO Recovery
      //Map<TezTaskID, TaskInfo> completedTasksFromPreviousRun,
      //int startCount,
      // TODO Metrics
      //MRAppMetrics metrics,
      TaskHeartbeatHandler thh, AppContext appContext,
      String processorName,
      boolean leafVertex, TaskLocationHint locationHint, Resource resource,
      Map<String, LocalResource> localResources,
      Map<String, String> environment,
      String javaOpts) {
    this.conf = conf;
    this.clock = clock;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
    this.attempts = Collections.emptyMap();
    // TODO TEZ-47 get from conf or API
    maxAttempts = this.conf.getInt(TezConfiguration.DAG_MAX_TASK_ATTEMPTS,
                              TezConfiguration.DAG_MAX_TASK_ATTEMPTS_DEFAULT);
    taskId = new TezTaskID(vertexId, partition);
    this.partition = partition;
    this.taskAttemptListener = taskAttemptListener;
    this.taskHeartbeatHandler = thh;
    this.eventHandler = eventHandler;
    this.credentials = credentials;
    this.jobToken = jobToken;
    // TODO Metrics
    //this.metrics = metrics;
    this.appContext = appContext;
    // TODO Security
    this.encryptedShuffle = false;
    //conf.getBoolean(MRConfig.SHUFFLE_SSL_ENABLED_KEY,
     //                                       MRConfig.SHUFFLE_SSL_ENABLED_DEFAULT);
    this.processorName = processorName;

    this.leafVertex = leafVertex;
    this.locationHint = locationHint;
    this.taskResource = resource;
    this.localResources = localResources;
    this.environment = environment;
    this.javaOpts = javaOpts;
    // TODO: Recovery
    /*
    // See if this is from a previous generation.
    if (completedTasksFromPreviousRun != null
        && completedTasksFromPreviousRun.containsKey(taskId)) {
      // This task has TaskAttempts from previous generation. We have to replay
      // them.
      LOG.info("Task is from previous run " + taskId);
      TaskInfo taskInfo = completedTasksFromPreviousRun.get(taskId);
      Map<TaskAttemptID, TaskAttemptInfo> allAttempts =
          taskInfo.getAllTaskAttempts();
      taskAttemptsFromPreviousGeneration = new ArrayList<TaskAttemptInfo>();
      taskAttemptsFromPreviousGeneration.addAll(allAttempts.values());
      Collections.sort(taskAttemptsFromPreviousGeneration,
        RECOVERED_ATTEMPTS_COMPARATOR);
    }

    if (taskAttemptsFromPreviousGeneration.isEmpty()) {
      // All the previous attempts are exhausted, now start with a new
      // generation.

      // All the new TaskAttemptIDs are generated based on MR
      // ApplicationAttemptID so that attempts from previous lives don't
      // over-step the current one. This assumes that a task won't have more
      // than 1000 attempts in its single generation, which is very reasonable.
      // Someone is nuts if he/she thinks he/she can live with 1000 TaskAttempts
      // and requires serious medical attention.
      nextAttemptNumber = (startCount - 1) * 1000;
    } else {
      // There are still some TaskAttempts from previous generation, use them
      nextAttemptNumber =
          taskAttemptsFromPreviousGeneration.remove(0).getAttemptId().getId();
    }
     */
    // This "this leak" is okay because the retained pointer is in an
    //  instance variable.
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
    return appContext.getDAG().getVertex(taskId.getVertexID());
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
    // TODO TEZPB This is broken. Records will not work without the PBImpl, which
    // is in a different package.
    TaskReport report = Records.newRecord(TaskReport.class);
    readLock.lock();
    try {
      report.setTaskId(taskId);
      report.setStartTime(getLaunchTime());
      report.setFinishTime(getFinishTime());
      report.setTaskState(getState());
      report.setProgress(getProgress());

      for (TaskAttempt attempt : attempts.values()) {
        if (TaskAttemptState.RUNNING.equals(attempt.getState())) {
          report.addRunningAttempt(attempt.getID());
        }
      }

      report.setSuccessfulAttempt(successfulAttempt);

      for (TaskAttempt att : attempts.values()) {
        String prefix = "AttemptID:" + att.getID() + " Info:";
        for (CharSequence cs : att.getDiagnostics()) {
          report.addDiagnostics(prefix + cs);

        }
      }

      // Add a copy of counters as the last step so that their lifetime on heap
      // is as small as possible.
      report.setCounters(getCounters());

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
    readLock.lock();
    boolean canCommit = false;
    try {
      if (commitAttempt != null) {
        canCommit = taskAttemptID.equals(commitAttempt);
        LOG.info("Result of canCommit for " + taskAttemptID + ":" + canCommit);
      }
    } finally {
      readLock.unlock();
    }
    return canCommit;
  }

  @Override
  public boolean needsWaitAfterOutputConsumable() {
    if (processorName.contains("InitialTaskWithInMemSort")) {
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
        taskAttemptListener, 0, conf,
        jobToken, credentials, clock, taskHeartbeatHandler,
        appContext, processorName, locationHint, taskResource,
        localResources, environment, javaOpts, (failedAttempts>0));
  }

  protected TaskAttempt getSuccessfulAttempt() {
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
            = new LinkedHashMap<TezTaskAttemptID, TaskAttempt>(maxAttempts);
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
    LOG.error("Invalid event " + type + " on Task " + this.taskId);
    eventHandler.handle(new DAGEventDiagnosticsUpdate(
        this.taskId.getVertexID().getDAGId(), "Invalid event " + type +
        " on Task " + this.taskId));
    eventHandler.handle(new DAGEvent(this.taskId.getVertexID().getDAGId(),
        DAGEventType.INTERNAL_ERROR));
  }

  private void sendTaskAttemptCompletionEvent(TezTaskAttemptID attemptId,
      TezDependentTaskCompletionEvent.Status status) {
    TaskAttempt attempt = attempts.get(attemptId);
    // raise the completion event only if the container is assigned
    // to nextAttemptNumber
    if (needsWaitAfterOutputConsumable()) {
      // An event may have been sent out during the OUTPUT_READY state itself.
      // Make sure the same event is not being sent out again.
      if (attemptId == outputConsumableAttempt
          && status == TezDependentTaskCompletionEvent.Status.SUCCEEDED) {
        if (outputConsumableAttemptSuccessSent) {
          return;
        }
      }
    }
    if (attempt.getNodeHttpAddress() != null) {

      String scheme = (encryptedShuffle) ? "https://" : "http://";
      String url = scheme
          + attempt.getNodeHttpAddress().split(":")[0] + ":"
          + attempt.getShufflePort();



      int runTime = 0;
      if (attempt.getFinishTime() != 0 && attempt.getLaunchTime() != 0)
        runTime = (int) (attempt.getFinishTime() - attempt.getLaunchTime());

      TezDependentTaskCompletionEvent tce = new TezDependentTaskCompletionEvent(
          -1, attemptId, status, url, runTime);

      // raise the event to job so that it adds the completion event to its
      // data structures
      eventHandler.handle(new VertexEventTaskAttemptCompleted(tce));
    }
  }

  // always called inside a transition, in turn inside the Write Lock
  private void handleTaskAttemptCompletion(TezTaskAttemptID attemptId,
      TezDependentTaskCompletionEvent.Status status) {
    this.sendTaskAttemptCompletionEvent(attemptId, status);
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
    this.eventHandler.handle(new DAGHistoryEvent(
        taskId.getVertexID().getDAGId(), startEvt));
  }

  protected void logJobHistoryTaskFinishedEvent() {
    // FIXME need to handle getting finish time as this function
    // is called from within a transition
    TaskFinishedEvent finishEvt = new TaskFinishedEvent(taskId,
        getVertex().getName(), getLaunchTime(), clock.getTime(),
        TaskState.SUCCEEDED, getCounters());
    this.eventHandler.handle(new DAGHistoryEvent(
        taskId.getVertexID().getDAGId(), finishEvt));
  }

  protected void logJobHistoryTaskFailedEvent(TaskState finalState) {
    TaskFinishedEvent finishEvt = new TaskFinishedEvent(taskId,
        getVertex().getName(), getLaunchTime(), clock.getTime(),
        finalState, getCounters());
    this.eventHandler.handle(new DAGHistoryEvent(
        taskId.getVertexID().getDAGId(), finishEvt));
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
            TezDependentTaskCompletionEvent.Status.SUCCEEDED);
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

  private static class AttemptCommitPendingTransition implements
      SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TaskEventTAUpdate ev = (TaskEventTAUpdate) event;
      // The nextAttemptNumber is commit pending, decide on set the
      // commitAttempt
      TezTaskAttemptID attemptID = ev.getTaskAttemptID();
      if (task.commitAttempt == null) {
        // TODO: validate attemptID
        task.commitAttempt = attemptID;
        LOG.info(attemptID + " given a go for committing the task output.");
      } else {
        // Don't think this can be a pluggable decision, so simply raise an
        // event for the TaskAttempt to delete its output.
        LOG.info(task.commitAttempt
            + " already given a go for committing the task output, so killing "
            + attemptID);
        task.eventHandler.handle(new TaskAttemptEventKillRequest(attemptID,
            "Output being committed by alternate attemptId."));
      }
    }
  }

  private static class AttemptSucceededTransition
      implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      task.handleTaskAttemptCompletion(
          ((TaskEventTAUpdate) event).getTaskAttemptID(),
          TezDependentTaskCompletionEvent.Status.SUCCEEDED);
      task.finishedAttempts++;
      --task.numberUncompletedAttempts;
      task.successfulAttempt = ((TaskEventTAUpdate) event).getTaskAttemptID();
      task.eventHandler.handle(new VertexEventTaskCompleted(
          task.taskId, TaskState.SUCCEEDED));
      LOG.info("Task succeeded with attempt " + task.successfulAttempt);
      // issue kill to all other attempts
      if (task.historyTaskStartGenerated) {
        task.logJobHistoryTaskFinishedEvent();
      }

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
      task.handleTaskAttemptCompletion(
          ((TaskEventTAUpdate) event).getTaskAttemptID(),
          TezDependentTaskCompletionEvent.Status.KILLED);
      task.finishedAttempts++;
      --task.numberUncompletedAttempts;
      if (task.successfulAttempt == null) {
        task.addAndScheduleAttempt();
      }
    }
  }


  private static class KillWaitAttemptKilledTransition implements
      MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal> {

    protected TaskStateInternal finalState = TaskStateInternal.KILLED;

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      task.handleTaskAttemptCompletion(
          ((TaskEventTAUpdate) event).getTaskAttemptID(),
          TezDependentTaskCompletionEvent.Status.KILLED);
      task.finishedAttempts++;
      // check whether all attempts are finished
      if (task.finishedAttempts == task.attempts.size()) {
        if (task.historyTaskStartGenerated) {
          task.logJobHistoryTaskFailedEvent(getExternalState(finalState));
        } else {
          LOG.debug("Not generating HistoryFinish event since start event not" +
          		" generated for task: " + task.getTaskId());
        }

        task.eventHandler.handle(
            new VertexEventTaskCompleted(task.taskId, getExternalState(finalState)));
        return finalState;
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
      if (castEvent.getTaskAttemptID().equals(task.commitAttempt)) {
        task.commitAttempt = null;
      }
      if (castEvent.getTaskAttemptID().equals(task.outputConsumableAttempt)) {
        task.outputConsumableAttempt = null;
        task.handleTaskAttemptCompletion(castEvent.getTaskAttemptID(),
            TezDependentTaskCompletionEvent.Status.FAILED);
      }

      // The attempt would have informed the scheduler about it's failure

      task.finishedAttempts++;
      if (task.failedAttempts < task.maxAttempts) {
        task.handleTaskAttemptCompletion(
            ((TaskEventTAUpdate) event).getTaskAttemptID(),
            TezDependentTaskCompletionEvent.Status.FAILED);
        // we don't need a new event if we already have a spare
        if (--task.numberUncompletedAttempts == 0
            && task.successfulAttempt == null) {
          task.addAndScheduleAttempt();
        }
      } else {
        task.handleTaskAttemptCompletion(
            ((TaskEventTAUpdate) event).getTaskAttemptID(),
            TezDependentTaskCompletionEvent.Status.TIPFAILED);

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

  private static class MapRetroactiveFailureTransition
      extends AttemptFailedTransition {

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      if (event instanceof TaskEventTAUpdate) {
        TaskEventTAUpdate castEvent = (TaskEventTAUpdate) event;
        if (task.getInternalState() == TaskStateInternal.SUCCEEDED &&
            !castEvent.getTaskAttemptID().equals(task.successfulAttempt)) {
          // don't allow a different task attempt to override a previous
          // succeeded state
          return TaskStateInternal.SUCCEEDED;
        }
      }

      if (task.leafVertex) {
        LOG.error("Unexpected event for task of leaf vertex " + event.getType());
        task.internalError(event.getType());
      }

      // tell the job about the rescheduling
      task.eventHandler.handle(
          new VertexEventTaskReschedule(task.taskId));
      // super.transition is mostly coded for the case where an
      //  UNcompleted task failed.  When a COMPLETED task retroactively
      //  fails, we have to let AttemptFailedTransition.transition
      //  believe that there's no redundancy.
      unSucceed(task);
      // fake increase in Uncomplete attempts for super.transition
      ++task.numberUncompletedAttempts;
      return super.transition(task, event);
    }

    @Override
    protected TaskStateInternal getDefaultState(TaskImpl task) {
      return TaskStateInternal.SCHEDULED;
    }
  }

  private static class MapRetroactiveKilledTransition implements
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
            TezDependentTaskCompletionEvent.Status.KILLED);
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
    if (attempt != null && !attempt.isFinished()) {
      eventHandler.handle(new TaskAttemptEventKillRequest(attempt.getID(),
          logMsg));
    }
  }

  private static class KillTransition
    implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
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
