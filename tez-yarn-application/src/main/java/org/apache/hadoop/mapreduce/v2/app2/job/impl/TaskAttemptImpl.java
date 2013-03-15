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

package org.apache.hadoop.mapreduce.v2.app2.job.impl;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.WrappedProgressSplitsBlock;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.MapAttemptFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.ReduceAttemptFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptStartedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptUnsuccessfulCompletionEvent;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app2.TaskHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.app2.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app2.job.TaskAttemptStateInternal;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEventCounterUpdate;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEventDiagnosticsUpdate;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEventTaskAttemptFetchFailure;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventDiagnosticsUpdate;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventContainerTerminated;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventContainerTerminating;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventNodeFailed;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventOutputConsumable;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventStartedRemotely;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventSchedule;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventStatusUpdate;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventStatusUpdate.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskEventTAUpdate;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerEventTAEnded;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerTALaunchRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.speculate.SpeculatorEvent;
import org.apache.hadoop.mapreduce.v2.app2.taskclean.TaskCleanupEvent;
import org.apache.hadoop.mapreduce.v2.common.DiagnosableEvent;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.mapreduce.task.impl.MRTaskContext;

import com.google.common.annotations.VisibleForTesting;

public abstract class TaskAttemptImpl implements TaskAttempt,
    EventHandler<TaskAttemptEvent> {
  
  // TODO Ensure MAPREDUCE-4457 is factored in. Also MAPREDUCE-4068.
  
  // TODO Consider TAL registartion in the TaskAttempt instead of the container.
  
  private static final Log LOG = LogFactory.getLog(TaskAttemptImpl.class);
  private static final String LINE_SEPARATOR = System
      .getProperty("line.separator");
  
  static final Counters EMPTY_COUNTERS = new Counters();
  private static final long MEMORY_SPLITS_RESOLUTION = 1024; //TODO Make configurable?

  protected final JobConf conf;
  protected final Path jobFile;
  protected final int partition;
  @SuppressWarnings("rawtypes")
  protected EventHandler eventHandler;
  private final TaskAttemptId attemptId;
  private final TaskId taskId;
  private final JobId jobId;
  private final Clock clock;
//  private final TaskAttemptListener taskAttemptListener;
  private final OutputCommitter committer;
  private final Resource resourceCapability;
  private final String[] dataLocalHosts;
  private final List<String> diagnostics = new ArrayList<String>();
  private final Lock readLock;
  private final Lock writeLock;
  protected final AppContext appContext;
  private final TaskHeartbeatHandler taskHeartbeatHandler;
  private Credentials credentials;
  protected Token<JobTokenIdentifier> jobToken;
  private long launchTime = 0;
  private long finishTime = 0;
  private WrappedProgressSplitsBlock progressSplitBlock;
  private int shufflePort = -1;
  private String trackerName;
  private int httpPort;
  

  // TODO Can these be replaced by the container object ?
  private ContainerId containerId;
  private NodeId containerNodeId;
  private String nodeHttpAddress;
  private String nodeRackName;

  private TaskAttemptStatus reportedStatus;
  
  private boolean speculatorContainerRequestSent = false;
  protected String tezModuleClassName;
  
  
  
  
  private static SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> 
    DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION =
      new DiagnosticInformationUpdater();
  
  private static SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
    STATUS_UPDATER = new StatusUpdaterTransition();
  
  private final StateMachine
      <TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent> stateMachine;
  
  protected static final FailedTransitionHelper FAILED_HELPER =
      new FailedTransitionHelper();
  
  protected static final KilledTransitionHelper KILLED_HELPER =
      new KilledTransitionHelper();
  
  private static StateMachineFactory
  <TaskAttemptImpl, TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent>
  stateMachineFactory
      = new StateMachineFactory
      <TaskAttemptImpl, TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent>
      (TaskAttemptStateInternal.NEW)
  
        .addTransition(TaskAttemptStateInternal.NEW, TaskAttemptStateInternal.START_WAIT, TaskAttemptEventType.TA_SCHEDULE, new ScheduleTaskattemptTransition())
        .addTransition(TaskAttemptStateInternal.NEW, TaskAttemptStateInternal.NEW, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptStateInternal.NEW, TaskAttemptStateInternal.FAILED, TaskAttemptEventType.TA_FAIL_REQUEST, new TerminateTransition(FAILED_HELPER))
        .addTransition(TaskAttemptStateInternal.NEW, TaskAttemptStateInternal.KILLED, TaskAttemptEventType.TA_KILL_REQUEST, new TerminateTransition(KILLED_HELPER))
        
        .addTransition(TaskAttemptStateInternal.START_WAIT, TaskAttemptStateInternal.RUNNING, TaskAttemptEventType.TA_STARTED_REMOTELY, new StartedTransition())
        .addTransition(TaskAttemptStateInternal.START_WAIT, TaskAttemptStateInternal.START_WAIT, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptStateInternal.START_WAIT, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_FAIL_REQUEST, new TerminatedBeforeRunningTransition(FAILED_HELPER))
        .addTransition(TaskAttemptStateInternal.START_WAIT, TaskAttemptStateInternal.KILL_IN_PROGRESS, TaskAttemptEventType.TA_KILL_REQUEST, new TerminatedBeforeRunningTransition(KILLED_HELPER))
        .addTransition(TaskAttemptStateInternal.START_WAIT, TaskAttemptStateInternal.KILL_IN_PROGRESS, TaskAttemptEventType.TA_NODE_FAILED, new NodeFailedBeforeRunningTransition())
        .addTransition(TaskAttemptStateInternal.START_WAIT, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_CONTAINER_TERMINATING, new ContainerTerminatingBeforeRunningTransition())
        .addTransition(TaskAttemptStateInternal.START_WAIT, TaskAttemptStateInternal.FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATED, new ContainerCompletedBeforeRunningTransition())
        
        .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.RUNNING, TaskAttemptEventType.TA_STATUS_UPDATE, STATUS_UPDATER)
        .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.RUNNING, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptEventType.TA_OUTPUT_CONSUMABLE, new OutputConsumableTransition()) //Optional, may not come in for all tasks.
        .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptEventType.TA_COMMIT_PENDING, new CommitPendingTransition())
        .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.SUCCEEDED, TaskAttemptEventType.TA_DONE, new SucceededTransition())
        .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_FAILED, new TerminatedWhileRunningTransition(FAILED_HELPER))
        .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_TIMED_OUT, new TerminatedWhileRunningTransition(FAILED_HELPER))
        .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_FAIL_REQUEST, new TerminatedWhileRunningTransition(FAILED_HELPER))
        .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.KILL_IN_PROGRESS, TaskAttemptEventType.TA_KILL_REQUEST, new TerminatedWhileRunningTransition(KILLED_HELPER))
        .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.KILL_IN_PROGRESS, TaskAttemptEventType.TA_NODE_FAILED, new TerminatedWhileRunningTransition(KILLED_HELPER))
        .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_CONTAINER_TERMINATING, new TerminatedWhileRunningTransition(FAILED_HELPER))
        .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATED, new ContaienrCompletedWhileRunningTransition())
        
        .addTransition(TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptEventType.TA_STATUS_UPDATE, STATUS_UPDATER)
        .addTransition(TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptEventType.TA_OUTPUT_CONSUMABLE) // Stuck RPC. The client retries in a loop.
        .addTransition(TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptEventType.TA_COMMIT_PENDING, new CommitPendingAtOutputConsumableTransition())
        .addTransition(TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptStateInternal.SUCCEEDED, TaskAttemptEventType.TA_DONE, new SucceededTransition())
        .addTransition(TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_FAILED, new TerminatedWhileRunningTransition(FAILED_HELPER))
        .addTransition(TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_TIMED_OUT, new TerminatedWhileRunningTransition(FAILED_HELPER))
        .addTransition(TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_FAIL_REQUEST, new TerminatedWhileRunningTransition(FAILED_HELPER))
        // TODO CREUSE Ensure TaskCompletionEvents are updated to reflect this. Something needs to go out to the job.
        .addTransition(TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptStateInternal.KILL_IN_PROGRESS, TaskAttemptEventType.TA_KILL_REQUEST, new TerminatedWhileRunningTransition(KILLED_HELPER))
        .addTransition(TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptStateInternal.KILL_IN_PROGRESS, TaskAttemptEventType.TA_NODE_FAILED, new TerminatedWhileRunningTransition(KILLED_HELPER))
        .addTransition(TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_CONTAINER_TERMINATING, new TerminatedWhileRunningTransition(FAILED_HELPER))
        .addTransition(TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptStateInternal.FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATED, new ContainerCompletedBeforeRunningTransition())
        .addTransition(TaskAttemptStateInternal.OUTPUT_CONSUMABLE, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURES, new TerminatedWhileRunningTransition(FAILED_HELPER))

        .addTransition(TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptEventType.TA_STATUS_UPDATE, STATUS_UPDATER)
        .addTransition(TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptEventType.TA_COMMIT_PENDING)
        .addTransition(TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptStateInternal.SUCCEEDED, TaskAttemptEventType.TA_DONE, new SucceededTransition())
        .addTransition(TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_FAILED, new TerminatedWhileRunningTransition(FAILED_HELPER))
        .addTransition(TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_TIMED_OUT, new TerminatedWhileRunningTransition(FAILED_HELPER))
        .addTransition(TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_FAIL_REQUEST, new TerminatedWhileRunningTransition(FAILED_HELPER))
        .addTransition(TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptStateInternal.KILL_IN_PROGRESS, TaskAttemptEventType.TA_KILL_REQUEST, new TerminatedWhileRunningTransition(KILLED_HELPER))
        .addTransition(TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptStateInternal.KILL_IN_PROGRESS, TaskAttemptEventType.TA_NODE_FAILED, new TerminatedWhileRunningTransition(KILLED_HELPER))
        .addTransition(TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_CONTAINER_TERMINATING, new TerminatedWhileRunningTransition(FAILED_HELPER))
        .addTransition(TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptStateInternal.FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATED, new ContainerCompletedBeforeRunningTransition())
        .addTransition(TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURES, new TerminatedWhileRunningTransition(FAILED_HELPER))

        .addTransition(TaskAttemptStateInternal.KILL_IN_PROGRESS, TaskAttemptStateInternal.KILLED, TaskAttemptEventType.TA_CONTAINER_TERMINATED, new ContainerCompletedWhileTerminating())
        .addTransition(TaskAttemptStateInternal.KILL_IN_PROGRESS, TaskAttemptStateInternal.KILL_IN_PROGRESS, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptStateInternal.KILL_IN_PROGRESS, TaskAttemptStateInternal.KILL_IN_PROGRESS, EnumSet.of(TaskAttemptEventType.TA_STARTED_REMOTELY, TaskAttemptEventType.TA_STATUS_UPDATE, TaskAttemptEventType.TA_OUTPUT_CONSUMABLE, TaskAttemptEventType.TA_COMMIT_PENDING, TaskAttemptEventType.TA_DONE, TaskAttemptEventType.TA_FAILED, TaskAttemptEventType.TA_TIMED_OUT, TaskAttemptEventType.TA_FAIL_REQUEST, TaskAttemptEventType.TA_KILL_REQUEST, TaskAttemptEventType.TA_NODE_FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATING, TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURES))
        
        .addTransition(TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptStateInternal.FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATED, new ContainerCompletedWhileTerminating())
        .addTransition(TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptStateInternal.FAIL_IN_PROGRESS, TaskAttemptStateInternal.FAIL_IN_PROGRESS, EnumSet.of(TaskAttemptEventType.TA_STARTED_REMOTELY, TaskAttemptEventType.TA_STATUS_UPDATE, TaskAttemptEventType.TA_OUTPUT_CONSUMABLE, TaskAttemptEventType.TA_COMMIT_PENDING, TaskAttemptEventType.TA_DONE, TaskAttemptEventType.TA_FAILED, TaskAttemptEventType.TA_TIMED_OUT, TaskAttemptEventType.TA_FAIL_REQUEST, TaskAttemptEventType.TA_KILL_REQUEST, TaskAttemptEventType.TA_NODE_FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATING, TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURES))
        
        .addTransition(TaskAttemptStateInternal.KILLED, TaskAttemptStateInternal.KILLED, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptStateInternal.KILLED, TaskAttemptStateInternal.KILLED, EnumSet.of(TaskAttemptEventType.TA_STARTED_REMOTELY, TaskAttemptEventType.TA_STATUS_UPDATE, TaskAttemptEventType.TA_OUTPUT_CONSUMABLE, TaskAttemptEventType.TA_COMMIT_PENDING, TaskAttemptEventType.TA_DONE, TaskAttemptEventType.TA_FAILED, TaskAttemptEventType.TA_TIMED_OUT, TaskAttemptEventType.TA_FAIL_REQUEST, TaskAttemptEventType.TA_KILL_REQUEST, TaskAttemptEventType.TA_NODE_FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATING, TaskAttemptEventType.TA_CONTAINER_TERMINATED, TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURES))

        .addTransition(TaskAttemptStateInternal.FAILED, TaskAttemptStateInternal.FAILED, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptStateInternal.FAILED, TaskAttemptStateInternal.FAILED, EnumSet.of(TaskAttemptEventType.TA_STARTED_REMOTELY, TaskAttemptEventType.TA_STATUS_UPDATE, TaskAttemptEventType.TA_OUTPUT_CONSUMABLE, TaskAttemptEventType.TA_COMMIT_PENDING, TaskAttemptEventType.TA_DONE, TaskAttemptEventType.TA_FAILED, TaskAttemptEventType.TA_TIMED_OUT, TaskAttemptEventType.TA_FAIL_REQUEST, TaskAttemptEventType.TA_KILL_REQUEST, TaskAttemptEventType.TA_NODE_FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATING, TaskAttemptEventType.TA_CONTAINER_TERMINATED, TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURES))
        
        // How will duplicate history events be handled ?
        // TODO Maybe consider not failing REDUCE tasks in this case. Also, MAP_TASKS in case there's only one phase in the job.
        .addTransition(TaskAttemptStateInternal.SUCCEEDED, TaskAttemptStateInternal.SUCCEEDED, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptStateInternal.SUCCEEDED, TaskAttemptStateInternal.KILLED, TaskAttemptEventType.TA_KILL_REQUEST, new TerminatedAfterSuccessTransition(KILLED_HELPER))
        .addTransition(TaskAttemptStateInternal.SUCCEEDED, TaskAttemptStateInternal.KILLED, TaskAttemptEventType.TA_NODE_FAILED, new TerminatedAfterSuccessTransition(KILLED_HELPER))
        .addTransition(TaskAttemptStateInternal.SUCCEEDED, TaskAttemptStateInternal.FAILED, TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURES, new TerminatedAfterSuccessTransition(FAILED_HELPER))
        .addTransition(TaskAttemptStateInternal.SUCCEEDED, TaskAttemptStateInternal.SUCCEEDED, EnumSet.of(TaskAttemptEventType.TA_TIMED_OUT, TaskAttemptEventType.TA_FAIL_REQUEST, TaskAttemptEventType.TA_CONTAINER_TERMINATING, TaskAttemptEventType.TA_CONTAINER_TERMINATED))
        
        
        .installTopology();

  // TODO Remove TaskAttemptListener from the constructor.
  @SuppressWarnings("rawtypes")
  public TaskAttemptImpl(TaskId taskId, int i, EventHandler eventHandler,
      TaskAttemptListener tal, Path jobFile, int partition, JobConf conf,
      String[] dataLocalHosts, OutputCommitter committer,
      Token<JobTokenIdentifier> jobToken, Credentials credentials, Clock clock,
      TaskHeartbeatHandler taskHeartbeatHandler, AppContext appContext,
      String tezModuleClassName) {
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.readLock = rwLock.readLock();
    this.writeLock = rwLock.writeLock();
    this.taskId = taskId;
    this.jobId = taskId.getJobId();
    this.attemptId = MRBuilderUtils.newTaskAttemptId(taskId, i);
    this.eventHandler = eventHandler;
    //Reported status
    this.jobFile = jobFile;
    this.partition = partition;
    this.conf = conf;
    this.dataLocalHosts = dataLocalHosts;
    this.committer = committer;
    this.jobToken = jobToken;
    this.credentials = credentials;
    this.clock = clock;
    this.taskHeartbeatHandler = taskHeartbeatHandler;
    this.appContext = appContext;
    this.resourceCapability = BuilderUtils.newResource(getMemoryRequired(conf,
        taskId.getTaskType()), getCpuRequired(conf, taskId.getTaskType()));
    this.reportedStatus = new TaskAttemptStatus();
    this.tezModuleClassName = tezModuleClassName;
    initTaskAttemptStatus(reportedStatus);
    RackResolver.init(conf);
    this.stateMachine = stateMachineFactory.make(this);
  }
  
  
  

  @Override
  public TaskAttemptId getID() {
    return attemptId;
  }
  
  protected abstract MRTaskContext createRemoteMRTaskContext();
  
  @Override
  public TaskAttemptReport getReport() {
    TaskAttemptReport result = Records.newRecord(TaskAttemptReport.class);
    readLock.lock();
    try {
      result.setTaskAttemptId(attemptId);
      //take the LOCAL state of attempt
      //DO NOT take from reportedStatus
      
      result.setTaskAttemptState(getState());
      result.setProgress(reportedStatus.progress);
      result.setStartTime(launchTime);
      result.setFinishTime(finishTime);
      result.setShuffleFinishTime(this.reportedStatus.shuffleFinishTime);
      result.setDiagnosticInfo(StringUtils.join(LINE_SEPARATOR, getDiagnostics()));
      result.setPhase(reportedStatus.phase);
      result.setStateString(reportedStatus.stateString);
      result.setCounters(TypeConverter.toYarn(getCounters()));
      result.setContainerId(this.getAssignedContainerID());
      result.setNodeManagerHost(trackerName);
      result.setNodeManagerHttpPort(httpPort);
      if (this.containerNodeId != null) {
        result.setNodeManagerPort(this.containerNodeId.getPort());
      }
      return result;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<String> getDiagnostics() {
    List<String> result = new ArrayList<String>();
    readLock.lock();
    try {
      result.addAll(diagnostics);
      return result;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Counters getCounters() {
    readLock.lock();
    try {
      Counters counters = reportedStatus.counters;
      if (counters == null) {
        counters = EMPTY_COUNTERS;
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
      return reportedStatus.progress;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskAttemptState getState() {
    readLock.lock();
    try {
      return getExternalState(stateMachine.getCurrentState());
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean isFinished() {
    readLock.lock();
    try {
      return (EnumSet.of(TaskAttemptStateInternal.SUCCEEDED,
          TaskAttemptStateInternal.FAILED,
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptStateInternal.KILLED,
          TaskAttemptStateInternal.KILL_IN_PROGRESS)
          .contains(getInternalState()));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public ContainerId getAssignedContainerID() {
    readLock.lock();
    try {
      return containerId;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String getAssignedContainerMgrAddress() {
    readLock.lock();
    try {
      return containerNodeId.toString();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public NodeId getNodeId() {
    readLock.lock();
    try {
      return containerNodeId;
    } finally {
      readLock.unlock();
    }
  }

  /**If container Assigned then return the node's address, otherwise null.
   */
  @Override
  public String getNodeHttpAddress() {
    readLock.lock();
    try {
      return nodeHttpAddress;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * If container Assigned then return the node's rackname, otherwise null.
   */
  @Override
  public String getNodeRackName() {
    this.readLock.lock();
    try {
      return this.nodeRackName;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public long getLaunchTime() {
    readLock.lock();
    try {
      return launchTime;
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
  public long getShuffleFinishTime() {
    readLock.lock();
    try {
      return this.reportedStatus.shuffleFinishTime;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getSortFinishTime() {
    readLock.lock();
    try {
      return this.reportedStatus.sortFinishTime;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public int getShufflePort() {
    readLock.lock();
    try {
      return shufflePort;
    } finally {
      readLock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handle(TaskAttemptEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing TaskAttemptEvent " + event.getTaskAttemptID() + " of type "
          + event.getType());
    }
    LOG.info("DEBUG: Processing " + event.getTaskAttemptID() + " of type "
        + event.getType() + " while in state: " + getInternalState());
    writeLock.lock();
    try {
      final TaskAttemptStateInternal oldState = getInternalState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state for "
            + this.attemptId, e);
        eventHandler.handle(new JobEventDiagnosticsUpdate(
            this.attemptId.getTaskId().getJobId(), "Invalid event " + event.getType() + 
            " on TaskAttempt " + this.attemptId));
        eventHandler.handle(new JobEvent(this.attemptId.getTaskId().getJobId(),
            JobEventType.INTERNAL_ERROR));
      }
      if (oldState != getInternalState()) {
          LOG.info(attemptId + " TaskAttempt Transitioned from " 
           + oldState + " to "
           + getInternalState());
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  @VisibleForTesting
  public TaskAttemptStateInternal getInternalState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }    
  }

  private static TaskAttemptState getExternalState(
      TaskAttemptStateInternal smState) {
    switch (smState) {
    case NEW:
    case START_WAIT:
      return TaskAttemptState.STARTING;
    case RUNNING:
      return TaskAttemptState.RUNNING;
    case COMMIT_PENDING:
    case OUTPUT_CONSUMABLE:
      return TaskAttemptState.COMMIT_PENDING;
    case FAILED:
    case FAIL_IN_PROGRESS:
      return TaskAttemptState.FAILED;
    case KILLED:
    case KILL_IN_PROGRESS:
      return TaskAttemptState.KILLED;
    case SUCCEEDED:
      return TaskAttemptState.SUCCEEDED;
    default:
      throw new YarnException("Attempt to convert invalid "
          + "stateMachineTaskAttemptState to externalTaskAttemptState: "
          + smState);
    }
  }

  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    this.eventHandler.handle(event);
  }

  private int getMemoryRequired(Configuration conf, TaskType taskType) {
    int memory = 1024;
    if (taskType == TaskType.MAP)  {
      memory =
          conf.getInt(MRJobConfig.MAP_MEMORY_MB,
              MRJobConfig.DEFAULT_MAP_MEMORY_MB);
    } else if (taskType == TaskType.REDUCE) {
      memory =
          conf.getInt(MRJobConfig.REDUCE_MEMORY_MB,
              MRJobConfig.DEFAULT_REDUCE_MEMORY_MB);
    }
    
    return memory;
  }

  private int getCpuRequired(Configuration conf, TaskType taskType) {
    int vcores = 1;
    if (taskType == TaskType.MAP) {
      vcores =
          conf.getInt(MRJobConfig.MAP_CPU_VCORES,
              MRJobConfig.DEFAULT_MAP_CPU_VCORES);
    } else if (taskType == TaskType.REDUCE) {
      vcores =
          conf.getInt(MRJobConfig.REDUCE_CPU_VCORES,
              MRJobConfig.DEFAULT_REDUCE_CPU_VCORES);
    }

    return vcores;
  }

  // always called in write lock
  private void setFinishTime() {
    // set the finish time only if launch time is set
    if (launchTime != 0) {
      finishTime = clock.getTime();
    }
  }

  // TOOD Merge some of these JobCounter events.
  private static JobEventCounterUpdate createJobCounterUpdateEventTALaunched(
      TaskAttemptImpl ta) {
    JobEventCounterUpdate jce = new JobEventCounterUpdate(ta.jobId);
    jce.addCounterUpdate(
        ta.taskId.getTaskType() == TaskType.MAP ? JobCounter.TOTAL_LAUNCHED_MAPS
            : JobCounter.TOTAL_LAUNCHED_REDUCES, 1);
    return jce;
  }

  private static JobEventCounterUpdate createJobCounterUpdateEventSlotMillis(
      TaskAttemptImpl ta) {
    JobEventCounterUpdate jce = new JobEventCounterUpdate(ta.jobId);
    long slotMillis = computeSlotMillis(ta);
    jce.addCounterUpdate(
        ta.taskId.getTaskType() == TaskType.MAP ? JobCounter.SLOTS_MILLIS_MAPS
            : JobCounter.SLOTS_MILLIS_REDUCES, slotMillis);
    return jce;
  }

  private static JobEventCounterUpdate createJobCounterUpdateEventTATerminated(
      TaskAttemptImpl taskAttempt, boolean taskAlreadyCompleted,
      TaskAttemptStateInternal taState) {
    TaskType taskType = taskAttempt.getID().getTaskId().getTaskType();
    JobEventCounterUpdate jce = new JobEventCounterUpdate(taskAttempt.getID()
        .getTaskId().getJobId());

    long slotMillisIncrement = computeSlotMillis(taskAttempt);

    if (taskType == TaskType.MAP) {
      if (taState == TaskAttemptStateInternal.FAILED) {
        jce.addCounterUpdate(JobCounter.NUM_FAILED_MAPS, 1);
      } else if (taState == TaskAttemptStateInternal.KILLED) {
        jce.addCounterUpdate(JobCounter.NUM_KILLED_MAPS, 1);
      }
      if (!taskAlreadyCompleted) {
        // dont double count the elapsed time
        jce.addCounterUpdate(JobCounter.SLOTS_MILLIS_MAPS, slotMillisIncrement);
      }
    } else {
      if (taState == TaskAttemptStateInternal.FAILED) {
        jce.addCounterUpdate(JobCounter.NUM_FAILED_REDUCES, 1);
      } else if (taState == TaskAttemptStateInternal.KILLED) {
        jce.addCounterUpdate(JobCounter.NUM_KILLED_REDUCES, 1);
      }
      if (!taskAlreadyCompleted) {
        // dont double count the elapsed time
        jce.addCounterUpdate(JobCounter.SLOTS_MILLIS_REDUCES,
            slotMillisIncrement);
      }
    }
    return jce;
  }

  private static long computeSlotMillis(TaskAttemptImpl taskAttempt) {
    TaskType taskType = taskAttempt.getID().getTaskId().getTaskType();
    int slotMemoryReq =
        taskAttempt.getMemoryRequired(taskAttempt.conf, taskType);

    int minSlotMemSize =
        taskAttempt.appContext.getClusterInfo().getMinContainerCapability()
            .getMemory();

    int simSlotsRequired =
        minSlotMemSize == 0 ? 0 : (int) Math.ceil((float) slotMemoryReq
            / minSlotMemSize);

    long slotMillisIncrement =
        simSlotsRequired
            * (taskAttempt.getFinishTime() - taskAttempt.getLaunchTime());
    return slotMillisIncrement;
  }

  // TODO Change to return a JobHistoryEvent.
  private static
      TaskAttemptUnsuccessfulCompletionEvent
  createTaskAttemptUnsuccessfulCompletionEvent(TaskAttemptImpl taskAttempt,
      TaskAttemptStateInternal attemptState) {
    TaskAttemptUnsuccessfulCompletionEvent tauce =
    new TaskAttemptUnsuccessfulCompletionEvent(
        TypeConverter.fromYarn(taskAttempt.attemptId),
        TypeConverter.fromYarn(taskAttempt.attemptId.getTaskId()
            .getTaskType()), attemptState.toString(),
        taskAttempt.finishTime,
        taskAttempt.containerNodeId == null ? "UNKNOWN"
            : taskAttempt.containerNodeId.getHost(),
        taskAttempt.containerNodeId == null ? -1 
            : taskAttempt.containerNodeId.getPort(),    
        taskAttempt.nodeRackName == null ? "UNKNOWN" 
            : taskAttempt.nodeRackName,
        StringUtils.join(
            LINE_SEPARATOR, taskAttempt.getDiagnostics()), taskAttempt
            .getProgressSplitBlock().burst());
    return tauce;
  }
  
  // TODO Incorporate MAPREDUCE-4838
  private JobHistoryEvent createTaskAttemptStartedEvent() {
    TaskAttemptStartedEvent tase = new TaskAttemptStartedEvent(
        TypeConverter.fromYarn(attemptId), TypeConverter.fromYarn(taskId
            .getTaskType()), launchTime, trackerName, httpPort, shufflePort,
        containerId, "", "");
    return new JobHistoryEvent(jobId, tase);

  }

  private WrappedProgressSplitsBlock getProgressSplitBlock() {
    readLock.lock();
    try {
      if (progressSplitBlock == null) {
        progressSplitBlock = new WrappedProgressSplitsBlock(conf.getInt(
            MRJobConfig.MR_AM_NUM_PROGRESS_SPLITS,
            MRJobConfig.DEFAULT_MR_AM_NUM_PROGRESS_SPLITS));
      }
      return progressSplitBlock;
    } finally {
      readLock.unlock();
    }
  }
  
  private void updateProgressSplits() {
    double newProgress = reportedStatus.progress;
    newProgress = Math.max(Math.min(newProgress, 1.0D), 0.0D);
    Counters counters = reportedStatus.counters;
    if (counters == null)
      return;

    WrappedProgressSplitsBlock splitsBlock = getProgressSplitBlock();
    if (splitsBlock != null) {
      long now = clock.getTime();
      long start = getLaunchTime();
      
      if (start == 0)
        return;

      if (start != 0 && now - start <= Integer.MAX_VALUE) {
        splitsBlock.getProgressWallclockTime().extend(newProgress,
            (int) (now - start));
      }

      Counter cpuCounter = counters.findCounter(TaskCounter.CPU_MILLISECONDS);
      if (cpuCounter != null && cpuCounter.getValue() <= Integer.MAX_VALUE) {
        splitsBlock.getProgressCPUTime().extend(newProgress,
            (int) cpuCounter.getValue()); // long to int? TODO: FIX. Same below
      }

      Counter virtualBytes = counters
        .findCounter(TaskCounter.VIRTUAL_MEMORY_BYTES);
      if (virtualBytes != null) {
        splitsBlock.getProgressVirtualMemoryKbytes().extend(newProgress,
            (int) (virtualBytes.getValue() / (MEMORY_SPLITS_RESOLUTION)));
      }

      Counter physicalBytes = counters
        .findCounter(TaskCounter.PHYSICAL_MEMORY_BYTES);
      if (physicalBytes != null) {
        splitsBlock.getProgressPhysicalMemoryKbytes().extend(newProgress,
            (int) (physicalBytes.getValue() / (MEMORY_SPLITS_RESOLUTION)));
      }
    }
  }
  
  @SuppressWarnings({ "unchecked" })
  private void logAttemptFinishedEvent(TaskAttemptStateInternal state) {
    //Log finished events only if an attempt started.
    if (getLaunchTime() == 0) return; 
    if (attemptId.getTaskId().getTaskType() == TaskType.MAP) {
      MapAttemptFinishedEvent mfe =
         new MapAttemptFinishedEvent(TypeConverter.fromYarn(attemptId),
         TypeConverter.fromYarn(attemptId.getTaskId().getTaskType()),
         state.toString(),
         this.reportedStatus.mapFinishTime,
         finishTime,
         this.containerNodeId == null ? "UNKNOWN"
             : this.containerNodeId.getHost(),
         this.containerNodeId == null ? -1 : this.containerNodeId.getPort(),
         this.nodeRackName == null ? "UNKNOWN" : this.nodeRackName,
         this.reportedStatus.stateString,
         getCounters(),
         getProgressSplitBlock().burst());
         eventHandler.handle(
           new JobHistoryEvent(attemptId.getTaskId().getJobId(), mfe));
    } else {
       ReduceAttemptFinishedEvent rfe =
         new ReduceAttemptFinishedEvent(TypeConverter.fromYarn(attemptId),
         TypeConverter.fromYarn(attemptId.getTaskId().getTaskType()),
         state.toString(),
         this.reportedStatus.shuffleFinishTime,
         this.reportedStatus.sortFinishTime,
         finishTime,
         this.containerNodeId == null ? "UNKNOWN"
             : this.containerNodeId.getHost(),
         this.containerNodeId == null ? -1 : this.containerNodeId.getPort(),
         this.nodeRackName == null ? "UNKNOWN" : this.nodeRackName,
         this.reportedStatus.stateString,
         getCounters(),
         getProgressSplitBlock().burst());
         eventHandler.handle(
           new JobHistoryEvent(attemptId.getTaskId().getJobId(), rfe));
    }
  }

  private void maybeSendSpeculatorContainerRequired() {
    if (!speculatorContainerRequestSent) {
      sendEvent(new SpeculatorEvent(taskId, +1));
      speculatorContainerRequestSent = true;
    }
  }

  private void maybeSendSpeculatorContainerNoLongerRequired() {
    if (speculatorContainerRequestSent) {
      sendEvent(new SpeculatorEvent(taskId, -1));
      speculatorContainerRequestSent = false;
    }
  }
  
  private void sendTaskAttemptCleanupEvent() {
    TaskAttemptContext taContext = new TaskAttemptContextImpl(this.conf,
        TypeConverter.fromYarn(this.attemptId));
    sendEvent(new TaskCleanupEvent(this.attemptId, this.committer, taContext));
  }

  protected String[] resolveHosts(String[] src) {
    return TaskAttemptImplHelpers.resolveHosts(src);
  }

  //////////////////////////////////////////////////////////////////////////////
  //                   Start of Transition Classes                            //
  //////////////////////////////////////////////////////////////////////////////

  protected static class ScheduleTaskattemptTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      TaskAttemptEventSchedule scheduleEvent = (TaskAttemptEventSchedule) event;
      // Event to speculator - containerNeeded++
      ta.maybeSendSpeculatorContainerRequired();

      // TODO Creating the remote task here may not be required in case of
      // recovery.

      // Create the remote task.
      MRTaskContext remoteTaskContext = ta.createRemoteMRTaskContext();
      // Create startTaskRequest

      String[] hostArray;
      String[] rackArray;
      if (scheduleEvent.isRescheduled()) {
        // No node/rack locality.
        hostArray = new String[0];
        rackArray = new String[0];
      } else {
        // Ask for node / rack locality.
        Set<String> racks = new HashSet<String>();
        for (String host : ta.dataLocalHosts) {
          racks.add(RackResolver.resolve(host).getNetworkLocation());
        }
        hostArray = ta.resolveHosts(ta.dataLocalHosts);
        rackArray = racks.toArray(new String[racks.size()]);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Asking for container launch with taskAttemptContext: "
            + remoteTaskContext);
      }
      // Send out a launch request to the scheduler.
      AMSchedulerTALaunchRequestEvent launchRequestEvent =
          new AMSchedulerTALaunchRequestEvent(
              ta.attemptId, scheduleEvent.isRescheduled(),
              ta.resourceCapability,
              remoteTaskContext, ta, ta.credentials, ta.jobToken, hostArray,
              rackArray);
      ta.sendEvent(launchRequestEvent);
    }
  }

  protected static class DiagnosticInformationUpdater implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      TaskAttemptEventDiagnosticsUpdate diagEvent =
          (TaskAttemptEventDiagnosticsUpdate) event;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Diagnostics update for " + ta.attemptId + ": "
            + diagEvent.getDiagnosticInfo());
      }
      ta.addDiagnosticInfo(diagEvent.getDiagnosticInfo());
    }
  }

  protected static class TerminateTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    TerminatedTransitionHelper helper;

    public TerminateTransition(TerminatedTransitionHelper helper) {
      this.helper = helper;
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      ta.setFinishTime();
      if (event instanceof DiagnosableEvent) {
        ta.addDiagnosticInfo(((DiagnosableEvent) event).getDiagnosticInfo());
      }
      
      ta.sendEvent(createJobCounterUpdateEventTATerminated(ta, false,
          helper.getTaskAttemptStateInternal()));
      if (ta.getLaunchTime() != 0) {
        // TODO For cases like this, recovery goes for a toss, since the the
        // attempt will not exist in the history file.
        ta.sendEvent(new JobHistoryEvent(ta.jobId,
            createTaskAttemptUnsuccessfulCompletionEvent(ta,
                helper.getTaskAttemptStateInternal())));
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Not generating HistoryFinish event since start event not "
              +
              "generated for taskAttempt: " + ta.getID());
        }
      }
      // Send out events to the Task - indicating TaskAttemptTermination(F/K)
      ta.sendEvent(new TaskEventTAUpdate(ta.attemptId,
          helper.getTaskEventType()));
    }    
  }

  protected static class StartedTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent origEvent) {
      TaskAttemptEventStartedRemotely event =
          (TaskAttemptEventStartedRemotely) origEvent;

      Container container = ta.appContext.getAllContainers()
          .get(event.getContainerId()).getContainer();

      ta.containerId = event.getContainerId();
      ta.containerNodeId = container.getNodeId();
      ta.nodeHttpAddress = container.getNodeHttpAddress();
      ta.nodeRackName = RackResolver.resolve(ta.containerNodeId.getHost())
          .getNetworkLocation();
      ta.launchTime = ta.clock.getTime();
      ta.shufflePort = event.getShufflePort();

      // TODO Resolve to host / IP in case of a local address.
      InetSocketAddress nodeHttpInetAddr = NetUtils
          .createSocketAddr(ta.nodeHttpAddress); // TODO: Costly?
      ta.trackerName = nodeHttpInetAddr.getHostName();
      ta.httpPort = nodeHttpInetAddr.getPort();
      ta.sendEvent(createJobCounterUpdateEventTALaunched(ta));

      LOG.info("TaskAttempt: [" + ta.attemptId + "] started."
          + " Is using containerId: [" + ta.containerId + "]"
          + " on NM: [" + ta.containerNodeId + "]");

      // JobHistoryEvent
      ta.sendEvent(ta.createTaskAttemptStartedEvent());

      // Inform the speculator about the container assignment.
      ta.maybeSendSpeculatorContainerNoLongerRequired();
      // Inform speculator about startTime
      ta.sendEvent(new SpeculatorEvent(ta.attemptId, true, ta.launchTime));

      // Inform the Task
      ta.sendEvent(new TaskEventTAUpdate(ta.attemptId,
          TaskEventType.T_ATTEMPT_LAUNCHED));

      ta.taskHeartbeatHandler.register(ta.attemptId);
    }
  }

  protected static class TerminatedBeforeRunningTransition extends
      TerminateTransition {

    public TerminatedBeforeRunningTransition(
        TerminatedTransitionHelper helper) {
      super(helper);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      // Inform the scheduler
      ta.sendEvent(new AMSchedulerEventTAEnded(ta.attemptId,
          ta.containerId, helper.getTaskAttemptState()));
      // Decrement speculator container request.
      ta.maybeSendSpeculatorContainerNoLongerRequired();
    }
  }

  protected static class NodeFailedBeforeRunningTransition extends
      TerminatedBeforeRunningTransition {

    public NodeFailedBeforeRunningTransition() {
      super(KILLED_HELPER);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      TaskAttemptEventNodeFailed nfEvent = (TaskAttemptEventNodeFailed) event;
      ta.addDiagnosticInfo(nfEvent.getDiagnosticInfo());
    }
  }

  protected static class ContainerTerminatingBeforeRunningTransition extends
      TerminatedBeforeRunningTransition {

    public ContainerTerminatingBeforeRunningTransition() {
      super(FAILED_HELPER);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      TaskAttemptEventContainerTerminating tEvent =
          (TaskAttemptEventContainerTerminating) event;
      ta.addDiagnosticInfo(tEvent.getDiagnosticInfo());
    }
  }

  protected static class ContainerCompletedBeforeRunningTransition extends
      TerminatedBeforeRunningTransition {
    public ContainerCompletedBeforeRunningTransition() {
      super(FAILED_HELPER);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.sendTaskAttemptCleanupEvent();

      TaskAttemptEventContainerTerminated tEvent =
          (TaskAttemptEventContainerTerminated) event;
      ta.addDiagnosticInfo(tEvent.getDiagnosticInfo());
    }
  }

  protected static class StatusUpdaterTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      TaskAttemptStatus newReportedStatus =
          ((TaskAttemptEventStatusUpdate) event)
              .getReportedTaskAttemptStatus();
      ta.reportedStatus = newReportedStatus;
      ta.reportedStatus.taskState = ta.getState();

      // Inform speculator of status.
      ta.sendEvent(new SpeculatorEvent(ta.reportedStatus, ta.clock.getTime()));

      ta.updateProgressSplits();

      // Inform the job about fetch failures if they exist.
      if (ta.reportedStatus.fetchFailedMaps != null
          && ta.reportedStatus.fetchFailedMaps.size() > 0) {
        ta.sendEvent(new JobEventTaskAttemptFetchFailure(ta.attemptId,
            ta.reportedStatus.fetchFailedMaps));
      }
      // TODO at some point. Nodes may be interested in FetchFailure info.
      // Can be used to blacklist nodes.
    }
  }

  protected static class OutputConsumableTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      TaskAttemptEventOutputConsumable orEvent =
          (TaskAttemptEventOutputConsumable) event;
      ta.shufflePort = orEvent.getOutputContext().getShufflePort();
      ta.sendEvent(new TaskEventTAUpdate(ta.attemptId,
          TaskEventType.T_ATTEMPT_OUTPUT_CONSUMABLE));
    }
  }

  protected static class CommitPendingTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      ta.sendEvent(new TaskEventTAUpdate(ta.attemptId,
          TaskEventType.T_ATTEMPT_COMMIT_PENDING));
    }
  }

  protected static class SucceededTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      ta.setFinishTime();
     //Inform the speculator.
      ta.sendEvent(new SpeculatorEvent(ta.reportedStatus, ta.finishTime));
      
      // Send out history event.
      ta.logAttemptFinishedEvent(TaskAttemptStateInternal.SUCCEEDED);
      ta.sendEvent(createJobCounterUpdateEventSlotMillis(ta));

      // Inform the Scheduler.
      ta.sendEvent(new AMSchedulerEventTAEnded(ta.attemptId,
          ta.containerId, TaskAttemptState.SUCCEEDED));
      
      // Inform the task.
      ta.sendEvent(new TaskEventTAUpdate(ta.attemptId,
          TaskEventType.T_ATTEMPT_SUCCEEDED));
      
      //Unregister from the TaskHeartbeatHandler.
      ta.taskHeartbeatHandler.unregister(ta.attemptId);

      // TODO maybe. For reuse ... Stacking pulls for a reduce task, even if the
      // TA finishes independently. // Will likely be the Job's responsibility.      
    }
  }

  protected static class TerminatedWhileRunningTransition extends
      TerminatedBeforeRunningTransition {

    public TerminatedWhileRunningTransition(TerminatedTransitionHelper helper) {
      super(helper);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.taskHeartbeatHandler.unregister(ta.attemptId);
    }
  }

  protected static class ContaienrCompletedWhileRunningTransition extends
      TerminatedBeforeRunningTransition {

    public ContaienrCompletedWhileRunningTransition() {
      super(FAILED_HELPER);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.sendTaskAttemptCleanupEvent();
    }
  }

  protected static class CommitPendingAtOutputConsumableTransition extends
      CommitPendingTransition {

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      // TODO Figure out the interaction between OUTPUT_CONSUMABLE AND
      // COMMIT_PENDING, Ideally both should not exist for the same task.
      super.transition(ta, event);
      LOG.info("Received a commit pending while in the OutputConsumable state");
    }
  }

  protected static class ContainerCompletedWhileTerminating implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      ta.sendTaskAttemptCleanupEvent();
      TaskAttemptEventContainerTerminated tEvent =
          (TaskAttemptEventContainerTerminated) event;
      ta.addDiagnosticInfo(tEvent.getDiagnosticInfo());
    }

  }

  protected static class TerminatedAfterSuccessTransition extends
      TerminatedBeforeRunningTransition {

    public TerminatedAfterSuccessTransition(TerminatedTransitionHelper helper) {
      super(helper);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.sendTaskAttemptCleanupEvent();
    }
  }
 

  private void initTaskAttemptStatus(TaskAttemptStatus result) {
    result.progress = 0.0f;
    result.phase = Phase.STARTING;
    result.stateString = "NEW";
    result.taskState = TaskAttemptState.NEW;
    Counters counters = EMPTY_COUNTERS;
    //    counters.groups = new HashMap<String, CounterGroup>();
    result.counters = counters;
  }
  
  private void addDiagnosticInfo(String diag) {
    if (diag != null && !diag.equals("")) {
      diagnostics.add(diag);
    }
  }

  protected interface TerminatedTransitionHelper {

    public TaskAttemptStateInternal getTaskAttemptStateInternal();

    public TaskAttemptState getTaskAttemptState();

    public TaskEventType getTaskEventType();
  }

  protected static class FailedTransitionHelper implements
      TerminatedTransitionHelper {

    @Override
    public TaskAttemptStateInternal getTaskAttemptStateInternal() {
      return TaskAttemptStateInternal.FAILED;
    }

    @Override
    public TaskAttemptState getTaskAttemptState() {
      return TaskAttemptState.FAILED;
    }

    @Override
    public TaskEventType getTaskEventType() {
      return TaskEventType.T_ATTEMPT_FAILED;
    }
  }

  

  protected static class KilledTransitionHelper implements
      TerminatedTransitionHelper {

    @Override
    public TaskAttemptStateInternal getTaskAttemptStateInternal() {
      return TaskAttemptStateInternal.KILLED;
    }

    @Override
    public TaskAttemptState getTaskAttemptState() {
      return TaskAttemptState.KILLED;
    }

    @Override
    public TaskEventType getTaskEventType() {
      return TaskEventType.T_ATTEMPT_KILLED;
    }
  }


  
}
