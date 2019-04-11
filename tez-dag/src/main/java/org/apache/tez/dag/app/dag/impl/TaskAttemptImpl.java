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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.StringUtils;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSubmitted;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventTAFailed;
import org.apache.tez.dag.app.dag.event.TaskEventTAKilled;
import org.apache.tez.dag.app.dag.event.TaskEventTALaunched;
import org.apache.tez.dag.app.dag.event.TaskEventTASucceeded;
import org.apache.tez.dag.app.rm.AMSchedulerEventTAStateUpdated;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.serviceplugins.api.TaskScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.oldrecords.TaskAttemptReport;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.RecoveryParser.TaskAttemptRecoveryData;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventCounterUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventDiagnosticsUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.DiagnosableEvent;
import org.apache.tez.dag.app.dag.event.RecoveryEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventAttemptFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventAttemptKilled;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventContainerTerminated;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventContainerTerminatedBySystem;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventTezEventUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventTerminationCauseEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventOutputFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStatusUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.dag.event.SpeculatorEventTaskAttemptStatusUpdate;
import org.apache.tez.dag.app.rm.AMSchedulerEventTAEnded;
import org.apache.tez.dag.app.rm.AMSchedulerEventTALaunchRequest;
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.recovery.records.RecoveryProtos.DataEventDependencyInfoProto;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TaskStatistics;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TaskAttemptImpl implements TaskAttempt,
    EventHandler<TaskAttemptEvent> {

  // TODO Ensure MAPREDUCE-4457 is factored in. Also MAPREDUCE-4068.
  // TODO Consider TAL registration in the TaskAttempt instead of the container.

  private static final Logger LOG = LoggerFactory.getLogger(TaskAttemptImpl.class);
  private static final String LINE_SEPARATOR = System
      .getProperty("line.separator");
  
  public static class DataEventDependencyInfo {
    long timestamp;
    TezTaskAttemptID taId;
    public DataEventDependencyInfo(long time, TezTaskAttemptID id) {
      this.timestamp = time;
      this.taId = id;
    }
    public long getTimestamp() {
      return timestamp;
    }
    public TezTaskAttemptID getTaskAttemptId() {
      return taId;
    }
    public static DataEventDependencyInfoProto toProto(DataEventDependencyInfo info) {
      DataEventDependencyInfoProto.Builder builder = DataEventDependencyInfoProto.newBuilder();
      builder.setTimestamp(info.timestamp);
      if (info.taId != null) {
        builder.setTaskAttemptId(info.taId.toString());
      }
      return builder.build();
    }
    
    public static DataEventDependencyInfo fromProto(DataEventDependencyInfoProto proto) {
      TezTaskAttemptID taId = null;
      if(proto.hasTaskAttemptId()) {
        taId = TezTaskAttemptID.fromString(proto.getTaskAttemptId());
      }
      return new DataEventDependencyInfo(proto.getTimestamp(), taId);
    }
  }

  static final TezCounters EMPTY_COUNTERS = new TezCounters();

  // Should not be used to access configuration. User vertex.VertexConfig instead
  protected final Configuration conf;
  @SuppressWarnings("rawtypes")
  protected EventHandler eventHandler;
  private final TezTaskAttemptID attemptId;
  private final Clock clock;
  private TaskAttemptTerminationCause terminationCause = TaskAttemptTerminationCause.UNKNOWN_ERROR;
  private final List<String> diagnostics = new ArrayList<String>();
  private final Lock readLock;
  private final Lock writeLock;
  protected final AppContext appContext;
  private final TaskHeartbeatHandler taskHeartbeatHandler;
  private TaskAttemptRecoveryData recoveryData;
  private long launchTime = 0;
  private long finishTime = 0;
  /** System.nanoTime for task launch time, if recorded in this JVM. */
  private Long launchTimeNs;
  /** System.nanoTime for task finish time, if recorded in this JVM. */
  private Long finishTimeNs;
  /** Whether the task was recovered from a prior AM; see getDurationNs. */
  private boolean isRecoveredDuration;
  private String trackerName;
  private int httpPort;

  // TODO Can these be replaced by the container object TEZ-1037
  private Container container;
  private long allocationTime;
  private ContainerId containerId;
  private NodeId containerNodeId;
  private String nodeHttpAddress;
  private String nodeRackName;
  
  private final Vertex vertex;
  private final Task task;
  private final TaskLocationHint locationHint;
  private final TaskSpec taskSpec;

  @VisibleForTesting
  boolean appendNextDataEvent = true;
  ArrayList<DataEventDependencyInfo> lastDataEvents = Lists.newArrayList();
  
  @VisibleForTesting
  TaskAttemptStatus reportedStatus;
  private DAGCounter localityCounter;
  
  org.apache.tez.runtime.api.impl.TaskStatistics statistics;

  long lastNotifyProgressTimestamp = 0;
  private final long hungIntervalMax;

  private List<TezEvent> taGeneratedEvents = Lists.newArrayList();

  // Used to store locality information when
  Set<String> taskHosts = new HashSet<String>();
  Set<String> taskRacks = new HashSet<String>();

  private Map<TezTaskAttemptID, Long> uniquefailedOutputReports = Maps.newHashMap();

  protected final boolean isRescheduled;
  private final Resource taskResource;
  private final ContainerContext containerContext;
  private final boolean leafVertex;
  
  private TezTaskAttemptID creationCausalTA;
  private long creationTime;
  private long scheduledTime;

  protected static final FailedTransitionHelper FAILED_HELPER =
      new FailedTransitionHelper();

  protected static final KilledTransitionHelper KILLED_HELPER =
      new KilledTransitionHelper();
  
  private static SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
      TERMINATED_AFTER_SUCCESS_HELPER = new TerminatedAfterSuccessHelper(KILLED_HELPER);

  private static SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
      STATUS_UPDATER = new StatusUpdaterTransition();

  private final StateMachine<TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent> stateMachine;

  // TODO TEZ-2003 (post) TEZ-2667 We may need some additional state management for STATUS_UPDATES, FAILED, KILLED coming in before
  // TASK_STARTED_REMOTELY. In case of a PUSH it's more intuitive to send TASK_STARTED_REMOTELY after communicating
  // with the listening service and getting a response, which in turn can trigger STATUS_UPDATES / FAILED / KILLED

  // TA_KILLED handled the same as TA_KILL_REQUEST. Just a different name indicating a request / already killed.
  private static StateMachineFactory
  <TaskAttemptImpl, TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent>
  stateMachineFactory
            = new StateMachineFactory
            <TaskAttemptImpl, TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent>
            (TaskAttemptStateInternal.NEW)

      .addTransition(TaskAttemptStateInternal.NEW,
          EnumSet.of(TaskAttemptStateInternal.NEW, TaskAttemptStateInternal.START_WAIT, TaskAttemptStateInternal.FAILED),
          TaskAttemptEventType.TA_SCHEDULE, new ScheduleTaskattemptTransition())
       // NEW -> FAILED due to TA_FAILED happens in recovery 
       // (No TaskAttemptStartedEvent, but with TaskAttemptFinishedEvent(FAILED)
      .addTransition(TaskAttemptStateInternal.NEW,
              TaskAttemptStateInternal.FAILED,
          TaskAttemptEventType.TA_FAILED, new TerminateTransition(FAILED_HELPER))
      .addTransition(TaskAttemptStateInternal.NEW,
          TaskAttemptStateInternal.KILLED,
          TaskAttemptEventType.TA_KILL_REQUEST,
          new TerminateTransition(KILLED_HELPER))
      // NEW -> KILLED due to TA_KILLED happens in recovery
      // (No TaskAttemptStartedEvent, but with TaskAttemptFinishedEvent(KILLED)    
      .addTransition(TaskAttemptStateInternal.NEW,
          TaskAttemptStateInternal.KILLED,
          TaskAttemptEventType.TA_KILLED,
          new TerminateTransition(KILLED_HELPER))
      // NEW -> SUCCEEDED due to TA_DONE happens in recovery
      // (with TaskAttemptStartedEvent and with TaskAttemptFinishedEvent(SUCCEEDED)    
      .addTransition(TaskAttemptStateInternal.NEW,
          TaskAttemptStateInternal.SUCCEEDED,
          TaskAttemptEventType.TA_DONE,
          new SucceededTransition())

      .addTransition(TaskAttemptStateInternal.START_WAIT,
          TaskAttemptStateInternal.SUBMITTED,
          TaskAttemptEventType.TA_SUBMITTED, new SubmittedTransition())
      .addTransition(TaskAttemptStateInternal.START_WAIT,
          TaskAttemptStateInternal.KILL_IN_PROGRESS,
          TaskAttemptEventType.TA_KILL_REQUEST,
          new TerminatedBeforeRunningTransition(KILLED_HELPER))
      .addTransition(TaskAttemptStateInternal.START_WAIT,
          TaskAttemptStateInternal.KILLED,
          TaskAttemptEventType.TA_KILLED,
          new TerminatedBeforeRunningTransition(KILLED_HELPER))
      .addTransition(TaskAttemptStateInternal.START_WAIT,
          TaskAttemptStateInternal.KILL_IN_PROGRESS,
          TaskAttemptEventType.TA_NODE_FAILED,
          new NodeFailedBeforeRunningTransition())
      .addTransition(TaskAttemptStateInternal.START_WAIT,
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptEventType.TA_CONTAINER_TERMINATING,
          new ContainerTerminatingBeforeRunningTransition())
      .addTransition(TaskAttemptStateInternal.START_WAIT,
          TaskAttemptStateInternal.FAILED,
          TaskAttemptEventType.TA_CONTAINER_TERMINATED,
          new ContainerCompletedBeforeRunningTransition())
      .addTransition(TaskAttemptStateInternal.START_WAIT,
          TaskAttemptStateInternal.KILLED,
          TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM,
          new ContainerCompletedBeforeRunningTransition(KILLED_HELPER))

      .addTransition(TaskAttemptStateInternal.SUBMITTED,
          TaskAttemptStateInternal.RUNNING,
          TaskAttemptEventType.TA_STARTED_REMOTELY, new StartedTransition())
      .addTransition(TaskAttemptStateInternal.SUBMITTED,
          TaskAttemptStateInternal.SUBMITTED,
          TaskAttemptEventType.TA_STATUS_UPDATE, STATUS_UPDATER)
      // Optional, may not come in for all tasks.
      .addTransition(TaskAttemptStateInternal.SUBMITTED,
          TaskAttemptStateInternal.SUCCEEDED, TaskAttemptEventType.TA_DONE,
          new SucceededTransition())
      .addTransition(TaskAttemptStateInternal.SUBMITTED,
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptEventType.TA_FAILED,
          new TerminatedWhileRunningTransition(FAILED_HELPER))
      .addTransition(TaskAttemptStateInternal.SUBMITTED,
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptEventType.TA_TIMED_OUT,
          new TerminatedWhileRunningTransition(FAILED_HELPER))
      .addTransition(TaskAttemptStateInternal.SUBMITTED,
          TaskAttemptStateInternal.KILL_IN_PROGRESS,
          TaskAttemptEventType.TA_KILL_REQUEST,
          new TerminatedWhileRunningTransition(KILLED_HELPER))
      .addTransition(TaskAttemptStateInternal.SUBMITTED,
          TaskAttemptStateInternal.KILLED,
          TaskAttemptEventType.TA_KILLED,
          new TerminatedWhileRunningTransition(KILLED_HELPER))
      .addTransition(TaskAttemptStateInternal.SUBMITTED,
          TaskAttemptStateInternal.KILL_IN_PROGRESS,
          TaskAttemptEventType.TA_NODE_FAILED,
          new TerminatedWhileRunningTransition(KILLED_HELPER))
      .addTransition(TaskAttemptStateInternal.SUBMITTED,
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptEventType.TA_CONTAINER_TERMINATING,
          new TerminatedWhileRunningTransition(FAILED_HELPER))
      .addTransition(TaskAttemptStateInternal.SUBMITTED,
          TaskAttemptStateInternal.FAILED,
          TaskAttemptEventType.TA_CONTAINER_TERMINATED,
          new ContainerCompletedWhileRunningTransition())
      .addTransition(TaskAttemptStateInternal.SUBMITTED,
          TaskAttemptStateInternal.KILLED,
          TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM,
          new ContainerCompletedWhileRunningTransition(KILLED_HELPER))
      .addTransition(
          TaskAttemptStateInternal.SUBMITTED,
          EnumSet.of(TaskAttemptStateInternal.FAIL_IN_PROGRESS,
              TaskAttemptStateInternal.SUBMITTED),
          TaskAttemptEventType.TA_OUTPUT_FAILED,
          new OutputReportedFailedTransition())
      // for recovery, needs to log the TA generated events in TaskAttemptFinishedEvent
      .addTransition(TaskAttemptStateInternal.SUBMITTED,
          TaskAttemptStateInternal.SUBMITTED,
          TaskAttemptEventType.TA_TEZ_EVENT_UPDATE,
          new TezEventUpdaterTransition())



      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.RUNNING,
          TaskAttemptEventType.TA_STATUS_UPDATE, STATUS_UPDATER)
      // Optional, may not come in for all tasks.
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.SUCCEEDED, TaskAttemptEventType.TA_DONE,
          new SucceededTransition())
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptEventType.TA_FAILED,
          new TerminatedWhileRunningTransition(FAILED_HELPER))
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptEventType.TA_TIMED_OUT,
          new TerminatedWhileRunningTransition(FAILED_HELPER))
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.KILL_IN_PROGRESS,
          TaskAttemptEventType.TA_KILL_REQUEST,
          new TerminatedWhileRunningTransition(KILLED_HELPER))
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.KILLED,
          TaskAttemptEventType.TA_KILLED,
          new TerminatedWhileRunningTransition(KILLED_HELPER))
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.KILL_IN_PROGRESS,
          TaskAttemptEventType.TA_NODE_FAILED,
          new TerminatedWhileRunningTransition(KILLED_HELPER))
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptEventType.TA_CONTAINER_TERMINATING,
          new TerminatedWhileRunningTransition(FAILED_HELPER))
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.FAILED,
          TaskAttemptEventType.TA_CONTAINER_TERMINATED,
          new ContainerCompletedWhileRunningTransition())
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.KILLED,
          TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM,
          new ContainerCompletedWhileRunningTransition(KILLED_HELPER))
      .addTransition(
          TaskAttemptStateInternal.RUNNING,
          EnumSet.of(TaskAttemptStateInternal.FAIL_IN_PROGRESS,
              TaskAttemptStateInternal.RUNNING),
          TaskAttemptEventType.TA_OUTPUT_FAILED,
          new OutputReportedFailedTransition())
       // for recovery, needs to log the TA generated events in TaskAttemptFinishedEvent    
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.RUNNING,
          TaskAttemptEventType.TA_TEZ_EVENT_UPDATE,
          new TezEventUpdaterTransition())

      .addTransition(TaskAttemptStateInternal.KILL_IN_PROGRESS,
          TaskAttemptStateInternal.KILLED,
          TaskAttemptEventType.TA_CONTAINER_TERMINATED,
          new ContainerCompletedWhileTerminating())
      .addTransition(
          TaskAttemptStateInternal.KILL_IN_PROGRESS,
          TaskAttemptStateInternal.KILL_IN_PROGRESS,
          EnumSet.of(TaskAttemptEventType.TA_STARTED_REMOTELY,
              TaskAttemptEventType.TA_SCHEDULE,
              TaskAttemptEventType.TA_SUBMITTED,
              TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM,
              TaskAttemptEventType.TA_TEZ_EVENT_UPDATE,
              TaskAttemptEventType.TA_STATUS_UPDATE,
              TaskAttemptEventType.TA_DONE, TaskAttemptEventType.TA_FAILED,
              TaskAttemptEventType.TA_TIMED_OUT,
              TaskAttemptEventType.TA_KILL_REQUEST,
              TaskAttemptEventType.TA_KILLED,
              TaskAttemptEventType.TA_NODE_FAILED,
              TaskAttemptEventType.TA_CONTAINER_TERMINATING,
              TaskAttemptEventType.TA_OUTPUT_FAILED))

      .addTransition(TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptStateInternal.FAILED,
          TaskAttemptEventType.TA_CONTAINER_TERMINATED,
          new ContainerCompletedWhileTerminating())
      .addTransition(
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          EnumSet.of(TaskAttemptEventType.TA_STARTED_REMOTELY,
              TaskAttemptEventType.TA_SCHEDULE,
              TaskAttemptEventType.TA_SUBMITTED,
              TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM,
              TaskAttemptEventType.TA_TEZ_EVENT_UPDATE,
              TaskAttemptEventType.TA_STATUS_UPDATE,
              TaskAttemptEventType.TA_DONE, TaskAttemptEventType.TA_FAILED,
              TaskAttemptEventType.TA_TIMED_OUT,
              TaskAttemptEventType.TA_KILL_REQUEST,
              TaskAttemptEventType.TA_KILLED,
              TaskAttemptEventType.TA_NODE_FAILED,
              TaskAttemptEventType.TA_CONTAINER_TERMINATING,
              TaskAttemptEventType.TA_OUTPUT_FAILED))

      .addTransition(
          TaskAttemptStateInternal.KILLED,
          TaskAttemptStateInternal.KILLED,
          EnumSet.of(TaskAttemptEventType.TA_STARTED_REMOTELY,
              TaskAttemptEventType.TA_SCHEDULE,
              TaskAttemptEventType.TA_SUBMITTED,
              TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM,
              TaskAttemptEventType.TA_TEZ_EVENT_UPDATE,
              TaskAttemptEventType.TA_STATUS_UPDATE,
              TaskAttemptEventType.TA_DONE, TaskAttemptEventType.TA_FAILED,
              TaskAttemptEventType.TA_TIMED_OUT,
              TaskAttemptEventType.TA_KILL_REQUEST,
              TaskAttemptEventType.TA_KILLED,
              TaskAttemptEventType.TA_NODE_FAILED,
              TaskAttemptEventType.TA_CONTAINER_TERMINATING,
              TaskAttemptEventType.TA_CONTAINER_TERMINATED,
              TaskAttemptEventType.TA_OUTPUT_FAILED))

      .addTransition(
          TaskAttemptStateInternal.FAILED,
          TaskAttemptStateInternal.FAILED,
          EnumSet.of(TaskAttemptEventType.TA_STARTED_REMOTELY,
              TaskAttemptEventType.TA_SCHEDULE,
              TaskAttemptEventType.TA_SUBMITTED,
              TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM,
              TaskAttemptEventType.TA_STATUS_UPDATE,
              TaskAttemptEventType.TA_DONE, TaskAttemptEventType.TA_FAILED,
              TaskAttemptEventType.TA_TIMED_OUT,
              TaskAttemptEventType.TA_KILL_REQUEST,
              TaskAttemptEventType.TA_KILLED,
              TaskAttemptEventType.TA_NODE_FAILED,
              TaskAttemptEventType.TA_CONTAINER_TERMINATING,
              TaskAttemptEventType.TA_CONTAINER_TERMINATED,
              TaskAttemptEventType.TA_OUTPUT_FAILED))

      // How will duplicate history events be handled ?
      // TODO Maybe consider not failing REDUCE tasks in this case. Also,
      // MAP_TASKS in case there's only one phase in the job.
      .addTransition(
          TaskAttemptStateInternal.SUCCEEDED,
          EnumSet.of(TaskAttemptStateInternal.KILLED,
              TaskAttemptStateInternal.SUCCEEDED),
          TaskAttemptEventType.TA_KILL_REQUEST,
          new TerminatedAfterSuccessTransition())
      .addTransition(
          TaskAttemptStateInternal.SUCCEEDED,
          EnumSet.of(TaskAttemptStateInternal.KILLED,
              TaskAttemptStateInternal.SUCCEEDED),
          TaskAttemptEventType.TA_KILLED,
          new TerminatedAfterSuccessTransition())
      .addTransition(
          TaskAttemptStateInternal.SUCCEEDED,
          EnumSet.of(TaskAttemptStateInternal.KILLED,
              TaskAttemptStateInternal.SUCCEEDED),
          TaskAttemptEventType.TA_NODE_FAILED,
          new TerminatedAfterSuccessTransition())
      .addTransition(
          TaskAttemptStateInternal.SUCCEEDED,
          EnumSet.of(TaskAttemptStateInternal.FAILED,
              TaskAttemptStateInternal.SUCCEEDED),
          TaskAttemptEventType.TA_OUTPUT_FAILED,
          new OutputReportedFailedTransition())
      .addTransition(
          TaskAttemptStateInternal.SUCCEEDED,
          TaskAttemptStateInternal.SUCCEEDED,
          EnumSet.of(TaskAttemptEventType.TA_TIMED_OUT,
              TaskAttemptEventType.TA_CONTAINER_TERMINATING,
              TaskAttemptEventType.TA_CONTAINER_TERMINATED,
              TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM))

        .installTopology();

  @SuppressWarnings("rawtypes")
  public TaskAttemptImpl(TezTaskAttemptID attemptId, EventHandler eventHandler,
      TaskCommunicatorManagerInterface taskCommunicatorManagerInterface, Configuration conf, Clock clock,
      TaskHeartbeatHandler taskHeartbeatHandler, AppContext appContext,
      boolean isRescheduled,
      Resource resource, ContainerContext containerContext, boolean leafVertex,
      Task task, TaskLocationHint locationHint, TaskSpec taskSpec) {
    this(attemptId, eventHandler, taskCommunicatorManagerInterface, conf, clock,
        taskHeartbeatHandler, appContext, isRescheduled, resource, containerContext, leafVertex,
        task, locationHint, taskSpec, null);
  }

  @SuppressWarnings("rawtypes")
  public TaskAttemptImpl(TezTaskAttemptID attemptId, EventHandler eventHandler,
      TaskCommunicatorManagerInterface taskCommunicatorManagerInterface, Configuration conf, Clock clock,
      TaskHeartbeatHandler taskHeartbeatHandler, AppContext appContext,
      boolean isRescheduled,
      Resource resource, ContainerContext containerContext, boolean leafVertex,
      Task task, TaskLocationHint locationHint, TaskSpec taskSpec,
      TezTaskAttemptID schedulingCausalTA) {

    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.readLock = rwLock.readLock();
    this.writeLock = rwLock.writeLock();
    this.attemptId = attemptId;
    this.eventHandler = eventHandler;
    //Reported status
    this.conf = conf;
    this.clock = clock;
    this.taskHeartbeatHandler = taskHeartbeatHandler;
    this.appContext = appContext;
    this.vertex = task.getVertex();
    this.task = task;
    this.locationHint = locationHint;
    this.taskSpec = taskSpec;
    this.creationCausalTA = schedulingCausalTA;
    this.creationTime = clock.getTime();

    this.reportedStatus = new TaskAttemptStatus(this.attemptId);
    initTaskAttemptStatus(reportedStatus);
    RackResolver.init(conf);
    this.stateMachine = stateMachineFactory.make(this);
    this.isRescheduled = isRescheduled;
    this.taskResource = resource;
    this.containerContext = containerContext;
    this.leafVertex = leafVertex;
    this.hungIntervalMax = conf.getLong(
        TezConfiguration.TEZ_TASK_PROGRESS_STUCK_INTERVAL_MS, 
        TezConfiguration.TEZ_TASK_PROGRESS_STUCK_INTERVAL_MS_DEFAULT);

    this.recoveryData = appContext.getDAGRecoveryData() == null ?
        null : appContext.getDAGRecoveryData().getTaskAttemptRecoveryData(attemptId);
  }

  @Override
  public TezTaskAttemptID getID() {
    return attemptId;
  }

  @Override
  public TezTaskID getTaskID() {
    return attemptId.getTaskID();
  }

  @Override
  public TezVertexID getVertexID() {
    return attemptId.getTaskID().getVertexID();
  }

  @Override
  public TezDAGID getDAGID() {
    return getVertexID().getDAGId();
  }
  
  public TezTaskAttemptID getSchedulingCausalTA() {
    return creationCausalTA;
  }

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
      //result.setShuffleFinishTime(this.reportedStatus.shuffleFinishTime);
      result.setDiagnosticInfo(StringUtils.join(getDiagnostics(), LINE_SEPARATOR));
      //result.setPhase(reportedStatus.phase);
      //result.setStateString(reportedStatus.statef);
      result.setCounters(getCounters());
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
  public TaskAttemptTerminationCause getTerminationCause() {
    return terminationCause;
  }

  @Override
  public TezCounters getCounters() {
    readLock.lock();
    try {
      reportedStatus.setLocalityCounter(this.localityCounter);
      TezCounters counters = reportedStatus.counters;
      if (counters == null) {
        counters = EMPTY_COUNTERS;
      }
      return counters;
    } finally {
      readLock.unlock();
    }
  }
  
  TaskStatistics getStatistics() {
    return this.statistics;
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
      return getStateNoLock();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskAttemptState getStateNoLock() {
    return getExternalState(stateMachine.getCurrentState());
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
  public Container getAssignedContainer() {
    readLock.lock();
    try {
      return container;
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


  /** @return task runtime duration in NS. */
  public long getDurationNs() {
    readLock.lock();
    try {
      if (isRecoveredDuration) {
        // NS values are not mappable between JVMs (per documentation, at
        // least), so just use the clock after recovery.
        return TimeUnit.MILLISECONDS.toNanos(launchTime == 0 ? 0
            : (finishTime == 0 ? clock.getTime() : finishTime) - launchTime);
      } else {
        long ft = (finishTimeNs == null ? System.nanoTime() : finishTimeNs);
        return (launchTimeNs == null) ? 0 : (ft - launchTimeNs);
      }
    } finally {
      readLock.unlock();
    }
  }

  public long getCreationTime() {
    readLock.lock();
    try {
      return creationTime;
    } finally {
      readLock.unlock();
    }
  }
  
  public TezTaskAttemptID getCreationCausalAttempt() {
    readLock.lock();
    try {
      return creationCausalTA;
    } finally {
      readLock.unlock();
    }
  }

  public long getAllocationTime() {
    readLock.lock();
    try {
      return allocationTime;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getScheduleTime() {
    readLock.lock();
    try {
      return scheduledTime;
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
  public Task getTask() {
    return task;
  }

  Vertex getVertex() {
    return vertex;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handle(TaskAttemptEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing TaskAttemptEvent " + event.getTaskAttemptID()
          + " of type " + event.getType() + " while in state "
          + getInternalState() + ". Event: " + event);
    }
    writeLock.lock();
    try {
      final TaskAttemptStateInternal oldState = getInternalState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state for "
            + this.attemptId, e);
        eventHandler.handle(new DAGEventDiagnosticsUpdate(
            this.attemptId.getTaskID().getVertexID().getDAGId(),
            "Invalid event " + event.getType() +
            " on TaskAttempt " + this.attemptId));
        eventHandler.handle(
            new DAGEvent(
                this.attemptId.getTaskID().getVertexID().getDAGId(),
                DAGEventType.INTERNAL_ERROR)
            );
      } catch (RuntimeException e) {
        LOG.error("Uncaught exception when handling event " + event.getType()
            + " at current state " + oldState + " for "
            + this.attemptId, e);
        eventHandler.handle(new DAGEventDiagnosticsUpdate(
            this.attemptId.getTaskID().getVertexID().getDAGId(),
            "Uncaught exception when handling event " + event.getType()
                + " on TaskAttempt " + this.attemptId
                + " at state " + oldState + ", error=" + e.getMessage()));
        eventHandler.handle(
            new DAGEvent(
                this.attemptId.getTaskID().getVertexID().getDAGId(),
                DAGEventType.INTERNAL_ERROR)
        );
      }
      if (oldState != getInternalState()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(attemptId + " TaskAttempt Transitioned from "
              + oldState + " to "
              + getInternalState() + " due to event "
              + event.getType());
        }
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
    case SUBMITTED:
      return TaskAttemptState.STARTING;
    case RUNNING:
      return TaskAttemptState.RUNNING;
    case FAILED:
    case FAIL_IN_PROGRESS:
      return TaskAttemptState.FAILED;
    case KILLED:
    case KILL_IN_PROGRESS:
      return TaskAttemptState.KILLED;
    case SUCCEEDED:
      return TaskAttemptState.SUCCEEDED;
    default:
      throw new TezUncheckedException("Attempt to convert invalid "
          + "stateMachineTaskAttemptState to externalTaskAttemptState: "
          + smState);
    }
  }

  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    this.eventHandler.handle(event);
  }

  // always called in write lock
  private void setFinishTime() {
    // set the finish time only if launch time is set
    if (launchTime != 0 && finishTime == 0) {
      finishTime = clock.getTime();
      // The default clock is not safe for measuring durations.
      finishTimeNs = System.nanoTime();
    }
  }

  // TOOD Merge some of these JobCounter events.
  private static DAGEventCounterUpdate createDAGCounterUpdateEventTALaunched(
      TaskAttemptImpl ta) {
    DAGEventCounterUpdate dagCounterEvent =
        new DAGEventCounterUpdate(
            ta.getDAGID()
            );
    dagCounterEvent.addCounterUpdate(DAGCounter.TOTAL_LAUNCHED_TASKS, 1);
    return dagCounterEvent;
  }

  private static DAGEventCounterUpdate createDAGCounterUpdateEventTAFinished(
      TaskAttemptImpl taskAttempt, TaskAttemptState taState) {
    DAGEventCounterUpdate jce =
        new DAGEventCounterUpdate(taskAttempt.getDAGID());

    if (taState == TaskAttemptState.FAILED) {
      jce.addCounterUpdate(DAGCounter.NUM_FAILED_TASKS, 1);
    } else if (taState == TaskAttemptState.KILLED) {
      jce.addCounterUpdate(DAGCounter.NUM_KILLED_TASKS, 1);
    } else if (taState == TaskAttemptState.SUCCEEDED ) {
      jce.addCounterUpdate(DAGCounter.NUM_SUCCEEDED_TASKS, 1);
    }

    long amSideWallClockTimeMs = TimeUnit.NANOSECONDS.toMillis(
        taskAttempt.getDurationNs());
    jce.addCounterUpdate(DAGCounter.WALL_CLOCK_MILLIS, amSideWallClockTimeMs);

    return jce;
  }

//  private static long computeSlotMillis(TaskAttemptImpl taskAttempt) {
//    int slotMemoryReq =
//        taskAttempt.taskResource.getMemory();
//
//    int minSlotMemSize =
//        taskAttempt.appContext.getClusterInfo().getMinContainerCapability()
//            .getMemory();
//
//    int simSlotsRequired =
//        minSlotMemSize == 0 ? 0 : (int) Math.ceil((float) slotMemoryReq
//            / minSlotMemSize);
//
//    long slotMillisIncrement =
//        simSlotsRequired
//            * (taskAttempt.getFinishTime() - taskAttempt.getLaunchTime());
//    return slotMillisIncrement;
//  }

  // TODO: JobHistory
  // TODO Change to return a JobHistoryEvent.
  /*
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
            taskAttempt.getDiagnostics(), LINE_SEPARATOR), taskAttempt
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
  */

//  private WrappedProgressSplitsBlock getProgressSplitBlock() {
//    return null;
//    // TODO
//    /*
//    readLock.lock();
//    try {
//      if (progressSplitBlock == null) {
//        progressSplitBlock = new WrappedProgressSplitsBlock(conf.getInt(
//            MRJobConfig.MR_AM_NUM_PROGRESS_SPLITS,
//            MRJobConfig.DEFAULT_MR_AM_NUM_PROGRESS_SPLITS));
//      }
//      return progressSplitBlock;
//    } finally {
//      readLock.unlock();
//    }
//    */
//  }

  /**
   * Records the launch time of the task.
   */
  private void setLaunchTime() {
    launchTime = clock.getTime();
    launchTimeNs = System.nanoTime();
  }

  private void updateProgressSplits() {
//    double newProgress = reportedStatus.progress;
//    newProgress = Math.max(Math.min(newProgress, 1.0D), 0.0D);
//    TezCounters counters = reportedStatus.counters;
//    if (counters == null)
//      return;
//
//    WrappedProgressSplitsBlock splitsBlock = getProgressSplitBlock();
//    if (splitsBlock != null) {
//      long now = clock.getTime();
//      long start = getLaunchTime();
//
//      if (start == 0)
//        return;
//
//      if (start != 0 && now - start <= Integer.MAX_VALUE) {
//        splitsBlock.getProgressWallclockTime().extend(newProgress,
//            (int) (now - start));
//      }
//
//      TezCounter cpuCounter = counters.findCounter(TaskCounter.CPU_MILLISECONDS);
//      if (cpuCounter != null && cpuCounter.getValue() <= Integer.MAX_VALUE) {
//        splitsBlock.getProgressCPUTime().extend(newProgress,
//            (int) cpuCounter.getValue()); // long to int? TODO: FIX. Same below
//      }
//
//      TezCounter virtualBytes = counters
//        .findCounter(TaskCounter.VIRTUAL_MEMORY_BYTES);
//      if (virtualBytes != null) {
//        splitsBlock.getProgressVirtualMemoryKbytes().extend(newProgress,
//            (int) (virtualBytes.getValue() / (MEMORY_SPLITS_RESOLUTION)));
//      }
//
//      TezCounter physicalBytes = counters
//        .findCounter(TaskCounter.PHYSICAL_MEMORY_BYTES);
//      if (physicalBytes != null) {
//        splitsBlock.getProgressPhysicalMemoryKbytes().extend(newProgress,
//            (int) (physicalBytes.getValue() / (MEMORY_SPLITS_RESOLUTION)));
//      }
//    }
  }

  private void sendTaskAttemptCleanupEvent() {
//    TaskAttemptContext taContext =
//        new TaskAttemptContextImpl(this.conf,
//            TezMRTypeConverter.fromTez(this.attemptId));
//    sendEvent(new TaskCleanupEvent(this.attemptId, this.committer, taContext));
  }
  
  private TaskLocationHint getTaskLocationHint() {
    return locationHint;
  }

  protected String[] resolveHosts(String[] src) {
    return TaskAttemptImplHelpers.resolveHosts(src);
  }

  protected void logJobHistoryAttemptStarted() {
    Preconditions.checkArgument(recoveryData == null);
    String inProgressLogsUrl = getInProgressLogsUrl();
    String completedLogsUrl = getCompletedLogsUrl();
    TaskAttemptStartedEvent startEvt = new TaskAttemptStartedEvent(
        attemptId, getVertex().getName(),
        launchTime, containerId, containerNodeId,
        inProgressLogsUrl, completedLogsUrl, nodeHttpAddress);
    this.appContext.getHistoryHandler().handle(
        new DAGHistoryEvent(getDAGID(), startEvt));
  }

  protected void logJobHistoryAttemptFinishedEvent(TaskAttemptStateInternal state) {
    Preconditions.checkArgument(recoveryData == null
        || recoveryData.getTaskAttemptFinishedEvent() == null,
        "log TaskAttemptFinishedEvent again in recovery when there's already another TaskAtttemptFinishedEvent");
    if (getLaunchTime() == 0) return;

    TaskAttemptFinishedEvent finishEvt = new TaskAttemptFinishedEvent(
        attemptId, getVertex().getName(), getLaunchTime(),
        getFinishTime(), TaskAttemptState.SUCCEEDED, null, null,
        "", getCounters(), lastDataEvents, taGeneratedEvents,
        creationTime, creationCausalTA, allocationTime,
        null, null, null, null, null);
    // FIXME how do we store information regd completion events
    this.appContext.getHistoryHandler().handle(
        new DAGHistoryEvent(getDAGID(), finishEvt));
  }

  protected void logJobHistoryAttemptUnsuccesfulCompletion(
      TaskAttemptState state, TaskFailureType taskFailureType) {
    Preconditions.checkArgument(recoveryData == null
        || recoveryData.getTaskAttemptFinishedEvent() == null,
        "log TaskAttemptFinishedEvent again in recovery when there's already another TaskAtttemptFinishedEvent");
    if (state == TaskAttemptState.FAILED && taskFailureType == null) {
      throw new IllegalStateException("FAILED state must be accompanied by a FailureType");
    }
    long finishTime = getFinishTime();
    ContainerId unsuccessfulContainerId = null;
    NodeId unsuccessfulContainerNodeId = null;
    String inProgressLogsUrl = null;
    String completedLogsUrl = null;
    if (finishTime <= 0) {
      finishTime = clock.getTime(); // comes here in case it was terminated before launch
      unsuccessfulContainerId = containerId;
      unsuccessfulContainerNodeId = containerNodeId;
      inProgressLogsUrl = getInProgressLogsUrl();
      completedLogsUrl = getCompletedLogsUrl();
    }
    TaskAttemptFinishedEvent finishEvt = new TaskAttemptFinishedEvent(
        attemptId, getVertex().getName(), getLaunchTime(),
        finishTime, state,
        taskFailureType,
        terminationCause,
        StringUtils.join(
            getDiagnostics(), LINE_SEPARATOR), getCounters(), lastDataEvents,
        taGeneratedEvents, creationTime, creationCausalTA, allocationTime,
        unsuccessfulContainerId, unsuccessfulContainerNodeId, inProgressLogsUrl, completedLogsUrl, nodeHttpAddress);
    // FIXME how do we store information regd completion events
    this.appContext.getHistoryHandler().handle(
        new DAGHistoryEvent(getDAGID(), finishEvt));
  }

  private String getInProgressLogsUrl() {
    String inProgressLogsUrl = null;
    if (getVertex().getServicePluginInfo().getContainerLauncherName().equals(
          TezConstants.getTezYarnServicePluginName())
        || getVertex().getServicePluginInfo().getContainerLauncherName().equals(
          TezConstants.getTezUberServicePluginName())) {
      if (containerId != null && nodeHttpAddress != null) {
        final String containerIdStr = containerId.toString();
        inProgressLogsUrl = nodeHttpAddress
            + "/" + "node/containerlogs"
            + "/" + containerIdStr
            + "/" + this.appContext.getUser();
      }
    } else {
      inProgressLogsUrl = appContext.getTaskCommunicatorManager().getInProgressLogsUrl(
          getVertex().getTaskCommunicatorIdentifier(),
          attemptId, containerNodeId);
    }
    return inProgressLogsUrl;
  }

  private String getCompletedLogsUrl() {
    String completedLogsUrl = null;
    if (getVertex().getServicePluginInfo().getContainerLauncherName().equals(
          TezConstants.getTezYarnServicePluginName())
        || getVertex().getServicePluginInfo().getContainerLauncherName().equals(
          TezConstants.getTezUberServicePluginName())) {
      if (containerId != null && containerNodeId != null && nodeHttpAddress != null) {
        final String containerIdStr = containerId.toString();
        if (conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
            YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)
            && conf.get(YarnConfiguration.YARN_LOG_SERVER_URL) != null) {
          String contextStr = "v_" + getVertex().getName()
              + "_" + this.attemptId.toString();
          completedLogsUrl = conf.get(YarnConfiguration.YARN_LOG_SERVER_URL)
              + "/" + containerNodeId.toString()
              + "/" + containerIdStr
              + "/" + contextStr
              + "/" + this.appContext.getUser();
        }
      }
    } else {
      completedLogsUrl = appContext.getTaskCommunicatorManager().getCompletedLogsUrl(
          getVertex().getTaskCommunicatorIdentifier(),
          attemptId, containerNodeId);
    }
    return completedLogsUrl;
  }

  //////////////////////////////////////////////////////////////////////////////
  //                   Start of Transition Classes                            //
  //////////////////////////////////////////////////////////////////////////////

  protected static class ScheduleTaskattemptTransition implements
    MultipleArcTransition<TaskAttemptImpl, TaskAttemptEvent, TaskAttemptStateInternal> {

    @Override
    public TaskAttemptStateInternal transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      if (ta.recoveryData != null) {
        TaskAttemptStartedEvent taStartedEvent =
            ta.recoveryData.getTaskAttemptStartedEvent();
        if (taStartedEvent != null) {
          ta.launchTime = taStartedEvent.getStartTime();
          ta.isRecoveredDuration = true;
          TaskAttemptFinishedEvent taFinishedEvent =
              ta.recoveryData.getTaskAttemptFinishedEvent();
          if (taFinishedEvent == null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Only TaskAttemptStartedEvent but no TaskAttemptFinishedEvent, "
                  + "send out TaskAttemptEventAttemptKilled to move it to KILLED");
            }
            ta.sendEvent(new TaskAttemptEventAttemptKilled(ta.getID(), 
                "Task Attempt killed in recovery due to can't recover the running task attempt",
                TaskAttemptTerminationCause.TERMINATED_AT_RECOVERY, true));
            return TaskAttemptStateInternal.NEW;
          }
        }
        // No matter whether TaskAttemptStartedEvent is seen, send corresponding event to move 
        // TA to the state of TaskAttemptFinishedEvent
        TaskAttemptFinishedEvent taFinishedEvent =
            ta.recoveryData.getTaskAttemptFinishedEvent();
        Preconditions.checkArgument(taFinishedEvent != null, "Both of TaskAttemptStartedEvent and TaskFinishedEvent is null,"
            + "taskAttemptId=" + ta.getID());
        switch (taFinishedEvent.getState()) {
          case FAILED:
            if (LOG.isDebugEnabled()) {
              LOG.debug("TaskAttemptFinishedEvent is seen with state of FAILED"
                  + ", send TA_FAILED to itself"
                  + ", attemptId=" + ta.attemptId);
            }
            ta.sendEvent(new TaskAttemptEventAttemptFailed(ta.getID(), TaskAttemptEventType.TA_FAILED,
                taFinishedEvent.getTaskFailureType(),
                taFinishedEvent.getDiagnostics(), taFinishedEvent.getTaskAttemptError(), true));
            break;
          case KILLED:
            if (LOG.isDebugEnabled()) {
              LOG.debug("TaskAttemptFinishedEvent is seen with state of KILLED"
                  + ", send TA_KILLED to itself"
                  + ", attemptId=" + ta.attemptId);
            }
            ta.sendEvent(new TaskAttemptEventAttemptKilled(ta.getID(),
                taFinishedEvent.getDiagnostics(), taFinishedEvent.getTaskAttemptError(), true));
            break;
          case SUCCEEDED:
            if (LOG.isDebugEnabled()) {
              LOG.debug("TaskAttemptFinishedEvent is seen with state of SUCCEEDED"
                  + ", send TA_DONE to itself"
                  + ", attemptId=" + ta.attemptId);
            }
            ta.sendEvent(new TaskAttemptEvent(ta.getID(), TaskAttemptEventType.TA_DONE));
            break;
          default:
            throw new TezUncheckedException("Invalid state in TaskAttemptFinishedEvent, state=" 
                + taFinishedEvent.getState() + ", taId=" + ta.getID());
        }
        return TaskAttemptStateInternal.NEW;
      }

      TaskAttemptEventSchedule scheduleEvent = (TaskAttemptEventSchedule) event;
      ta.scheduledTime = ta.clock.getTime();

      // Create startTaskRequest

      String[] requestHosts = new String[0];

      // Compute node/rack location request even if re-scheduled.
      Set<String> racks = new HashSet<String>();
      // TODO Post TEZ-2003. Allow for a policy in the VMPlugin to define locality for different attempts.
      TaskLocationHint locationHint = ta.getTaskLocationHint();
      if (locationHint != null) {
        if (locationHint.getRacks() != null) {
          racks.addAll(locationHint.getRacks());
        }
        if (locationHint.getHosts() != null) {
          for (String host : locationHint.getHosts()) {
            racks.add(RackResolver.resolve(host).getNetworkLocation());
          }
          requestHosts = ta.resolveHosts(locationHint.getHosts()
              .toArray(new String[locationHint.getHosts().size()]));
        }
      }

      ta.taskHosts.addAll(Arrays.asList(requestHosts));
      ta.taskRacks = racks;

      // Ask for hosts / racks only if not a re-scheduled task.
      if (ta.isRescheduled && ta.getVertex().getVertexConfig().getTaskRescheduleRelaxedLocality()) {
        locationHint = null;
      }

      LOG.debug("Asking for container launch with taskAttemptContext: {}", ta.taskSpec);
      
      // Send out a launch request to the scheduler.
      int priority;
      if (ta.isRescheduled  && ta.getVertex().getVertexConfig().getTaskRescheduleHigherPriority()) {
        // higher priority for rescheduled attempts
        priority = scheduleEvent.getPriorityHighLimit();
      } else {
        priority = (scheduleEvent.getPriorityHighLimit() + scheduleEvent.getPriorityLowLimit()) / 2;
      }

      // TODO Jira post TEZ-2003 getVertex implementation is very inefficient. This should be via references, instead of locked table lookups.
      Vertex vertex = ta.getVertex();
      AMSchedulerEventTALaunchRequest launchRequestEvent = new AMSchedulerEventTALaunchRequest(
          ta.attemptId, ta.taskResource, ta.taskSpec, ta, locationHint,
          priority, ta.containerContext,
          vertex.getTaskSchedulerIdentifier(),
          vertex.getContainerLauncherIdentifier(),
          vertex.getTaskCommunicatorIdentifier());

      ta.sendEvent(launchRequestEvent);
      return TaskAttemptStateInternal.START_WAIT;
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
      // This transition should not be invoked directly, if a scheduler event has already been sent out.
      // Sub-classes should be used if a scheduler request has been sent.

      // in both normal and recovery flow make sure diagnostics etc. are correctly assigned
      if (event instanceof DiagnosableEvent) {
        ta.addDiagnosticInfo(((DiagnosableEvent) event).getDiagnosticInfo());
      }
      if (event instanceof TaskAttemptEventTerminationCauseEvent) {
        ta.trySetTerminationCause(((TaskAttemptEventTerminationCauseEvent) event).getTerminationCause());
      } else {
        throw new TezUncheckedException("Invalid event received in TerminateTransition"
                + ", requiredClass=TaskAttemptEventTerminationCauseEvent"
                + ", eventClass=" + event.getClass().getName());
      }

      if (event instanceof TaskAttemptEventContainerTerminated) {
        TaskAttemptEventContainerTerminated tEvent = (TaskAttemptEventContainerTerminated) event;
        AMContainer amContainer = ta.appContext.getAllContainers().get(tEvent.getContainerId());
        Container container = amContainer.getContainer();

        ta.allocationTime = amContainer.getCurrentTaskAttemptAllocationTime();
        ta.container = container;
        ta.containerId = tEvent.getContainerId();
        ta.containerNodeId = container.getNodeId();
        ta.nodeHttpAddress = StringInterner.weakIntern(container.getNodeHttpAddress());
      }

      if (event instanceof TaskAttemptEventContainerTerminatedBySystem) {
        TaskAttemptEventContainerTerminatedBySystem tEvent = (TaskAttemptEventContainerTerminatedBySystem) event;
        AMContainer amContainer = ta.appContext.getAllContainers().get(tEvent.getContainerId());
        Container container = amContainer.getContainer();

        ta.allocationTime = amContainer.getCurrentTaskAttemptAllocationTime();
        ta.container = container;
        ta.containerId = tEvent.getContainerId();
        ta.containerNodeId = container.getNodeId();
        ta.nodeHttpAddress = StringInterner.weakIntern(container.getNodeHttpAddress());
      }

      if (ta.recoveryData == null ||
          ta.recoveryData.getTaskAttemptFinishedEvent() == null) {
        ta.setFinishTime();
        ta.logJobHistoryAttemptUnsuccesfulCompletion(helper
            .getTaskAttemptState(), helper.getFailureType(event));
      } else {
        ta.finishTime = ta.recoveryData.getTaskAttemptFinishedEvent().getFinishTime();
        ta.isRecoveredDuration = true;
      }

      if (event instanceof RecoveryEvent) {
        RecoveryEvent rEvent = (RecoveryEvent)event;
        if (rEvent.isFromRecovery()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Faked TerminateEvent from recovery, taskAttemptId=" + ta.getID());
          }
        }
      }
      ta.sendEvent(createDAGCounterUpdateEventTAFinished(ta,
          helper.getTaskAttemptState()));
      // Send out events to the Task - indicating TaskAttemptTermination(F/K)
      ta.sendEvent(helper.getTaskEvent(ta.attemptId, event));
    }
  }

  protected static class SubmittedTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent origEvent) {
      TaskAttemptEventSubmitted event = (TaskAttemptEventSubmitted) origEvent;

      AMContainer amContainer = ta.appContext.getAllContainers().get(event.getContainerId());
      Container container = amContainer.getContainer();

      ta.allocationTime = amContainer.getCurrentTaskAttemptAllocationTime();
      ta.container = container;
      ta.containerId = event.getContainerId();
      ta.containerNodeId = container.getNodeId();
      ta.nodeHttpAddress = StringInterner.weakIntern(container.getNodeHttpAddress());
      ta.nodeRackName = StringInterner.weakIntern(RackResolver.resolve(ta.containerNodeId.getHost())
          .getNetworkLocation());
      ta.lastNotifyProgressTimestamp = ta.clock.getTime();

      ta.setLaunchTime();

      // TODO Resolve to host / IP in case of a local address.
      InetSocketAddress nodeHttpInetAddr = NetUtils
          .createSocketAddr(ta.nodeHttpAddress); // TODO: Costly?
      ta.trackerName = StringInterner.weakIntern(nodeHttpInetAddr.getHostName());
      ta.httpPort = nodeHttpInetAddr.getPort();
      ta.sendEvent(createDAGCounterUpdateEventTALaunched(ta));

      LOG.info("TaskAttempt: [" + ta.attemptId + "] submitted."
          + " Is using containerId: [" + ta.containerId + "]" + " on NM: ["
          + ta.containerNodeId + "]");

      // JobHistoryEvent.
      // The started event represents when the attempt was submitted to the executor.
      ta.logJobHistoryAttemptStarted();

      // TODO Remove after HDFS-5098
      // Compute LOCALITY counter for this task.
      if (ta.taskHosts.contains(ta.containerNodeId.getHost())) {
        ta.localityCounter = DAGCounter.DATA_LOCAL_TASKS;
      } else if (ta.taskRacks.contains(ta.nodeRackName)) {
        ta.localityCounter = DAGCounter.RACK_LOCAL_TASKS;
      } else {
        // Not computing this if the task does not have locality information.
        if (ta.getTaskLocationHint() != null) {
          ta.localityCounter = DAGCounter.OTHER_LOCAL_TASKS;
        }
      }

      // Inform the Task
      ta.sendEvent(new TaskEventTALaunched(ta.attemptId));

      if (ta.isSpeculationEnabled()) {
        ta.sendEvent(new SpeculatorEventTaskAttemptStatusUpdate(ta.attemptId, TaskAttemptState.RUNNING,
            ta.launchTime, true));
      }

      ta.sendEvent(
          new AMSchedulerEventTAStateUpdated(ta, TaskScheduler.SchedulerTaskState.SUBMITTED,
              ta.getVertex().getTaskSchedulerIdentifier()));
      ta.taskHeartbeatHandler.register(ta.attemptId);
    }
  }

  protected static class StartedTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent taskAttemptEvent) {
      ta.sendEvent(
          new AMSchedulerEventTAStateUpdated(ta, TaskScheduler.SchedulerTaskState.STARTED,
              ta.getVertex().getTaskSchedulerIdentifier()));
      // Nothing specific required for recovery, since recovery processes the START/END events
      // only and moves the attempt to a final state, or an initial state.
    }
  }
  
  private boolean isSpeculationEnabled() {
    return conf.getBoolean(TezConfiguration.TEZ_AM_SPECULATION_ENABLED,
        TezConfiguration.TEZ_AM_SPECULATION_ENABLED_DEFAULT);
  }

  protected static class TerminatedBeforeRunningTransition extends
      TerminateTransition {

    public TerminatedBeforeRunningTransition(
        TerminatedTransitionHelper helper) {
      super(helper);
    }
    
    protected boolean sendSchedulerEvent() {
      return true;
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      // Inform the scheduler
      if (sendSchedulerEvent()) {
        ta.sendEvent(new AMSchedulerEventTAEnded(ta, ta.containerId, helper
            .getTaskAttemptState(), TezUtilsInternal.toTaskAttemptEndReason(ta.terminationCause),
            ta instanceof DiagnosableEvent ? ((DiagnosableEvent)ta).getDiagnosticInfo() : null,
            ta.getVertex().getTaskSchedulerIdentifier()));
      }
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
    }
  }

  protected static class ContainerCompletedBeforeRunningTransition extends
      TerminatedBeforeRunningTransition {
    public ContainerCompletedBeforeRunningTransition() {
      super(FAILED_HELPER);
    }
    
    public ContainerCompletedBeforeRunningTransition(TerminatedTransitionHelper helper) {
      super(helper);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.sendTaskAttemptCleanupEvent();
    }

  }

  protected static class StatusUpdaterTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      TaskAttemptEventStatusUpdate sEvent = (TaskAttemptEventStatusUpdate) event; 
      TaskStatusUpdateEvent statusEvent = sEvent.getStatusEvent();
      ta.reportedStatus.state = ta.getState();
      ta.reportedStatus.progress = statusEvent.getProgress();
      if (statusEvent.getCounters() != null) {
        ta.reportedStatus.counters = statusEvent.getCounters();
      }
      if (statusEvent.getStatistics() != null) {
        ta.statistics = statusEvent.getStatistics();
      }
      if (statusEvent.getProgressNotified()) {
        ta.lastNotifyProgressTimestamp = ta.clock.getTime();
      } else {
        long currTime = ta.clock.getTime();
        if (ta.hungIntervalMax > 0 && ta.lastNotifyProgressTimestamp > 0 &&
            currTime - ta.lastNotifyProgressTimestamp > ta.hungIntervalMax) {
          // task is hung
          String diagnostics = "Attempt failed because it appears to make no progress for " + 
          ta.hungIntervalMax + "ms";
          LOG.info(diagnostics + " " + ta.getID());
          // send event that will fail this attempt
          ta.sendEvent(
              new TaskAttemptEventAttemptFailed(ta.getID(),
                  TaskAttemptEventType.TA_FAILED,
                  TaskFailureType.NON_FATAL,
                  diagnostics, 
                  TaskAttemptTerminationCause.NO_PROGRESS)
              );
        }
      }
      
      if (sEvent.getReadErrorReported()) {
        // if there is a read error then track the next last data event
        ta.appendNextDataEvent = true;
      }

      ta.updateProgressSplits();

      if (ta.isSpeculationEnabled()) {
        ta.sendEvent(new SpeculatorEventTaskAttemptStatusUpdate(ta.attemptId, ta.getState(),
            ta.clock.getTime()));
      }
    }
  }

  protected static class TezEventUpdaterTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      TaskAttemptEventTezEventUpdate tezEventUpdate = (TaskAttemptEventTezEventUpdate)event;
      ta.taGeneratedEvents.addAll(tezEventUpdate.getTezEvents());
    }
  }

  protected static class SucceededTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {

      // If TaskAttempt is recovered to SUCCEEDED, send events generated by this TaskAttempt to vertex
      // for its downstream consumers. For normal dag execution, the events are sent by TaskAttmeptListener
      // for performance consideration.
      if (ta.recoveryData != null && ta.recoveryData.isTaskAttemptSucceeded()) {
        TaskAttemptFinishedEvent taFinishedEvent = ta.recoveryData
            .getTaskAttemptFinishedEvent();
        if (LOG.isDebugEnabled()) {
          LOG.debug("TaskAttempt is recovered to SUCCEEDED, attemptId=" + ta.attemptId);
        }
        ta.reportedStatus.counters = taFinishedEvent.getCounters();
        List<TezEvent> tezEvents = taFinishedEvent.getTAGeneratedEvents();
        if (tezEvents != null && !tezEvents.isEmpty()) {
          ta.sendEvent(new VertexEventRouteEvent(ta.getVertexID(), tezEvents));
        }
        ta.finishTime = taFinishedEvent.getFinishTime();
        ta.isRecoveredDuration = true;
      } else {
        ta.setFinishTime();
        // Send out history event.
        ta.logJobHistoryAttemptFinishedEvent(TaskAttemptStateInternal.SUCCEEDED);
      }

      ta.sendEvent(createDAGCounterUpdateEventTAFinished(ta,
          TaskAttemptState.SUCCEEDED));

      // Inform the Scheduler.
      ta.sendEvent(new AMSchedulerEventTAEnded(ta, ta.containerId,
          TaskAttemptState.SUCCEEDED, null, null, ta.getVertex().getTaskSchedulerIdentifier()));

      // Inform the task.
      ta.sendEvent(new TaskEventTASucceeded(ta.attemptId));

      // Unregister from the TaskHeartbeatHandler.
      ta.taskHeartbeatHandler.unregister(ta.attemptId);
      
      ta.reportedStatus.state = TaskAttemptState.SUCCEEDED;
      ta.reportedStatus.progress = 1.0f;
      
      if (ta.isSpeculationEnabled()) {
        ta.sendEvent(new SpeculatorEventTaskAttemptStatusUpdate(ta.attemptId, TaskAttemptState.SUCCEEDED,
            ta.clock.getTime()));
      }

      // TODO maybe. For reuse ... Stacking pulls for a reduce task, even if the
      // TA finishes independently. // Will likely be the Job's responsibility.

    }
  }

  protected static class TerminatedWhileRunningTransition extends
      TerminatedBeforeRunningTransition {

    public TerminatedWhileRunningTransition(
        TerminatedTransitionHelper helper) {
      super(helper);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.taskHeartbeatHandler.unregister(ta.attemptId);
      ta.reportedStatus.state = helper.getTaskAttemptState(); // FAILED or KILLED
      if (ta.isSpeculationEnabled()) {
        ta.sendEvent(new SpeculatorEventTaskAttemptStatusUpdate(ta.attemptId, helper.getTaskAttemptState(),
            ta.clock.getTime()));
      }
    }
  }

  protected static class ContainerCompletedWhileRunningTransition extends
      TerminatedWhileRunningTransition {
    public ContainerCompletedWhileRunningTransition() {
      super(FAILED_HELPER);
    }
    
    public ContainerCompletedWhileRunningTransition(TerminatedTransitionHelper helper) {
      super(helper);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.sendTaskAttemptCleanupEvent();
    }
  }

  protected static class ContainerCompletedWhileTerminating implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      ta.sendTaskAttemptCleanupEvent();
      TaskAttemptEventContainerTerminated tEvent = (TaskAttemptEventContainerTerminated) event;
      ta.addDiagnosticInfo(tEvent.getDiagnosticInfo());
    }

  }

  protected static class TerminatedAfterSuccessHelper extends
      TerminatedBeforeRunningTransition {

    @Override
    protected boolean sendSchedulerEvent() {
      // since the success transition would have sent the event
      // there is no need to send it again
      return false;
    }
    
    public TerminatedAfterSuccessHelper(TerminatedTransitionHelper helper) {
      super(helper);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.sendTaskAttemptCleanupEvent();
    }

  }

  protected static class TerminatedAfterSuccessTransition implements
      MultipleArcTransition<TaskAttemptImpl, TaskAttemptEvent, TaskAttemptStateInternal> {
    @Override
    public TaskAttemptStateInternal transition(TaskAttemptImpl attempt, TaskAttemptEvent event) {
      boolean fromRecovery = (event instanceof RecoveryEvent
          && ((RecoveryEvent) event).isFromRecovery());
      attempt.recoveryData = null;
      if (!fromRecovery && attempt.leafVertex) {
        return TaskAttemptStateInternal.SUCCEEDED;
      }
      // TODO - TEZ-834. This assumes that the outputs were on that node
      attempt.sendInputFailedToConsumers();
      TaskAttemptImpl.TERMINATED_AFTER_SUCCESS_HELPER.transition(attempt, event);
      return TaskAttemptStateInternal.KILLED;
    }
  }
  
  protected static class OutputReportedFailedTransition implements
  MultipleArcTransition<TaskAttemptImpl, TaskAttemptEvent, TaskAttemptStateInternal> {

    @Override
    public TaskAttemptStateInternal transition(TaskAttemptImpl attempt,
        TaskAttemptEvent event) {
      TaskAttemptEventOutputFailed outputFailedEvent = 
          (TaskAttemptEventOutputFailed) event;
      TezEvent tezEvent = outputFailedEvent.getInputFailedEvent();
      TezTaskAttemptID failedDestTaId = tezEvent.getSourceInfo().getTaskAttemptID();
      InputReadErrorEvent readErrorEvent = (InputReadErrorEvent)tezEvent.getEvent();
      int failedInputIndexOnDestTa = readErrorEvent.getIndex();
      if (readErrorEvent.getVersion() != attempt.getID().getId()) {
        throw new TezUncheckedException(attempt.getID()
            + " incorrectly blamed for read error from " + failedDestTaId
            + " at inputIndex " + failedInputIndexOnDestTa + " version"
            + readErrorEvent.getVersion());
      }
      LOG.info(attempt.getID()
            + " blamed for read error from " + failedDestTaId
            + " at inputIndex " + failedInputIndexOnDestTa);
      long time = attempt.clock.getTime();
      Long firstErrReportTime = attempt.uniquefailedOutputReports.get(failedDestTaId);
      if (firstErrReportTime == null) {
        attempt.uniquefailedOutputReports.put(failedDestTaId, time);
        firstErrReportTime = time;
      }

      int maxAllowedOutputFailures = attempt.getVertex().getVertexConfig()
          .getMaxAllowedOutputFailures();
      int maxAllowedTimeForTaskReadErrorSec = attempt.getVertex()
          .getVertexConfig().getMaxAllowedTimeForTaskReadErrorSec();
      double maxAllowedOutputFailuresFraction = attempt.getVertex()
          .getVertexConfig().getMaxAllowedOutputFailuresFraction();

      int readErrorTimespanSec = (int)((time - firstErrReportTime)/1000);
      boolean crossTimeDeadline = readErrorTimespanSec >= maxAllowedTimeForTaskReadErrorSec;

      int runningTasks = attempt.appContext.getCurrentDAG().getVertex(
          failedDestTaId.getTaskID().getVertexID()).getRunningTasks();
      float failureFraction = runningTasks > 0 ? ((float) attempt.uniquefailedOutputReports.size()) / runningTasks : 0;
      boolean withinFailureFractionLimits =
          (failureFraction <= maxAllowedOutputFailuresFraction);
      boolean withinOutputFailureLimits =
          (attempt.uniquefailedOutputReports.size() < maxAllowedOutputFailures);

      // If needed we can launch a background task without failing this task
      // to generate a copy of the output just in case.
      // If needed we can consider only running consumer tasks
      if (!crossTimeDeadline && withinFailureFractionLimits && withinOutputFailureLimits) {
        return attempt.getInternalState();
      }
      String message = attempt.getID() + " being failed for too many output errors. "
          + "failureFraction=" + failureFraction
          + ", MAX_ALLOWED_OUTPUT_FAILURES_FRACTION="
          + maxAllowedOutputFailuresFraction
          + ", uniquefailedOutputReports=" + attempt.uniquefailedOutputReports.size()
          + ", MAX_ALLOWED_OUTPUT_FAILURES=" + maxAllowedOutputFailures
          + ", MAX_ALLOWED_TIME_FOR_TASK_READ_ERROR_SEC="
          + maxAllowedTimeForTaskReadErrorSec
          + ", readErrorTimespan=" + readErrorTimespanSec;
      LOG.info(message);
      attempt.addDiagnosticInfo(message);
      // send input failed event
      attempt.sendInputFailedToConsumers();
      // Not checking for leafVertex since a READ_ERROR should only be reported for intermediate tasks.
      if (attempt.getInternalState() == TaskAttemptStateInternal.SUCCEEDED) {
        (new TerminatedAfterSuccessHelper(FAILED_HELPER)).transition(
            attempt, event);
        return TaskAttemptStateInternal.FAILED;
      } else {
        (new TerminatedWhileRunningTransition(FAILED_HELPER)).transition(
            attempt, event);
        return TaskAttemptStateInternal.FAIL_IN_PROGRESS;
      }
      // TODO at some point. Nodes may be interested in FetchFailure info.
      // Can be used to blacklist nodes.
    }
  }
  
  @VisibleForTesting
  protected void sendInputFailedToConsumers() {
    Vertex vertex = getVertex();
    Map<Vertex, Edge> edges = vertex.getOutputVertices();
    if (edges != null && !edges.isEmpty()) {
      List<TezEvent> tezIfEvents = Lists.newArrayListWithCapacity(edges.size());
      for (Vertex edgeVertex : edges.keySet()) {
        tezIfEvents.add(new TezEvent(new InputFailedEvent(), 
            new EventMetaData(EventProducerConsumerType.SYSTEM, 
                vertex.getName(), 
                edgeVertex.getName(), 
                getID()), appContext.getClock().getTime()));
      }
      sendEvent(new VertexEventRouteEvent(vertex.getVertexId(), tezIfEvents));
    }
  }
  
  private void trySetTerminationCause(TaskAttemptTerminationCause err) {
    // keep only the first error cause
    if (terminationCause == TaskAttemptTerminationCause.UNKNOWN_ERROR) {
      terminationCause = err;
    }
  }

  private void initTaskAttemptStatus(TaskAttemptStatus result) {
    result.progress = 0.0f;
    // result.phase = Phase.STARTING;
    //result.stateString = "NEW";
    result.state = TaskAttemptState.NEW;
    //TezCounters counters = EMPTY_COUNTERS;
    //result.counters = counters;
  }

  private void addDiagnosticInfo(String diag) {
    if (diag != null && !diag.equals("")) {
      diagnostics.add(diag);
    }
  }

  protected interface TerminatedTransitionHelper {

    TaskAttemptStateInternal getTaskAttemptStateInternal();

    TaskAttemptState getTaskAttemptState();

    TaskEvent getTaskEvent(TezTaskAttemptID taskAttemptId, TaskAttemptEvent event);

    TaskFailureType getFailureType(TaskAttemptEvent event);
  }

  protected static class FailedTransitionHelper implements
      TerminatedTransitionHelper {
    public TaskAttemptStateInternal getTaskAttemptStateInternal() {
      return TaskAttemptStateInternal.FAILED;
    }

    @Override
    public TaskAttemptState getTaskAttemptState() {
      return TaskAttemptState.FAILED;
    }

    @Override
    public TaskEvent getTaskEvent(TezTaskAttemptID taskAttemptId,
                                  TaskAttemptEvent event) {
      return new TaskEventTAFailed(taskAttemptId, getFailureType(event), event);
    }

    @Override
    public TaskFailureType getFailureType(TaskAttemptEvent event) {
      if (event instanceof TaskAttemptEventAttemptFailed) {
        return ( ((TaskAttemptEventAttemptFailed) event).getTaskFailureType());
      } else {
        // For alternate failure scenarios like OUTPUT_FAILED, CONTAINER_TERMINATING, NODE_FAILED, etc
        return TaskFailureType.NON_FATAL;
      }
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
    public TaskEvent getTaskEvent(TezTaskAttemptID taskAttemptId,
                                  TaskAttemptEvent event) {
      return new TaskEventTAKilled(taskAttemptId, event);
    }

    @Override
    public TaskFailureType getFailureType(TaskAttemptEvent event) {
      return null;
    }
  }

  @Override
  public String toString() {
    return getID().toString();
  }


  @Override
  public void setLastEventSent(TezEvent lastEventSent) {
    writeLock.lock();
    try {
      // TEZ-3066 ideally Heartbeat just happens in FAIL_IN_PROGRESS & KILL_IN_PROGRESS,
      // add other states here just in case. create TEZ-3068 for a more elegant solution.
      if (!EnumSet.of(TaskAttemptStateInternal.FAIL_IN_PROGRESS,
        TaskAttemptStateInternal.KILL_IN_PROGRESS,
        TaskAttemptStateInternal.FAILED,
        TaskAttemptStateInternal.KILLED,
        TaskAttemptStateInternal.SUCCEEDED).contains(getInternalState())) {
        DataEventDependencyInfo info = new DataEventDependencyInfo(
          lastEventSent.getEventReceivedTime(), lastEventSent.getSourceInfo().getTaskAttemptID());
        // task attempt id may be null for input data information events
        if (appendNextDataEvent) {
          appendNextDataEvent = false;
          lastDataEvents.add(info);
        } else {
          // over-write last event - array list makes it quick
          lastDataEvents.set(lastDataEvents.size() - 1, info);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }
}
