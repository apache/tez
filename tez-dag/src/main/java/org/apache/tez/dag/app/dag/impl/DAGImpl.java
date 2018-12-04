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
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tez.Utils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.LimitExceededException;
import org.apache.tez.dag.app.dag.event.DAGEventTerminateDag;
import org.apache.tez.dag.app.dag.event.DiagnosableEvent;
import org.apache.tez.state.OnStateChangedCallback;
import org.apache.tez.state.StateMachineTez;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.counters.AggregateTezCounters;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.Scope;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.client.DAGStatusBuilder;
import org.apache.tez.dag.api.client.ProgressBuilder;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.VertexStatusBuilder;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanGroupInputEdgeInfo;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;
import org.apache.tez.dag.api.records.DAGProtos.PlanVertexGroupInfo;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.RecoveryParser.VertexRecoveryData;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;
import org.apache.tez.dag.app.RecoveryParser.DAGRecoveryData;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGReport;
import org.apache.tez.dag.app.dag.DAGScheduler;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.DAGTerminationCause;
import org.apache.tez.dag.app.dag.StateChangeNotifier;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.VertexTerminationCause;
import org.apache.tez.dag.app.dag.event.CallableEvent;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventDAGFinished;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventCommitCompleted;
import org.apache.tez.dag.app.dag.event.DAGEventCounterUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventDiagnosticsUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventRecoverEvent;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventStartDag;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.DAGEventVertexCompleted;
import org.apache.tez.dag.app.dag.event.DAGEventVertexReRunning;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventRecoverVertex;
import org.apache.tez.dag.app.dag.event.VertexEventTermination;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.DAGCommitStartedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitStartedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.utils.TaskSpecificLaunchCmdOption;
import org.apache.tez.dag.utils.RelocalizationUtils;
import org.apache.tez.dag.utils.TezBuilderUtils;
import org.apache.tez.runtime.api.OutputCommitter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/** Implementation of Job interface. Maintains the state machines of Job.
 * The read and write calls use ReadWriteLock for concurrency.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class DAGImpl implements org.apache.tez.dag.app.dag.DAG,
  EventHandler<DAGEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(DAGImpl.class);
  private static final String LINE_SEPARATOR = System
      .getProperty("line.separator");

  //final fields
  private final TezDAGID dagId;
  private final Clock clock;

  // TODO Recovery
  //private final List<AMInfo> amInfos;
  private final Lock dagStatusLock = new ReentrantLock();
  private final Condition dagCompletionCondition = dagStatusLock.newCondition();
  private final AtomicBoolean isFinalState = new AtomicBoolean(false);
  private final Lock readLock;
  private final Lock writeLock;
  private final String dagName;
  private final TaskCommunicatorManagerInterface taskCommunicatorManagerInterface;
  private final TaskHeartbeatHandler taskHeartbeatHandler;
  private final Object tasksSyncHandle = new Object();

  private AtomicBoolean committed = new AtomicBoolean(false);
  private AtomicBoolean aborted = new AtomicBoolean(false);
  private AtomicBoolean commitCanceled = new AtomicBoolean(false);
  boolean commitAllOutputsOnSuccess = true;

  @VisibleForTesting
  DAGScheduler dagScheduler;

  private final EventHandler eventHandler;
  // TODO Metrics
  //private final MRAppMetrics metrics;
  private final String userName;
  private final AppContext appContext;
  private final UserGroupInformation dagUGI;
  private final ACLManager aclManager;
  private final org.apache.tez.dag.api.Vertex.VertexExecutionContext defaultExecutionContext;
  @VisibleForTesting
  StateChangeNotifier entityUpdateTracker;

  volatile Map<TezVertexID, Vertex> vertices = new HashMap<TezVertexID, Vertex>();
  @VisibleForTesting
  Map<String, Edge> edges = new HashMap<String, Edge>();
  ArrayList<BitSet> vertexDescendants;
  private TezCounters dagCounters = new TezCounters();
  private Object fullCountersLock = new Object();
  @VisibleForTesting
  TezCounters fullCounters = null;
  private TezCounters cachedCounters = null;
  private long cachedCountersTimestamp = 0;
  private Set<TezVertexID> reRunningVertices = new HashSet<TezVertexID>();

  // Combined configs for the DAG
  private final Configuration dagConf;
  // DAG specific configs only
  // Useful when trying to serialize only the diff from global configs
  private final Configuration dagOnlyConf;

  private final DAGPlan jobPlan;

  private final AtomicBoolean internalErrorTriggered = new AtomicBoolean(false);

  Map<String, LocalResource> localResources;
  
  long startDAGCpuTime = 0;
  long startDAGGCTime = 0;

  private final List<String> diagnostics = new ArrayList<String>();

  private TaskSpecificLaunchCmdOption taskSpecificLaunchCmdOption;

  private static final DagStateChangedCallback STATE_CHANGED_CALLBACK = new DagStateChangedCallback();

  @VisibleForTesting
  Map<OutputKey, ListenableFuture<Void>> commitFutures
    = new HashMap<OutputKey, ListenableFuture<Void>>();

  private static final DiagnosticsUpdateTransition
      DIAGNOSTIC_UPDATE_TRANSITION = new DiagnosticsUpdateTransition();
  private static final InternalErrorTransition
      INTERNAL_ERROR_TRANSITION = new InternalErrorTransition();
  private static final CounterUpdateTransition COUNTER_UPDATE_TRANSITION =
      new CounterUpdateTransition();
  private static final DAGSchedulerUpdateTransition
          DAG_SCHEDULER_UPDATE_TRANSITION = new DAGSchedulerUpdateTransition();
  private static final CommitCompletedTransition COMMIT_COMPLETED_TRANSITION =
      new CommitCompletedTransition();

  protected static final
    StateMachineFactory<DAGImpl, DAGState, DAGEventType, DAGEvent>
       stateMachineFactory
     = new StateMachineFactory<DAGImpl, DAGState, DAGEventType, DAGEvent>
              (DAGState.NEW)

          // Transitions from NEW state
          .addTransition(DAGState.NEW, DAGState.NEW,
              DAGEventType.DAG_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          // either recovered to FINISHED state or recovered to NEW to rerun the dag based on the recovery data
          .addTransition(DAGState.NEW,
              EnumSet.of(DAGState.NEW, DAGState.SUCCEEDED,
                  DAGState.FAILED, DAGState.KILLED,
                  DAGState.ERROR),
              DAGEventType.DAG_RECOVER,
              new RecoverTransition())
          .addTransition(DAGState.NEW, DAGState.NEW,
              DAGEventType.DAG_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition
              (DAGState.NEW,
              EnumSet.of(DAGState.INITED, DAGState.FAILED),
              DAGEventType.DAG_INIT,
              new InitTransition())
          .addTransition(DAGState.NEW, EnumSet.of(DAGState.KILLED, DAGState.FAILED),
              DAGEventType.DAG_TERMINATE,
              new KillNewJobTransition())
          .addTransition(DAGState.NEW, DAGState.ERROR,
              DAGEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from INITED state
          .addTransition(DAGState.INITED, DAGState.INITED,
              DAGEventType.DAG_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(DAGState.INITED, DAGState.INITED,
              DAGEventType.DAG_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(DAGState.INITED, DAGState.RUNNING,
              DAGEventType.DAG_START,
              new StartTransition())
          .addTransition(DAGState.INITED, EnumSet.of(DAGState.KILLED, DAGState.FAILED),
              DAGEventType.DAG_TERMINATE,
              new KillInitedJobTransition())
          .addTransition(DAGState.INITED, DAGState.ERROR,
              DAGEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from RUNNING state
          .addTransition
              (DAGState.RUNNING,
              EnumSet.of(DAGState.RUNNING, DAGState.COMMITTING,
                  DAGState.SUCCEEDED, DAGState.TERMINATING,DAGState.FAILED),
              DAGEventType.DAG_VERTEX_COMPLETED,
              new VertexCompletedTransition())
          .addTransition(DAGState.RUNNING, EnumSet.of(DAGState.RUNNING, DAGState.TERMINATING),
              DAGEventType.DAG_VERTEX_RERUNNING,
              new VertexReRunningTransition())
          .addTransition(DAGState.RUNNING, DAGState.TERMINATING,
              DAGEventType.DAG_TERMINATE, new DAGKilledTransition())
          .addTransition(DAGState.RUNNING, DAGState.RUNNING,
              DAGEventType.DAG_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(DAGState.RUNNING, DAGState.RUNNING,
              DAGEventType.DAG_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(DAGState.RUNNING, DAGState.RUNNING,
              DAGEventType.DAG_SCHEDULER_UPDATE,
              DAG_SCHEDULER_UPDATE_TRANSITION)
          .addTransition(
              DAGState.RUNNING,
              DAGState.ERROR, DAGEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(DAGState.RUNNING,
              EnumSet.of(DAGState.RUNNING, DAGState.TERMINATING),
              DAGEventType.DAG_COMMIT_COMPLETED,
              new CommitCompletedWhileRunning())

          // Transitions from COMMITTING state.
          .addTransition(DAGState.COMMITTING,
              EnumSet.of(DAGState.COMMITTING, DAGState.TERMINATING, DAGState.FAILED, DAGState.SUCCEEDED),
              DAGEventType.DAG_COMMIT_COMPLETED,
              COMMIT_COMPLETED_TRANSITION)
          .addTransition(DAGState.COMMITTING, DAGState.TERMINATING, 
              DAGEventType.DAG_TERMINATE,
              new DAGKilledWhileCommittingTransition())
          .addTransition(
              DAGState.COMMITTING,
              DAGState.ERROR,
              DAGEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(DAGState.COMMITTING, DAGState.COMMITTING,
              DAGEventType.DAG_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(DAGState.COMMITTING, DAGState.COMMITTING,
              DAGEventType.DAG_SCHEDULER_UPDATE,
              DAG_SCHEDULER_UPDATE_TRANSITION)
          .addTransition(DAGState.COMMITTING, DAGState.TERMINATING,
              DAGEventType.DAG_VERTEX_RERUNNING,
              new VertexRerunWhileCommitting())
          .addTransition(DAGState.COMMITTING, DAGState.COMMITTING,
              DAGEventType.DAG_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)

          // Transitions from TERMINATING state.
          .addTransition
              (DAGState.TERMINATING,
              EnumSet.of(DAGState.TERMINATING, DAGState.KILLED, DAGState.FAILED,
                  DAGState.ERROR),
              DAGEventType.DAG_VERTEX_COMPLETED,
              new VertexCompletedTransition())
          .addTransition(DAGState.TERMINATING, DAGState.TERMINATING,
              DAGEventType.DAG_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(DAGState.TERMINATING, DAGState.TERMINATING,
              DAGEventType.DAG_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              DAGState.TERMINATING,
              DAGState.ERROR, DAGEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(
              DAGState.TERMINATING,
              EnumSet.of(DAGState.TERMINATING, DAGState.FAILED, DAGState.KILLED, DAGState.ERROR),
              DAGEventType.DAG_COMMIT_COMPLETED,
              COMMIT_COMPLETED_TRANSITION)

              // Ignore-able events
          .addTransition(DAGState.TERMINATING, DAGState.TERMINATING,
              EnumSet.of(DAGEventType.DAG_TERMINATE,
                         DAGEventType.DAG_VERTEX_RERUNNING,
                         DAGEventType.DAG_SCHEDULER_UPDATE))

          // Transitions from SUCCEEDED state
          .addTransition(DAGState.SUCCEEDED, DAGState.SUCCEEDED,
              DAGEventType.DAG_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(DAGState.SUCCEEDED, DAGState.SUCCEEDED,
              DAGEventType.DAG_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              DAGState.SUCCEEDED,
              DAGState.ERROR, DAGEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(DAGState.SUCCEEDED, DAGState.SUCCEEDED,
              EnumSet.of(DAGEventType.DAG_TERMINATE,
                  DAGEventType.DAG_SCHEDULER_UPDATE,
                  DAGEventType.DAG_VERTEX_COMPLETED))

          // Transitions from FAILED state
          .addTransition(DAGState.FAILED, DAGState.FAILED,
              DAGEventType.DAG_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(DAGState.FAILED, DAGState.FAILED,
              DAGEventType.DAG_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              DAGState.FAILED,
              DAGState.ERROR, DAGEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(DAGState.FAILED, DAGState.FAILED,
              EnumSet.of(DAGEventType.DAG_TERMINATE,
                  DAGEventType.DAG_START,
                  DAGEventType.DAG_VERTEX_RERUNNING,
                  DAGEventType.DAG_SCHEDULER_UPDATE,
                  DAGEventType.DAG_VERTEX_COMPLETED))

          // Transitions from KILLED state
          .addTransition(DAGState.KILLED, DAGState.KILLED,
              DAGEventType.DAG_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(DAGState.KILLED, DAGState.KILLED,
              DAGEventType.DAG_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              DAGState.KILLED,
              DAGState.ERROR, DAGEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(DAGState.KILLED, DAGState.KILLED,
              EnumSet.of(DAGEventType.DAG_TERMINATE,
                  DAGEventType.DAG_START,
                  DAGEventType.DAG_VERTEX_RERUNNING,
                  DAGEventType.DAG_SCHEDULER_UPDATE,
                  DAGEventType.DAG_VERTEX_COMPLETED))

          // No transitions from INTERNAL_ERROR state. Ignore all.
          .addTransition(
              DAGState.ERROR,
              DAGState.ERROR,
              EnumSet.of(
                  DAGEventType.DAG_TERMINATE,
                  DAGEventType.DAG_INIT,
                  DAGEventType.DAG_START,
                  DAGEventType.DAG_VERTEX_COMPLETED,
                  DAGEventType.DAG_VERTEX_RERUNNING,
                  DAGEventType.DAG_SCHEDULER_UPDATE,
                  DAGEventType.DAG_DIAGNOSTIC_UPDATE,
                  DAGEventType.INTERNAL_ERROR,
                  DAGEventType.DAG_COUNTER_UPDATE))
          .addTransition(DAGState.ERROR, DAGState.ERROR,
              DAGEventType.DAG_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          // create the topology tables
          .installTopology();

  private final StateMachineTez<DAGState, DAGEventType, DAGEvent, DAGImpl> stateMachine;

  //changing fields while the job is running
  @VisibleForTesting
  int numCompletedVertices = 0;
  private int numVertices;
  private int numSuccessfulVertices = 0;
  private int numFailedVertices = 0;
  private int numKilledVertices = 0;
  private boolean isUber = false;
  private DAGTerminationCause terminationCause;
  private Credentials credentials;

  @VisibleForTesting
  long initTime;
  @VisibleForTesting
  long startTime;
  @VisibleForTesting
  long finishTime;

  Map<String, VertexGroupInfo> vertexGroups = Maps.newHashMap();
  Map<String, List<VertexGroupInfo>> vertexGroupInfo = Maps.newHashMap();
  private DAGRecoveryData recoveryData;

  static class VertexGroupInfo {
    String groupName;
    Set<String> groupMembers;
    Set<String> outputs;
    Map<String, InputDescriptor> edgeMergedInputs;
    int successfulMembers;
    int successfulCommits;
    boolean commitStarted;

    VertexGroupInfo(PlanVertexGroupInfo groupInfo) {
      groupName = groupInfo.getGroupName();
      groupMembers = Sets.newHashSet(groupInfo.getGroupMembersList());
      edgeMergedInputs = Maps.newHashMapWithExpectedSize(groupInfo.getEdgeMergedInputsCount());
      for (PlanGroupInputEdgeInfo edgInfo : groupInfo.getEdgeMergedInputsList()) {
        edgeMergedInputs.put(edgInfo.getDestVertexName(),
            DagTypeConverters.convertInputDescriptorFromDAGPlan(edgInfo.getMergedInput()));
      }
      outputs = Sets.newHashSet(groupInfo.getOutputsList());
      successfulMembers = 0;
      successfulCommits = 0;
      commitStarted = false;
    }

    public boolean isInCommitting() {
      return commitStarted && successfulCommits < outputs.size();
    }

    public boolean isCommitted() {
      return commitStarted && successfulCommits == outputs.size();
    }
  }


  public DAGImpl(TezDAGID dagId,
      Configuration amConf,
      DAGPlan jobPlan,
      EventHandler eventHandler,
      TaskCommunicatorManagerInterface taskCommunicatorManagerInterface,
      Credentials dagCredentials,
      Clock clock,
      String appUserName,
      TaskHeartbeatHandler thh,
      AppContext appContext) {
    this.dagId = dagId;
    this.jobPlan = jobPlan;
    this.dagConf = new Configuration(amConf);
    this.dagOnlyConf = new Configuration(false);
    Iterator<PlanKeyValuePair> iter =
        jobPlan.getDagConf().getConfKeyValuesList().iterator();
    // override the amConf by using DAG level configuration
    while (iter.hasNext()) {
      PlanKeyValuePair keyValPair = iter.next();
      TezConfiguration.validateProperty(keyValPair.getKey(), Scope.DAG);
      this.dagConf.set(keyValPair.getKey(), keyValPair.getValue());
      this.dagOnlyConf.set(keyValPair.getKey(), keyValPair.getValue());
    }
    this.dagName = (jobPlan.getName() != null) ? jobPlan.getName() : "<missing app name>";
    this.userName = appUserName;
    this.clock = clock;
    this.appContext = appContext;

    this.taskCommunicatorManagerInterface = taskCommunicatorManagerInterface;
    this.taskHeartbeatHandler = thh;
    this.eventHandler = eventHandler;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    this.localResources = DagTypeConverters.createLocalResourceMapFromDAGPlan(jobPlan
        .getLocalResourceList());

    this.credentials = dagCredentials;
    if (this.credentials == null) {
      try {
        dagUGI = UserGroupInformation.getCurrentUser();
      } catch (IOException e) {
        throw new TezUncheckedException("Failed to set UGI for dag based on currentUser", e);
      }
    } else {
      dagUGI = UserGroupInformation.createRemoteUser(this.userName);
      dagUGI.addCredentials(this.credentials);
    }

    this.aclManager = new ACLManager(appContext.getAMACLManager(), dagUGI.getShortUserName(),
        this.jobPlan.getAclInfo());
    // this is only for recovery in case it does not call the init transition
    this.startDAGCpuTime = appContext.getCumulativeCPUTime();
    this.startDAGGCTime = appContext.getCumulativeGCTime();
    if (jobPlan.hasDefaultExecutionContext()) {
      defaultExecutionContext = DagTypeConverters.convertFromProto(jobPlan.getDefaultExecutionContext());
    } else {
      defaultExecutionContext = null;
    }
    
    this.taskSpecificLaunchCmdOption = new TaskSpecificLaunchCmdOption(dagConf);
    // This "this leak" is okay because the retained pointer is in an
    //  instance variable.
    stateMachine = new StateMachineTez<DAGState, DAGEventType, DAGEvent, DAGImpl>(
        stateMachineFactory.make(this), this);
    augmentStateMachine();
    this.entityUpdateTracker = new StateChangeNotifier(this);
  }

  private void augmentStateMachine() {
    stateMachine
        .registerStateEnteredCallback(DAGState.SUCCEEDED,
            STATE_CHANGED_CALLBACK)
        .registerStateEnteredCallback(DAGState.FAILED,
            STATE_CHANGED_CALLBACK)
        .registerStateEnteredCallback(DAGState.KILLED,
            STATE_CHANGED_CALLBACK)
        .registerStateEnteredCallback(DAGState.ERROR,
            STATE_CHANGED_CALLBACK);
  }

  private static class DagStateChangedCallback
      implements OnStateChangedCallback<DAGState, DAGImpl> {
    @Override
    public void onStateChanged(DAGImpl dag, DAGState dagState) {
      dag.isFinalState.set(true);
      dag.dagStatusLock.lock();
      try {
        dag.dagCompletionCondition.signal();
      } finally {
        dag.dagStatusLock.unlock();
      }
    }
  }

  protected StateMachine<DAGState, DAGEventType, DAGEvent> getStateMachine() {
    return stateMachine;
  }

  @Override
  public TezDAGID getID() {
    return dagId;
  }
  
  @Override
  public Map<String, LocalResource> getLocalResources() {
    return localResources;
  }

  // TODO maybe removed after TEZ-74
  @Override
  public Configuration getConf() {
    return dagConf;
  }

  @Override
  public DAGPlan getJobPlan() {
    return jobPlan;
  }

  @Override
  public EventHandler getEventHandler() {
    return this.eventHandler;
  }

  @Override
  public Vertex getVertex(TezVertexID vertexID) {
    readLock.lock();
    try {
      return vertices.get(vertexID);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean isUber() {
    return isUber;
  }

  @Override
  public Credentials getCredentials() {
    return this.credentials;
  }

  @Override
  public UserGroupInformation getDagUGI() {
    return this.dagUGI;
  }

  @Override
  public ACLManager getACLManager() {
    return this.aclManager;
  }

  @Override
  public Map<String, TezVertexID> getVertexNameIDMapping() {
    this.readLock.lock();
    try {
      Map<String, TezVertexID> idNameMap = new HashMap<String, TezVertexID>();
      for (Vertex v : getVertices().values()) {
        idNameMap.put(v.getName(), v.getVertexId());
      }
      return idNameMap;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public long getStartTime() {
    readLock.lock();
    try {
      return this.startTime;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public StateChangeNotifier getStateChangeNotifier() {
    return entityUpdateTracker;
  }

  @Override
  public org.apache.tez.dag.api.Vertex.VertexExecutionContext getDefaultExecutionContext() {
    return defaultExecutionContext;
  }

  @Override
  public TezCounters getAllCounters() {

    readLock.lock();

    try {
      DAGState state = getInternalState();
      if (state == DAGState.ERROR || state == DAGState.FAILED
          || state == DAGState.KILLED || state == DAGState.SUCCEEDED) {
        this.mayBeConstructFinalFullCounters();
        return fullCounters;
      }

      // dag not yet finished. update cpu time counters
      updateCpuCounters();
      TezCounters counters = new TezCounters();
      counters.incrAllCounters(dagCounters);
      return aggrTaskCounters(counters, vertices.values());

    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TezCounters getCachedCounters() {

    readLock.lock();

    try {
      // FIXME a better lightweight approach for counters is needed
      if (fullCounters == null && cachedCounters != null
          && ((cachedCountersTimestamp+10000) > System.currentTimeMillis())) {
        LOG.info("Asked for counters"
            + ", cachedCountersTimestamp=" + cachedCountersTimestamp
            + ", currentTime=" + System.currentTimeMillis());
        return cachedCounters;
      }

      cachedCountersTimestamp = System.currentTimeMillis();
      if (inTerminalState()) {
        this.mayBeConstructFinalFullCounters();
        return fullCounters;
      }

      // dag not yet finished. update cpu time counters
      updateCpuCounters();
      TezCounters counters = new TezCounters();
      counters.incrAllCounters(dagCounters);
      return aggrTaskCounters(counters, vertices.values());

    } finally {
      readLock.unlock();
    }
  }

  boolean inTerminalState() {
    DAGState state = getInternalState();
    if (state == DAGState.ERROR || state == DAGState.FAILED
        || state == DAGState.KILLED || state == DAGState.SUCCEEDED) {
      return true;
    }
    return false;
  }

  public static TezCounters aggrTaskCounters(
      TezCounters counters, Collection<Vertex> vertices) {
    for (Vertex vertex : vertices) {
      counters.aggrAllCounters(vertex.getAllCounters());
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
  public DAGReport getReport() {
    readLock.lock();
    try {
      StringBuilder diagsb = new StringBuilder();
      for (String s : getDiagnostics()) {
        diagsb.append(s).append("\n");
      }

      if (getInternalState() == DAGState.NEW) {
        /*
        return MRBuilderUtils.newJobReport(dagId, dagName, username, state,
            appSubmitTime, startTime, finishTime, setupProgress, 0.0f, 0.0f,
            cleanupProgress, jobFile, amInfos, isUber, diagsb.toString());
            */
        // TODO
        return TezBuilderUtils.newDAGReport();
      }

      // TODO
      return TezBuilderUtils.newDAGReport();
      /*
      return MRBuilderUtils.newJobReport(dagId, dagName, username, state,
          appSubmitTime, startTime, finishTime, setupProgress,
          this.mapProgress, this.reduceProgress,
          cleanupProgress, jobFile, amInfos, isUber, diagsb.toString());
          */
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public float getProgress() {
    this.readLock.lock();
    try {
      float progress = 0.0f;
      for (Vertex v : getVertices().values()) {
        float vertexProgress = v.getProgress();
        if (vertexProgress >= 0.0f && vertexProgress <= 1.0f) {
          progress += vertexProgress;
        }
      }
      float dagProgress = progress / getTotalVertices();
      if (dagProgress >= 0.0f && progress <= 1.0f) {
        return dagProgress;
      } else {
        return 0.0f;
      }
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public float getCompletedTaskProgress() {
    this.readLock.lock();
    try {
      int totalTasks = 0;
      int completedTasks = 0;
      for (Vertex v : getVertices().values()) {
        int vTotalTasks = v.getTotalTasks();
        int vCompletedTasks = v.getSucceededTasks();
        if (vTotalTasks > 0) {
          totalTasks += vTotalTasks;
          completedTasks += vCompletedTasks;
        }
      }
      if (totalTasks == 0) {
        DAGState state = getStateMachine().getCurrentState();
        if (state == DAGState.ERROR || state == DAGState.FAILED
            || state == DAGState.KILLED || state == DAGState.SUCCEEDED) {
          return 1.0f;
        } else {
          return 0.0f;
        }
      }
      return ((float)completedTasks/totalTasks);
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Map<TezVertexID, Vertex> getVertices() {
    synchronized (tasksSyncHandle) {
      return Collections.unmodifiableMap(vertices);
    }
  }

  @Override
  public DAGState getState() {
    readLock.lock();
    try {
      return getStateMachine().getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  // monitoring apis
  @Override
  public DAGStatusBuilder getDAGStatus(Set<StatusGetOpts> statusOptions) {
    DAGStatusBuilder status = new DAGStatusBuilder();
    int totalTaskCount = 0;
    int totalSucceededTaskCount = 0;
    int totalRunningTaskCount = 0;
    int totalFailedTaskCount = 0;
    int totalKilledTaskCount = 0;
    int totalFailedTaskAttemptCount = 0;
    int totalKilledTaskAttemptCount = 0;
    int totalRejectedTaskAttemptCount = 0;
    readLock.lock();
    try {
      for(Map.Entry<String, Vertex> entry : vertexMap.entrySet()) {
        ProgressBuilder progress = entry.getValue().getVertexProgress();
        status.addVertexProgress(entry.getKey(), progress);
        totalTaskCount += progress.getTotalTaskCount();
        totalSucceededTaskCount += progress.getSucceededTaskCount();
        totalRunningTaskCount += progress.getRunningTaskCount();
        totalFailedTaskCount += progress.getFailedTaskCount();
        totalKilledTaskCount += progress.getKilledTaskCount();
        totalFailedTaskAttemptCount += progress.getFailedTaskAttemptCount();
        totalKilledTaskAttemptCount += progress.getKilledTaskAttemptCount();
        totalRejectedTaskAttemptCount += progress.getRejectedTaskAttemptCount();
      }
      ProgressBuilder dagProgress = new ProgressBuilder();
      dagProgress.setTotalTaskCount(totalTaskCount);
      dagProgress.setSucceededTaskCount(totalSucceededTaskCount);
      dagProgress.setRunningTaskCount(totalRunningTaskCount);
      dagProgress.setFailedTaskCount(totalFailedTaskCount);
      dagProgress.setKilledTaskCount(totalKilledTaskCount);
      dagProgress.setFailedTaskAttemptCount(totalFailedTaskAttemptCount);
      dagProgress.setKilledTaskAttemptCount(totalKilledTaskAttemptCount);
      dagProgress.setRejectedTaskAttemptCount(totalRejectedTaskAttemptCount);
      status.setState(getState());
      status.setDiagnostics(diagnostics);
      status.setDAGProgress(dagProgress);
      if (statusOptions.contains(StatusGetOpts.GET_COUNTERS)) {
        status.setDAGCounters(getAllCounters());
      }
      return status;
    } finally {
      readLock.unlock();
    }
  }

  public DAGStatusBuilder getDAGStatus(Set<StatusGetOpts> statusOptions,
                                       long timeoutMillis) throws TezException {
    long timeoutNanos = timeoutMillis * 1000l * 1000l;
    if (timeoutMillis < 0) {
      // Return only on SUCCESS
      timeoutNanos = Long.MAX_VALUE;
    }
    if (timeoutMillis == 0 || isComplete()) {
      return getDAGStatus(statusOptions);
    }
    while (true) {
      long nanosLeft;
      dagStatusLock.lock();
      try {
        // Check within the lock to ensure we don't end up waiting after the notify has happened
        if (isFinalState.get()) {
          break;
        }
        nanosLeft = dagCompletionCondition.awaitNanos(timeoutNanos);
      } catch (InterruptedException e) {
        throw new TezException("Interrupted while waiting for dag to complete", e);
      } finally {
        dagStatusLock.unlock();
      }
      if (nanosLeft <= 0) {
        // Time expired.
        break;
      } else {
        timeoutNanos = nanosLeft;
      }
    }
    return getDAGStatus(statusOptions);
  }

  private ProgressBuilder getDAGProgress() {
    int totalTaskCount = 0;
    int totalSucceededTaskCount = 0;
    int totalRunningTaskCount = 0;
    int totalFailedTaskCount = 0;
    int totalKilledTaskCount = 0;
    int totalFailedTaskAttemptCount = 0;
    int totalKilledTaskAttemptCount = 0;
    int totalRejectedTaskAttemptCount = 0;
    readLock.lock();
    try {
      for(Map.Entry<String, Vertex> entry : vertexMap.entrySet()) {
        ProgressBuilder progress = entry.getValue().getVertexProgress();
        totalTaskCount += progress.getTotalTaskCount();
        totalSucceededTaskCount += progress.getSucceededTaskCount();
        totalRunningTaskCount += progress.getRunningTaskCount();
        totalFailedTaskCount += progress.getFailedTaskCount();
        totalKilledTaskCount += progress.getKilledTaskCount();
        totalFailedTaskAttemptCount += progress.getFailedTaskAttemptCount();
        totalKilledTaskAttemptCount += progress.getKilledTaskAttemptCount();
        totalRejectedTaskAttemptCount += progress.getRejectedTaskAttemptCount();
      }
      ProgressBuilder dagProgress = new ProgressBuilder();
      dagProgress.setTotalTaskCount(totalTaskCount);
      dagProgress.setSucceededTaskCount(totalSucceededTaskCount);
      dagProgress.setRunningTaskCount(totalRunningTaskCount);
      dagProgress.setFailedTaskCount(totalFailedTaskCount);
      dagProgress.setKilledTaskCount(totalKilledTaskCount);
      dagProgress.setFailedTaskAttemptCount(totalFailedTaskAttemptCount);
      dagProgress.setKilledTaskAttemptCount(totalKilledTaskAttemptCount);
      dagProgress.setRejectedTaskAttemptCount(totalRejectedTaskAttemptCount);
      return dagProgress;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public VertexStatusBuilder getVertexStatus(String vertexName,
      Set<StatusGetOpts> statusOptions) {
    Vertex vertex = vertexMap.get(vertexName);
    if(vertex == null) {
      return null;
    }
    return vertex.getVertexStatus(statusOptions);
  }
  
  public TaskAttemptImpl getTaskAttempt(TezTaskAttemptID taId) {
    return (TaskAttemptImpl) getVertex(taId.getTaskID().getVertexID()).getTask(taId.getTaskID())
        .getAttempt(taId);
  }

  public TaskImpl getTask(TezTaskID tId) {
    return (TaskImpl) getVertex(tId.getVertexID()).getTask(tId);
  }

  protected void initializeVerticesAndStart() {
    for (Vertex v : vertices.values()) {
      if (v.getInputVerticesCount() == 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Initing root vertex " + v.getLogIdentifier());
        }
        eventHandler.handle(new VertexEvent(v.getVertexId(),
            VertexEventType.V_INIT));
      }
    }
    for (Vertex v : vertices.values()) {
      if (v.getInputVerticesCount() == 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Starting root vertex " + v.getLogIdentifier());
        }
        eventHandler.handle(new VertexEvent(v.getVertexId(),
            VertexEventType.V_START));
      }
    }
  }

  private void commitOutput(OutputCommitter outputCommitter) throws Exception {
    final OutputCommitter committer = outputCommitter;
    getDagUGI().doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        committer.commitOutput();
        return null;
      }
    });
  }

  // either commit when all vertices are completed or just finish if there's no committer
  private synchronized DAGState commitOrFinish() {

    // commit all other outputs
    // we come here for successful dag completion and when outputs need to be
    // committed at the end for all or none visibility
    Map<OutputKey, CallableEvent> commitEvents = new HashMap<OutputKey, CallableEvent>();
    // commit all shared outputs
    for (final VertexGroupInfo groupInfo : vertexGroups.values()) {
      if (!groupInfo.outputs.isEmpty()) {
        groupInfo.commitStarted = true;
        final Vertex v = getVertex(groupInfo.groupMembers.iterator().next());
        try {
          TezUtilsInternal.setHadoopCallerContext(appContext.getHadoopShim(), v.getVertexId());
          for (final String outputName : groupInfo.outputs) {
            final OutputKey outputKey = new OutputKey(outputName, groupInfo.groupName, true);
            CommitCallback groupCommitCallback = new CommitCallback(outputKey);
            CallableEvent groupCommitCallableEvent = new CallableEvent(groupCommitCallback) {
              @Override
              public Void call() throws Exception {
                OutputCommitter committer = v.getOutputCommitters().get(outputName);
                LOG.info("Committing output: " + outputKey);
                commitOutput(committer);
                return null;
              }
            };
            commitEvents.put(outputKey, groupCommitCallableEvent);
          }
        } finally {
          appContext.getHadoopShim().clearHadoopCallerContext();
        }
      }
    }

    for (final Vertex vertex : vertices.values()) {
      if (vertex.getOutputCommitters() == null) {
        LOG.info("No output committers for vertex: " + vertex.getLogIdentifier());
        continue;
      }
      Map<String, OutputCommitter> outputCommitters =
          new HashMap<String, OutputCommitter>(vertex.getOutputCommitters());
      Set<String> sharedOutputs = vertex.getSharedOutputs();
      // remove shared outputs
      if (sharedOutputs != null) {
        Iterator<Map.Entry<String, OutputCommitter>> iter = outputCommitters
            .entrySet().iterator();
        while (iter.hasNext()) {
          if (sharedOutputs.contains(iter.next().getKey())) {
            iter.remove();
          }
        }
      }
      if (outputCommitters.isEmpty()) {
        LOG.info("No exclusive output committers for vertex: " + vertex.getLogIdentifier());
        continue;
      }
      try {
        TezUtilsInternal.setHadoopCallerContext(appContext.getHadoopShim(), vertex.getVertexId());
        for (final Map.Entry<String, OutputCommitter> entry : outputCommitters.entrySet()) {
          if (vertex.getState() != VertexState.SUCCEEDED) {
            throw new TezUncheckedException("Vertex: " + vertex.getLogIdentifier() +
                " not in SUCCEEDED state. State= " + vertex.getState());
          }
          OutputKey outputKey = new OutputKey(entry.getKey(), vertex.getName(), false);
          CommitCallback commitCallback = new CommitCallback(outputKey);
          CallableEvent commitCallableEvent = new CallableEvent(commitCallback) {
            @Override
            public Void call() throws Exception {
              LOG.info("Committing output: " + entry.getKey() + " for vertex: "
                  + vertex.getLogIdentifier() + ", outputName: " + entry.getKey());
              commitOutput(entry.getValue());
              return null;
            }
          };
          commitEvents.put(outputKey, commitCallableEvent);
        }
      } finally {
        appContext.getHadoopShim().clearHadoopCallerContext();
      }
    }
    
    if (!commitEvents.isEmpty()) {
      try {
        LOG.info("Start writing dag commit event, " + getID());
        appContext.getHistoryHandler().handleCriticalEvent(new DAGHistoryEvent(getID(),
            new DAGCommitStartedEvent(getID(), clock.getTime())));
      } catch (IOException e) {
        LOG.error("Failed to send commit event to history/recovery handler", e);
        trySetTerminationCause(DAGTerminationCause.RECOVERY_FAILURE);
        return finished(DAGState.FAILED);
      }
      for (Map.Entry<OutputKey,CallableEvent> entry : commitEvents.entrySet()) {
        ListenableFuture<Void> commitFuture = appContext.getExecService().submit(entry.getValue());
        Futures.addCallback(commitFuture, entry.getValue().getCallback());
        commitFutures.put(entry.getKey(), commitFuture);
      }
    }

    if (commitFutures.isEmpty()) {
      // no commit needs to be done
      return finished(DAGState.SUCCEEDED);
    } else {
      return DAGState.COMMITTING;
    }
  }

  private void abortOutputs() {
    if (this.aborted.getAndSet(true)) {
      LOG.info("Ignoring multiple output abort");
      return ;
    }
    // come here because dag failed or
    // dag succeeded and all or none semantics were on and a commit failed.
    // Some output may be aborted multiple times if it is shared output.
    // It should be OK for it to be aborted multiple times.
    for (Vertex vertex : vertices.values()) {
      ((VertexImpl)vertex).abortVertex(VertexStatus.State.FAILED);
    }
  }

  @Override
  /**
   * The only entry point to change the DAG.
   */
  public void handle(DAGEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing DAGEvent " + event.getDAGId() + " of type "
          + event.getType() + " while in state " + getInternalState()
          + ". Event: " + event);
    }
    try {
      writeLock.lock();
      DAGState oldState = getInternalState();
      try {
         getStateMachine().doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        String message = "Invalid event " + event.getType() + " on Dag " + this.dagId
            + " at currentState=" + oldState;
        LOG.error("Can't handle " + message, e);
        addDiagnostic(message);
        eventHandler.handle(new DAGEvent(this.dagId,
            DAGEventType.INTERNAL_ERROR));
      } catch (RuntimeException e) {
        String message = "Uncaught Exception when handling event " + event.getType()
            + " on Dag " + this.dagId + " at currentState=" + oldState;
        LOG.error(message, e);
        addDiagnostic(message);
        if (!internalErrorTriggered.getAndSet(true)) {
          // to prevent a recursive loop
          eventHandler.handle(new DAGEvent(this.dagId,
              DAGEventType.INTERNAL_ERROR));
        }
      }
      //notify the eventhandler of state change
      if (oldState != getInternalState()) {
        LOG.info(dagId + " transitioned from " + oldState + " to "
                 + getInternalState() + " due to event " + event.getType());
      }
    }

    finally {
      writeLock.unlock();
    }
  }

  @Private
  public DAGState getInternalState() {
    readLock.lock();
    try {
     return getStateMachine().getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  void setFinishTime() {
    finishTime = clock.getTime();
  }

  synchronized void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  private Map<String, Integer> constructTaskStats(ProgressBuilder progressBuilder) {
    Map<String, Integer> taskStats = new HashMap<String, Integer>();
    taskStats.put(ATSConstants.NUM_COMPLETED_TASKS, progressBuilder.getTotalTaskCount());
    taskStats.put(ATSConstants.NUM_SUCCEEDED_TASKS, progressBuilder.getSucceededTaskCount());
    taskStats.put(ATSConstants.NUM_FAILED_TASKS, progressBuilder.getFailedTaskCount());
    taskStats.put(ATSConstants.NUM_KILLED_TASKS, progressBuilder.getKilledTaskCount());
    taskStats.put(ATSConstants.NUM_FAILED_TASKS_ATTEMPTS,
        progressBuilder.getFailedTaskAttemptCount());
    taskStats.put(ATSConstants.NUM_KILLED_TASKS_ATTEMPTS,
        progressBuilder.getKilledTaskAttemptCount());
    return taskStats;
  }

  void logJobHistoryInitedEvent() {
    if (recoveryData == null
        || recoveryData.getDAGInitializedEvent() == null) {
      DAGInitializedEvent initEvt = new DAGInitializedEvent(this.dagId,
          clock.getTime(), this.userName, this.dagName, this.getVertexNameIDMapping());
      this.appContext.getHistoryHandler().handle(
          new DAGHistoryEvent(dagId, initEvt));
    }
  }

  void logJobHistoryStartedEvent() {
    if (recoveryData == null
        || recoveryData.getDAGStartedEvent() == null) {
      DAGStartedEvent startEvt = new DAGStartedEvent(this.dagId,
          this.startTime, this.userName, this.dagName);
      this.appContext.getHistoryHandler().handle(
          new DAGHistoryEvent(dagId, startEvt));
    }
  }

  void logJobHistoryFinishedEvent(TezCounters counters) throws IOException {
    if (recoveryData == null
        || recoveryData.getDAGFinishedEvent() == null) {
      Map<String, Integer> taskStats = constructTaskStats(getDAGProgress());
      if (finishTime < startTime) {
        LOG.warn("DAG finish time is smaller than start time. "
            + "startTime=" + startTime
            + ", finishTime=" + finishTime
        );
      }
      DAGFinishedEvent finishEvt = new DAGFinishedEvent(dagId, startTime,
          finishTime, DAGState.SUCCEEDED, "", counters,
          this.userName, this.dagName, taskStats, this.appContext.getApplicationAttemptId(),
          this.jobPlan);
      this.appContext.getHistoryHandler().handleCriticalEvent(
          new DAGHistoryEvent(dagId, finishEvt));
    }
  }

  void logJobHistoryUnsuccesfulEvent(DAGState state, TezCounters counters) throws IOException {
    if (recoveryData == null
        || recoveryData.getDAGFinishedEvent() == null) {
      Map<String, Integer> taskStats = constructTaskStats(getDAGProgress());
      if (finishTime < startTime) {
        LOG.warn("DAG finish time is smaller than start time. "
            + "startTime=" + startTime
            + ", finishTime=" + finishTime
        );
      }
      DAGFinishedEvent finishEvt = new DAGFinishedEvent(dagId, startTime,
          finishTime, state,
          StringUtils.join(getDiagnostics(), LINE_SEPARATOR),
          counters, this.userName, this.dagName, taskStats,
          this.appContext.getApplicationAttemptId(), this.jobPlan);
      this.appContext.getHistoryHandler().handleCriticalEvent(
          new DAGHistoryEvent(dagId, finishEvt));
    }
  }

  // triggered by vertex_complete
  static DAGState checkVerticesForCompletion(DAGImpl dag) {
    LOG.info("Checking vertices for DAG completion"
        + ", numCompletedVertices=" + dag.numCompletedVertices
        + ", numSuccessfulVertices=" + dag.numSuccessfulVertices
        + ", numFailedVertices=" + dag.numFailedVertices
        + ", numKilledVertices=" + dag.numKilledVertices
        + ", numVertices=" + dag.numVertices
        + ", commitInProgress=" + dag.commitFutures.size() 
        + ", terminationCause=" + dag.terminationCause);

    // log in case of accounting error.
    if (dag.numCompletedVertices > dag.numVertices) {
      LOG.error("vertex completion accounting issue: numCompletedVertices > numVertices"
          + ", numCompletedVertices=" + dag.numCompletedVertices
          + ", numVertices=" + dag.numVertices
          );
    }

    if (dag.numCompletedVertices == dag.numVertices) {
      //Only succeed if vertices complete successfully and no terminationCause is registered.
      if(dag.numSuccessfulVertices == dag.numVertices && dag.isCommittable()) {
        if (dag.commitAllOutputsOnSuccess && !dag.committed.getAndSet(true)) {
          // start dag commit if there's any commit or just finish dag if no commit
          return dag.commitOrFinish();
        } else if (!dag.commitFutures.isEmpty()) {
          // vertex group commits are running
          return DAGState.COMMITTING;
        } else {
          // no vertex group commits or vertex group commits are done
          return dag.finished(DAGState.SUCCEEDED);
        }
      } else {
        // check commits before move to COMPLETED state.
        if (dag.commitFutures.isEmpty()) {
          return finishWithTerminationCause(dag);
        } else {
          return DAGState.TERMINATING;
        }
      }
    }

    //return the current state, Job not finished yet
    return dag.getInternalState();
  }

  // Only start commit when it is in RUNNING/COMMITTING and terminationCause is null
  private boolean isCommittable() {
    return terminationCause == null
        && (getState() == DAGState.RUNNING || getState() == DAGState.COMMITTING);
  }

  // triggered by commit_complete, checkCommitsForCompletion should only been called in COMMITTING/TERMINATING
  static DAGState checkCommitsForCompletion(DAGImpl dag) {
    LOG.info("Checking commits for DAG completion"
        + ", numCompletedVertices=" + dag.numCompletedVertices
        + ", numSuccessfulVertices=" + dag.numSuccessfulVertices
        + ", numFailedVertices=" + dag.numFailedVertices
        + ", numKilledVertices=" + dag.numKilledVertices
        + ", numVertices=" + dag.numVertices
        + ", commitInProgress=" + dag.commitFutures.size() 
        + ", terminationCause=" + dag.terminationCause);

    // continue the commits if DAG#isCommittable return true, otherwise go to TERMINATING or finish dag.
    if (dag.isCommittable()) {
      Preconditions.checkState(dag.getState() == DAGState.COMMITTING,
          "DAG should be in COMMITTING state, but in " + dag.getState());
      if (!dag.commitFutures.isEmpty()) {
        // pending commits are running
        return DAGState.COMMITTING;
      } else {
        return dag.finished(DAGState.SUCCEEDED);
      }
    } else {
      Preconditions.checkState(dag.getState() == DAGState.TERMINATING
              || dag.getState() == DAGState.COMMITTING,
          "DAG should be in COMMITTING/TERMINATING state, but in " + dag.getState());
      if (!dag.commitFutures.isEmpty() || dag.numCompletedVertices != dag.numVertices) {
        // pending commits are running or still some vertices are not completed
        return DAGState.TERMINATING;
      } else {
        return finishWithTerminationCause(dag);
      }
    }
  }

  private static DAGState finishWithTerminationCause(DAGImpl dag) {
    Preconditions.checkArgument(dag.getTerminationCause() != null, "TerminationCause is not set.");
    String diagnosticMsg =  "DAG did not succeed due to " + dag.terminationCause
        + ". failedVertices:" + dag.numFailedVertices
        + " killedVertices:" + dag.numKilledVertices;
    LOG.info(diagnosticMsg);
    dag.addDiagnostic(diagnosticMsg);
    return dag.finished(dag.getTerminationCause().getFinishedState());
  }

  private void updateCpuCounters() {
    long stopDAGCpuTime = appContext.getCumulativeCPUTime();
    long totalDAGCpuTime = stopDAGCpuTime - startDAGCpuTime;
    long stopDAGGCTime = appContext.getCumulativeGCTime();
    long totalDAGGCTime = stopDAGGCTime - startDAGGCTime;
    dagCounters.findCounter(DAGCounter.AM_CPU_MILLISECONDS).setValue(totalDAGCpuTime);
    dagCounters.findCounter(DAGCounter.AM_GC_TIME_MILLIS).setValue(totalDAGGCTime);
  }
  
  private DAGState finished(DAGState finalState) {
    boolean dagError = false;
    try {
      if (finishTime == 0) {
        setFinishTime();
      }
      entityUpdateTracker.stop();

      // update cpu time counters before finishing the dag
      updateCpuCounters();
      TezCounters counters = null;
      try {
        counters = constructFinalFullcounters();
      } catch (LimitExceededException e) {
        addDiagnostic("Counters limit exceeded: " + e.getMessage());
        finalState = DAGState.FAILED;
      }

      try {
        if (finalState == DAGState.SUCCEEDED) {
          logJobHistoryFinishedEvent(counters);
        } else {
          logJobHistoryUnsuccesfulEvent(finalState, counters);
        }
      } catch (IOException e) {
        LOG.warn("Failed to persist recovery event for DAG completion"
            + ", dagId=" + dagId
            + ", finalState=" + finalState, e);
        dagError = true;
      }

      if (finalState != DAGState.SUCCEEDED) {
        abortOutputs();
      }
    } catch (Exception e) {
      dagError = true;
      LOG.warn("Encountered exception while DAG finish", e);
    }
    if (dagError) {
      eventHandler.handle(new DAGAppMasterEventDAGFinished(getID(), DAGState.ERROR));
    } else {
      eventHandler.handle(new DAGAppMasterEventDAGFinished(getID(), finalState));
    }

    LOG.info("DAG: " + getID() + " finished with state: " + finalState);

    return finalState;
  }

  @Override
  public String getUserName() {
    return userName;
  }

  @Override
  public int getIndex() {
    return dagId.getId();
  }

  @Override
  public String getName() {
    return dagName;
  }

  @Override
  public int getTotalVertices() {
    readLock.lock();
    try {
      return numVertices;
    } finally {
      readLock.unlock();
    }

  }

  @Override
  public BitSet getVertexDescendants(int vertexIndex) {
    readLock.lock();
    try {
      return vertexDescendants.get(vertexIndex);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public int getSuccessfulVertices() {
    readLock.lock();
    try {
      return numSuccessfulVertices;
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
  boolean trySetTerminationCause(DAGTerminationCause trigger) {
    if(terminationCause == null){
      terminationCause = trigger;
      return true;
    }
    return false;
  }

  DAGTerminationCause getTerminationCause() {
    readLock.lock();
    try {
      return terminationCause;
    } finally {
      readLock.unlock();
    }
  }


  DAGState initializeDAG() {
    commitAllOutputsOnSuccess = dagConf.getBoolean(
        TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS_DEFAULT);

    // If we have no vertices, fail the dag
    numVertices = getJobPlan().getVertexCount();
    if (numVertices == 0) {
      addDiagnostic("No vertices for dag");
      trySetTerminationCause(DAGTerminationCause.ZERO_VERTICES);
      return finished(DAGState.FAILED);
    }

    if (jobPlan.getVertexGroupsCount() > 0) {
      for (PlanVertexGroupInfo groupInfo : jobPlan.getVertexGroupsList()) {
        vertexGroups.put(groupInfo.getGroupName(), new VertexGroupInfo(groupInfo));
      }
      for (VertexGroupInfo groupInfo : vertexGroups.values()) {
        for (String vertexName : groupInfo.groupMembers) {
          List<VertexGroupInfo> groupList = vertexGroupInfo.get(vertexName);
          if (groupList == null) {
            groupList = Lists.newLinkedList();
            vertexGroupInfo.put(vertexName, groupList);
          }
          groupList.add(groupInfo);
        }
      }
    }

    // create the vertices`
    for (int i=0; i < numVertices; ++i) {
      String vertexName = getJobPlan().getVertex(i).getName();
      VertexImpl v = createVertex(this, vertexName, i);
      addVertex(v);
    }

    // check task resources, only check it in non-local mode
    if (!appContext.isLocal()) {
      for (Vertex v : vertexMap.values()) {
        // TODO TEZ-2003 (post) TEZ-2624 Ideally, this should be per source.
        if (v.getTaskResource().compareTo(appContext.getClusterInfo().getMaxContainerCapability()) > 0) {
          String msg = "Vertex's TaskResource is beyond the cluster container capability," +
              "Vertex=" + v.getLogIdentifier() +", Requested TaskResource=" + v.getTaskResource()
              + ", Cluster MaxContainerCapability=" + appContext.getClusterInfo().getMaxContainerCapability();
          LOG.error(msg);
          addDiagnostic(msg);
          finished(DAGState.FAILED);
          return DAGState.FAILED;
        }
      }
    }

    try {
      createDAGEdges(this);
    } catch (TezException e2) {
      String msg = "Fail to create edges, " + ExceptionUtils.getStackTrace(e2);
      addDiagnostic(msg);
      LOG.error(msg);
      trySetTerminationCause(DAGTerminationCause.INIT_FAILURE);
      finished(DAGState.FAILED);
      return DAGState.FAILED;
    }
    Map<String,EdgePlan> edgePlans = DagTypeConverters.createEdgePlanMapFromDAGPlan(getJobPlan().getEdgeList());

    // setup the dag
    for (Vertex v : vertices.values()) {
      parseVertexEdges(this, edgePlans, v);
    }

    computeVertexDescendants();

    // Initialize the edges, now that the payload and vertices have been set.
    for (Edge e : edges.values()) {
      try {
        e.initialize();
      } catch (AMUserCodeException ex) {
        String msg = "Exception in " + ex.getSource();
        LOG.error(msg, ex);
        addDiagnostic(msg + ", " + ex.getMessage() + ", "
            + ExceptionUtils.getStackTrace(ex.getCause()));
        finished(DAGState.FAILED);
        return DAGState.FAILED;
      }
    }

    try {
      assignDAGScheduler(this);
    } catch (TezException e1) {
      String msg = "Fail to assign DAGScheduler for dag:" + dagName + " due to "
          + ExceptionUtils.getStackTrace(e1);
      LOG.error(msg);
      addDiagnostic(msg);
      trySetTerminationCause(DAGTerminationCause.INIT_FAILURE);
      finished(DAGState.FAILED);
      return DAGState.FAILED;
    }

    for (Map.Entry<String, VertexGroupInfo> entry : vertexGroups.entrySet()) {
      String groupName = entry.getKey();
      VertexGroupInfo groupInfo = entry.getValue();
      if (!groupInfo.outputs.isEmpty()) {
        // shared outputs
        for (String vertexName : groupInfo.groupMembers) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Setting shared outputs for group: " + groupName +
                " on vertex: " + vertexName);
          }
          Vertex v = getVertex(vertexName);
          v.addSharedOutputs(groupInfo.outputs);
        }
      }
    }

    // This is going to override the previously generated file
    // which didn't have the priorities
    Utils.generateDAGVizFile(this, jobPlan, dagScheduler);
    return DAGState.INITED;
  }

  private void createDAGEdges(DAGImpl dag) throws TezException {
    for (EdgePlan edgePlan : dag.getJobPlan().getEdgeList()) {
      EdgeProperty edgeProperty = DagTypeConverters
          .createEdgePropertyMapFromDAGPlan(edgePlan);

      // edge manager may be also set via API when using custom edge type
      dag.edges.put(edgePlan.getId(),
          new Edge(edgeProperty, dag.getEventHandler(), dagConf));
    }
  }

  private void computeVertexDescendants() {
    vertexDescendants = new ArrayList<>(numVertices);
    for (int i = 0; i < numVertices; ++i) {
      vertexDescendants.add(new BitSet(numVertices));
    }
    BitSet verticesVisited = new BitSet(numVertices);
    for (Vertex v : vertices.values()) {
      computeVertexDescendants(verticesVisited, v);
    }
  }

  private BitSet computeVertexDescendants(BitSet verticesVisited, Vertex v) {
    int vertexIndex = v.getVertexId().getId();
    BitSet descendants = vertexDescendants.get(vertexIndex);
    if (!verticesVisited.get(vertexIndex)) {
      for (Vertex child : v.getOutputVertices().keySet()) {
        descendants.set(child.getVertexId().getId());
        BitSet childDescendants = computeVertexDescendants(verticesVisited, child);
        descendants.or(childDescendants);
      }
      verticesVisited.set(vertexIndex);
    }
    return descendants;
  }

  private static void assignDAGScheduler(DAGImpl dag) throws TezException {
    String dagSchedulerClassName = dag.dagConf.get(TezConfiguration.TEZ_AM_DAG_SCHEDULER_CLASS,
        TezConfiguration.TEZ_AM_DAG_SCHEDULER_CLASS_DEFAULT);
    LOG.info("Using DAG Scheduler: " + dagSchedulerClassName);
    dag.dagScheduler = ReflectionUtils.createClazzInstance(dagSchedulerClassName, new Class<?>[] {
        DAG.class, EventHandler.class}, new Object[] {dag, dag.eventHandler});
    for (Vertex v : dag.vertices.values()) {
      dag.dagScheduler.addVertexConcurrencyLimit(v.getVertexId(), v.getMaxTaskConcurrency());
    }
  }

  private static VertexImpl createVertex(DAGImpl dag, String vertexName, int vId) {
    TezVertexID vertexId = TezBuilderUtils.newVertexID(dag.getID(), vId);

    VertexPlan vertexPlan = dag.getJobPlan().getVertex(vId);
    VertexLocationHint vertexLocationHint = DagTypeConverters
        .convertFromDAGPlan(vertexPlan.getTaskLocationHintList());

    VertexImpl v = new VertexImpl(
        vertexId, vertexPlan, vertexName, dag.dagConf,
        dag.eventHandler, dag.taskCommunicatorManagerInterface,
        dag.clock, dag.taskHeartbeatHandler,
        !dag.commitAllOutputsOnSuccess, dag.appContext, vertexLocationHint,
        dag.vertexGroups, dag.taskSpecificLaunchCmdOption, dag.entityUpdateTracker,
        dag.dagOnlyConf);
    return v;
  }

  // hooks up this VertexImpl to input and output EdgeProperties
  private static void parseVertexEdges(DAGImpl dag, Map<String, EdgePlan> edgePlans, Vertex vertex) {
    VertexPlan vertexPlan = vertex.getVertexPlan();

    Map<Vertex, Edge> inVertices =
        new HashMap<Vertex, Edge>();

    Map<Vertex, Edge> outVertices =
        new HashMap<Vertex, Edge>();

    for(String inEdgeId : vertexPlan.getInEdgeIdList()){
      EdgePlan edgePlan = edgePlans.get(inEdgeId);
      Vertex inVertex = dag.vertexMap.get(edgePlan.getInputVertexName());
      Edge edge = dag.edges.get(inEdgeId);
      edge.setSourceVertex(inVertex);
      edge.setDestinationVertex(vertex);
      inVertices.put(inVertex, edge);
    }

    for(String outEdgeId : vertexPlan.getOutEdgeIdList()){
      EdgePlan edgePlan = edgePlans.get(outEdgeId);
      Vertex outVertex = dag.vertexMap.get(edgePlan.getOutputVertexName());
      Edge edge = dag.edges.get(outEdgeId);
      edge.setSourceVertex(vertex);
      edge.setDestinationVertex(outVertex);
      outVertices.put(outVertex, edge);
    }

    vertex.setInputVertices(inVertices);
    vertex.setOutputVertices(outVertices);
  }

  /**
   * 2 cases of recovery:
   * <ul>
   * <li>
   * 1. For the completed dag, recover the dag to the desired state and also its vertices,
   *    but not task & task attempt. This recovery is sync call (after this Transition, 
   *    DAG & vertices are all recovered to the desired state)
   * </li>
   * <li>
   * 2. For the non-completed dag, recover the dag as normal dag execution. The only difference
   *    is setting the recoveryData before sending DAG_INIT event so that some steps in the execution
   *    will be skipped based on the recoveryData
   * </li>
   * </ul>
   */
  private static class RecoverTransition
      implements MultipleArcTransition<DAGImpl, DAGEvent, DAGState> {

    @Override
    public DAGState transition(DAGImpl dag, DAGEvent dagEvent) {
      DAGEventRecoverEvent recoverEvent = (DAGEventRecoverEvent)dagEvent;
      // With desired state, represents the case that DAG is completed
      if (recoverEvent.hasDesiredState()) {
        VertexState vertexDesiredState = null;
        switch (recoverEvent.getDesiredState()) {
        case SUCCEEDED:
          vertexDesiredState = VertexState.SUCCEEDED;
          break;
        case FAILED:
          vertexDesiredState = VertexState.FAILED;
          break;
        case KILLED:
          vertexDesiredState = VertexState.KILLED;
          break;
        case ERROR:
          vertexDesiredState = VertexState.ERROR;
          break;
        default:
          String msg = "Invalid desired state of DAG"
              + ", dagName=" + dag.getName()
              + ", state=" + recoverEvent.getDesiredState();
          LOG.warn(msg);
          dag.addDiagnostic(msg);
          return dag.finished(DAGState.ERROR);
        }
        // Initialize dag synchronously to generate the vertices and recover its vertices to the desired state.
        dag.initializeDAG();
        for (Vertex v : dag.vertexMap.values()) {
          dag.eventHandler.handle(new VertexEventRecoverVertex(v.getVertexId(), vertexDesiredState));
        }
        dag.addDiagnostic("DAG is recovered to finished state:" + recoverEvent.getDesiredState()
            + ", but will only recover partial data due to incomplete recovery data");
        return dag.finished(recoverEvent.getDesiredState());
      }

      // for the cases that DAG is not completed, recover it as normal dag execution.
      dag.recoveryData = recoverEvent.getRecoveredDagData();
      dag.appContext.setDAGRecoveryData(dag.recoveryData);
      dag.getEventHandler().handle(new DAGEvent(dag.getID(), DAGEventType.DAG_INIT));
      dag.getEventHandler().handle(new DAGEventStartDag(dag.getID(), dag.recoveryData.additionalUrlsForClasspath));
      return DAGState.NEW;
    }
  }

  private static class InitTransition
      implements MultipleArcTransition<DAGImpl, DAGEvent, DAGState> {

    /**
     * Note that this transition method is called directly (and synchronously)
     * by MRAppMaster's init() method (i.e., no RPC, no thread-switching;
     * just plain sequential call within AM context), so we can trigger
     * modifications in AM state from here (at least, if AM is written that
     * way; MR version is).
     */
    @Override
    public DAGState transition(DAGImpl dag, DAGEvent event) {
      // TODO Metrics
      //dag.metrics.submittedJob(dag);
      //dag.metrics.preparingJob(dag);
      if (dag.recoveryData != null && dag.recoveryData.getDAGInitializedEvent() != null) {
        dag.initTime = dag.recoveryData.getDAGInitializedEvent().getInitTime();
      } else {
        dag.initTime = dag.clock.getTime();
      }
      dag.startDAGCpuTime = dag.appContext.getCumulativeCPUTime();
      dag.startDAGGCTime = dag.appContext.getCumulativeGCTime();

      DAGState state = dag.initializeDAG();
      if (state != DAGState.INITED) {
        dag.trySetTerminationCause(DAGTerminationCause.INIT_FAILURE);
        return state;
      }

      // TODO Metrics
      //dag.metrics.endPreparingJob(dag);
      dag.logJobHistoryInitedEvent();
      return DAGState.INITED;


    }

  } // end of InitTransition

  public static class StartTransition
  implements SingleArcTransition<DAGImpl, DAGEvent> {
    /**
     * This transition executes in the event-dispatcher thread, though it's
     * triggered in MRAppMaster's startJobs() method.
     */
    @Override
    public void transition(DAGImpl dag, DAGEvent event) {
      if (dag.recoveryData != null && dag.recoveryData.getDAGStartedEvent() != null) {
        dag.startTime = dag.recoveryData.getDAGStartedEvent().getStartTime();
      } else {
        dag.startTime = dag.clock.getTime();
      }
      DAGEventStartDag startEvent = (DAGEventStartDag) event;
      List<URL> additionalUrlsForClasspath = startEvent.getAdditionalUrlsForClasspath();
      if (additionalUrlsForClasspath != null) {
        LOG.info("Added additional resources : [" + additionalUrlsForClasspath  + "] to classpath");
        RelocalizationUtils.addUrlsToClassPath(additionalUrlsForClasspath);
      }
      // TODO Metrics
      //job.metrics.runningJob(job);

      // Start all vertices with no incoming edges when job starts
      dag.initializeVerticesAndStart();
      dag.logJobHistoryStartedEvent();
    }
  }

  // use LinkedHashMap to ensure the vertex order (TEZ-1065)
  LinkedHashMap<String, Vertex> vertexMap = new LinkedHashMap<String, Vertex>();
  void addVertex(Vertex v) {
    vertices.put(v.getVertexId(), v);
    vertexMap.put(v.getName(), v);
  }

  @Override
  public Vertex getVertex(String vertexName) {
    return vertexMap.get(vertexName);
  }

  private void mayBeConstructFinalFullCounters() {
    // Calculating full-counters. This should happen only once for the job.
    synchronized (this.fullCountersLock) {
      if (this.fullCounters != null) {
        // Already constructed. Just return.
        return;
      }
      this.fullCounters = this.constructFinalFullcounters();
    }
  }

  @Private
  public TezCounters constructFinalFullcounters() {
    final AggregateTezCounters aggregateTezCounters = new AggregateTezCounters();
    aggregateTezCounters.aggrAllCounters(dagCounters);
    for (Vertex v : this.vertices.values()) {
      aggregateTezCounters.aggrAllCounters(v.getAllCounters());
    }
    return aggregateTezCounters;
  }

  /**
   * Set the terminationCause and send a kill-message to all vertices.
   * The vertex-kill messages are only sent once.
   */
  void enactKill(DAGTerminationCause dagTerminationCause,
      VertexTerminationCause vertexTerminationCause) {

    if(trySetTerminationCause(dagTerminationCause)){
      for (Vertex v : vertices.values()) {
        eventHandler.handle(
            new VertexEventTermination(v.getVertexId(), vertexTerminationCause)
            );
      }
    }
  }

  private void addDiagnostics(DiagnosableEvent event) {
    if (event.getDiagnosticInfo() != null && !event.getDiagnosticInfo().isEmpty()) {
      addDiagnostic(event.getDiagnosticInfo());
    }
  }

  // Task-start has been moved out of InitTransition, so this arc simply
  // hardcodes 0 for both map and reduce finished tasks.
  private static class KillNewJobTransition implements
      MultipleArcTransition<DAGImpl, DAGEvent, DAGState> {

    @Override
    public DAGState transition(DAGImpl dag, DAGEvent dagEvent) {
      DAGEventTerminateDag event = (DAGEventTerminateDag) dagEvent;
      dag.setFinishTime();
      dag.trySetTerminationCause(event.getTerminationCause());
      dag.addDiagnostic("Dag received [" + event.getType() + ", " + event.getTerminationCause() +
          "] in NEW state.");
      dag.addDiagnostics(event);
      return dag.finished(event.getTerminationCause().getFinishedState());
    }

  }

  private static class KillInitedJobTransition implements
      MultipleArcTransition<DAGImpl, DAGEvent, DAGState> {

    @Override
    public DAGState transition(DAGImpl dag, DAGEvent dagEvent) {
      DAGEventTerminateDag event = (DAGEventTerminateDag) dagEvent;
      dag.trySetTerminationCause(event.getTerminationCause());
      dag.addDiagnostic("Dag received [" + event.getType() + ", " + event.getTerminationCause() +
          "] in INITED state.");
      dag.addDiagnostics(event);
      return dag.finished(event.getTerminationCause().getFinishedState());
    }

  }

  private static class DAGKilledTransition
      implements SingleArcTransition<DAGImpl, DAGEvent> {
    @Override
    public void transition(DAGImpl job, DAGEvent dagEvent) {
      DAGEventTerminateDag event = (DAGEventTerminateDag) dagEvent;
      String msg = "Dag received [" + event.getType() + ", " + event.getTerminationCause() +
          "] in RUNNING state.";
      LOG.info(msg);
      job.addDiagnostic(msg);
      job.addDiagnostics(event);
      job.enactKill(event.getTerminationCause(), VertexTerminationCause.DAG_TERMINATED);
      // Commit may happen when dag is still in RUNNING (vertex group commit)
      job.cancelCommits();
      // TODO Metrics
      //job.metrics.endRunningJob(job);
    }


  }

  private static class DAGKilledWhileCommittingTransition
    implements SingleArcTransition<DAGImpl, DAGEvent> {

    @Override
    public void transition(DAGImpl dag, DAGEvent dagEvent) {
      DAGEventTerminateDag event = (DAGEventTerminateDag) dagEvent;
      String diag = "Dag received [" + event.getType() + ", " + event.getTerminationCause() +
          "] in COMMITTING state.";
      LOG.info(diag);
      dag.addDiagnostic(diag);
      dag.addDiagnostics(event);
      dag.cancelCommits();
      dag.trySetTerminationCause(event.getTerminationCause());
    }
  }

  private void cancelCommits() {
    if (!this.commitCanceled.getAndSet(true)) {
      for (Map.Entry<OutputKey, ListenableFuture<Void>> entry : commitFutures.entrySet()) {
        OutputKey outputKey = entry.getKey();
        LOG.info("Canceling commit of output=" + outputKey);
        entry.getValue().cancel(true);
      }
    }
  }

  private static class VertexCompletedTransition implements
      MultipleArcTransition<DAGImpl, DAGEvent, DAGState> {

    @Override
    public DAGState transition(DAGImpl job, DAGEvent event) {
      boolean forceTransitionToKillWait = false;

      DAGEventVertexCompleted vertexEvent = (DAGEventVertexCompleted) event;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received a vertex completion event"
            + ", vertexId=" + vertexEvent.getVertexId()
            + ", vertexState=" + vertexEvent.getVertexState());
      }
      Vertex vertex = job.vertices.get(vertexEvent.getVertexId());
      job.numCompletedVertices++;
      if (vertexEvent.getVertexState() == VertexState.SUCCEEDED) {
        forceTransitionToKillWait = !(job.vertexSucceeded(vertex));
      }
      else if (vertexEvent.getVertexState() == VertexState.FAILED) {
        job.enactKill(
            DAGTerminationCause.VERTEX_FAILURE, VertexTerminationCause.OTHER_VERTEX_FAILURE);
        job.cancelCommits();
        job.vertexFailed(vertex);
        forceTransitionToKillWait = true;
      }
      else if (vertexEvent.getVertexState() == VertexState.KILLED) {
        job.vertexKilled(vertex);
        job.cancelCommits();
        forceTransitionToKillWait = true;
      }

      job.reRunningVertices.remove(vertex.getVertexId());

      LOG.info("Vertex " + vertex.getLogIdentifier() + " completed."
          + ", numCompletedVertices=" + job.numCompletedVertices
          + ", numSuccessfulVertices=" + job.numSuccessfulVertices
          + ", numFailedVertices=" + job.numFailedVertices
          + ", numKilledVertices=" + job.numKilledVertices
          + ", numVertices=" + job.numVertices);

      // if the job has not finished but a failure/kill occurred, then force the transition to KILL_WAIT.
      DAGState state = checkVerticesForCompletion(job);
      if(state == DAGState.RUNNING && forceTransitionToKillWait){
        job.cancelCommits();
        return DAGState.TERMINATING;
      }
      else {
        return state;
      }
    }

  }

  private Collection<TezVertexID> getVertexIds(Collection<String> vertexNames) {
    List<TezVertexID> vertexIds = new ArrayList<TezVertexID>(vertexNames.size());
    for (String name : vertexNames) {
      vertexIds.add(getVertexNameIDMapping().get(name));
    }
    return vertexIds;
  }

  private static class VertexReRunningTransition implements
    MultipleArcTransition<DAGImpl, DAGEvent, DAGState> {

    @Override
    public DAGState transition(DAGImpl job, DAGEvent event) {
      DAGEventVertexReRunning vertexEvent = (DAGEventVertexReRunning) event;
      Vertex vertex = job.vertices.get(vertexEvent.getVertexId());
      boolean failed = job.vertexReRunning(vertex);
      if (!failed) {
        job.numCompletedVertices--;
      }

      LOG.info("Vertex " + vertex.getLogIdentifier() + " re-running."
          + ", numCompletedVertices=" + job.numCompletedVertices
          + ", numSuccessfulVertices=" + job.numSuccessfulVertices
          + ", numFailedVertices=" + job.numFailedVertices
          + ", numKilledVertices=" + job.numKilledVertices
          + ", numVertices=" + job.numVertices);

      if (failed) {
        return DAGState.TERMINATING;
      }
      return DAGState.RUNNING;
    }
  }

  private boolean vertexSucceeded(Vertex vertex) {
    numSuccessfulVertices++;
    boolean recoveryFailed = false;
    if (!commitAllOutputsOnSuccess && isCommittable()) {
      // committing successful outputs immediately. check for shared outputs
      List<VertexGroupInfo> groupsList = vertexGroupInfo.get(vertex.getName());
      if (groupsList != null) {
        List<VertexGroupInfo> commitList = Lists.newArrayListWithCapacity(groupsList
            .size());
        for (VertexGroupInfo groupInfo : groupsList) {
          groupInfo.successfulMembers++;
          if (groupInfo.groupMembers.size() == groupInfo.successfulMembers
              && !groupInfo.outputs.isEmpty()) {
            // group has outputs and all vertex members are done
            LOG.info("All members of group: " + groupInfo.groupName
                + " are succeeded. Commiting outputs");
            commitList.add(groupInfo);
          }
        }
        for (VertexGroupInfo groupInfo : commitList) {
          if (recoveryData != null && recoveryData.isVertexGroupCommitted(groupInfo.groupName)) {
            LOG.info("VertexGroup was already committed as per recovery"
                + " data, groupName=" + groupInfo.groupName);
            for (String vertexName : groupInfo.groupMembers) {
              VertexRecoveryData vertexRecoveryData =
                  recoveryData.getVertexRecoveryData(getVertex(vertexName).getVertexId());
              Preconditions.checkArgument(vertexRecoveryData != null,"Vertex Group has been committed"
                  + ", but no VertexRecoveryData found for its vertex " + vertexName);
              VertexFinishedEvent vertexFinishedEvent = vertexRecoveryData.getVertexFinishedEvent();
              Preconditions.checkArgument(vertexFinishedEvent!= null,"Vertex Group has been committed"
                  + ", but no VertexFinishedEvent found in its vertex " + vertexName);
              Preconditions.checkArgument(vertexFinishedEvent.getState() == VertexState.SUCCEEDED,
                  "Vertex Group has been committed, but unexpected vertex state of its vertex "
                  + vertexName + ", vertexstate=" + vertexFinishedEvent.getState());
            }
            continue;
          }
          groupInfo.commitStarted = true;
          final Vertex v = getVertex(groupInfo.groupMembers.iterator().next());
          try {
            Collection<TezVertexID> vertexIds = getVertexIds(groupInfo.groupMembers);
            appContext.getHistoryHandler().handleCriticalEvent(new DAGHistoryEvent(getID(),
                new VertexGroupCommitStartedEvent(dagId, groupInfo.groupName,
                    vertexIds, clock.getTime())));
          } catch (IOException e) {
            LOG.error("Failed to send commit recovery event to handler", e);
            recoveryFailed = true;
          }
          if (!recoveryFailed) {
            for (final String outputName : groupInfo.outputs) {
              OutputKey outputKey = new OutputKey(outputName, groupInfo.groupName, true);
              CommitCallback groupCommitCallback = new CommitCallback(outputKey);
              CallableEvent groupCommitCallableEvent = new CallableEvent(groupCommitCallback) {
                public Void call() throws Exception {
                  OutputCommitter committer = v.getOutputCommitters().get(outputName);
                  LOG.info("Committing output: " + outputName);
                  commitOutput(committer);
                  return null;
                };
              };
              ListenableFuture<Void> groupCommitFuture = appContext.getExecService().submit(groupCommitCallableEvent);
              Futures.addCallback(groupCommitFuture, groupCommitCallableEvent.getCallback());
              commitFutures.put(outputKey, groupCommitFuture);
            }
          }
        }
      }
    }
    if (recoveryFailed) {
      LOG.info("Recovery failure occurred during commit");
      enactKill(DAGTerminationCause.RECOVERY_FAILURE,
          VertexTerminationCause.COMMIT_FAILURE);
    }
    return !recoveryFailed;
  }

  private boolean vertexReRunning(Vertex vertex) {
    reRunningVertices.add(vertex.getVertexId());
    numSuccessfulVertices--;
    addDiagnostic("Vertex re-running"
      + ", vertexName=" + vertex.getName()
      + ", vertexId=" + vertex.getVertexId());

    if (!commitAllOutputsOnSuccess) {
      // partial output may already have been in committing or committed. fail if so
      List<VertexGroupInfo> groupList = vertexGroupInfo.get(vertex.getName());
      if (groupList != null) {
        for (VertexGroupInfo groupInfo : groupList) {
          if (groupInfo.isInCommitting()) {
            String msg = "Aborting job as committing vertex: "
                + vertex.getLogIdentifier() + " is re-running";
            LOG.info(msg);
            addDiagnostic(msg);
            enactKill(DAGTerminationCause.VERTEX_RERUN_IN_COMMITTING,
                VertexTerminationCause.VERTEX_RERUN_IN_COMMITTING);
            return true;
          } else if (groupInfo.isCommitted()) {
            String msg = "Aborting job as committed vertex: "
                + vertex.getLogIdentifier() + " is re-running";
            LOG.info(msg);
            addDiagnostic(msg);
            enactKill(DAGTerminationCause.VERTEX_RERUN_AFTER_COMMIT,
                VertexTerminationCause.VERTEX_RERUN_AFTER_COMMIT);
            return true;
          } else {
            groupInfo.successfulMembers--;
          }
        }
      }
    }
    return false;
  }

  private void vertexFailed(Vertex vertex) {
    numFailedVertices++;
    addDiagnostic("Vertex failed"
        + ", vertexName=" + vertex.getName()
        + ", vertexId=" + vertex.getVertexId()
        + ", diagnostics=" + vertex.getDiagnostics());
    // TODO: Metrics
    //job.metrics.failedTask(task);
  }

  private void vertexKilled(Vertex vertex) {
    numKilledVertices++;
    addDiagnostic("Vertex killed"
      + ", vertexName=" + vertex.getName()
      + ", vertexId=" + vertex.getVertexId()
      + ", diagnostics=" + vertex.getDiagnostics());
    // TODO: Metrics
    //job.metrics.killedTask(task);
  }

  private void addDiagnostic(String diag) {
    diagnostics.add(diag);
  }

  private static class DiagnosticsUpdateTransition implements
      SingleArcTransition<DAGImpl, DAGEvent> {
    @Override
    public void transition(DAGImpl job, DAGEvent event) {
      job.addDiagnostic(((DAGEventDiagnosticsUpdate) event)
          .getDiagnosticUpdate());
    }
  }

  private static class CounterUpdateTransition implements
      SingleArcTransition<DAGImpl, DAGEvent> {
    @Override
    public void transition(DAGImpl job, DAGEvent event) {
      DAGEventCounterUpdate jce = (DAGEventCounterUpdate) event;
      for (DAGEventCounterUpdate.CounterIncrementalUpdate ci : jce
          .getCounterUpdates()) {
        job.dagCounters.findCounter(ci.getCounterKey()).increment(
          ci.getIncrementValue());
      }
    }
  }

  private static class DAGSchedulerUpdateTransition implements
    SingleArcTransition<DAGImpl, DAGEvent> {
    @Override
    public void transition(DAGImpl dag, DAGEvent event) {
      DAGEventSchedulerUpdate sEvent = (DAGEventSchedulerUpdate) event;
      switch(sEvent.getUpdateType()) {
        case TA_SCHEDULE:
          dag.dagScheduler.scheduleTask(sEvent);
          break;
        case TA_COMPLETED:
          dag.dagScheduler.taskCompleted(sEvent);
          break;
        default:
          throw new TezUncheckedException("Unknown DAGEventSchedulerUpdate:"
                                  + sEvent.getUpdateType());
      }
    }
  }

  private static class CommitCompletedWhileRunning implements
    MultipleArcTransition<DAGImpl, DAGEvent, DAGState>{

    @Override
    public DAGState transition(DAGImpl dag, DAGEvent event) {
      DAGEventCommitCompleted commitCompletedEvent = (DAGEventCommitCompleted)event;
      if (dag.commitCompleted(commitCompletedEvent)) {
        return DAGState.RUNNING;
      } else {
        return DAGState.TERMINATING;
      }
    }
    
  }

  private static class CommitCompletedTransition implements
    MultipleArcTransition<DAGImpl, DAGEvent, DAGState>{

    @Override
    public DAGState transition(DAGImpl dag, DAGEvent event) {
      DAGEventCommitCompleted commitCompletedEvent = (DAGEventCommitCompleted)event;
      dag.commitCompleted(commitCompletedEvent);
      return checkCommitsForCompletion(dag);
    }
  }

  private boolean commitCompleted(DAGEventCommitCompleted commitCompletedEvent) {
    Preconditions.checkState(commitFutures.remove(commitCompletedEvent.getOutputKey()) != null,
        "Unknown commit:" + commitCompletedEvent.getOutputKey());

    boolean commitFailed = false;
    boolean recoveryFailed = false;
    if (commitCompletedEvent.isSucceeded()) {
      LOG.info("Commit succeeded for output:" + commitCompletedEvent.getOutputKey());
      OutputKey outputKey = commitCompletedEvent.getOutputKey();
      if (outputKey.isVertexGroupOutput){
        VertexGroupInfo vertexGroup = vertexGroups.get(outputKey.getEntityName());
        vertexGroup.successfulCommits++;
        if (vertexGroup.isCommitted()) {
          if (!commitAllOutputsOnSuccess) {
            try {
              Collection<TezVertexID> vertexIds = getVertexIds(vertexGroup.groupMembers);
              appContext.getHistoryHandler().handleCriticalEvent(new DAGHistoryEvent(getID(),
                  new VertexGroupCommitFinishedEvent(getID(), commitCompletedEvent.getOutputKey().getEntityName(),
                      vertexIds, clock.getTime())));
            } catch (IOException e) {
              String diag = "Failed to send commit recovery event to handler, " + ExceptionUtils.getStackTrace(e);
              addDiagnostic(diag);
              LOG.error(diag);
              recoveryFailed = true;
            }
          }
        }
      }
    } else {
      String diag = "Commit failed for output: " + commitCompletedEvent.getOutputKey()
          + ", " + ExceptionUtils.getStackTrace(commitCompletedEvent.getException());
      addDiagnostic(diag);
      LOG.error(diag);
      commitFailed = true;
    }

    if (commitFailed) {
      enactKill(DAGTerminationCause.COMMIT_FAILURE, VertexTerminationCause.OTHER_VERTEX_FAILURE);
      cancelCommits();
    }
    if (recoveryFailed){
      enactKill(DAGTerminationCause.RECOVERY_FAILURE, VertexTerminationCause.OTHER_VERTEX_FAILURE);
      cancelCommits();
    }
    return !commitFailed && !recoveryFailed;
  }


  private static class VertexRerunWhileCommitting implements
    SingleArcTransition<DAGImpl, DAGEvent> {

    @Override
    public void transition(DAGImpl dag, DAGEvent event) {
      LOG.info("Vertex rerun while dag it is COMMITTING");
      DAGEventVertexReRunning rerunEvent = (DAGEventVertexReRunning)event;
      Vertex vertex = dag.getVertex(rerunEvent.getVertexId());
      dag.reRunningVertices.add(vertex.getVertexId());
      dag.numSuccessfulVertices--;
      dag.numCompletedVertices--;
      dag.addDiagnostic("Vertex re-running"
          + ", vertexName=" + vertex.getName()
          + ", vertexId=" + vertex.getVertexId());
      dag.cancelCommits();
      dag.enactKill(DAGTerminationCause.VERTEX_RERUN_IN_COMMITTING, VertexTerminationCause.VERTEX_RERUN_IN_COMMITTING);
    }

  }

  // TODO TEZ-2250 go to TERMINATING to wait for all vertices and commits completed
  private static class InternalErrorTransition implements
      SingleArcTransition<DAGImpl, DAGEvent> {
    @Override
    public void transition(DAGImpl dag, DAGEvent event) {
      String diagnostics = null;
      if (event instanceof DiagnosableEvent) {
        DiagnosableEvent errEvent = (DiagnosableEvent) event;
        diagnostics = errEvent.getDiagnosticInfo();
        dag.addDiagnostic(diagnostics);
      }

      LOG.info(dag.getID() + " terminating due to internal error. "
          + (diagnostics == null? "" : " Error=" + diagnostics));
      // terminate all vertices
      dag.enactKill(DAGTerminationCause.INTERNAL_ERROR, VertexTerminationCause.INTERNAL_ERROR);
      dag.setFinishTime();
      dag.cancelCommits();
      dag.finished(DAGState.ERROR);
    }
  }

  @Override
  public boolean isComplete() {
    readLock.lock();
    try {
      DAGState state = getState();
      if (state.equals(DAGState.SUCCEEDED)
          || state.equals(DAGState.FAILED)
          || state.equals(DAGState.KILLED)
          || state.equals(DAGState.ERROR)) {
        return true;
      }
      return false;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public DAGScheduler getDAGScheduler() {
    return dagScheduler;
  }

  // output of either vertex or vertex group
  public static class OutputKey {
    String outputName;
    String entityName; // vertex name or vertex group name
    boolean isVertexGroupOutput;

    public OutputKey(String outputName, String entityName, boolean isVertexGroupOutput) {
      super();
      this.outputName = outputName;
      this.entityName = entityName;
      this.isVertexGroupOutput = isVertexGroupOutput;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
          + ((entityName == null) ? 0 : entityName.hashCode());
      result = prime * result + (isVertexGroupOutput ? 1231 : 1237);
      result = prime * result
          + ((outputName == null) ? 0 : outputName.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      OutputKey other = (OutputKey) obj;
      if (entityName == null) {
        if (other.entityName != null)
          return false;
      } else if (!entityName.equals(other.entityName))
        return false;
      if (isVertexGroupOutput != other.isVertexGroupOutput)
        return false;
      if (outputName == null) {
        if (other.outputName != null)
          return false;
      } else if (!outputName.equals(other.outputName))
        return false;
      return true;
    }

    public String getEntityName() {
      return entityName;
    }

    @Override
    public String toString() {
      return "outputName:" + outputName + " of vertex/vertexGroup:" + entityName
          + " isVertexGroupOutput:" + isVertexGroupOutput;
    }
  }

  private class CommitCallback implements FutureCallback<Void> {

    private OutputKey outputKey;

    public CommitCallback(OutputKey outputKey) {
      this.outputKey = outputKey;
    }

    @Override
    public void onSuccess(Void result) {
      eventHandler.handle(new DAGEventCommitCompleted(dagId, outputKey, true, null));
    }

    @Override
    public void onFailure(Throwable t) {
      eventHandler.handle(new DAGEventCommitCompleted(dagId, outputKey, false, t));
    }
  }
}
