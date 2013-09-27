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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.DAGStatusBuilder;
import org.apache.tez.dag.api.client.ProgressBuilder;
import org.apache.tez.dag.api.client.VertexStatusBuilder;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAGTerminationCause;
import org.apache.tez.dag.app.dag.DAGReport;
import org.apache.tez.dag.app.dag.DAGScheduler;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexTerminationCause;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventCounterUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventDiagnosticsUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdateTAAssigned;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.DAGEventVertexCompleted;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventDAGFinished;
import org.apache.tez.dag.app.dag.event.DAGEventVertexReRunning;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.app.dag.event.VertexEventTermination;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.utils.TezBuilderUtils;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.runtime.library.common.security.JobTokenIdentifier;
import org.apache.tez.runtime.library.common.security.JobTokenSecretManager;
import org.apache.tez.runtime.library.common.security.TokenCache;

import com.google.common.annotations.VisibleForTesting;

/** Implementation of Job interface. Maintains the state machines of Job.
 * The read and write calls use ReadWriteLock for concurrency.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class DAGImpl implements org.apache.tez.dag.app.dag.DAG,
  EventHandler<DAGEvent> {

  private static final Log LOG = LogFactory.getLog(DAGImpl.class);
  private static final String LINE_SEPARATOR = System
      .getProperty("line.separator");

  //final fields
  private final TezDAGID dagId;
  private final Clock clock;
  private final ApplicationACLsManager aclsManager;

  // TODO Recovery
  //private final List<AMInfo> amInfos;
  private final Lock readLock;
  private final Lock writeLock;
  private final String dagName;
  private final TaskAttemptListener taskAttemptListener;
  private final TaskHeartbeatHandler taskHeartbeatHandler;
  private final Object tasksSyncHandle = new Object();

  @VisibleForTesting
  DAGScheduler dagScheduler;

  private final EventHandler eventHandler;
  // TODO Metrics
  //private final MRAppMetrics metrics;
  private final String userName;
  private final String queueName;
  private final AppContext appContext;

  volatile Map<TezVertexID, Vertex> vertices = new HashMap<TezVertexID, Vertex>();
  private Map<String, Edge> edges = new HashMap<String, Edge>();
  private TezCounters dagCounters = new TezCounters();
  private Object fullCountersLock = new Object();
  private TezCounters fullCounters = null;
  private Set<TezVertexID> reRunningVertices = new HashSet<TezVertexID>();

  public final Configuration conf;
  private final DAGPlan jobPlan;

  private final List<String> diagnostics = new ArrayList<String>();

  private static final DiagnosticsUpdateTransition
      DIAGNOSTIC_UPDATE_TRANSITION = new DiagnosticsUpdateTransition();
  private static final InternalErrorTransition
      INTERNAL_ERROR_TRANSITION = new InternalErrorTransition();
  private static final CounterUpdateTransition COUNTER_UPDATE_TRANSITION =
      new CounterUpdateTransition();
  private static final DAGSchedulerUpdateTransition
          DAG_SCHEDULER_UPDATE_TRANSITION = new DAGSchedulerUpdateTransition();

  protected static final
    StateMachineFactory<DAGImpl, DAGState, DAGEventType, DAGEvent>
       stateMachineFactory
     = new StateMachineFactory<DAGImpl, DAGState, DAGEventType, DAGEvent>
              (DAGState.NEW)

          // Transitions from NEW state
          .addTransition(DAGState.NEW, DAGState.NEW,
              DAGEventType.DAG_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(DAGState.NEW, DAGState.NEW,
              DAGEventType.DAG_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition
              (DAGState.NEW,
              EnumSet.of(DAGState.INITED, DAGState.FAILED),
              DAGEventType.DAG_INIT,
              new InitTransition())
          .addTransition(DAGState.NEW, DAGState.KILLED,
              DAGEventType.DAG_KILL,
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
          .addTransition(DAGState.INITED, DAGState.KILLED,
              DAGEventType.DAG_KILL,
              new KillInitedJobTransition())
          .addTransition(DAGState.INITED, DAGState.ERROR,
              DAGEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from RUNNING state
          .addTransition
              (DAGState.RUNNING,
              EnumSet.of(DAGState.RUNNING, DAGState.SUCCEEDED, DAGState.TERMINATING,DAGState.FAILED),
              DAGEventType.DAG_VERTEX_COMPLETED,
              new VertexCompletedTransition())
          .addTransition(DAGState.RUNNING, DAGState.RUNNING,
              DAGEventType.DAG_VERTEX_RERUNNING,
              new VertexReRunningTransition())
          .addTransition(DAGState.RUNNING, DAGState.TERMINATING,
              DAGEventType.DAG_KILL, new DAGKilledTransition())
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

          // Transitions from TERMINATING state.
          .addTransition
              (DAGState.TERMINATING,
              EnumSet.of(DAGState.TERMINATING, DAGState.KILLED, DAGState.FAILED),
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

              // Ignore-able events
          .addTransition(DAGState.TERMINATING, DAGState.TERMINATING,
              EnumSet.of(DAGEventType.DAG_KILL,
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
              EnumSet.of(DAGEventType.DAG_KILL,
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
              EnumSet.of(DAGEventType.DAG_KILL,
                  DAGEventType.DAG_VERTEX_RERUNNING,
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
              EnumSet.of(DAGEventType.DAG_KILL,
                  DAGEventType.DAG_START,
                  DAGEventType.DAG_VERTEX_RERUNNING,
                  DAGEventType.DAG_SCHEDULER_UPDATE,
                  DAGEventType.DAG_VERTEX_COMPLETED))

          // No transitions from INTERNAL_ERROR state. Ignore all.
          .addTransition(
              DAGState.ERROR,
              DAGState.ERROR,
              EnumSet.of(DAGEventType.DAG_INIT,
                  DAGEventType.DAG_KILL,
                  DAGEventType.DAG_VERTEX_COMPLETED,
                  DAGEventType.DAG_DIAGNOSTIC_UPDATE,
                  DAGEventType.INTERNAL_ERROR))
          .addTransition(DAGState.ERROR, DAGState.ERROR,
              DAGEventType.DAG_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          // create the topology tables
          .installTopology();

  private final StateMachine<DAGState, DAGEventType, DAGEvent> stateMachine;

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
  private Token<JobTokenIdentifier> jobToken;
  private JobTokenSecretManager jobTokenSecretManager;

  private long initTime;
  private long startTime;
  private long finishTime;

  public DAGImpl(TezDAGID dagId,
      Configuration conf,
      DAGPlan jobPlan,
      EventHandler eventHandler,
      TaskAttemptListener taskAttemptListener,
      JobTokenSecretManager jobTokenSecretManager,
      Credentials fsTokenCredentials, Clock clock,
      String appUserName,
      TaskHeartbeatHandler thh,
      AppContext appContext) {
    this.dagId = dagId;
    this.jobPlan = jobPlan;
    this.conf = conf;
    this.dagName = (jobPlan.getName() != null) ? jobPlan.getName() : "<missing app name>";

    this.userName = appUserName;
    this.clock = clock;
    this.appContext = appContext;
    this.queueName = conf.get(MRJobConfig.QUEUE_NAME, "default");

    this.taskAttemptListener = taskAttemptListener;
    this.taskHeartbeatHandler = thh;
    this.eventHandler = eventHandler;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    this.credentials = fsTokenCredentials;
    this.jobTokenSecretManager = jobTokenSecretManager;

    this.aclsManager = new ApplicationACLsManager(conf);

    // This "this leak" is okay because the retained pointer is in an
    //  instance variable.
    stateMachine = stateMachineFactory.make(this);
  }

  protected StateMachine<DAGState, DAGEventType, DAGEvent> getStateMachine() {
    return stateMachine;
  }

  @Override
  public TezDAGID getID() {
    return dagId;
  }

  // TODO maybe removed after TEZ-74
  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public DAGPlan getJobPlan() {
    return jobPlan;
  }

  EventHandler getEventHandler() {
    return this.eventHandler;
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI,
      ApplicationAccessType jobOperation) {
    return aclsManager.checkAccess(callerUGI, jobOperation, userName,
        this.dagId.getApplicationId());
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
  public TezCounters getAllCounters() {

    readLock.lock();

    try {
      DAGState state = getInternalState();
      if (state == DAGState.ERROR || state == DAGState.FAILED
          || state == DAGState.KILLED || state == DAGState.SUCCEEDED) {
        this.mayBeConstructFinalFullCounters();
        return fullCounters;
      }

      TezCounters counters = new TezCounters();
      counters.incrAllCounters(dagCounters);
      return incrTaskCounters(counters, vertices.values());

    } finally {
      readLock.unlock();
    }
  }

  public static TezCounters incrTaskCounters(
      TezCounters counters, Collection<Vertex> vertices) {
    for (Vertex vertex : vertices) {
      counters.incrAllCounters(vertex.getAllCounters());
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
        progress += v.getProgress();
      }
      return progress / getTotalVertices();
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
  public DAGStatusBuilder getDAGStatus() {
    DAGStatusBuilder status = new DAGStatusBuilder();
    int totalTaskCount = 0;
    int totalSucceededTaskCount = 0;
    int totalRunningTaskCount = 0;
    int totalFailedTaskCount = 0;
    int totalKilledTaskCount = 0;
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
      }
      ProgressBuilder dagProgress = new ProgressBuilder();
      dagProgress.setTotalTaskCount(totalTaskCount);
      dagProgress.setSucceededTaskCount(totalSucceededTaskCount);
      dagProgress.setRunningTaskCount(totalRunningTaskCount);
      dagProgress.setFailedTaskCount(totalFailedTaskCount);
      dagProgress.setKilledTaskCount(totalKilledTaskCount);
      status.setState(getState());
      status.setDiagnostics(diagnostics);
      status.setDAGProgress(dagProgress);
      return status;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public VertexStatusBuilder getVertexStatus(String vertexName) {
    Vertex vertex = vertexMap.get(vertexName);
    if(vertex == null) {
      return null;
    }
    return vertex.getVertexStatus();
  }


  protected void startRootVertices() {
    for (Vertex v : vertices.values()) {
      if (v.getInputVerticesCount() == 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Starting root vertex " + v.getName());
        }
        eventHandler.handle(new VertexEvent(v.getVertexId(),
            VertexEventType.V_START));
      }
    }
  }

  protected void initializeVertices() {
    for (Vertex v : vertices.values()) {
      eventHandler.handle(new VertexEvent(v.getVertexId(),
          VertexEventType.V_INIT));
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
        LOG.error("Can't handle this event at current state", e);
        addDiagnostic("Invalid event " + event.getType() +
            " on Job " + this.dagId);
        eventHandler.handle(new DAGEvent(this.dagId,
            DAGEventType.INTERNAL_ERROR));
      }
      //notify the eventhandler of state change
      if (oldState != getInternalState()) {
        LOG.info(dagId + " transitioned from " + oldState + " to "
                 + getInternalState());
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

  void logJobHistoryFinishedEvent() {
    this.setFinishTime();
    DAGFinishedEvent finishEvt = new DAGFinishedEvent(dagId, startTime,
        finishTime, DAGStatus.State.SUCCEEDED, "", getAllCounters());
    this.eventHandler.handle(
        new DAGHistoryEvent(finishEvt));
  }

  void logJobHistoryInitedEvent() {
    // FIXME should we have more information in this event?
    // numVertices, etc?
    DAGStartedEvent startEvt = new DAGStartedEvent(this.dagId,
        this.initTime, this.startTime);
    this.eventHandler.handle(
        new DAGHistoryEvent(startEvt));
  }

  void logJobHistoryUnsuccesfulEvent(DAGStatus.State state) {
    DAGFinishedEvent finishEvt = new DAGFinishedEvent(dagId, startTime,
        clock.getTime(), state,
        StringUtils.join(LINE_SEPARATOR, getDiagnostics()),
        getAllCounters());
    this.eventHandler.handle(
        new DAGHistoryEvent(finishEvt));
  }

  static DAGState checkJobForCompletion(DAGImpl dag) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking dag completion"
          + ", numCompletedVertices=" + dag.numCompletedVertices
          + ", numSuccessfulVertices=" + dag.numSuccessfulVertices
          + ", numFailedVertices=" + dag.numFailedVertices
          + ", numKilledVertices=" + dag.numKilledVertices
          + ", numVertices=" + dag.numVertices
          + ", terminationCause=" + dag.terminationCause);
    }

    // log in case of accounting error.
    if (dag.numCompletedVertices > dag.numVertices) {
      LOG.error("vertex completion accounting issue: numCompletedVertices > numVertices"
          + ", numCompletedVertices=" + dag.numCompletedVertices
          + ", numVertices=" + dag.numVertices
          );
    }

    if (dag.numCompletedVertices == dag.numVertices) {
      //Only succeed if vertices complete successfully and no terminationCause is registered.
      if(dag.numSuccessfulVertices == dag.numVertices && dag.terminationCause == null) {
        dag.setFinishTime();
        dag.logJobHistoryFinishedEvent();
        return dag.finished(DAGState.SUCCEEDED);
      }
      else if(dag.terminationCause == DAGTerminationCause.DAG_KILL ){
        dag.setFinishTime();
        String diagnosticMsg = "DAG killed due to user-initiated kill." +
            " failedVertices:" + dag.numFailedVertices +
            " killedVertices:" + dag.numKilledVertices;
        LOG.info(diagnosticMsg);
        dag.addDiagnostic(diagnosticMsg);
        dag.abortJob(DAGStatus.State.KILLED);
        return dag.finished(DAGState.KILLED);
      }
      if(dag.terminationCause == DAGTerminationCause.VERTEX_FAILURE ){
        dag.setFinishTime();
        String diagnosticMsg = "DAG failed due to vertex failure." +
            " failedVertices:" + dag.numFailedVertices +
            " killedVertices:" + dag.numKilledVertices;
        LOG.info(diagnosticMsg);
        dag.addDiagnostic(diagnosticMsg);
        dag.abortJob(DAGStatus.State.FAILED);
        return dag.finished(DAGState.FAILED);
      }
      else {
        // should never get here.
        throw new TezUncheckedException("All vertices complete, but cannot determine final state of DAG"
            + ", numCompletedVertices=" + dag.numCompletedVertices
            + ", numSuccessfulVertices=" + dag.numSuccessfulVertices
            + ", numFailedVertices=" + dag.numFailedVertices
            + ", numKilledVertices=" + dag.numKilledVertices
            + ", numVertices=" + dag.numVertices
            + ", terminationCause=" + dag.terminationCause);
      }
    }

    //return the current state, Job not finished yet
    return dag.getInternalState();
  }

  DAGState finished(DAGState finalState) {
    // TODO Metrics
    /*
    if (getInternalState() == DAGState.RUNNING) {
      metrics.endRunningJob(this);
    }
    */
    if (finishTime == 0) setFinishTime();
    eventHandler.handle(new DAGAppMasterEventDAGFinished(getID(), finalState));

    // TODO Metrics
    /*
    switch (finalState) {
      case KILLED:
        metrics.killedJob(this);
        break;
      case FAILED:
        metrics.failedJob(this);
        break;
      case SUCCEEDED:
        metrics.completedJob(this);
    }
    */
    return finalState;
  }

  @Override
  public String getUserName() {
    return userName;
  }

  @Override
  public String getQueueName() {
    return queueName;
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

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app2.job.Job#getJobACLs()
   */
  @Override
  public Map<ApplicationAccessType, String> getJobACLs() {
    // TODO ApplicationACLs
    return null;
  }

  // TODO Recovery
  /*
  @Override
  public List<AMInfo> getAMInfos() {
    return amInfos;
  }
  */

  public static class InitTransition
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
      try {
        setup(dag);

        // If we have no vertices, fail the dag
        dag.numVertices = dag.getJobPlan().getVertexCount();
        if (dag.numVertices == 0) {
          dag.addDiagnostic("No vertices for dag");
          dag.trySetTerminationCause(DAGTerminationCause.ZERO_VERTICES);
          dag.abortJob(DAGStatus.State.FAILED);
          return dag.finished(DAGState.FAILED);
        }

        checkTaskLimits();

        // create the vertices
        for (int i=0; i < dag.numVertices; ++i) {
          String vertexName = dag.getJobPlan().getVertex(i).getName();
          VertexImpl v = createVertex(dag, vertexName, i);
          dag.addVertex(v);
        }

        createDAGEdges(dag);
        Map<String,EdgePlan> edgePlans = DagTypeConverters.createEdgePlanMapFromDAGPlan(dag.getJobPlan().getEdgeList());

        // setup the dag
        for (Vertex v : dag.vertices.values()) {
          parseVertexEdges(dag, edgePlans, v);
        }

        assignDAGScheduler(dag);

        // TODO Metrics
        //dag.metrics.endPreparingJob(dag);
        return DAGState.INITED;

      } catch (IOException e) {
        LOG.warn("Job init failed", e);
        dag.addDiagnostic("Job init failed : "
            + StringUtils.stringifyException(e));
        dag.trySetTerminationCause(DAGTerminationCause.INIT_FAILURE);
        dag.abortJob(DAGStatus.State.FAILED);
        // TODO Metrics
        //dag.metrics.endPreparingJob(dag);
        return dag.finished(DAGState.FAILED);
      }
    }

    private void createDAGEdges(DAGImpl dag) {
      for (EdgePlan edgePlan : dag.getJobPlan().getEdgeList()) {
        EdgeProperty edgeProperty = DagTypeConverters
            .createEdgePropertyMapFromDAGPlan(edgePlan);
        // edge manager may be also set via API when using custom edge type
        dag.edges.put(edgePlan.getId(),
            new Edge(edgeProperty, dag.getEventHandler()));
      }
    }

    private void assignDAGScheduler(DAGImpl dag) {
      if (dag.conf.getBoolean(TezConfiguration.TEZ_AM_AGGRESSIVE_SCHEDULING,
          TezConfiguration.TEZ_AM_AGGRESSIVE_SCHEDULING_DEFAULT)) {
        LOG.info("Using Natural order dag scheduler due to aggressive scheduling");
        dag.dagScheduler = new DAGSchedulerNaturalOrder(dag, dag.eventHandler);
      } else {
        boolean isMRR = true;
        for (Vertex vertex : dag.vertices.values()) {
          Map<Vertex, Edge> outVertices = vertex.getOutputVertices();
          Map<Vertex, Edge> inVertices = vertex.getInputVertices();
          if (!(outVertices == null || outVertices.isEmpty() || (outVertices
              .size() == 1 && outVertices.values().iterator().next().getEdgeProperty()
              .getDataMovementType() == EdgeProperty.DataMovementType.SCATTER_GATHER))) {
            // more than 1 output OR single output is not bipartite
            isMRR = false;
            break;
          }
          if (!(inVertices == null || inVertices.isEmpty() || (inVertices
              .size() == 1 && inVertices.values().iterator().next().getEdgeProperty()
              .getDataMovementType() == EdgeProperty.DataMovementType.SCATTER_GATHER))) {
            // more than 1 output OR single output is not bipartite
            isMRR = false;
            break;
          }
        }

        if (isMRR) {
          LOG.info("Using MRR dag scheduler");
          dag.dagScheduler = new DAGSchedulerMRR(
              dag,
              dag.eventHandler,
              dag.appContext.getTaskScheduler(),
              dag.conf
                  .getFloat(
                      TezConfiguration.TEZ_AM_SLOWSTART_DAG_SCHEDULER_MIN_SHUFFLE_RESOURCE_FRACTION,
                      TezConfiguration.TEZ_AM_SLOWSTART_DAG_SCHEDULER_MIN_SHUFFLE_RESOURCE_FRACTION_DEFAULT));
        } else {
          LOG.info("Using Natural order dag scheduler");
          dag.dagScheduler = new DAGSchedulerNaturalOrder(dag, dag.eventHandler);
        }
      }
    }

    private VertexImpl createVertex(DAGImpl dag, String vertexName, int vId) {
      TezVertexID vertexId = TezBuilderUtils.newVertexID(dag.getID(), vId);

      VertexPlan vertexPlan = dag.getJobPlan().getVertex(vId);
      VertexLocationHint vertexLocationHint = DagTypeConverters
          .convertFromDAGPlan(vertexPlan.getTaskLocationHintList());

      VertexImpl v = new VertexImpl(
          vertexId, vertexPlan, vertexName, dag.conf,
          dag.eventHandler, dag.taskAttemptListener,
          dag.credentials, dag.clock,
          dag.taskHeartbeatHandler, dag.appContext,
          vertexLocationHint);
      if (vertexPlan.getInputsCount() > 0) {
        v.setAdditionalInputs(vertexPlan.getInputsList());
      }
      if (vertexPlan.getOutputsCount() > 0) {
        v.setAdditionalOutputs(vertexPlan.getOutputsList());
      }
      return v;
    }

    // hooks up this VertexImpl to input and output EdgeProperties
    private void parseVertexEdges(DAGImpl dag, Map<String, EdgePlan> edgePlans, Vertex vertex) {
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

    protected void setup(DAGImpl job) throws IOException {
      job.initTime = job.clock.getTime();
      String dagIdString = job.dagId.toString().replace("application", "job");

      // Prepare the TaskAttemptListener server for authentication of Containers
      // TaskAttemptListener gets the information via jobTokenSecretManager.
      JobTokenIdentifier identifier =
          new JobTokenIdentifier(new Text(dagIdString));
      job.jobToken =
          new Token<JobTokenIdentifier>(identifier, job.jobTokenSecretManager);
      job.jobToken.setService(identifier.getJobId());
      // Add it to the jobTokenSecretManager so that TaskAttemptListener server
      // can authenticate containers(tasks)
      job.jobTokenSecretManager.addTokenForJob(dagIdString, job.jobToken);
      LOG.info("Adding job token for " + dagIdString
          + " to jobTokenSecretManager");

      // Populate the jobToken into job credentials.
      TokenCache.setJobToken(job.jobToken, job.credentials);
    }

    /**
     * If the number of tasks are greater than the configured value
     * throw an exception that will fail job initialization
     */
    private void checkTaskLimits() {
      // no code, for now
    }
  } // end of InitTransition

  public static class StartTransition
  implements SingleArcTransition<DAGImpl, DAGEvent> {
    /**
     * This transition executes in the event-dispatcher thread, though it's
     * triggered in MRAppMaster's startJobs() method.
     */
    @Override
    public void transition(DAGImpl job, DAGEvent event) {
      job.startTime = job.clock.getTime();
      job.initializeVertices();
      job.logJobHistoryInitedEvent();
      // TODO Metrics
      //job.metrics.runningJob(job);

      // Start all vertices with no incoming edges when job starts
      job.startRootVertices();
    }
  }

  private void abortJob(DAGStatus.State abortState) {
    // TODO: DAG Committer
    logJobHistoryUnsuccesfulEvent(abortState);
  }

  Map<String, Vertex> vertexMap = new HashMap<String, Vertex>();
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
      this.constructFinalFullcounters();
    }
  }

  @Private
  public void constructFinalFullcounters() {
    this.fullCounters = new TezCounters();
    this.fullCounters.incrAllCounters(dagCounters);
    for (Vertex v : this.vertices.values()) {
      this.fullCounters.incrAllCounters(v.getAllCounters());
    }
  }

  /**
   * Set the terminationCause and send a kill-message to all vertices.
   * The vertex-kill messages are only sent once.
   * @param the trigger that is causing the DAG to transition to KILLED/FAILED
   * @param event The type of kill event to send to the vertices.
   */
  void enactKill(DAGTerminationCause dagTerminationCause, VertexTerminationCause vertexTerminationCause) {

    if(trySetTerminationCause(dagTerminationCause)){
      for (Vertex v : vertices.values()) {
        eventHandler.handle(
            new VertexEventTermination(v.getVertexId(), vertexTerminationCause)
            );
      }
    }
  }

  // Task-start has been moved out of InitTransition, so this arc simply
  // hardcodes 0 for both map and reduce finished tasks.
  private static class KillNewJobTransition
  implements SingleArcTransition<DAGImpl, DAGEvent> {
    @Override
    public void transition(DAGImpl job, DAGEvent event) {
      job.setFinishTime();
      job.logJobHistoryUnsuccesfulEvent(DAGStatus.State.KILLED);
      job.trySetTerminationCause(DAGTerminationCause.DAG_KILL);
      job.finished(DAGState.KILLED);
    }
  }

  private static class KillInitedJobTransition
  implements SingleArcTransition<DAGImpl, DAGEvent> {
    @Override
    public void transition(DAGImpl job, DAGEvent event) {
      job.trySetTerminationCause(DAGTerminationCause.DAG_KILL);
      job.abortJob(DAGStatus.State.KILLED);
      job.addDiagnostic("Job received Kill in INITED state.");
      job.finished(DAGState.KILLED);
    }
  }

  private static class DAGKilledTransition
      implements SingleArcTransition<DAGImpl, DAGEvent> {
    @Override
    public void transition(DAGImpl job, DAGEvent event) {
      job.addDiagnostic("Job received Kill while in RUNNING state.");
      job.enactKill(DAGTerminationCause.DAG_KILL, VertexTerminationCause.DAG_KILL);
      // TODO Metrics
      //job.metrics.endRunningJob(job);
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
        if (!job.reRunningVertices.contains(vertex.getVertexId())) {
          // vertex succeeded for the first time 
          job.dagScheduler.vertexCompleted(vertex);
        }
        job.vertexSucceeded(vertex);
      }
      else if (vertexEvent.getVertexState() == VertexState.FAILED) {
        job.enactKill(DAGTerminationCause.VERTEX_FAILURE, VertexTerminationCause.OTHER_VERTEX_FAILURE);
        job.vertexFailed(vertex);
        forceTransitionToKillWait = true;
      }
      else if (vertexEvent.getVertexState() == VertexState.KILLED) {
        job.vertexKilled(vertex);
        forceTransitionToKillWait = true;
      }
      
      job.reRunningVertices.remove(vertex.getVertexId());

      LOG.info("Vertex " + vertex.getVertexId() + " completed."
          + ", numCompletedVertices=" + job.numCompletedVertices
          + ", numSuccessfulVertices=" + job.numSuccessfulVertices
          + ", numFailedVertices=" + job.numFailedVertices
          + ", numKilledVertices=" + job.numKilledVertices
          + ", numVertices=" + job.numVertices);

      // if the job has not finished but a failure/kill occurred, then force the transition to KILL_WAIT.
      DAGState state = checkJobForCompletion(job);
      if(state == DAGState.RUNNING && forceTransitionToKillWait){
        return DAGState.TERMINATING;
      }
      else {
        return state;
      }
    }

  }

  private static class VertexReRunningTransition implements
      SingleArcTransition<DAGImpl, DAGEvent> {
    @Override
    public void transition(DAGImpl job, DAGEvent event) {
      DAGEventVertexReRunning vertexEvent = (DAGEventVertexReRunning) event;
      Vertex vertex = job.vertices.get(vertexEvent.getVertexId());
      job.numCompletedVertices--;
      job.vertexReRunning(vertex);


      LOG.info("Vertex " + vertex.getVertexId() + " re-running."
          + ", numCompletedVertices=" + job.numCompletedVertices
          + ", numSuccessfulVertices=" + job.numSuccessfulVertices
          + ", numFailedVertices=" + job.numFailedVertices
          + ", numKilledVertices=" + job.numKilledVertices
          + ", numVertices=" + job.numVertices);
    }
  }

  private void vertexSucceeded(Vertex vertex) {
    numSuccessfulVertices++;
    // TODO: Metrics
    //job.metrics.completedTask(task);
  }

  private void vertexReRunning(Vertex vertex) {
    reRunningVertices.add(vertex.getVertexId());
    numSuccessfulVertices--;
    addDiagnostic("Vertex re-running " + vertex.getVertexId());
    // TODO: Metrics
    //job.metrics.completedTask(task);
  }

  private void vertexFailed(Vertex vertex) {
    numFailedVertices++;
    addDiagnostic("Vertex failed " + vertex.getVertexId());
    // TODO: Metrics
    //job.metrics.failedTask(task);
  }

  private void vertexKilled(Vertex vertex) {
    numKilledVertices++;
    addDiagnostic("Vertex killed " + vertex.getVertexId());
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
      case TA_SCHEDULED:
        DAGEventSchedulerUpdateTAAssigned taEvent =
                              (DAGEventSchedulerUpdateTAAssigned) sEvent;
        dag.dagScheduler.taskScheduled(taEvent);
        break;
      case TA_SUCCEEDED:
        dag.dagScheduler.taskSucceeded(sEvent);
        break;
      default:
        throw new TezUncheckedException("Unknown DAGEventSchedulerUpdate:"
                                + sEvent.getUpdateType());
    }
  }
}

  private static class InternalErrorTransition implements
      SingleArcTransition<DAGImpl, DAGEvent> {
    @Override
    public void transition(DAGImpl job, DAGEvent event) {
      //TODO Is this JH event required.
      LOG.info(job.getID() + " terminating due to internal error");
      // terminate all vertices
      job.enactKill(DAGTerminationCause.INTERNAL_ERROR,
          VertexTerminationCause.INTERNAL_ERROR);
      job.setFinishTime();
      job.logJobHistoryUnsuccesfulEvent(DAGStatus.State.FAILED);
      job.finished(DAGState.ERROR);
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
}
