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
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.DAGProtos.VertexPlan;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.client.impl.TezBuilderUtils;
import org.apache.tez.dag.api.impl.DAGStatus;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAGReport;
import org.apache.tez.dag.app.dag.DAGScheduler;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.event.DAGEventCounterUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventDiagnosticsUpdate;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.DAGFinishEvent;
import org.apache.tez.dag.app.dag.event.DAGEventVertexCompleted;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.utils.DAGApps;
import org.apache.tez.engine.common.security.JobTokenIdentifier;
import org.apache.tez.engine.common.security.JobTokenSecretManager;
import org.apache.tez.engine.common.security.TokenCache;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;

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
  private final ApplicationAttemptId applicationAttemptId;
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
  
  private DAGScheduler dagScheduler;

  private final EventHandler eventHandler;
  // TODO Metrics
  //private final MRAppMetrics metrics;
  private final String userName;
  private final String queueName;
  private final long appSubmitTime;
  private final AppContext appContext;

  volatile Map<TezVertexID, Vertex> vertices = new HashMap<TezVertexID, Vertex>();
  private Map<String, EdgeProperty> edges = new HashMap<String, EdgeProperty>(); 
  private TezCounters dagCounters = new TezCounters();
  private Object fullCountersLock = new Object();
  private TezCounters fullCounters = null;

  public final TezConfiguration conf;
  private final DAGPlan jobPlan;

  //fields initialized in init
  private FileSystem fs;
  private Path remoteJobSubmitDir;
  public Path remoteJobConfFile;

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
              EnumSet.of(DAGState.RUNNING, DAGState.SUCCEEDED, DAGState.FAILED),
              DAGEventType.DAG_VERTEX_COMPLETED,
              new VertexCompletedTransition())
          .addTransition
              (DAGState.RUNNING,
              EnumSet.of(DAGState.RUNNING, DAGState.SUCCEEDED, DAGState.FAILED),
              DAGEventType.DAG_COMPLETED,
              new JobNoTasksCompletedTransition())
          .addTransition(DAGState.RUNNING, DAGState.KILL_WAIT,
              DAGEventType.DAG_KILL, new KillVerticesTransition())
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

          // Transitions from KILL_WAIT state.
          .addTransition
              (DAGState.KILL_WAIT,
              EnumSet.of(DAGState.KILL_WAIT, DAGState.KILLED),
              DAGEventType.DAG_VERTEX_COMPLETED,
              new VertexCompletedTransition())
          .addTransition(DAGState.KILL_WAIT, DAGState.KILL_WAIT,
              DAGEventType.DAG_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(DAGState.KILL_WAIT, DAGState.KILL_WAIT,
              DAGEventType.DAG_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              DAGState.KILL_WAIT,
              DAGState.ERROR, DAGEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

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
  private int numVertices;
  private int numCompletedVertices = 0;
  private int numSuccessfulVertices = 0;
  private int numFailedVertices = 0;
  private int numKilledVertices = 0;
  private boolean isUber = false;

  private Credentials fsTokens;
  private Token<JobTokenIdentifier> jobToken;
  private JobTokenSecretManager jobTokenSecretManager;

  private long initTime;
  private long startTime;
  private long finishTime;

  public DAGImpl(TezDAGID dagId, ApplicationAttemptId applicationAttemptId,
      TezConfiguration conf,
      DAGPlan jobPlan,
      EventHandler eventHandler,
      TaskAttemptListener taskAttemptListener,
      JobTokenSecretManager jobTokenSecretManager,
      Credentials fsTokenCredentials, Clock clock,
      // TODO Metrics
      //MRAppMetrics metrics,
      String appUserName,
      long appSubmitTime,
      // TODO Recovery
      //List<AMInfo> amInfos,
      TaskHeartbeatHandler thh,
      AppContext appContext) {
    this.applicationAttemptId = applicationAttemptId;
    this.dagId = dagId;
    this.jobPlan = jobPlan;
    this.conf = conf;
    this.dagName = (jobPlan.getName() != null) ? jobPlan.getName() : "<missing app name>";
    
    this.userName = appUserName;
    // TODO Metrics
    //this.metrics = metrics;
    this.clock = clock;
    // TODO Recovery
    //this.amInfos = amInfos;
    this.appContext = appContext;
    this.queueName = conf.get(MRJobConfig.QUEUE_NAME, "default");
    this.appSubmitTime = appSubmitTime;

    this.taskAttemptListener = taskAttemptListener;
    this.taskHeartbeatHandler = thh;
    this.eventHandler = eventHandler;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    this.fsTokens = fsTokenCredentials;
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
  public TezConfiguration getConf() {
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

  protected void startRootVertices() {
    for (Vertex v : vertices.values()) {
      if (v.getInputVerticesCount() == 0) {
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
    LOG.info("DEBUG: Processing DAGEvent " + event.getDAGId() + " of type "
        + event.getType() + " while in state " + getInternalState()
        + ". Event: " + event);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing DAGEvent " + event.getDAGId() + " of type "
          + event.getType() + " while in state " + getInternalState());
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
    DAGFinishedEvent finishEvt = new DAGFinishedEvent(dagId, finishTime,
        DAGStatus.State.SUCCEEDED, "");
    this.eventHandler.handle(
        new DAGHistoryEvent(dagId, finishEvt));
  }

  void logJobHistoryInitedEvent() {
    // FIXME should we have more information in this event?
    // numVertices, etc?
    DAGStartedEvent startEvt = new DAGStartedEvent(this.dagId,
        this.initTime, this.startTime);
    this.eventHandler.handle(
        new DAGHistoryEvent(dagId, startEvt));
  }

  void logJobHistoryUnsuccesfulEvent(DAGStatus.State state) {
    DAGFinishedEvent finishEvt = new DAGFinishedEvent(dagId, clock.getTime(),
        state, StringUtils.join(LINE_SEPARATOR, getDiagnostics()));
    this.eventHandler.handle(
        new DAGHistoryEvent(dagId, finishEvt));
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

  static DAGState checkJobForCompletion(DAGImpl dag) {

    LOG.info("ZZZZ: Checking dag completion"
        + ", numCompletedVertices=" + dag.numCompletedVertices
        + ", numSuccessfulVertices=" + dag.numSuccessfulVertices
        + ", numFailedVertices=" + dag.numFailedVertices
        + ", numKilledVertices=" + dag.numKilledVertices
        + ", numVertices=" + dag.numVertices);

    if (dag.numFailedVertices > 0) {
      dag.setFinishTime();
      String diagnosticMsg = "DAG failed as vertices failed. " +
          " failedVertices:" + dag.numFailedVertices +
          " killedVertices:" + dag.numKilledVertices;
      LOG.info(diagnosticMsg);
      dag.addDiagnostic(diagnosticMsg);
      dag.abortJob(DAGStatus.State.FAILED);
      return dag.finished(DAGState.FAILED);
    }

    if(dag.numSuccessfulVertices == dag.numVertices) {
      dag.setFinishTime();
      dag.logJobHistoryFinishedEvent();
      return dag.finished(DAGState.SUCCEEDED);
    }

    if (dag.numCompletedVertices == dag.numVertices) {
      // this means the dag has some killed vertices
      String diagnosticMsg = "DAG killed. " +
          " failedVertices:" + dag.numFailedVertices +
          " killedVertices:" + dag.numKilledVertices;
      LOG.info(diagnosticMsg);
      dag.addDiagnostic(diagnosticMsg);
      assert dag.numKilledVertices > 0;
      dag.setFinishTime();
      dag.abortJob(DAGStatus.State.KILLED);
      return dag.finished(DAGState.KILLED);
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
    eventHandler.handle(new DAGFinishEvent(dagId));

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

//  /**
//   * ChainMapper and ChainReducer must execute in parallel, so they're not
//   * compatible with uberization/LocalContainerLauncher (100% sequential).
//   */
//  private boolean isChainJob(Configuration conf) {
//    boolean isChainJob = false;
//    try {
//      String mapClassName = conf.get(MRJobConfig.MAP_CLASS_ATTR);
//      if (mapClassName != null) {
//        Class<?> mapClass = Class.forName(mapClassName);
//        if (ChainMapper.class.isAssignableFrom(mapClass))
//          isChainJob = true;
//      }
//    } catch (ClassNotFoundException cnfe) {
//      // don't care; assume it's not derived from ChainMapper
//    }
//    try {
//      String reduceClassName = conf.get(MRJobConfig.REDUCE_CLASS_ATTR);
//      if (reduceClassName != null) {
//        Class<?> reduceClass = Class.forName(reduceClassName);
//        if (ChainReducer.class.isAssignableFrom(reduceClass))
//          isChainJob = true;
//      }
//    } catch (ClassNotFoundException cnfe) {
//      // don't care; assume it's not derived from ChainReducer
//    }
//    return isChainJob;
//  }

  /*
  private int getBlockSize() {
    String inputClassName = conf.get(MRJobConfig.INPUT_FORMAT_CLASS_ATTR);
    if (inputClassName != null) {
      Class<?> inputClass - Class.forName(inputClassName);
      if (FileInputFormat<K, V>)
    }
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
        dag.fs = dag.getFileSystem(dag.conf);

        checkTaskLimits();

        // TODO: Committer
        /*
        if (dag.newApiCommitter) {
          dag.dagContext = new JobContextImpl(dag.conf,
              dag.oldJobId);
        } else {
          dag.dagContext = new org.apache.hadoop.mapred.JobContextImpl(
              dag.conf, dag.oldJobId);
        }

        // do the setup
        dag.committer.setupJob(dag.dagContext);
        dag.setupProgress = 1.0f;
        */

        // create the vertices
        dag.numVertices = dag.getJobPlan().getVertexCount();
        for (int i=0; i < dag.numVertices; ++i) {
          String vertexName = dag.getJobPlan().getVertex(i).getName();
          VertexImpl v = createVertex(dag, vertexName, i);
          dag.addVertex(v);
        }

        dag.edges = DagTypeConverters.createEdgePropertyMapFromDAGPlan(dag.getJobPlan().getEdgeList());
        Map<String,EdgePlan> edgePlans = DagTypeConverters.createEdgePlanMapFromDAGPlan(dag.getJobPlan().getEdgeList());
        
        // setup the dag
        for (Vertex v : dag.vertices.values()) {
          parseVertexEdges(dag, edgePlans, v);
        }

        dag.dagScheduler = new DAGSchedulerNaturalOrder(dag, dag.eventHandler);
        //dag.dagScheduler = new DAGSchedulerMRR(dag, dag.eventHandler);

        // TODO Metrics
        //dag.metrics.endPreparingJob(dag);
        return DAGState.INITED;

      } catch (IOException e) {
        LOG.warn("Job init failed", e);
        dag.addDiagnostic("Job init failed : "
            + StringUtils.stringifyException(e));
        dag.abortJob(DAGStatus.State.FAILED);
        // TODO Metrics
        //dag.metrics.endPreparingJob(dag);
        return dag.finished(DAGState.FAILED);
      }
    }

    private VertexImpl createVertex(DAGImpl dag, String vertexName, int vId) {
      TezVertexID vertexId = TezBuilderUtils.newVertexID(dag.getID(), vId);
      
      VertexPlan vertexPlan = dag.getJobPlan().getVertex(vId);
      VertexLocationHint vertexLocationHint = DagTypeConverters.convertFromDAGPlan(vertexPlan.getTaskLocationHintList());
        
      return new VertexImpl(
          vertexId, vertexPlan, vertexName, dag.conf,
          dag.eventHandler, dag.taskAttemptListener,
          dag.jobToken, dag.fsTokens, dag.clock,
          dag.taskHeartbeatHandler, dag.appContext,
          vertexLocationHint);
    }

    // hooks up this VertexImpl to input and output EdgeProperties
    private void parseVertexEdges(DAGImpl dag, Map<String, EdgePlan> edgePlans, Vertex vertex) {
      VertexPlan vertexPlan = vertex.getVertexPlan();

      Map<Vertex, EdgeProperty> inVertices =
          new HashMap<Vertex, EdgeProperty>();

      Map<Vertex, EdgeProperty> outVertices =
          new HashMap<Vertex, EdgeProperty>();

      for(String inEdgeId : vertexPlan.getInEdgeIdList()){
        EdgePlan edgePlan = edgePlans.get(inEdgeId);
        Vertex inVertex = dag.vertexMap.get(edgePlan.getInputVertexName());    
        EdgeProperty edgeProp = dag.edges.get(inEdgeId);
        inVertices.put(inVertex, edgeProp);
      }
      
      for(String outEdgeId : vertexPlan.getOutEdgeIdList()){
        EdgePlan edgePlan = edgePlans.get(outEdgeId);
        Vertex outVertex = dag.vertexMap.get(edgePlan.getOutputVertexName());    
        EdgeProperty edgeProp = dag.edges.get(outEdgeId);
        outVertices.put(outVertex, edgeProp);
      }
      
      vertex.setInputVertices(inVertices);
      vertex.setOutputVertices(outVertices);
    }

    protected void setup(DAGImpl job) throws IOException {
      job.initTime = job.clock.getTime();
      String dagIdString = job.dagId.toString();
      
      dagIdString.replace("application", "job");
      
      // TODO remove - TEZ-71
      String user =
        UserGroupInformation.getCurrentUser().getShortUserName();
      Path path = DAGApps.getStagingAreaDir(job.conf, user);
      if(LOG.isDebugEnabled()) {
        LOG.debug("startJobs: parent=" + path + " child=" + dagIdString);
      }

      job.remoteJobSubmitDir =
          FileSystem.get(job.conf).makeQualified(
              new Path(path, dagIdString));

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

      // Upload the jobTokens onto the remote FS so that ContainerManager can
      // localize it to be used by the Containers(tasks)
      Credentials tokenStorage = new Credentials();
      // TODO Consider sending the jobToken over RPC.
      TokenCache.setJobToken(job.jobToken, tokenStorage);

      if (UserGroupInformation.isSecurityEnabled()) {
        tokenStorage.addAll(job.fsTokens);
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

			// If we have no tasks, just transition to job completed
      if (job.numVertices == 0) {
        job.eventHandler.handle(
            new DAGEvent(job.dagId, DAGEventType.DAG_COMPLETED));
      }

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

  Vertex getVertex(String vertexName) {
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

  // Task-start has been moved out of InitTransition, so this arc simply
  // hardcodes 0 for both map and reduce finished tasks.
  private static class KillNewJobTransition
  implements SingleArcTransition<DAGImpl, DAGEvent> {
    @Override
    public void transition(DAGImpl job, DAGEvent event) {
      job.setFinishTime();
      job.logJobHistoryUnsuccesfulEvent(DAGStatus.State.KILLED);
      job.finished(DAGState.KILLED);
    }
  }

  private static class KillInitedJobTransition
  implements SingleArcTransition<DAGImpl, DAGEvent> {
    @Override
    public void transition(DAGImpl job, DAGEvent event) {
      job.abortJob(DAGStatus.State.KILLED);
      job.addDiagnostic("Job received Kill in INITED state.");
      job.finished(DAGState.KILLED);
    }
  }

  private static class KillVerticesTransition
      implements SingleArcTransition<DAGImpl, DAGEvent> {
    @Override
    public void transition(DAGImpl job, DAGEvent event) {
      job.addDiagnostic("Job received Kill while in RUNNING state.");
      for (Vertex v : job.vertices.values()) {
        job.eventHandler.handle(
            new VertexEvent(v.getVertexId(), VertexEventType.V_KILL)
            );
      }
      // TODO Metrics
      //job.metrics.endRunningJob(job);
    }
  }

  private static class VertexCompletedTransition implements
      MultipleArcTransition<DAGImpl, DAGEvent, DAGState> {

    @Override
    public DAGState transition(DAGImpl job, DAGEvent event) {

      DAGEventVertexCompleted vertexEvent = (DAGEventVertexCompleted) event;
      LOG.info("DEBUG: Received a vertex completion event"
          + ", vertexId=" + vertexEvent.getVertexId()
          + ", vertexState=" + vertexEvent.getVertexState());

      Vertex vertex = job.vertices.get(vertexEvent.getVertexId());
      job.numCompletedVertices++;
      if (vertexEvent.getVertexState() == VertexState.SUCCEEDED) {
        vertexSucceeded(job, vertex);
      } else if (vertexEvent.getVertexState() == VertexState.FAILED) {
        vertexFailed(job, vertex);
      } else if (vertexEvent.getVertexState() == VertexState.KILLED) {
        vertexKilled(job, vertex);
      }

      LOG.info("ZZZZ: Vertex completed."
          + ", numCompletedVertices=" + job.numCompletedVertices
          + ", numSuccessfulVertices=" + job.numSuccessfulVertices
          + ", numFailedVertices=" + job.numFailedVertices
          + ", numKilledVertices=" + job.numKilledVertices
          + ", numVertices=" + job.numVertices);

      job.dagScheduler.vertexCompleted(vertex);

      return checkJobForCompletion(job);
    }

    private void vertexSucceeded(DAGImpl job, Vertex vertex) {
      job.numSuccessfulVertices++;
      // TODO: Metrics
      //job.metrics.completedTask(task);
    }

    private void vertexFailed(DAGImpl job, Vertex vertex) {
      job.numFailedVertices++;
      job.addDiagnostic("Vertex failed " + vertex.getVertexId());
      // TODO: Metrics
      //job.metrics.failedTask(task);
    }

    private void vertexKilled(DAGImpl job, Vertex vertex) {
      job.numKilledVertices++;
      job.addDiagnostic("Vertex killed " + vertex.getVertexId());
      // TODO: Metrics
      //job.metrics.killedTask(task);
    }
  }

  // Transition class for handling jobs with no vertices
  static class JobNoTasksCompletedTransition implements
  MultipleArcTransition<DAGImpl, DAGEvent, DAGState> {

    @Override
    public DAGState transition(DAGImpl dag, DAGEvent event) {
      return checkJobForCompletion(dag);
    }
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
      default:
        LOG.warn("Unknown DAGEventSchedulerUpdate:" + sEvent.getUpdateType());
    }
  }
}

  private static class InternalErrorTransition implements
      SingleArcTransition<DAGImpl, DAGEvent> {
    @Override
    public void transition(DAGImpl job, DAGEvent event) {
      //TODO Is this JH event required.
      job.setFinishTime();
      job.logJobHistoryUnsuccesfulEvent(DAGStatus.State.FAILED);
      job.finished(DAGState.ERROR);
    }
  }
}
