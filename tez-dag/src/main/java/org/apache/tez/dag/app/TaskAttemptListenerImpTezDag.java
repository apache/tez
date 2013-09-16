/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.dag.app;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.TezTaskStatus;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.common.records.ProceedToCompletionResponse;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventDiagnosticsUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventOutputConsumable;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStartedRemotely;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStatusUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStatusUpdate.TaskAttemptStatus;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.rm.container.AMContainerImpl;
import org.apache.tez.dag.app.rm.container.AMContainerTask;
import org.apache.tez.dag.app.security.authorize.MRAMPolicyProvider;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.engine.common.security.JobTokenSecretManager;
import org.apache.tez.engine.newapi.events.TaskAttemptFailedEvent;
import org.apache.tez.engine.newapi.impl.TezEvent;
import org.apache.tez.engine.newapi.impl.TezHeartbeatRequest;
import org.apache.tez.engine.newapi.impl.TezHeartbeatResponse;
import org.apache.tez.engine.records.OutputContext;
import org.apache.tez.engine.records.TezDependentTaskCompletionEvent;
import org.apache.tez.engine.records.TezTaskDependencyCompletionEventsUpdate;

@SuppressWarnings("unchecked")
public class TaskAttemptListenerImpTezDag extends AbstractService implements
    TezTaskUmbilicalProtocol, TaskAttemptListener {

  private static final ContainerTask TASK_FOR_INVALID_JVM = new ContainerTask(
      null, true);

  private static ProceedToCompletionResponse COMPLETION_RESPONSE_NO_WAIT =
      new ProceedToCompletionResponse(true, true);

  private static final Log LOG = LogFactory
      .getLog(TaskAttemptListenerImpTezDag.class);

  private final AppContext context;

  protected final TaskHeartbeatHandler taskHeartbeatHandler;
  protected final ContainerHeartbeatHandler containerHeartbeatHandler;
  private final JobTokenSecretManager jobTokenSecretManager;
  private InetSocketAddress address;
  private Server server;


  class AttemptInfo {
    AttemptInfo(ContainerId containerId) {
      this.containerId = containerId;
      this.lastReponse = null;
      this.lastRequestId = -1;
    }
    ContainerId containerId;
    long lastRequestId;
    TezHeartbeatResponse lastReponse;
  }
  private ConcurrentMap<TezTaskAttemptID, AttemptInfo> attemptToInfoMap =
      new ConcurrentHashMap<TezTaskAttemptID, AttemptInfo>();

  private Set<ContainerId> registeredContainers = Collections
      .newSetFromMap(new ConcurrentHashMap<ContainerId, Boolean>());

  public TaskAttemptListenerImpTezDag(AppContext context,
      TaskHeartbeatHandler thh, ContainerHeartbeatHandler chh,
      JobTokenSecretManager jobTokenSecretManager) {
    super(TaskAttemptListenerImpTezDag.class.getName());
    this.context = context;
    this.jobTokenSecretManager = jobTokenSecretManager;
    this.taskHeartbeatHandler = thh;
    this.containerHeartbeatHandler = chh;
  }

  @Override
  public void serviceStart() {
    startRpcServer();
  }

  protected void startRpcServer() {
    Configuration conf = getConfig();
    try {
      server = new RPC.Builder(conf)
          .setProtocol(TezTaskUmbilicalProtocol.class)
          .setBindAddress("0.0.0.0")
          .setPort(0)
          .setInstance(this)
          .setNumHandlers(
              conf.getInt(TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT,
                  TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT))
          .setSecretManager(jobTokenSecretManager).build();

      // Enable service authorization?
      if (conf.getBoolean(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
          false)) {
        refreshServiceAcls(conf, new MRAMPolicyProvider());
      }

      server.start();
      this.address = NetUtils.getConnectAddress(server);
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
  }

  void refreshServiceAcls(Configuration configuration,
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  @Override
  public void serviceStop() {
    stopRpcServer();
  }

  protected void stopRpcServer() {
    if (server != null) {
      server.stop();
    }
  }

  public InetSocketAddress getAddress() {
    return address;
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return versionID;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(this, protocol,
        clientVersion, clientMethodsHash);
  }

  @Override
  public TezTaskDependencyCompletionEventsUpdate getDependentTasksCompletionEvents(
      int fromEventIdx, int maxEvents,
      TezTaskAttemptID taskAttemptID) {

    LOG.info("Dependency Completion Events request from " + taskAttemptID
        + ". fromEventID " + fromEventIdx + " maxEvents " + maxEvents);

    // TODO: shouldReset is never used. See TT. Ask for Removal.
    boolean shouldReset = false;
    TezDependentTaskCompletionEvent[] events =
        context.getCurrentDAG().
            getVertex(taskAttemptID.getTaskID().getVertexID()).
                getTaskAttemptCompletionEvents(taskAttemptID, fromEventIdx, maxEvents);

    taskHeartbeatHandler.progressing(taskAttemptID);
    pingContainerHeartbeatHandler(taskAttemptID);

    // No filters for now. Only required events stored in a vertex.

    return new TezTaskDependencyCompletionEventsUpdate(events,shouldReset);
  }

  @Override
  public ContainerTask getTask(ContainerContext containerContext)
      throws IOException {

    ContainerTask task = null;

    if (containerContext == null || containerContext.getContainerIdentifier() == null) {
      LOG.info("Invalid task request with an empty containerContext or containerId");
      task = TASK_FOR_INVALID_JVM;
    } else {
      ContainerId containerId = ConverterUtils.toContainerId(containerContext
          .getContainerIdentifier());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Container with id: " + containerId + " asked for a task");
      }
      if (!registeredContainers.contains(containerId)) {
        if(context.getAllContainers().get(containerId) == null)
          LOG.info("Container with id: " + containerId
              + " is invalid and will be killed");
        else
          LOG.info("Container with id: " + containerId
              + " is valid and will be killed");
        task = TASK_FOR_INVALID_JVM;
      } else {
        pingContainerHeartbeatHandler(containerId);
        AMContainerTask taskContext = pullTaskAttemptContext(containerId);
        if (taskContext.shouldDie()) {
          LOG.info("No more tasks for container with id : " + containerId
              + ". Asking it to die");
          task = TASK_FOR_INVALID_JVM; // i.e. ask the child to die.
        } else {
          if (taskContext.getTask() == null) {
            LOG.info("No task currently assigned to Container with id: "
                + containerId);
          } else {
            registerTaskAttempt(taskContext.getTask().getTaskAttemptID(),
                containerId);
            task = new ContainerTask(taskContext.getTask(), false);
            context.getEventHandler().handle(
                new TaskAttemptEventStartedRemotely(taskContext.getTask()
                    .getTaskAttemptID(), containerId, context
                    .getApplicationACLs(), context.getAllContainers()
                    .get(containerId).getShufflePort()));
            LOG.info("Container with id: " + containerId + " given task: "
                + taskContext.getTask().getTaskAttemptID());
          }
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("getTask returning task: " + task);
    }
    return task;
  }

  @Override
  public boolean statusUpdate(TezTaskAttemptID taskAttemptId,
      TezTaskStatus taskStatus) throws IOException, InterruptedException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Status update from: " + taskAttemptId);
    }
    taskHeartbeatHandler.progressing(taskAttemptId);
    pingContainerHeartbeatHandler(taskAttemptId);
    TaskAttemptStatus taskAttemptStatus = new TaskAttemptStatus();
    taskAttemptStatus.id = taskAttemptId;
    // Task sends the updated progress to the TT.
    taskAttemptStatus.progress = taskStatus.getProgress();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Progress of TaskAttempt " + taskAttemptId + " is : "
          + taskStatus.getProgress());
    }

    // Task sends the updated state-string to the TT.
    taskAttemptStatus.stateString = taskStatus.getStateString();

    // Set the output-size when map-task finishes. Set by the task itself.
    // outputSize is never used.
    taskAttemptStatus.outputSize = taskStatus.getLocalOutputSize();

    // TODO Phase
    // Task sends the updated phase to the TT.
    //taskAttemptStatus.phase = MRxTypeConverters.toYarn(taskStatus.getPhase());

    // TODO MRXAM3 - AVoid the 10 layers of convresion.
    // Counters are updated by the task. Convert counters into new format as
    // that is the primary storage format inside the AM to avoid multiple
    // conversions and unnecessary heap usage.
    taskAttemptStatus.counters = taskStatus.getCounters();


    // Map Finish time set by the task (map only)
    // TODO CLEANMRXAM - maybe differentiate between map / reduce / types
    if (taskStatus.getMapFinishTime() != 0) {
      taskAttemptStatus.mapFinishTime = taskStatus.getMapFinishTime();
    }

    // Shuffle Finish time set by the task (reduce only).
    if (taskStatus.getShuffleFinishTime() != 0) {
      taskAttemptStatus.shuffleFinishTime = taskStatus.getShuffleFinishTime();
    }

    // Sort finish time set by the task (reduce only).
    if (taskStatus.getSortFinishTime() != 0) {
      taskAttemptStatus.sortFinishTime = taskStatus.getSortFinishTime();
    }

    // Not Setting the task state. Used by speculation - will be set in
    // TaskAttemptImpl
    // taskAttemptStatus.taskState =
    // TypeConverter.toYarn(taskStatus.getRunState());

    // set the fetch failures
    if (taskStatus.getFailedDependencies() != null
        && taskStatus.getFailedDependencies().size() > 0) {
      LOG.warn("Failed dependencies are not handled at the moment." +
      		" The job is likely to fail / hang");
      taskAttemptStatus.fetchFailedMaps = new ArrayList<TezTaskAttemptID>();
      for (TezTaskAttemptID failedAttemptId : taskStatus
          .getFailedDependencies()) {
        taskAttemptStatus.fetchFailedMaps.add(failedAttemptId);
      }
    }

    // Task sends the information about the nextRecordRange to the TT

    // TODO: The following are not needed here, but needed to be set somewhere
    // inside AppMaster.
    // taskStatus.getRunState(); // Set by the TT/JT. Transform into a state
    // TODO
    // taskStatus.getStartTime(); // Used to be set by the TaskTracker. This
    // should be set by getTask().
    // taskStatus.getFinishTime(); // Used to be set by TT/JT. Should be set
    // when task finishes
    // // This was used by TT to do counter updates only once every minute. So
    // this
    // // isn't ever changed by the Task itself.
    // taskStatus.getIncludeCounters();

    context.getEventHandler().handle(
        new TaskAttemptEventStatusUpdate(taskAttemptStatus.id,
            taskAttemptStatus));
    return true;
  }

  @Override
  public void reportDiagnosticInfo(TezTaskAttemptID taskAttemptId, String trace)
      throws IOException {
    LOG.info("Diagnostics report from " + taskAttemptId.toString() + ": "
        + trace);

    taskHeartbeatHandler.progressing(taskAttemptId);
    pingContainerHeartbeatHandler(taskAttemptId);

    // This is mainly used for cases where we want to propagate exception traces
    // of tasks that fail.

    // This call exists as a hadoop mapreduce legacy wherein all changes in
    // counters/progress/phase/output-size are reported through statusUpdate()
    // call but not diagnosticInformation.
    context.getEventHandler().handle(
        new TaskAttemptEventDiagnosticsUpdate(taskAttemptId, trace));

  }

  @Override
  public boolean ping(TezTaskAttemptID taskAttemptId) throws IOException {
    LOG.info("Ping from " + taskAttemptId.toString());
    taskHeartbeatHandler.pinged(taskAttemptId);
    pingContainerHeartbeatHandler(taskAttemptId);
    return true;
  }

  @Override
  public void done(TezTaskAttemptID taskAttemptId) throws IOException {
    LOG.info("Done acknowledgement from " + taskAttemptId.toString());

    taskHeartbeatHandler.progressing(taskAttemptId);
    pingContainerHeartbeatHandler(taskAttemptId);

    context.getEventHandler().handle(
        new TaskAttemptEvent(taskAttemptId, TaskAttemptEventType.TA_DONE));

  }

  /**
   * TaskAttempt is reporting that it is in commit_pending and it is waiting for
   * the commit Response
   *
   * <br/>
   * Commit it a two-phased protocol. First the attempt informs the
   * ApplicationMaster that it is
   * {@link #commitPending(TaskAttemptID, TaskStatus)}. Then it repeatedly polls
   * the ApplicationMaster whether it {@link #canCommit(TaskAttemptID)} This is
   * a legacy from the centralized commit protocol handling by the JobTracker.
   */
  @Override
  public void commitPending(TezTaskAttemptID taskAttemptId, TezTaskStatus taskStatus)
      throws IOException, InterruptedException {
    LOG.info("Commit-pending state update from " + taskAttemptId.toString());
    // An attempt is asking if it can commit its output. This can be decided
    // only by the task which is managing the multiple attempts. So redirect the
    // request there.
    taskHeartbeatHandler.progressing(taskAttemptId);
    pingContainerHeartbeatHandler(taskAttemptId);
    //Ignorable TaskStatus? - since a task will send a LastStatusUpdate
    context.getEventHandler().handle(
        new TaskAttemptEvent(
            taskAttemptId,
            TaskAttemptEventType.TA_COMMIT_PENDING)
        );
  }

  /**
   * Child checking whether it can commit.
   *
   * <br/>
   * Commit is a two-phased protocol. First the attempt informs the
   * ApplicationMaster that it is
   * {@link #commitPending(TaskAttemptID, TaskStatus)}. Then it repeatedly polls
   * the ApplicationMaster whether it {@link #canCommit(TaskAttemptID)} This is
   * a legacy from the centralized commit protocol handling by the JobTracker.
   */
  @Override
  public boolean canCommit(TezTaskAttemptID taskAttemptId) throws IOException {
    LOG.info("Commit go/no-go request from " + taskAttemptId.toString());
    // An attempt is asking if it can commit its output. This can be decided
    // only by the task which is managing the multiple attempts. So redirect the
    // request there.
    taskHeartbeatHandler.progressing(taskAttemptId);
    pingContainerHeartbeatHandler(taskAttemptId);

    DAG job = context.getCurrentDAG();
    Task task =
        job.getVertex(taskAttemptId.getTaskID().getVertexID()).
            getTask(taskAttemptId.getTaskID());
    return task.canCommit(taskAttemptId);
  }

  @Override
  public void shuffleError(TezTaskAttemptID taskId, String message)
      throws IOException {
    // TODO: This isn't really used in any MR code. Ask for removal.
  }

  @Override
  public void fsError(TezTaskAttemptID taskAttemptId, String message)
      throws IOException {
    // This happens only in Child.
    LOG.fatal("Task: " + taskAttemptId + " - failed due to FSError: " + message);
    reportDiagnosticInfo(taskAttemptId, "FSError: " + message);

    context.getEventHandler().handle(
        new TaskAttemptEvent(taskAttemptId, TaskAttemptEventType.TA_FAILED));
  }

  @Override
  public void fatalError(TezTaskAttemptID taskAttemptId, String message)
      throws IOException {
    // This happens only in Child and in the Task.
    LOG.fatal("Task: " + taskAttemptId + " - exited : " + message);
    reportDiagnosticInfo(taskAttemptId, "Error: " + message);

    context.getEventHandler().handle(
        new TaskAttemptEvent(taskAttemptId, TaskAttemptEventType.TA_FAILED));
  }

  @Override
  public void outputReady(TezTaskAttemptID taskAttemptId,
      OutputContext outputContext) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("AttemptId: " + taskAttemptId + " reported output context: "
          + outputContext);
    }
    context.getEventHandler().handle(
        new TaskAttemptEventOutputConsumable(taskAttemptId, outputContext));
  }

  @Override
  public ProceedToCompletionResponse
      proceedToCompletion(TezTaskAttemptID taskAttemptId) throws IOException {

    // The async nature of the processing combined with the 1 second interval
    // between polls (MRTask) implies tasks end up wasting upto 1 second doing
    // nothing. Similarly for CA_COMMIT.

    /*
    DAG job = context.getCurrentDAG();
    Task task =
        job.getVertex(taskAttemptId.getTaskID().getVertexID()).
            getTask(taskAttemptId.getTaskID());

    // TODO In-Memory Shuffle
    if (task.needsWaitAfterOutputConsumable()) {
      TezTaskAttemptID outputReadyAttempt = task.getOutputConsumableAttempt();
      if (outputReadyAttempt != null) {
        if (!outputReadyAttempt.equals(taskAttemptId)) {
          LOG.info("Telling taksAttemptId: "
              + taskAttemptId
              + " to die, since the outputReady atempt for this task is different: "
              + outputReadyAttempt);
          return new ProceedToCompletionResponse(true, true);
        }
      }
      boolean reducesDone = true;
      for (Task rTask : job.getTasks(TaskType.REDUCE).values()) {
        if (rTask.getState() != TaskState.SUCCEEDED) {
          // TODO EVENTUALLY - could let the map tasks exit after reduces are
          // done with the shuffle phase, instead of waiting for the reduces to
          // complete.
          reducesDone = false;
          break;
        }
      }
      if (reducesDone) {
        return new ProceedToCompletionResponse(false, true);
      } else {
        return new ProceedToCompletionResponse(false, false);
      }
    } else {
      return COMPLETION_RESPONSE_NO_WAIT;
    }
    */
    return COMPLETION_RESPONSE_NO_WAIT;
  }


  @Override
  public void unregisterTaskAttempt(TezTaskAttemptID attemptId) {
    attemptToInfoMap.remove(attemptId);
  }

  public AMContainerTask pullTaskAttemptContext(ContainerId containerId) {
    AMContainerImpl container = (AMContainerImpl) context.getAllContainers()
        .get(containerId);
    return container.pullTaskContext();
  }

  @Override
  public void registerRunningContainer(ContainerId containerId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("ContainerId: " + containerId
          + " registered with TaskAttemptListener");
    }
    registeredContainers.add(containerId);
  }

  @Override
  public void registerTaskAttempt(TezTaskAttemptID attemptId,
      ContainerId containerId) {
    AttemptInfo attemptInfo = new AttemptInfo(containerId);
    attemptToInfoMap.put(attemptId, attemptInfo);
  }

  @Override
  public void unregisterRunningContainer(ContainerId containerId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Unregistering Container from TaskAttemptListener: "
          + containerId);
    }
    registeredContainers.remove(containerId);
  }

  private void pingContainerHeartbeatHandler(ContainerId containerId) {
    containerHeartbeatHandler.pinged(containerId);
  }

  private void pingContainerHeartbeatHandler(TezTaskAttemptID taskAttemptId) {
    ContainerId containerId = attemptToInfoMap.get(taskAttemptId).containerId;
    if (containerId != null) {
      containerHeartbeatHandler.pinged(containerId);
    } else {
      LOG.warn("Handling communication from attempt: " + taskAttemptId
          + ", ContainerId not known for this attempt");
    }
  }

  @Override
  public TezHeartbeatResponse heartbeat(TezHeartbeatRequest request)
      throws IOException, TezException {
    long requestId = request.getRequestId();
    LOG.info("Received request id " + requestId + " from child JVM");
    TezTaskAttemptID taskAttemptID = request.getCurrentTaskAttemptID();
    if (taskAttemptID == null) {
      TezHeartbeatResponse response = new TezHeartbeatResponse();
      response.setLastRequestId(requestId);
      return response;
    }
    AttemptInfo attemptInfo = attemptToInfoMap.get(taskAttemptID);
    if(attemptInfo == null) {
      throw new TezException("Attempt " + taskAttemptID
          + " is not recognized for heartbeat");
    }
    synchronized (attemptInfo) {
      if(attemptInfo.lastRequestId == requestId) {
        LOG.warn("Old sequenceId received: " + requestId + ", Re-sending last response to client");
        return attemptInfo.lastReponse;
      }
      if(attemptInfo.lastRequestId != -1
         && attemptInfo.lastRequestId+1 < requestId) {
        // TODO NEWTEZ fix checking of last req id to ensure heartbeat works
        // across attempts with container reuse
        throw new TezException("Attempt " + taskAttemptID
            + " has invalid request id. Expected: "
            + attemptInfo.lastRequestId+1
            + " and actual: " + requestId);
      }
      
      // not safe to multiple call from same task
      LOG.info("Ping from " + taskAttemptID.toString());
      List<TezEvent> inEvents = request.getEvents();
      if(inEvents!=null && inEvents.size()>0) {    
        TezVertexID vertexId = taskAttemptID.getTaskID().getVertexID();
        context.getEventHandler().handle(new VertexEventRouteEvent(vertexId, inEvents));
      }
      taskHeartbeatHandler.pinged(taskAttemptID);
      pingContainerHeartbeatHandler(taskAttemptID);
      TezHeartbeatResponse response = new TezHeartbeatResponse();
      response.setLastRequestId(requestId);
      List<TezEvent> outEvents = context
          .getCurrentDAG()
          .getVertex(taskAttemptID.getTaskID().getVertexID())
          .getTask(taskAttemptID.getTaskID())
          .getTaskAttemptTezEvents(taskAttemptID, request.getStartIndex(),
              request.getMaxEvents());
      response.setEvents(outEvents);
      attemptInfo.lastRequestId = requestId;
      attemptInfo.lastReponse = response;
      return response;
    }
  }

  @Override
  public void taskAttemptFailed(TezTaskAttemptID taskAttemptId,
      TezEvent tezAttemptFailedEvent) throws IOException {
    TaskAttemptFailedEvent taskFailedEvent =
        (TaskAttemptFailedEvent) tezAttemptFailedEvent.getEvent();
    LOG.fatal("Task Attempt: " + taskAttemptId + " - failed : "
        + taskFailedEvent.getDiagnostics());
    reportDiagnosticInfo(taskAttemptId, "Error: "
        + taskFailedEvent.getDiagnostics());

    context.getEventHandler().handle(
        new TaskAttemptEvent(taskAttemptId, TaskAttemptEventType.TA_FAILED));

  }

  @Override
  public void taskAttemptCompleted(TezTaskAttemptID taskAttemptId,
      TezEvent taskAttemptCompletedEvent) throws IOException {
    LOG.info("Task attempt completed acknowledgement from "
      + taskAttemptId.toString());

    taskHeartbeatHandler.progressing(taskAttemptId);
    pingContainerHeartbeatHandler(taskAttemptId);

    context.getEventHandler().handle(
        new TaskAttemptEvent(taskAttemptId, TaskAttemptEventType.TA_DONE));

  }

}
