/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app.rm;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.ContainerSignatureMatcher;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.serviceplugins.api.DagInfo;
import org.apache.tez.serviceplugins.api.ServicePluginError;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;

public class TaskSchedulerContextImpl implements TaskSchedulerContext {

  private final TaskSchedulerManager taskSchedulerManager;
  private final AppContext appContext;
  private final int schedulerId;
  private final String trackingUrl;
  private final long customClusterIdentifier;
  private final String appHostName;
  private final int clientPort;
  private final UserPayload initialUserPayload;

  public TaskSchedulerContextImpl(TaskSchedulerManager taskSchedulerManager, AppContext appContext,
                                  int schedulerId, String trackingUrl, long customClusterIdentifier,
                                  String appHostname, int clientPort,
                                  UserPayload initialUserPayload) {
    this.taskSchedulerManager = taskSchedulerManager;
    this.appContext = appContext;
    this.schedulerId = schedulerId;
    this.trackingUrl = trackingUrl;
    this.customClusterIdentifier = customClusterIdentifier;
    this.appHostName = appHostname;
    this.clientPort = clientPort;
    this.initialUserPayload = initialUserPayload;

  }

  // this may end up being called for a task+container pair that the app
  // has not heard about. this can happen because of a race between
  // taskAllocated() upcall and deallocateTask() downcall
  @Override
  public void taskAllocated(Object task, Object appCookie, Container container) {
    taskSchedulerManager.taskAllocated(schedulerId, task, appCookie, container);
  }

  @Override
  public void containerCompleted(Object taskLastAllocated, ContainerStatus containerStatus) {
    taskSchedulerManager.containerCompleted(schedulerId, taskLastAllocated, containerStatus);
  }

  @Override
  public void containerBeingReleased(ContainerId containerId) {
    taskSchedulerManager.containerBeingReleased(schedulerId, containerId);
  }

  @Override
  public void nodesUpdated(List<NodeReport> updatedNodes) {
    taskSchedulerManager.nodesUpdated(schedulerId, updatedNodes);
  }

  @Override
  public void appShutdownRequested() {
    taskSchedulerManager.appShutdownRequested(schedulerId);
  }

  @Override
  public void setApplicationRegistrationData(Resource maxContainerCapability,
                                             Map<ApplicationAccessType, String> appAcls,
                                             ByteBuffer clientAMSecretKey,
                                             String queueName) {
    taskSchedulerManager.setApplicationRegistrationData(schedulerId, maxContainerCapability,
        appAcls, clientAMSecretKey, queueName);
  }

  @Override
  public float getProgress() {
    return taskSchedulerManager.getProgress(schedulerId);
  }

  @Override
  public void preemptContainer(ContainerId containerId) {
    taskSchedulerManager.preemptContainer(schedulerId, containerId);
  }

  @Override
  public AppFinalStatus getFinalAppStatus() {
    return taskSchedulerManager.getFinalAppStatus();
  }

  @Override
  public UserPayload getInitialUserPayload() {
    return initialUserPayload;
  }


  @Override
  public String getAppTrackingUrl() {
    return trackingUrl;
  }

  @Override
  public long getCustomClusterIdentifier() {
    return customClusterIdentifier;
  }

  @Override
  public ContainerSignatureMatcher getContainerSignatureMatcher() {
    return taskSchedulerManager.getContainerSignatureMatcher();
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return appContext.getApplicationAttemptId();
  }

  @Nullable
  @Override
  public DagInfo getCurrentDagInfo() {
    return appContext.getCurrentDAG();
  }

  @Override
  public String getAppHostName() {
    return appHostName;
  }

  @Override
  public int getAppClientPort() {
    return clientPort;
  }

  @Override
  public boolean isSession() {
    return appContext.isSession();
  }

  @Override
  public AMState getAMState() {
    switch (appContext.getAMState()) {

      case NEW:
      case INITED:
      case IDLE:
        return AMState.IDLE;
      case RECOVERING:
        // TODO Is this correct for recovery ?
      case RUNNING:
        return AMState.RUNNING_APP;
      case SUCCEEDED:
      case FAILED:
      case KILLED:
      case ERROR:
        return AMState.COMPLETED;
      default:
        throw new TezUncheckedException("Unexpected state " + appContext.getAMState());
    }
  }

  @Override
  public int getVertexIndexForTask(Object task) {
    return taskSchedulerManager.getVertexIndexForTask(task);
  }

  @Override
  public void reportError(ServicePluginError servicePluginError, String diagnostics,
                          DagInfo dagInfo) {
    Preconditions.checkNotNull(servicePluginError, "ServicePluginError must be specified");
    taskSchedulerManager.reportError(schedulerId, servicePluginError, diagnostics, dagInfo);
  }
}
