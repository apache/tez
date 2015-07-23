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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.ContainerSignatureMatcher;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;

public class TaskSchedulerContextImpl implements TaskSchedulerContext {

  private final TaskSchedulerEventHandler tseh;
  private final AppContext appContext;
  private final int schedulerId;
  private final String trackingUrl;
  private final long customClusterIdentifier;
  private final String appHostName;
  private final int clientPort;
  private final Configuration conf;

  public TaskSchedulerContextImpl(TaskSchedulerEventHandler tseh, AppContext appContext,
                                  int schedulerId, String trackingUrl, long customClusterIdentifier,
                                  String appHostname, int clientPort,
                                  Configuration conf) {
    this.tseh = tseh;
    this.appContext = appContext;
    this.schedulerId = schedulerId;
    this.trackingUrl = trackingUrl;
    this.customClusterIdentifier = customClusterIdentifier;
    this.appHostName = appHostname;
    this.clientPort = clientPort;
    this.conf = conf;

  }

  @Override
  public void taskAllocated(Object task, Object appCookie, Container container) {
    tseh.taskAllocated(schedulerId, task, appCookie, container);
  }

  @Override
  public void containerCompleted(Object taskLastAllocated, ContainerStatus containerStatus) {
    tseh.containerCompleted(schedulerId, taskLastAllocated, containerStatus);
  }

  @Override
  public void containerBeingReleased(ContainerId containerId) {
    tseh.containerBeingReleased(schedulerId, containerId);
  }

  @Override
  public void nodesUpdated(List<NodeReport> updatedNodes) {
    tseh.nodesUpdated(schedulerId, updatedNodes);
  }

  @Override
  public void appShutdownRequested() {
    tseh.appShutdownRequested(schedulerId);
  }

  @Override
  public void setApplicationRegistrationData(Resource maxContainerCapability,
                                             Map<ApplicationAccessType, String> appAcls,
                                             ByteBuffer clientAMSecretKey) {
    tseh.setApplicationRegistrationData(schedulerId, maxContainerCapability, appAcls, clientAMSecretKey);
  }

  @Override
  public void onError(Throwable t) {
    tseh.onError(schedulerId, t);
  }

  @Override
  public float getProgress() {
    return tseh.getProgress(schedulerId);
  }

  @Override
  public void preemptContainer(ContainerId containerId) {
    tseh.preemptContainer(schedulerId, containerId);
  }

  @Override
  public AppFinalStatus getFinalAppStatus() {
    return tseh.getFinalAppStatus();
  }

  @Override
  public Configuration getInitialConfiguration() {
    return conf;
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
    return tseh.getContainerSignatureMatcher();
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return appContext.getApplicationAttemptId();
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
}
