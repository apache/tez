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

package org.apache.tez.dag.app;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventUserServiceFatalError;
import org.apache.tez.dag.app.launcher.ContainerLauncherManager;
import org.apache.tez.serviceplugins.api.ContainerLauncherContext;
import org.apache.tez.serviceplugins.api.DagInfo;
import org.apache.tez.serviceplugins.api.ServicePluginError;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.dag.app.rm.container.AMContainerEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEventCompleted;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunchFailed;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunched;
import org.apache.tez.dag.app.rm.container.AMContainerEventStopFailed;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class ContainerLauncherContextImpl implements ContainerLauncherContext {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerLauncherContextImpl.class);
  private final AppContext context;
  private final ContainerLauncherManager containerLauncherManager;
  private final TaskCommunicatorManagerInterface tal;
  private final UserPayload initialUserPayload;
  private final int containerLauncherIndex;

  public ContainerLauncherContextImpl(AppContext appContext, ContainerLauncherManager containerLauncherManager,
                                      TaskCommunicatorManagerInterface tal,
                                      UserPayload initialUserPayload, int containerLauncherIndex) {
    Preconditions.checkNotNull(appContext, "AppContext cannot be null");
    Preconditions.checkNotNull(appContext, "ContainerLauncherManager cannot be null");
    Preconditions.checkNotNull(tal, "TaskCommunicator cannot be null");
    this.context = appContext;
    this.containerLauncherManager = containerLauncherManager;
    this.tal = tal;
    this.initialUserPayload = initialUserPayload;
    this.containerLauncherIndex = containerLauncherIndex;
  }

  @Override
  public void containerLaunched(ContainerId containerId) {
    context.getEventHandler().handle(
        new AMContainerEventLaunched(containerId));
    ContainerLaunchedEvent lEvt = new ContainerLaunchedEvent(
        containerId, context.getClock().getTime(), context.getApplicationAttemptId());
    context.getHistoryHandler().handle(new DAGHistoryEvent(
        null, lEvt));

  }

  @Override
  public void containerLaunchFailed(ContainerId containerId, String diagnostics) {
    context.getEventHandler().handle(new AMContainerEventLaunchFailed(containerId, diagnostics));
  }

  @Override
  public void containerStopRequested(ContainerId containerId) {
    context.getEventHandler().handle(
        new AMContainerEvent(containerId, AMContainerEventType.C_NM_STOP_SENT));
  }

  @Override
  public void containerStopFailed(ContainerId containerId, String diagnostics) {
    context.getEventHandler().handle(
        new AMContainerEventStopFailed(containerId, diagnostics));
  }

  @Override
  public void containerCompleted(ContainerId containerId, int exitStatus, String diagnostics,
                                 TaskAttemptEndReason endReason) {
    context.getEventHandler().handle(new AMContainerEventCompleted(containerId, exitStatus, diagnostics, TezUtilsInternal
        .fromTaskAttemptEndReason(
            endReason)));
  }

  @Override
  public UserPayload getInitialUserPayload() {
    return initialUserPayload;
  }

  @Override
  public int getNumNodes(String sourceName) {
    int sourceIndex = context.getTaskScheduerIdentifier(sourceName);
    int numNodes = context.getNodeTracker().getNumNodes(sourceIndex);
    return numNodes;
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return context.getApplicationAttemptId();
  }

  @Nullable
  @Override
  public DagInfo getCurrentDagInfo() {
    return context.getCurrentDAG();
  }

  @Override
  public Object getTaskCommunicatorMetaInfo(String taskCommName) {
    int taskCommId = context.getTaskCommunicatorIdentifier(taskCommName);
    try {
      return tal.getTaskCommunicator(taskCommId).getMetaInfo();
    } catch (Exception e) {
      String msg = "Error in retrieving meta-info from TaskCommunicator"
          + ", communicatorName=" + context.getTaskCommunicatorName(taskCommId);
      LOG.error(msg, e);
      context.getEventHandler().handle(
          new DAGAppMasterEventUserServiceFatalError(
              DAGAppMasterEventType.TASK_COMMUNICATOR_SERVICE_FATAL_ERROR,
              msg, e));
    }
    return null;
  }

  @Override
  public void reportError(ServicePluginError servicePluginError, String message, DagInfo dagInfo) {
    Preconditions.checkNotNull(servicePluginError, "ServiceError must be specified");
    containerLauncherManager.reportError(containerLauncherIndex, servicePluginError, message, dagInfo);
  }


}
