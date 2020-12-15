/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app.launcher;

import java.net.UnknownHostException;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tez.common.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.Utils;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.app.ServicePluginLifecycleAbstractService;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventUserServiceFatalError;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.serviceplugins.api.ContainerLaunchRequest;
import org.apache.tez.serviceplugins.api.ContainerLauncher;
import org.apache.tez.serviceplugins.api.ContainerLauncherContext;
import org.apache.tez.serviceplugins.api.ContainerStopRequest;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerLauncherContextImpl;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;
import org.apache.tez.dag.app.rm.ContainerLauncherEvent;
import org.apache.tez.dag.app.rm.ContainerLauncherLaunchRequestEvent;
import org.apache.tez.serviceplugins.api.DagInfo;
import org.apache.tez.serviceplugins.api.ServicePluginError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerLauncherManager extends AbstractService
    implements EventHandler<ContainerLauncherEvent> {

  static final Logger LOG = LoggerFactory.getLogger(TezContainerLauncherImpl.class);

  @VisibleForTesting
  final ContainerLauncherWrapper containerLaunchers[];
  @VisibleForTesting
  final ContainerLauncherContext containerLauncherContexts[];
  protected final ServicePluginLifecycleAbstractService[] containerLauncherServiceWrappers;
  private final AppContext appContext;
  private final boolean isIncompleteCtor;



  // Accepting conf to setup final parameters, if required.
  public ContainerLauncherManager(AppContext context,
                                  TaskCommunicatorManagerInterface taskCommunicatorManagerInterface,
                                  String workingDirectory,
                                  List<NamedEntityDescriptor> containerLauncherDescriptors,
                                  boolean isLocalMode) throws TezException {
    super(ContainerLauncherManager.class.getName());

    this.isIncompleteCtor = false;
    this.appContext = context;
    if (containerLauncherDescriptors == null || containerLauncherDescriptors.isEmpty()) {
      throw new IllegalArgumentException("ContainerLauncherDescriptors must be specified");
    }
    containerLauncherContexts = new ContainerLauncherContext[containerLauncherDescriptors.size()];
    containerLaunchers = new ContainerLauncherWrapper[containerLauncherDescriptors.size()];
    containerLauncherServiceWrappers = new ServicePluginLifecycleAbstractService[containerLauncherDescriptors.size()];


    for (int i = 0; i < containerLauncherDescriptors.size(); i++) {
      UserPayload userPayload = containerLauncherDescriptors.get(i).getUserPayload();
      ContainerLauncherContext containerLauncherContext =
          new ContainerLauncherContextImpl(context, this, taskCommunicatorManagerInterface, userPayload, i);
      containerLauncherContexts[i] = containerLauncherContext;
      containerLaunchers[i] = new ContainerLauncherWrapper(createContainerLauncher(containerLauncherDescriptors.get(i), context,
          containerLauncherContext, taskCommunicatorManagerInterface, workingDirectory, i, isLocalMode));
      containerLauncherServiceWrappers[i] = new ServicePluginLifecycleAbstractService<>(containerLaunchers[i].getContainerLauncher());
    }
  }

  @VisibleForTesting
  public ContainerLauncherManager(AppContext context) {
    super(ContainerLauncherManager.class.getName());
    this.isIncompleteCtor = true;
    this.appContext = context;
    containerLaunchers = new ContainerLauncherWrapper[1];
    containerLauncherContexts = new ContainerLauncherContext[1];
    containerLauncherServiceWrappers = new ServicePluginLifecycleAbstractService[1];
  }

  // To be used with the constructor which accepts the AppContext only, and is for testing.
  @VisibleForTesting
  public void setContainerLauncher(ContainerLauncher containerLauncher) {
    Preconditions.checkState(isIncompleteCtor == true, "Can only be used with the Test constructor");
    containerLaunchers[0] = new ContainerLauncherWrapper(containerLauncher);
    containerLauncherContexts[0] = containerLauncher.getContext();
    containerLauncherServiceWrappers[0] = new ServicePluginLifecycleAbstractService<>(containerLauncher);
  }

  @VisibleForTesting
  ContainerLauncher createContainerLauncher(
      NamedEntityDescriptor containerLauncherDescriptor,
      AppContext context,
      ContainerLauncherContext containerLauncherContext,
      TaskCommunicatorManagerInterface taskCommunicatorManagerInterface,
      String workingDirectory,
      int containerLauncherIndex,
      boolean isPureLocalMode) throws TezException {
    if (containerLauncherDescriptor.getEntityName().equals(
        TezConstants.getTezYarnServicePluginName())) {
      return createYarnContainerLauncher(containerLauncherContext);
    } else if (containerLauncherDescriptor.getEntityName()
        .equals(TezConstants.getTezUberServicePluginName())) {
      return createUberContainerLauncher(containerLauncherContext, context,
          taskCommunicatorManagerInterface,
          workingDirectory, isPureLocalMode);
    } else {
      return createCustomContainerLauncher(containerLauncherContext, containerLauncherDescriptor);
    }
  }

  @VisibleForTesting
  ContainerLauncher createYarnContainerLauncher(ContainerLauncherContext containerLauncherContext) {
    LOG.info("Creating DefaultContainerLauncher");
    return new TezContainerLauncherImpl(containerLauncherContext);
  }

  @VisibleForTesting
  ContainerLauncher createUberContainerLauncher(ContainerLauncherContext containerLauncherContext,
                                                AppContext context,
                                                TaskCommunicatorManagerInterface taskCommunicatorManagerInterface,
                                                String workingDirectory,
                                                boolean isLocalMode) throws TezException {
    LOG.info("Creating LocalContainerLauncher");
    // TODO Post TEZ-2003. LocalContainerLauncher is special cased, since it makes use of
    // extensive internals which are only available at runtime. Will likely require
    // some kind of runtime binding of parameters in the payload to work correctly.
    try {
      return
          new LocalContainerLauncher(containerLauncherContext, context,
              taskCommunicatorManagerInterface,
              workingDirectory, isLocalMode);
    } catch (UnknownHostException e) {
      throw new TezUncheckedException(e);
    }
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  ContainerLauncher createCustomContainerLauncher(ContainerLauncherContext containerLauncherContext,
                                                  NamedEntityDescriptor containerLauncherDescriptor)
                                                      throws TezException {
    LOG.info("Creating container launcher {}:{} ", containerLauncherDescriptor.getEntityName(),
        containerLauncherDescriptor.getClassName());
    return ReflectionUtils.createClazzInstance(containerLauncherDescriptor.getClassName(),
        new Class[]{ContainerLauncherContext.class},
        new Object[]{containerLauncherContext});
  }

  @Override
  public void serviceInit(Configuration conf) {
    for (int i = 0 ; i < containerLaunchers.length ; i++) {
      containerLauncherServiceWrappers[i].init(conf);
    }
  }

  @Override
  public void serviceStart() {
    for (int i = 0 ; i < containerLaunchers.length ; i++) {
      containerLauncherServiceWrappers[i].start();
    }
  }

  @Override
  public void serviceStop() {
    for (int i = 0 ; i < containerLaunchers.length ; i++) {
      containerLauncherServiceWrappers[i].stop();
    }
  }

  public void dagComplete(TezDAGID dag, JobTokenSecretManager secretManager) {
    for (int i = 0 ; i < containerLaunchers.length ; i++) {
      containerLaunchers[i].dagComplete(dag, secretManager);
    }
  }

  public void dagSubmitted() {
    // Nothing to do right now. Indicates that a new DAG has been submitted and
    // the context has updated information.
  }


  @Override
  public void handle(ContainerLauncherEvent event) {
    int launcherId = event.getLauncherId();
    String schedulerName = appContext.getTaskSchedulerName(event.getSchedulerId());
    String taskCommName = appContext.getTaskCommunicatorName(event.getTaskCommId());
    switch (event.getType()) {
      case CONTAINER_LAUNCH_REQUEST:
        ContainerLauncherLaunchRequestEvent launchEvent = (ContainerLauncherLaunchRequestEvent) event;
        ContainerLaunchRequest launchRequest =
            new ContainerLaunchRequest(launchEvent.getNodeId(), launchEvent.getContainerId(),
                launchEvent.getContainerToken(), launchEvent.getContainerLaunchContext(),
                launchEvent.getContainer(), schedulerName,
                taskCommName);
        try {
          containerLaunchers[launcherId].launchContainer(launchRequest);
        } catch (Exception e) {
          String msg = "Error when launching container"
              + ", containerLauncher=" + Utils.getContainerLauncherIdentifierString(launcherId, appContext)
              + ", scheduler=" + Utils.getTaskSchedulerIdentifierString(event.getSchedulerId(), appContext)
              + ", taskCommunicator=" + Utils.getTaskCommIdentifierString(event.getTaskCommId(), appContext);
          LOG.error(msg, e);
          sendEvent(
              new DAGAppMasterEventUserServiceFatalError(
                  DAGAppMasterEventType.CONTAINER_LAUNCHER_SERVICE_FATAL_ERROR,
                  msg, e));
        }
        break;
      case CONTAINER_STOP_REQUEST:
        ContainerStopRequest stopRequest =
            new ContainerStopRequest(event.getNodeId(), event.getContainerId(),
                event.getContainerToken(), schedulerName, taskCommName);
        try {
          containerLaunchers[launcherId].stopContainer(stopRequest);
        } catch (Exception e) {
          String msg = "Error when stopping container"
              + ", containerLauncher=" + Utils.getContainerLauncherIdentifierString(launcherId, appContext)
              + ", scheduler=" + Utils.getTaskSchedulerIdentifierString(event.getSchedulerId(), appContext)
              + ", taskCommunicator=" + Utils.getTaskCommIdentifierString(event.getTaskCommId(), appContext);
          LOG.error(msg, e);
          sendEvent(
              new DAGAppMasterEventUserServiceFatalError(
                  DAGAppMasterEventType.CONTAINER_LAUNCHER_SERVICE_FATAL_ERROR,
                  msg, e));
        }
        break;
    }
  }

  public void reportError(int containerLauncherIndex, ServicePluginError servicePluginError,
                          String diagnostics,
                          DagInfo dagInfo) {
    if (servicePluginError.getErrorType() == ServicePluginError.ErrorType.PERMANENT) {
      String msg = "Fatal Error reported by ContainerLauncher"
          + ", containerLauncher=" +
          Utils.getContainerLauncherIdentifierString(containerLauncherIndex, appContext)
          + ", servicePluginError=" + servicePluginError
          + ", diagnostics= " + (diagnostics == null ? "" : diagnostics);
      LOG.error(msg);
      sendEvent(
          new DAGAppMasterEventUserServiceFatalError(
              DAGAppMasterEventType.CONTAINER_LAUNCHER_SERVICE_FATAL_ERROR,
              msg, null));
    } else {
      Utils
          .processNonFatalServiceErrorReport(
              Utils.getContainerLauncherIdentifierString(containerLauncherIndex, appContext),
              servicePluginError,
              diagnostics, dagInfo,
              appContext, "ContainerLauncher");
    }
  }

  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    appContext.getEventHandler().handle(event);
  }

  public String getContainerLauncherClassName(int containerLauncherIndex) {
    return containerLaunchers[containerLauncherIndex].getContainerLauncher().getClass().getName();
  }
}
