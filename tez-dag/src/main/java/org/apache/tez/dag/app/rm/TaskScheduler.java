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

package org.apache.tez.dag.app.rm;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.rm.TaskScheduler.TaskSchedulerAppCallback.AppFinalStatus;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

/* TODO not yet updating cluster nodes on every allocate response
 * from RMContainerRequestor
   import org.apache.tez.dag.app.rm.node.AMNodeEventNodeCountUpdated;
    if (clusterNmCount != lastClusterNmCount) {
      LOG.info("Num cluster nodes changed from " + lastClusterNmCount + " to "
          + clusterNmCount);
      eventHandler.handle(new AMNodeEventNodeCountUpdated(clusterNmCount));
    }
 */
public class TaskScheduler extends AbstractService
                             implements AMRMClientAsync.CallbackHandler {
  private static final Log LOG = LogFactory.getLog(TaskScheduler.class);

  public interface TaskSchedulerAppCallback {
    public class AppFinalStatus {
      public final FinalApplicationStatus exitStatus;
      public final String exitMessage;
      public final String postCompletionTrackingUrl;
      public AppFinalStatus(FinalApplicationStatus exitStatus,
                             String exitMessage,
                             String posCompletionTrackingUrl) {
        this.exitStatus = exitStatus;
        this.exitMessage = exitMessage;
        this.postCompletionTrackingUrl = posCompletionTrackingUrl;
      }
    }
    // upcall to app must be outside locks
    public void taskAllocated(Object task,
                               Object appCookie,
                               Container container);
    // this may end up being called for a task+container pair that the app
    // has not heard about. this can happen because of a race between
    // taskAllocated() upcall and deallocateTask() downcall
    public void containerCompleted(Object taskLastAllocated,
                                    ContainerStatus containerStatus);
    public void nodesUpdated(List<NodeReport> updatedNodes);
    public void appShutdownRequested();
    public void setApplicationRegistrationData(
                                Resource maxContainerCapability,
                                Map<ApplicationAccessType, String> appAcls
                                );
    public void onError(Throwable t);
    public float getProgress();
    public AppFinalStatus getFinalAppStatus();
  }

  final AMRMClientAsync<CookieContainerRequest> amRmClient;
  final TaskSchedulerAppCallback appClient;

  // Container Re-Use configuration
  private boolean shouldReuseContainers;
  private boolean reuseRackLocal;
  private boolean reuseNonLocal;

  Map<Object, CookieContainerRequest> taskRequests =
                  new HashMap<Object, CookieContainerRequest>();
  // LinkedHashMap is need in getProgress()
  LinkedHashMap<Object, Container> taskAllocations =
                  new LinkedHashMap<Object, Container>();
  Map<ContainerId, Object> containerAssigments =
                  new HashMap<ContainerId, Object>();
  HashMap<ContainerId, Object> releasedContainers =
                  new HashMap<ContainerId, Object>();

  Resource totalResources = Resource.newInstance(0, 0);
  Resource allocatedResources = Resource.newInstance(0, 0);

  final String appHostName;
  final int appHostPort;
  final String appTrackingUrl;

  boolean isStopped = false;

  private ContainerAssigner NODE_LOCAL_ASSIGNER = new NodeLocalContainerAssigner();
  private ContainerAssigner RACK_LOCAL_ASSIGNER = new RackLocalContainerAssigner();
  private ContainerAssigner NON_LOCAL_ASSIGNER = new NonLocalContainerAssigner();

  class CRCookie {
    Object task;
    Object appCookie;

    Object getTask() {
      return task;
    }

    Object getAppCookie() {
      return appCookie;
    }
  }

  class CookieContainerRequest extends ContainerRequest {
    CRCookie cookie;
    public CookieContainerRequest(Resource capability, String[] hosts,
        String[] racks, Priority priority, CRCookie cookie) {
      super(capability, hosts, racks, priority);
      this.cookie = cookie;
    }

    CRCookie getCookie() {
      return cookie;
    }
  }

  public TaskScheduler(TaskSchedulerAppCallback appClient,
                        String appHostName,
                        int appHostPort,
                        String appTrackingUrl) {
    super(TaskScheduler.class.getName());
    this.appClient = appClient;
    this.amRmClient = AMRMClientAsync.createAMRMClientAsync(1000, this);
    this.appHostName = appHostName;
    this.appHostPort = appHostPort;
    this.appTrackingUrl = appTrackingUrl;
  }

  @Private
  @VisibleForTesting
  TaskScheduler(TaskSchedulerAppCallback appClient,
      String appHostName,
      int appHostPort,
      String appTrackingUrl,
      AMRMClientAsync<CookieContainerRequest> client) {
    super(TaskScheduler.class.getName());
    this.appClient = appClient;
    this.amRmClient = client;
    this.appHostName = appHostName;
    this.appHostPort = appHostPort;
    this.appTrackingUrl = appTrackingUrl;
  }

  public Resource getAvailableResources() {
    return amRmClient.getAvailableResources();
  }

  public int getClusterNodeCount() {
    return amRmClient.getClusterNodeCount();
  }

  // AbstractService methods
  @Override
  public synchronized void serviceInit(Configuration conf) {

    amRmClient.init(conf);
    int heartbeatIntervalMax = conf.getInt(
        TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX,
        TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX_DEFAULT);
    amRmClient.setHeartbeatInterval(heartbeatIntervalMax);

    shouldReuseContainers = conf.getBoolean(
        TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED,
        TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED_DEFAULT);
    reuseRackLocal = conf.getBoolean(
        TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED,
        TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED_DEFAULT);
    reuseNonLocal = conf
        .getBoolean(
            TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED,
            TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED_DEFAULT);
    LOG.info("TaskScheduler initialized with configuration: " +
            "maxRMHeartbeatInterval: " + heartbeatIntervalMax +
            ", containerReuseEnabled: " + shouldReuseContainers +
            ", reuseRackLocal: " + reuseRackLocal +
            ", reuseNonLocal: " + reuseNonLocal);
  }

  @Override
  public void serviceStart() {
    try {
      RegisterApplicationMasterResponse response = null;
      synchronized (this) {
        amRmClient.start();
        response = amRmClient.registerApplicationMaster(appHostName,
                                                        appHostPort,
                                                        appTrackingUrl);
      }
      // upcall to app outside locks
      appClient.setApplicationRegistrationData(
                                      response.getMaximumResourceCapability(),
                                      response.getApplicationACLs());
    } catch (YarnException e) {
      LOG.error("Yarn Exception while registering", e);
      throw new TezUncheckedException(e);
    } catch (IOException e) {
      LOG.error("IO Exception while registering", e);
      throw new TezUncheckedException(e);
    }
  }

  @Override
  public void serviceStop() {
    // upcall to app outside of locks
    AppFinalStatus status = appClient.getFinalAppStatus();
    try {
      // TODO TEZ-36 dont unregister automatically after reboot sent by RM
      synchronized (this) {
        isStopped = true;
        amRmClient.unregisterApplicationMaster(status.exitStatus,
                                               status.exitMessage,
                                               status.postCompletionTrackingUrl);
      }

      // call client.stop() without lock client will attempt to stop the callback
      // operation and at the same time the callback operation might be trying
      // to get our lock.
      amRmClient.stop();
    } catch (YarnException e) {
      LOG.error("Yarn Exception while unregistering ", e);
      throw new TezUncheckedException(e);
    } catch (IOException e) {
      LOG.error("IOException while unregistering ", e);
      throw new TezUncheckedException(e);
    }
  }

  // AMRMClientAsync interface methods
  @Override
  public void onContainersCompleted(List<ContainerStatus> statuses) {
    if(isStopped) {
      return;
    }
    Map<Object, ContainerStatus> appContainerStatus =
                        new HashMap<Object, ContainerStatus>(statuses.size());
    synchronized (this) {
      for(ContainerStatus containerStatus : statuses) {
        ContainerId completedId = containerStatus.getContainerId();
        Object task = releasedContainers.remove(completedId);
        if(task != null){
          // TODO later we may want to check if exit code matched expectation
          // e.g. successful container should not come back fail exit code after
          // being released
          // completion of a container we had released earlier
          // an allocated container completed. notify app
          LOG.info("Released container completed:" + completedId +
                   " last allocated to task: " + task);
          appContainerStatus.put(task, containerStatus);
          continue;
        }

        // not found in released containers. check currently allocated containers
        // no need to release this container as the RM has already completed it
        task = unAssignContainer(completedId, false);
        if(task != null) {
          // completion of a container we have allocated currently
          // an allocated container completed. notify app
          LOG.info("Allocated container completed:" + completedId +
                   " last allocated to task: " + task);
          appContainerStatus.put(task, containerStatus);
          continue;
        }

        // container neither allocated nor released
        LOG.info("Ignoring unknown container: " + containerStatus.getContainerId());
      }
    }

    // upcall to app must be outside locks
    for(Entry<Object, ContainerStatus> entry : appContainerStatus.entrySet()) {
      appClient.containerCompleted(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void onContainersAllocated(List<Container> containers) {
    if (isStopped) {
      return;
    }
    Map<CookieContainerRequest, Container> assignedContainers = null;

    synchronized (this) {
      assignedContainers = assignAllocatedContainers(containers, false);
    }

    // upcall to app must be outside locks
    for (Entry<CookieContainerRequest, Container> entry : assignedContainers
        .entrySet()) {
      informAppAboutAssignments(entry.getKey(), entry.getValue());
    }
  }

  /*
   * Separate calls should be made for contianers being reused and new
   * containers.
   */
  private synchronized Map<CookieContainerRequest, Container> assignAllocatedContainers(
      List<Container> initialContainerList, boolean isBeingReused) {
    List<Container> containers = Lists.newLinkedList(initialContainerList);
    Map<CookieContainerRequest, Container> assignedContainers = new HashMap<CookieContainerRequest, Container>(
        containers.size());
    assignContainersWithLocation(containers, isBeingReused,
        NODE_LOCAL_ASSIGNER, assignedContainers);
    assignContainersWithLocation(containers, isBeingReused,
        RACK_LOCAL_ASSIGNER, assignedContainers);
    assignContainersWithLocation(containers, isBeingReused, NON_LOCAL_ASSIGNER,
        assignedContainers);

    releaseUnassignedContainers(containers);
    return assignedContainers;
  }

  @Override
  public void onShutdownRequest() {
    if(isStopped) {
      return;
    }
    // upcall to app must be outside locks
    appClient.appShutdownRequested();
  }

  @Override
  public void onNodesUpdated(List<NodeReport> updatedNodes) {
    if(isStopped) {
      return;
    }
    // ignore bad nodes for now
    // upcall to app must be outside locks
    appClient.nodesUpdated(updatedNodes);
  }

  @Override
  public float getProgress() {
    if(isStopped) {
      return 1;
    }

    if(totalResources.getMemory() == 0) {
      // assume this is the first allocate callback. nothing is allocated.
      // available resource = totalResource
      // TODO this will not handle dynamic changes in resources
      totalResources = Resources.clone(getAvailableResources());
      LOG.info("App total resource memory: " + totalResources.getMemory() +
               " cpu: " + totalResources.getVirtualCores() +
               " taskAllocations: " + taskAllocations.size());
    }

    preemptIfNeeded();

    return appClient.getProgress();
  }

  @Override
  public void onError(Throwable t) {
    if(isStopped) {
      return;
    }
    appClient.onError(t);
  }

  public synchronized Resource getTotalResources() {
    return totalResources;
  }

  public synchronized void allocateTask(Object task,
                                           Resource capability,
                                           String[] hosts,
                                           String[] racks,
                                           Priority priority,
                                           Object clientCookie) {
    // TODO check for nulls etc
    // TODO extra memory allocation
    CRCookie cookie = new CRCookie();
    cookie.task = task;
    cookie.appCookie = clientCookie;
    CookieContainerRequest request =
             new CookieContainerRequest(capability,
                                         hosts,
                                         racks,
                                         priority,
                                         cookie);

    addTaskRequest(task, request);
    LOG.info("Allocation request for task: " + task +
             " with request: " + request);
  }

  /**
   * @param task
   *          the task to de-allocate.
   * @param taskSucceeded
   *          specify whether the task succeeded or failed.
   * @return the container used for the task if the container is being
   *         released, otherwise null.
   */
  public Container deallocateTask(Object task, boolean taskSucceeded) {
    CookieContainerRequest request = null;
    Map<CookieContainerRequest, Container> assignedContainers = null;
    Container container = null;

    synchronized (this) {
      request =  removeTaskRequest(task);
      if (request != null) {
        // task not allocated yet
        LOG.info("Deallocating task: " + task + " before allocation");
        return null;
      }

      // task request not present. Look in allocations
      container = unAssignContainer(task);
      if (container == null) {
        // task neither requested nor allocated.
        LOG.info("Ignoring removal of unknown task: " + task);
        container = null;
      } else {
        LOG.info("Deallocated task: " + task + " from container: "
            + container.getId());

        if (!taskSucceeded || !shouldReuseContainers) {
          releaseContainer(container.getId(), task);
        } else {
          assignedContainers = assignAllocatedContainers(
              Collections.singletonList(container), true);
        }

      }
    }

    // up call outside of the lock.
    if (assignedContainers != null && assignedContainers.size() == 1) {
      informAppAboutAssignments(assignedContainers.entrySet().iterator().next()
          .getKey(), container);
      container = null;
    }
    return container;
  }

  public synchronized Object deallocateContainer(ContainerId containerId) {
    Object task = unAssignContainer(containerId, true);
    if(task != null) {
      LOG.info("Deallocated container: " + containerId +
               " from task: " + task);
      return task;
    }

    LOG.info("Ignoring dealloction of unknown container: " + containerId);
    return null;
  }

  synchronized void preemptIfNeeded() {
    Resource freeResources = Resources.subtract(totalResources,
        allocatedResources);
    LOG.info("Allocated resource memory: " + allocatedResources.getMemory() +
             " cpu:" + allocatedResources.getVirtualCores());
    assert freeResources.getMemory() >= 0;

    CookieContainerRequest highestPriRequest = null;
    for(CookieContainerRequest request : taskRequests.values()) {
      if(highestPriRequest == null) {
        highestPriRequest = request;
      } else if(isHigherPriority(request.getPriority(),
                                   highestPriRequest.getPriority())){
        highestPriRequest = request;
      }
    }
    if(highestPriRequest != null &&
       !fitsIn(highestPriRequest.getCapability(), freeResources)) {
      // highest priority request will not fit in existing free resources
      // free up some more
      // TODO this is subject to error wrt RM resource normalization
      Map.Entry<Object, Container> preemptedEntry = null;
      for(Map.Entry<Object, Container> entry : taskAllocations.entrySet()) {
        if(!isHigherPriority(highestPriRequest.getPriority(),
                             entry.getValue().getPriority())) {
          // higher or same priority
          continue;
        }
        if(preemptedEntry == null ||
           !isHigherPriority(entry.getValue().getPriority(),
                             preemptedEntry.getValue().getPriority())) {
          // keep the lower priority or the one added later
          preemptedEntry = entry;
        }
      }
      if(preemptedEntry != null) {
        // found something to preempt
        LOG.info("Preempting task: " + preemptedEntry.getKey() +
            " to free resource for request: " + highestPriRequest +
            " . Current free resources: " + freeResources);
        deallocateContainer(preemptedEntry.getValue().getId());
        // app client will be notified when after container is killed
        // and we get its completed container status
      }
    }
  }

  private boolean fitsIn(Resource toFit, Resource resource) {
    // YARN-893 prevents using correct library code
    //return Resources.fitsIn(toFit, resource);
    return resource.getMemory() >= toFit.getMemory();
  }

  private CookieContainerRequest getMatchingRequest(
                                      Container container, String location) {
    Priority priority = container.getPriority();
    Resource capability = container.getResource();
    CookieContainerRequest assigned = null;
    List<? extends Collection<CookieContainerRequest>> requestsList =
        amRmClient.getMatchingRequests(priority, location, capability);

    if(requestsList.size() > 0) {
      // pick first one
      for(Collection<CookieContainerRequest> requests : requestsList) {
        Iterator<CookieContainerRequest> iterator = requests.iterator();
        if(iterator.hasNext()) {
          assigned = requests.iterator().next();
        }
      }
    }

    return assigned;
  }

  private Object getTask(CookieContainerRequest request) {
    return request.getCookie().getTask();
  }

  private void releaseContainer(ContainerId containerId, Object task) {
    amRmClient.releaseAssignedContainer(containerId);
    if(task != null) {
      releasedContainers.put(containerId, task);
    }
  }

  private void assignContainer(Object task,
      Container container,
      CookieContainerRequest assigned) {
    CookieContainerRequest request = removeTaskRequest(task);
    assert request != null;
    //assert assigned.equals(request);

    Container result = taskAllocations.put(task, container);
    assert result == null;
    containerAssigments.put(container.getId(), task);

    Resources.addTo(allocatedResources, container.getResource());
  }

  private CookieContainerRequest removeTaskRequest(Object task) {
    CookieContainerRequest request = taskRequests.remove(task);
    if(request != null) {
      // remove all references of the request from AMRMClient
      amRmClient.removeContainerRequest(request);
    }
    return request;
  }

  private void addTaskRequest(Object task,
                                CookieContainerRequest request) {
    // TODO TEZ-37 fix duplicate handling
    taskRequests.put(task, request);
    amRmClient.addContainerRequest(request);
  }

  private Container unAssignContainer(Object task) {
    Container container = taskAllocations.remove(task);
    if(container == null) {
      return null;
    }
    Resources.subtractFrom(allocatedResources, container.getResource());
    assert allocatedResources.getMemory() >= 0;
    containerAssigments.remove(container.getId());
    return container;
  }

  private Object unAssignContainer(ContainerId containerId,
                                    boolean releaseIfFound) {
    Object task = containerAssigments.remove(containerId);
    if(task == null) {
      return null;
    }
    Container container = taskAllocations.remove(task);
    assert container != null;
    Resources.subtractFrom(allocatedResources, container.getResource());
    assert allocatedResources.getMemory() >= 0;
    if(releaseIfFound) {
      releaseContainer(containerId, task);
    }
    return task;
  }

  private boolean isHigherPriority(Priority lhs, Priority rhs) {
    return lhs.getPriority() < rhs.getPriority();
  }

  /**
   * Assigns allocated containers using the specified assigner. The list of
   * allocated containers is removed from the specified container list.
   *
   * Separate calls should be made for contianers being reused and new
   * containers.
   *
   * @param containers
   *          containers to be assigned.
   * @param isBeingReused
   *          specifies whether all containers specified by the list of
   *          containers are being reused or not.
   * @param assigner
   *          the assigner to use - nodeLocal, rackLocal, nonLocal
   * @param assignedContainers container assignments are populated into this map.
   * @return
   */
  private synchronized void assignContainersWithLocation(
      List<Container> containers, boolean isBeingReused,
      ContainerAssigner assigner,
      Map<CookieContainerRequest, Container> assignedContainers) {

    Iterator<Container> containerIterator = containers.iterator();
    while (containerIterator.hasNext()) {
      Container container = containerIterator.next();
      CookieContainerRequest assigned = assigner.assignAllocatedContainer(container,
          isBeingReused);
      if (assigned != null) {
        assignedContainers.put(assigned, container);
        containerIterator.remove();
      }
    }
  }

  private void releaseUnassignedContainers(List<Container> containers) {
    for (Container container : containers) {
      releaseContainer(container.getId(), null);
      LOG.info("No RM requests matching container: " + container);
    }
  }

  private void informAppAboutAssignments(CookieContainerRequest assigned,
      Container container) {
    appClient.taskAllocated(getTask(assigned), assigned.getCookie().appCookie,
        container);
  }

  private abstract class ContainerAssigner {
    public abstract CookieContainerRequest assignAllocatedContainer(
        Container container, boolean isBeingReused);

    public void doBookKeepingForAssignedContainer(CookieContainerRequest assigned,
        Container container, String matchedLocation, boolean isBeingReused) {
      if (assigned == null) {
        return;
      }
      Object task = getTask(assigned);
      assert task != null;
      assignContainer(task, container, assigned);

      LOG.info("Assigning container: " + container + " for task: " + task
          + " at locality: " + matchedLocation + " with reuse: "
          + isBeingReused + " resource memory: "
          + container.getResource().getMemory() + " cpu: "
          + container.getResource().getVirtualCores());
    }
  }

  private class NodeLocalContainerAssigner extends ContainerAssigner {
    @Override
    public CookieContainerRequest assignAllocatedContainer(Container container,
        boolean isBeingReused) {
      String location = container.getNodeId().getHost();
      CookieContainerRequest assigned = getMatchingRequest(container, location);
      doBookKeepingForAssignedContainer(assigned, container, location,
          isBeingReused);
      return assigned;
    }
  }

  // TODO TEZ-330 Ignore reuseRackLocak/reuseNonLocal in case the initial
  // request did not have any locality information.
  private class RackLocalContainerAssigner extends ContainerAssigner {
    @Override
    public CookieContainerRequest assignAllocatedContainer(Container container,
        boolean isBeingReused) {
      if (!isBeingReused || TaskScheduler.this.reuseRackLocal) {
        String location = RackResolver.resolve(container.getNodeId().getHost())
            .getNetworkLocation();
        CookieContainerRequest assigned = getMatchingRequest(container,
            location);
        doBookKeepingForAssignedContainer(assigned, container, location,
            isBeingReused);
        return assigned;
      }
      return null;
    }
  }

  private class NonLocalContainerAssigner extends ContainerAssigner {
    @Override
    public CookieContainerRequest assignAllocatedContainer(Container container,
        boolean isBeingReused) {
      if (!isBeingReused || TaskScheduler.this.reuseNonLocal) {
        String location = ResourceRequest.ANY;
        CookieContainerRequest assigned = getMatchingRequest(container,
            location);
        doBookKeepingForAssignedContainer(assigned, container, location,
            isBeingReused);
        return assigned;
      }
      return null;
    }
  }

}
