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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnRuntimeException;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.AMRMClient.StoredContainerRequest;
import org.apache.hadoop.yarn.client.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.rm.TaskScheduler.TaskSchedulerAppCallback.AppFinalStatus;

import com.google.common.annotations.VisibleForTesting;

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
                                Resource minContainerCapability,
                                Resource maxContainerCapability,
                                Map<ApplicationAccessType, String> appAcls
                                );
    public void onError(Exception e);
    public float getProgress();
    public AppFinalStatus getFinalAppStatus();
  }
  
  final AMRMClientAsync<CookieContainerRequest> amRmClient;
  final TaskSchedulerAppCallback appClient;
  
  Map<Object, CookieContainerRequest> taskRequests =  
                  new HashMap<Object, CookieContainerRequest>();
  Map<Object, Container> taskAllocations = 
                  new HashMap<Object, Container>();
  Map<ContainerId, Object> containerAssigments = 
                  new HashMap<ContainerId, Object>();
  HashMap<ContainerId, Object> releasedContainers = 
                  new HashMap<ContainerId, Object>();
  
  final String appHostName;
  final int appHostPort;
  final String appTrackingUrl;
  
  boolean isStopped = false; 
  
  class CRCookie {
    Object task;
    Object appCookie;
  }
  
  class CookieContainerRequest extends StoredContainerRequest {
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
  
  public TaskScheduler(ApplicationAttemptId id, 
                        TaskSchedulerAppCallback appClient,
                        String appHostName, 
                        int appHostPort,
                        String appTrackingUrl) {
    super(TaskScheduler.class.getName());
    this.appClient = appClient;
    this.amRmClient = 
        new AMRMClientAsync<CookieContainerRequest>(id, 1000, this);
    this.appHostName = appHostName;
    this.appHostPort = appHostPort;
    this.appTrackingUrl = appTrackingUrl;
  }
  
  @Private
  @VisibleForTesting
  TaskScheduler(ApplicationAttemptId id, 
      TaskSchedulerAppCallback appClient,
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
  
  public Resource getClusterAvailableResources() {
    return amRmClient.getClusterAvailableResources();
  }
  
  public int getClusterNodeCount() {
    return amRmClient.getClusterNodeCount();
  }
  
  // AbstractService methods
  @Override
  public synchronized void init(Configuration conf) {
    super.init(conf);
    amRmClient.init(conf);
  }
  
  @Override
  public void start() {
    try {
      RegisterApplicationMasterResponse response = null;
      synchronized (this) {
        amRmClient.start();
        super.start();
        response = amRmClient.registerApplicationMaster(appHostName, 
                                                        appHostPort, 
                                                        appTrackingUrl);
      }
      // upcall to app outside locks
      appClient.setApplicationRegistrationData(
                                      response.getMinimumResourceCapability(),
                                      response.getMaximumResourceCapability(),
                                      response.getApplicationACLs());
    } catch (YarnException e) {
      LOG.error("Yarn Exception while registering", e);
      throw new YarnRuntimeException(e);
    } catch (IOException e) {
      LOG.error("IO Exception while registering", e);
      throw new YarnRuntimeException(e);
    }
  }
  
  @Override
  public void stop() {
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
      super.stop();
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
    if(isStopped) {
      return;
    }
    Map<CookieContainerRequest, Container> appContainers = 
        new HashMap<CookieContainerRequest, Container>(containers.size());
    synchronized (this) {
      for(Container container : containers) {
        String location = container.getNodeId().getHost();
        CookieContainerRequest assigned = getMatchingRequest(container, location);
        if(assigned == null) {
          location = RackResolver.resolve(location).getNetworkLocation();
          assigned = getMatchingRequest(container, location);
        }
        if(assigned == null) {
          location = ResourceRequest.ANY;
          assigned = getMatchingRequest(container, location);
        }
        if(assigned == null) {
          // not matched anything. release container
          // Probably we cancelled a request and RM allocated that to us 
          // before RM heard of the cancellation
          releaseContainer(container.getId(), null);
          LOG.info("No RM requests matching container: " + container);
          continue;
        }
        
        Object task = getTask(assigned);
        assert task != null;
        assignContainer(task, container, assigned);
        appContainers.put(assigned, container);
              
        LOG.info("Assigning container: " + container + 
            " for task: " + task + 
            " at locality: " + location);
        
      }
    }
    
    // upcall to app must be outside locks
    for (Entry<CookieContainerRequest, Container> entry : 
                                        appContainers.entrySet()) {
      CookieContainerRequest assigned = entry.getKey();
      appClient.taskAllocated(getTask(assigned), assigned.getCookie().appCookie,
          entry.getValue());
    }   
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
    return appClient.getProgress();
  }

  @Override
  public void onError(Exception e) {
    if(isStopped) {
      return;
    }
    appClient.onError(e);
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
  
  public synchronized Container deallocateTask(Object task) {
    CookieContainerRequest request = removeTaskRequest(task);
    if(request != null) {
      // task not allocated yet
      LOG.info("Deallocating task: " + task + " before allocation");
      return null;
    }
    
    // task request not present. Look in allocations
    Container container = unAssignContainer(task, true);
    if(container != null) {
      LOG.info("Deallocated task: " + task +
               " from container: " + container.getId());
      return container;
    }
    
    // task neither requested nor allocated.
    LOG.info("Ignoring removal of unknown task: " + task);
    return null;
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
    return request.getCookie().task;
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
  
  private Container unAssignContainer(Object task, boolean releaseIfFound) {
    Container container = taskAllocations.remove(task);
    if(container == null) {
      return null;
    }
    containerAssigments.remove(container.getId());
    if(releaseIfFound) {
      releaseContainer(container.getId(), task);
    }
    return container;
  }
  
  private Object unAssignContainer(ContainerId containerId, 
                                    boolean releaseIfFound) {
    Object task = containerAssigments.remove(containerId);
    if(task == null) {
      return null;
    }
    taskAllocations.remove(task);
    if(releaseIfFound) {
      releaseContainer(containerId, task);
    }
    return task;
  }

  
}
