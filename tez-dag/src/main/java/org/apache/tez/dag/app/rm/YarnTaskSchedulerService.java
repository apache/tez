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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
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
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.DAGAppMasterState;
import org.apache.tez.dag.app.rm.TaskSchedulerService.TaskSchedulerAppCallback.AppFinalStatus;
import org.apache.tez.dag.app.rm.container.ContainerSignatureMatcher;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/* TODO not yet updating cluster nodes on every allocate response
 * from RMContainerRequestor
   import org.apache.tez.dag.app.rm.node.AMNodeEventNodeCountUpdated;
    if (clusterNmCount != lastClusterNmCount) {
      LOG.info("Num cluster nodes changed from " + lastClusterNmCount + " to "
          + clusterNmCount);
      eventHandler.handle(new AMNodeEventNodeCountUpdated(clusterNmCount));
    }
 */
public class YarnTaskSchedulerService extends TaskSchedulerService
                             implements AMRMClientAsync.CallbackHandler {
  private static final Log LOG = LogFactory.getLog(YarnTaskSchedulerService.class);



  final TezAMRMClientAsync<CookieContainerRequest> amRmClient;
  final TaskSchedulerAppCallback realAppClient;
  final TaskSchedulerAppCallback appClientDelegate;
  final ContainerSignatureMatcher containerSignatureMatcher;
  ExecutorService appCallbackExecutor;

  // Container Re-Use configuration
  private boolean shouldReuseContainers;
  private boolean reuseRackLocal;
  private boolean reuseNonLocal;

  Map<Object, CookieContainerRequest> taskRequests =
                  new HashMap<Object, CookieContainerRequest>();
  // LinkedHashMap is need in getProgress()
  LinkedHashMap<Object, Container> taskAllocations =
                  new LinkedHashMap<Object, Container>();
  /**
   * Tracks last task assigned to a known container.
   */
  Map<ContainerId, Object> containerAssignments =
                  new HashMap<ContainerId, Object>();
  // Remove inUse depending on resolution of TEZ-1129
  Set<ContainerId> inUseContainers = Sets.newHashSet(); 
  HashMap<ContainerId, Object> releasedContainers =
                  new HashMap<ContainerId, Object>();
  /**
   * Map of containers currently being held by the TaskScheduler.
   */
  Map<ContainerId, HeldContainer> heldContainers =
      new HashMap<ContainerId, HeldContainer>();
  
  Set<Priority> priorityHasAffinity = Sets.newHashSet();
  
  Set<NodeId> blacklistedNodes = Collections
      .newSetFromMap(new ConcurrentHashMap<NodeId, Boolean>());
  
  Resource totalResources = Resource.newInstance(0, 0);
  Resource allocatedResources = Resource.newInstance(0, 0);
  long numHeartbeats = 0;
  long heartbeatAtLastPreemption = 0;
  int numHeartbeatsBetweenPreemptions = 0;
  
  final String appHostName;
  final int appHostPort;
  final String appTrackingUrl;
  final AppContext appContext;
  private AtomicBoolean hasUnregistered = new AtomicBoolean(false);

  AtomicBoolean isStopped = new AtomicBoolean(false);

  private ContainerAssigner NODE_LOCAL_ASSIGNER = new NodeLocalContainerAssigner();
  private ContainerAssigner RACK_LOCAL_ASSIGNER = new RackLocalContainerAssigner();
  private ContainerAssigner NON_LOCAL_ASSIGNER = new NonLocalContainerAssigner();

  DelayedContainerManager delayedContainerManager;
  long localitySchedulingDelay;
  long idleContainerTimeoutMin;
  long idleContainerTimeoutMax = 0;
  int sessionNumMinHeldContainers = 0;
  int preemptionPercentage = 0; 
  
  Set<ContainerId> sessionMinHeldContainers = Sets.newHashSet();
  
  RandomDataGenerator random = new RandomDataGenerator();

  @VisibleForTesting
  protected AtomicBoolean shouldUnregister = new AtomicBoolean(false);

  static class CRCookie {
    // Do not use these variables directly. Can caused mocked unit tests to fail.
    private Object task;
    private Object appCookie;
    private Object containerSignature;
    
    CRCookie(Object task, Object appCookie, Object containerSignature) {
      this.task = task;
      this.appCookie = appCookie;
      this.containerSignature = containerSignature;
    }

    Object getTask() {
      return task;
    }

    Object getAppCookie() {
      return appCookie;
    }
    
    Object getContainerSignature() {
      return containerSignature;
    }
  }

  class CookieContainerRequest extends ContainerRequest {
    CRCookie cookie;
    ContainerId affinitizedContainerId;

    public CookieContainerRequest(
        Resource capability,
        String[] hosts,
        String[] racks,
        Priority priority,
        CRCookie cookie) {
      super(capability, hosts, racks, priority);
      this.cookie = cookie;
    }

    public CookieContainerRequest(
        Resource capability,
        ContainerId containerId,
        String[] hosts,
        String[] racks,
        Priority priority,
        CRCookie cookie) {
      this(capability, hosts, racks, priority, cookie);
      this.affinitizedContainerId = containerId;
    }

    CRCookie getCookie() {
      return cookie;
    }
    
    ContainerId getAffinitizedContainer() {
      return affinitizedContainerId;
    }
  }

  public YarnTaskSchedulerService(TaskSchedulerAppCallback appClient,
                        ContainerSignatureMatcher containerSignatureMatcher,
                        String appHostName,
                        int appHostPort,
                        String appTrackingUrl,
                        AppContext appContext) {
    super(YarnTaskSchedulerService.class.getName());
    this.realAppClient = appClient;
    this.appCallbackExecutor = createAppCallbackExecutorService();
    this.containerSignatureMatcher = containerSignatureMatcher;
    this.appClientDelegate = createAppCallbackDelegate(appClient);
    this.amRmClient = TezAMRMClientAsync.createAMRMClientAsync(1000, this);
    this.appHostName = appHostName;
    this.appHostPort = appHostPort;
    this.appTrackingUrl = appTrackingUrl;
    this.appContext = appContext;
  }

  @Private
  @VisibleForTesting
  YarnTaskSchedulerService(TaskSchedulerAppCallback appClient,
      ContainerSignatureMatcher containerSignatureMatcher,
      String appHostName,
      int appHostPort,
      String appTrackingUrl,
      TezAMRMClientAsync<CookieContainerRequest> client,
      AppContext appContext) {
    super(YarnTaskSchedulerService.class.getName());
    this.realAppClient = appClient;
    this.appCallbackExecutor = createAppCallbackExecutorService();
    this.containerSignatureMatcher = containerSignatureMatcher;
    this.appClientDelegate = createAppCallbackDelegate(appClient);
    this.amRmClient = client;
    this.appHostName = appHostName;
    this.appHostPort = appHostPort;
    this.appTrackingUrl = appTrackingUrl;
    this.appContext = appContext;
  }

  @VisibleForTesting
  ExecutorService createAppCallbackExecutorService() {
    return Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("TaskSchedulerAppCaller #%d").setDaemon(true).build());
  }
  
  @Override
  public Resource getAvailableResources() {
    return amRmClient.getAvailableResources();
  }

  @Override
  public int getClusterNodeCount() {
    // this can potentially be cheaper after YARN-1722
    return amRmClient.getClusterNodeCount();
  }

  TaskSchedulerAppCallback createAppCallbackDelegate(
      TaskSchedulerAppCallback realAppClient) {
    return new TaskSchedulerAppCallbackWrapper(realAppClient,
        appCallbackExecutor);
  }

  @Override
  public void setShouldUnregister() {
    this.shouldUnregister.set(true);
  }

  @Override
  public boolean hasUnregistered() {
    return hasUnregistered.get();
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
    Preconditions.checkArgument(
      ((!reuseRackLocal && !reuseNonLocal) || (reuseRackLocal)),
      "Re-use Rack-Local cannot be disabled if Re-use Non-Local has been"
      + " enabled");

    localitySchedulingDelay = conf.getLong(
      TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS,
      TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS_DEFAULT);
    Preconditions.checkArgument(localitySchedulingDelay >= 0,
        "Locality Scheduling delay should be >=0");

    idleContainerTimeoutMin = conf.getLong(
        TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS,
        TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS_DEFAULT);
    Preconditions.checkArgument(idleContainerTimeoutMin >= 0 || idleContainerTimeoutMin == -1,
      "Idle container release min timeout should be either -1 or >=0");
    
    idleContainerTimeoutMax = conf.getLong(
        TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS,
        TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS_DEFAULT);
    Preconditions.checkArgument(
        idleContainerTimeoutMax >= 0 && idleContainerTimeoutMax >= idleContainerTimeoutMin,
        "Idle container release max timeout should be >=0 and >= " + 
        TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS);
    
    sessionNumMinHeldContainers = conf.getInt(TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS, 
        TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS_DEFAULT);
    Preconditions.checkArgument(sessionNumMinHeldContainers >= 0, 
        "Session minimum held containers should be >=0");
    
    preemptionPercentage = conf.getInt(TezConfiguration.TEZ_AM_PREEMPTION_PERCENTAGE, 
        TezConfiguration.TEZ_AM_PREEMPTION_PERCENTAGE_DEFAULT);
    Preconditions.checkArgument(preemptionPercentage >= 0 && preemptionPercentage <= 100,
        "Preemption percentage should be between 0-100");
    
    numHeartbeatsBetweenPreemptions = conf.getInt(
        TezConfiguration.TEZ_AM_PREEMPTION_HEARTBEATS_BETWEEN_PREEMPTIONS,
        TezConfiguration.TEZ_AM_PREEMPTION_HEARTBEATS_BETWEEN_PREEMPTIONS_DEFAULT);
    Preconditions.checkArgument(numHeartbeatsBetweenPreemptions >= 1, 
        "Heartbeats between preemptions should be >=1");

    delayedContainerManager = new DelayedContainerManager();
    LOG.info("TaskScheduler initialized with configuration: " +
            "maxRMHeartbeatInterval: " + heartbeatIntervalMax +
            ", containerReuseEnabled: " + shouldReuseContainers +
            ", reuseRackLocal: " + reuseRackLocal +
            ", reuseNonLocal: " + reuseNonLocal + 
            ", localitySchedulingDelay: " + localitySchedulingDelay +
            ", preemptionPercentage: " + preemptionPercentage +
            ", numHeartbeatsBetweenPreemptions" + numHeartbeatsBetweenPreemptions +
            ", idleContainerMinTimeout=" + idleContainerTimeoutMin +
            ", idleContainerMaxTimeout=" + idleContainerTimeoutMax +
            ", sessionMinHeldContainers=" + sessionNumMinHeldContainers);
  }

  @Override
  public void serviceStart() {
    try {
      RegisterApplicationMasterResponse response;
      synchronized (this) {
        amRmClient.start();
        response = amRmClient.registerApplicationMaster(appHostName,
                                                        appHostPort,
                                                        appTrackingUrl);
      }
      // upcall to app outside locks
      appClientDelegate.setApplicationRegistrationData(
          response.getMaximumResourceCapability(),
          response.getApplicationACLs(),
          response.getClientToAMTokenMasterKey());

      delayedContainerManager.start();
    } catch (YarnException e) {
      LOG.error("Yarn Exception while registering", e);
      throw new TezUncheckedException(e);
    } catch (IOException e) {
      LOG.error("IO Exception while registering", e);
      throw new TezUncheckedException(e);
    }
  }

  @Override
  public void serviceStop() throws InterruptedException {
    // upcall to app outside of locks
    try {
      delayedContainerManager.shutdown();
      // Wait for contianers to be released.
      delayedContainerManager.join(2000l);
      synchronized (this) {
        isStopped.set(true);
        if (shouldUnregister.get()) {
          AppFinalStatus status = appClientDelegate.getFinalAppStatus();
          LOG.info("Unregistering application from RM"
              + ", exitStatus=" + status.exitStatus
              + ", exitMessage=" + status.exitMessage
              + ", trackingURL=" + status.postCompletionTrackingUrl);
          amRmClient.unregisterApplicationMaster(status.exitStatus,
              status.exitMessage,
              status.postCompletionTrackingUrl);
          LOG.info("Successfully unregistered application from RM");
          hasUnregistered.set(true);
        }
      }

      // call client.stop() without lock client will attempt to stop the callback
      // operation and at the same time the callback operation might be trying
      // to get our lock.
      amRmClient.stop();
      appCallbackExecutor.shutdown();
      appCallbackExecutor.awaitTermination(1000l, TimeUnit.MILLISECONDS);
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
    if (isStopped.get()) {
      return;
    }
    Map<Object, ContainerStatus> appContainerStatus =
                        new HashMap<Object, ContainerStatus>(statuses.size());
    synchronized (this) {
      for(ContainerStatus containerStatus : statuses) {
        ContainerId completedId = containerStatus.getContainerId();
        HeldContainer delayedContainer = heldContainers.get(completedId);

        Object task = releasedContainers.remove(completedId);
        if(task != null){
          if (delayedContainer != null) {
            LOG.warn("Held container should be null since releasedContainer is not");
          }
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
        if (delayedContainer != null) {
          heldContainers.remove(completedId);
          Resources.subtract(allocatedResources, delayedContainer.getContainer().getResource());
        } else {
          LOG.warn("Held container expected to be not null for a non-AM-released container");
        }
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
    for (Entry<Object, ContainerStatus> entry : appContainerStatus.entrySet()) {
      appClientDelegate.containerCompleted(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void onContainersAllocated(List<Container> containers) {
    if (isStopped.get()) {
      return;
    }
    Map<CookieContainerRequest, Container> assignedContainers;

    if (LOG.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder();
      for (Container container: containers) {
        sb.append(container.getId()).append(", ");
      }
      LOG.debug("Assigned New Containers: " + sb.toString());
    }

    synchronized (this) {
      if (!shouldReuseContainers) {
        List<Container> modifiableContainerList = Lists.newLinkedList(containers);
        assignedContainers = assignNewlyAllocatedContainers(
            modifiableContainerList);
      } else {
        // unify allocations
        pushNewContainerToDelayed(containers);
        return;
      }
    }

    // upcall to app must be outside locks
    informAppAboutAssignments(assignedContainers);
  }

  /**
   * Tries assigning the list of specified containers. Optionally, release
   * containers or add them to the delayed container queue.
   *
   * The flags apply to all containers in the specified lists. So, separate
   * calls should be made based on the expected behaviour.
   *
   * @param containers
   *          The list of containers to be assigned. The list *may* be modified
   *          in place based on allocations and releases.
   * @return Assignments.
   */
  private synchronized Map<CookieContainerRequest, Container>
      assignNewlyAllocatedContainers(Iterable<Container> containers) {

    Map<CookieContainerRequest, Container> assignedContainers =
        new HashMap<CookieContainerRequest, Container>();
    assignNewContainersWithLocation(containers,
      NODE_LOCAL_ASSIGNER, assignedContainers);
    assignNewContainersWithLocation(containers,
      RACK_LOCAL_ASSIGNER, assignedContainers);
    assignNewContainersWithLocation(containers,
      NON_LOCAL_ASSIGNER, assignedContainers);

    // Release any unassigned containers given by the RM
    releaseUnassignedContainers(containers);

    return assignedContainers;
  }

  private synchronized Map<CookieContainerRequest, Container>
      tryAssignReUsedContainers(Iterable<Container> containers) {

    Map<CookieContainerRequest, Container> assignedContainers =
      new HashMap<CookieContainerRequest, Container>();

    // Honor locality and match as many as possible
    assignReUsedContainersWithLocation(containers,
      NODE_LOCAL_ASSIGNER, assignedContainers, true);
    assignReUsedContainersWithLocation(containers,
      RACK_LOCAL_ASSIGNER, assignedContainers, true);
    assignReUsedContainersWithLocation(containers,
      NON_LOCAL_ASSIGNER, assignedContainers, true);

    return assignedContainers;
  }
  
  @VisibleForTesting
  long getHeldContainerExpireTime(long startTime) {
    long expireTime = (startTime + idleContainerTimeoutMin);
    if (idleContainerTimeoutMin != -1 && idleContainerTimeoutMin < idleContainerTimeoutMax) {
      long expireTimeMax = startTime + idleContainerTimeoutMax;
      expireTime = random.nextLong(expireTime, expireTimeMax);
    }
    
    return expireTime;
  }

  /**
   * Try to assign a re-used container
   * @param heldContainer Container to be used to assign to tasks
   * @return Assigned container map
   */

  private synchronized Map<CookieContainerRequest, Container>
      assignDelayedContainer(HeldContainer heldContainer) {

    DAGAppMasterState state = appContext.getAMState();
    boolean isNew = heldContainer.isNew();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to assign a delayed container"
        + ", containerId=" + heldContainer.getContainer().getId()
        + ", nextScheduleTime=" + heldContainer.getNextScheduleTime()
        + ", containerExpiryTime=" + heldContainer.getContainerExpiryTime()
        + ", AMState=" + state
        + ", matchLevel=" + heldContainer.getLocalityMatchLevel()
        + ", taskRequestsCount=" + taskRequests.size()
        + ", heldContainers=" + heldContainers.size()
        + ", delayedContainers=" + delayedContainerManager.delayedContainers.size()
        + ", isNew=" + isNew);
    }

    if (state.equals(DAGAppMasterState.IDLE) || taskRequests.isEmpty()) {
      // reset locality level on held container
      // if sessionDelay defined, push back into delayed queue if not already
      // done so

      // Compute min held containers.
      if (appContext.isSession() && sessionNumMinHeldContainers > 0 &&
          sessionMinHeldContainers.isEmpty()) {
        // session mode and need to hold onto containers and not done so already
        determineMinHeldContainers();
      }
      
      heldContainer.resetLocalityMatchLevel();
      long currentTime = System.currentTimeMillis();
      boolean releaseContainer = false;

      if (isNew || (heldContainer.getContainerExpiryTime() <= currentTime
          && idleContainerTimeoutMin != -1)) {
        // container idle timeout has expired or is a new unused container. 
        // new container is possibly a spurious race condition allocation.
        if (!isNew && appContext.isSession() && 
            sessionMinHeldContainers.contains(heldContainer.getContainer().getId())) {
          // Not a potentially spurious new container.
          // In session mode and container in set of chosen min held containers
          // increase the idle container expire time to maintain sanity with 
          // the rest of the code
          heldContainer.setContainerExpiryTime(getHeldContainerExpireTime(currentTime));
        } else {
          releaseContainer = true;
        }
      }
      
      if (releaseContainer) {
        LOG.info("No taskRequests. Container's idle timeout delay expired or is new. " +
            "Releasing container"
            + ", containerId=" + heldContainer.container.getId()
            + ", containerExpiryTime="
            + heldContainer.getContainerExpiryTime()
            + ", idleTimeout=" + idleContainerTimeoutMin
            + ", taskRequestsCount=" + taskRequests.size()
            + ", heldContainers=" + heldContainers.size()
            + ", delayedContainers=" + delayedContainerManager.delayedContainers.size()
            + ", isNew=" + isNew);
          releaseUnassignedContainers(
              Lists.newArrayList(heldContainer.container));        
      } else {
        // no outstanding work and container idle timeout not expired
        if (LOG.isDebugEnabled()) {
          LOG.debug("Holding onto idle container with no work. CId: "
              + heldContainer.getContainer().getId() + " with expiry: "
              + heldContainer.getContainerExpiryTime() + " currentTime: "
              + currentTime + " next look: "
              + (currentTime + localitySchedulingDelay));
        }
        // put back and wait for new requests until expiry
        heldContainer.resetLocalityMatchLevel();
        delayedContainerManager.addDelayedContainer(
            heldContainer.getContainer(), currentTime
                + localitySchedulingDelay);        
      }
   } else if (state.equals(DAGAppMasterState.RUNNING)) {
      // clear min held containers since we need to allocate to tasks
      sessionMinHeldContainers.clear();
      HeldContainer.LocalityMatchLevel localityMatchLevel =
        heldContainer.getLocalityMatchLevel();
      Map<CookieContainerRequest, Container> assignedContainers =
        new HashMap<CookieContainerRequest, Container>();

      Container containerToAssign = heldContainer.container;

      heldContainer.incrementAssignmentAttempts();
      // Each time a container is seen, we try node, rack and non-local in that
      // order depending on matching level allowed

      // if match level is NEW or NODE, match only at node-local
      // always try node local matches for other levels
      if (isNew
          || localityMatchLevel.equals(HeldContainer.LocalityMatchLevel.NEW)
          || localityMatchLevel.equals(HeldContainer.LocalityMatchLevel.NODE)
          || localityMatchLevel.equals(HeldContainer.LocalityMatchLevel.RACK)
          || localityMatchLevel.equals(HeldContainer.LocalityMatchLevel.NON_LOCAL)) {
        assignReUsedContainerWithLocation(containerToAssign,
            NODE_LOCAL_ASSIGNER, assignedContainers, true);
        if (LOG.isDebugEnabled() && assignedContainers.isEmpty()) {
          LOG.info("Failed to assign tasks to delayed container using node"
            + ", containerId=" + heldContainer.getContainer().getId());
        }
      }

      // if re-use allowed at rack
      // match against rack if match level is RACK or NON-LOCAL
      // if scheduling delay is 0, match at RACK allowed without a sleep
      if (assignedContainers.isEmpty()) {
        if ((reuseRackLocal || isNew) && (localitySchedulingDelay == 0 ||
          (localityMatchLevel.equals(HeldContainer.LocalityMatchLevel.RACK)
            || localityMatchLevel.equals(
              HeldContainer.LocalityMatchLevel.NON_LOCAL)))) {
          assignReUsedContainerWithLocation(containerToAssign,
              RACK_LOCAL_ASSIGNER, assignedContainers, false);
          if (LOG.isDebugEnabled() && assignedContainers.isEmpty()) {
            LOG.info("Failed to assign tasks to delayed container using rack"
              + ", containerId=" + heldContainer.getContainer().getId());
          }
        }
      }

      // if re-use allowed at non-local
      // match against rack if match level is NON-LOCAL
      // if scheduling delay is 0, match at NON-LOCAL allowed without a sleep
      if (assignedContainers.isEmpty()) {
        if ((reuseNonLocal || isNew) && (localitySchedulingDelay == 0
            || localityMatchLevel.equals(
                HeldContainer.LocalityMatchLevel.NON_LOCAL))) {
         assignReUsedContainerWithLocation(containerToAssign,
              NON_LOCAL_ASSIGNER, assignedContainers, false);
          if (LOG.isDebugEnabled() && assignedContainers.isEmpty()) {
            LOG.info("Failed to assign tasks to delayed container using non-local"
                + ", containerId=" + heldContainer.getContainer().getId());
          }
        }
      }

      if (assignedContainers.isEmpty()) {

        long currentTime = System.currentTimeMillis();

        // Release container if final expiry time is reached
        // Dont release a new container. The RM may not give us new ones
        // The assumption is that the expire time is larger than the sum of all
        // locality delays. So if we hit the expire time then we have already 
        // tried to assign at all locality levels.
        // We run the risk of not being able to retain min held containers but 
        // if we are not being able to assign containers to pending tasks then 
        // we cannot avoid releasing containers. Or else we may not be able to 
        // get new containers from YARN to match the pending request
        if (!isNew && heldContainer.getContainerExpiryTime() <= currentTime
          && idleContainerTimeoutMin != -1) {
          LOG.info("Container's idle timeout expired. Releasing container"
            + ", containerId=" + heldContainer.container.getId()
            + ", containerExpiryTime="
            + heldContainer.getContainerExpiryTime()
            + ", idleTimeoutMin=" + idleContainerTimeoutMin);
          releaseUnassignedContainers(
            Lists.newArrayList(heldContainer.container));
        } else {

          // Let's decide if this container has hit the end of the road

          // EOL true if container's match level is NON-LOCAL
          boolean hitFinalMatchLevel = localityMatchLevel.equals(
            HeldContainer.LocalityMatchLevel.NON_LOCAL);
          if (!hitFinalMatchLevel) {
            // EOL also true if locality delay is 0
            // or rack-local or non-local is disabled
            heldContainer.incrementLocalityMatchLevel();
            if (localitySchedulingDelay == 0 ||
                (!reuseRackLocal
                  || (!reuseNonLocal &&
                    heldContainer.getLocalityMatchLevel().equals(
                        HeldContainer.LocalityMatchLevel.NON_LOCAL)))) {
              hitFinalMatchLevel = true;
            }
            // the above if-stmt does not apply to new containers since they will
            // be matched at all locality levels. So there finalMatchLevel cannot
            // be short-circuited
            if (localitySchedulingDelay > 0 && isNew) {
              hitFinalMatchLevel = false;
            }
          }
          
          if (hitFinalMatchLevel) {
            boolean safeToRelease = true;
            Priority topPendingPriority = amRmClient.getTopPriority();
            Priority containerPriority = heldContainer.container.getPriority();
            if (isNew && topPendingPriority != null &&
                containerPriority.compareTo(topPendingPriority) < 0) {
              // this container is of lower priority and given to us by the RM for
              // a task that will be matched after the current top priority. Keep 
              // this container for those pending tasks since the RM is not going
              // to give this container to us again
              safeToRelease = false;
            }
            
            // Are there any pending requests at any priority?
            // release if there are tasks or this is not a session
            if (safeToRelease && 
                (!taskRequests.isEmpty() || !appContext.isSession())) {
              LOG.info("Releasing held container as either there are pending but "
                + " unmatched requests or this is not a session"
                + ", containerId=" + heldContainer.container.getId()
                + ", pendingTasks=" + taskRequests.size()
                + ", isSession=" + appContext.isSession()
                + ". isNew=" + isNew);
              releaseUnassignedContainers(
                Lists.newArrayList(heldContainer.container));
            } else {
              // if no tasks, treat this like an idle session
              heldContainer.resetLocalityMatchLevel();
              delayedContainerManager.addDelayedContainer(
                heldContainer.getContainer(),
                currentTime + localitySchedulingDelay);
            }
          } else {
            // Schedule delay container to match at a later try
            delayedContainerManager.addDelayedContainer(
                heldContainer.getContainer(),
                currentTime + localitySchedulingDelay);
          }
        }
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("Delayed container assignment successful"
            + ", containerId=" + heldContainer.getContainer().getId());
      }

      return assignedContainers;
    } else {
      // ignore all other cases?
      LOG.warn("Received a request to assign re-used containers when AM was "
        + " in state: " + state + ". Ignoring request and releasing container"
        + ": " + heldContainer.getContainer().getId());
      releaseUnassignedContainers(Lists.newArrayList(heldContainer.container));
    }

    return null;
  }

  @Override
  public synchronized void resetMatchLocalityForAllHeldContainers() {
    for (HeldContainer heldContainer : heldContainers.values()) {
      heldContainer.resetLocalityMatchLevel();
    }
    synchronized(delayedContainerManager) {
      delayedContainerManager.notify();
    }
  }

  @Override
  public void onShutdownRequest() {
    if (isStopped.get()) {
      return;
    }
    // upcall to app must be outside locks
    appClientDelegate.appShutdownRequested();
  }

  @Override
  public void onNodesUpdated(List<NodeReport> updatedNodes) {
    if (isStopped.get()) {
      return;
    }
    // ignore bad nodes for now
    // upcall to app must be outside locks
    appClientDelegate.nodesUpdated(updatedNodes);
  }

  @Override
  public float getProgress() {
    if (isStopped.get()) {
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

    numHeartbeats++;
    preemptIfNeeded();

    return appClientDelegate.getProgress();
  }

  @Override
  public void onError(Throwable t) {
    if (isStopped.get()) {
      return;
    }
    appClientDelegate.onError(t);
  }

  @Override
  public Resource getTotalResources() {
    return totalResources;
  }

  @Override
  public synchronized void blacklistNode(NodeId nodeId) {
    LOG.info("Blacklisting node: " + nodeId);
    amRmClient.addNodeToBlacklist(nodeId);
    blacklistedNodes.add(nodeId);
  }
  
  @Override
  public synchronized void unblacklistNode(NodeId nodeId) {
    if (blacklistedNodes.remove(nodeId)) {
      LOG.info("UnBlacklisting node: " + nodeId);
      amRmClient.removeNodeFromBlacklist(nodeId);
    }
  }
  
  @Override
  public synchronized void allocateTask(
      Object task,
      Resource capability,
      String[] hosts,
      String[] racks,
      Priority priority,
      Object containerSignature,
      Object clientCookie) {

    // XXX Have ContainerContext implement an interface defined by TaskScheduler.
    // TODO check for nulls etc
    // TODO extra memory allocation
    CRCookie cookie = new CRCookie(task, clientCookie, containerSignature);
    CookieContainerRequest request = new CookieContainerRequest(
      capability, hosts, racks, priority, cookie);

    addRequestAndTrigger(task, request, hosts, racks);
  }
  
  @Override
  public synchronized void allocateTask(
      Object task,
      Resource capability,
      ContainerId containerId,
      Priority priority,
      Object containerSignature,
      Object clientCookie) {

    HeldContainer heldContainer = heldContainers.get(containerId);
    String[] hosts = null;
    String[] racks = null;
    if (heldContainer != null) {
      Container container = heldContainer.getContainer();
      if (canFit(capability, container.getResource())) {
        // just specify node and use YARN's soft locality constraint for the rest
        hosts = new String[1];
        hosts[0] = container.getNodeId().getHost();
        priorityHasAffinity.add(priority);
      } else {
        LOG.warn("Matching requested to container: " + containerId +
            " but requested capability: " + capability + 
            " does not fit in container resource: "  + container.getResource());
      }
    } else {
      LOG.warn("Matching requested to unknown container: " + containerId);
    }
    
    CRCookie cookie = new CRCookie(task, clientCookie, containerSignature);
    CookieContainerRequest request = new CookieContainerRequest(
      capability, containerId, hosts, racks, priority, cookie);

    addRequestAndTrigger(task, request, hosts, racks);
  }
  
  private void addRequestAndTrigger(Object task, CookieContainerRequest request,
      String[] hosts, String[] racks) {
    addTaskRequest(task, request);
    // See if any of the delayedContainers can be used for this task.
    delayedContainerManager.triggerScheduling(true);
    LOG.info("Allocation request for task: " + task +
      " with request: " + request + 
      " host: " + ((hosts!=null&&hosts.length>0)?hosts[0]:"null") +
      " rack: " + ((racks!=null&&racks.length>0)?racks[0]:"null"));
  }

  /**
   * @param task
   *          the task to de-allocate.
   * @param taskSucceeded
   *          specify whether the task succeeded or failed.
   * @return true if a container is assigned to this task.
   */
  @Override
  public boolean deallocateTask(Object task, boolean taskSucceeded) {
    Map<CookieContainerRequest, Container> assignedContainers = null;

    synchronized (this) {
      CookieContainerRequest request = removeTaskRequest(task);
      if (request != null) {
        // task not allocated yet
        LOG.info("Deallocating task: " + task + " before allocation");
        return false;
      }

      // task request not present. Look in allocations
      Container container = doBookKeepingForTaskDeallocate(task);
      if (container == null) {
        // task neither requested nor allocated.
        LOG.info("Ignoring removal of unknown task: " + task);
        return false;
      } else {
        LOG.info("Deallocated task: " + task + " from container: "
            + container.getId());

        if (!taskSucceeded || !shouldReuseContainers) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Releasing container, containerId=" + container.getId()
                + ", taskSucceeded=" + taskSucceeded
                + ", reuseContainersFlag=" + shouldReuseContainers);
          }
          releaseContainer(container.getId());
        } else {
          // Don't attempt to delay containers if delay is 0.
          HeldContainer heldContainer = heldContainers.get(container.getId());
          if (heldContainer != null) {
            heldContainer.resetLocalityMatchLevel();
            long currentTime = System.currentTimeMillis();
            if (idleContainerTimeoutMin > 0) {
              heldContainer.setContainerExpiryTime(getHeldContainerExpireTime(currentTime));
            }
            assignedContainers = assignDelayedContainer(heldContainer);
          } else {
            LOG.info("Skipping container after task deallocate as container is"
                + " no longer running, containerId=" + container.getId());
          }
        }
      }
    }

    // up call outside of the lock.
    if (assignedContainers != null && assignedContainers.size() == 1) {
      informAppAboutAssignments(assignedContainers);
    }
    return true;
  }
  
  @Override
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

  boolean canFit(Resource arg0, Resource arg1) {
    int mem0 = arg0.getMemory();
    int mem1 = arg1.getMemory();
    int cpu0 = arg0.getVirtualCores();
    int cpu1 = arg1.getVirtualCores();
    
    if(mem0 <= mem1 && cpu0 <= cpu1) { 
      return true;
    }
    return false; 
  }

  static int scaleDownByPreemptionPercentage(int original, int percent) {
    return (int) Math.ceil((original * percent)/100.f);
  }
  
  void preemptIfNeeded() {
    if (preemptionPercentage == 0) {
      // turned off
      return;
    }
    ContainerId[] preemptedContainers = null;
    int numPendingRequestsToService = 0;
    synchronized (this) {
      Resource freeResources = amRmClient.getAvailableResources();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Allocated resource memory: " + allocatedResources.getMemory() +
          " cpu:" + allocatedResources.getVirtualCores() + 
          " delayedContainers: " + delayedContainerManager.delayedContainers.size() +
          " heartbeats: " + numHeartbeats + " lastPreemptionHeartbeat: " + heartbeatAtLastPreemption);
      }
      assert freeResources.getMemory() >= 0;
  
      CookieContainerRequest highestPriRequest = null;
      int numHighestPriRequests = 0;
      for(CookieContainerRequest request : taskRequests.values()) {
        if(highestPriRequest == null) {
          highestPriRequest = request;
          numHighestPriRequests = 1;
        } else if(isHigherPriority(request.getPriority(),
                                     highestPriRequest.getPriority())){
          highestPriRequest = request;
          numHighestPriRequests = 1;
        } else if (request.getPriority().equals(highestPriRequest.getPriority())) {
          numHighestPriRequests++;
        }
      }
      
      if (highestPriRequest == null) {
        // nothing pending
        return;
      }
      
      if(fitsIn(highestPriRequest.getCapability(), freeResources)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Highest pri request: " + highestPriRequest + " fits in available resources "
              + freeResources);
        }
        return;
      }
      // highest priority request will not fit in existing free resources
      // free up some more
      // TODO this is subject to error wrt RM resource normalization
      
      numPendingRequestsToService = scaleDownByPreemptionPercentage(numHighestPriRequests,
          preemptionPercentage);

      if (numPendingRequestsToService < 1) {
        return;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to service " + numPendingRequestsToService + " out of total "
            + numHighestPriRequests + " pending requests at pri: "
            + highestPriRequest.getPriority());
      }
      
      for (int i=0; i<numPendingRequestsToService; ++i) {
        // This request must have been considered for matching with all existing 
        // containers when request was made.
        Container lowestPriNewContainer = null;
        // could not find anything to preempt. Check if we can release unused 
        // containers
        for (HeldContainer heldContainer : delayedContainerManager.delayedContainers) {
          if (!heldContainer.isNew()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Reused container exists. Wait for assignment loop to release it. "
                  + heldContainer.getContainer().getId());
            }
            return;
          }
          if (heldContainer.geNumAssignmentAttempts() < 3) {
            // we havent tried to assign this container at node/rack/ANY
            if (LOG.isDebugEnabled()) {
              LOG.debug("Brand new container. Wait for assignment loop to match it. "
                  + heldContainer.getContainer().getId());
            }
            return;
          }
          Container container = heldContainer.getContainer();
          if (lowestPriNewContainer == null ||
              isHigherPriority(lowestPriNewContainer.getPriority(), container.getPriority())){
            // there is a lower priority new container
            lowestPriNewContainer = container;
          }
        }
        
        if (lowestPriNewContainer != null) {
          LOG.info("Preempting new container: " + lowestPriNewContainer.getId() +
              " with priority: " + lowestPriNewContainer.getPriority() + 
              " to free resource for request: " + highestPriRequest +
              " . Current free resources: " + freeResources);
          numPendingRequestsToService--;
          releaseUnassignedContainers(Collections.singletonList(lowestPriNewContainer));
          // We are returning an unused resource back the RM. The RM thinks it 
          // has serviced our initial request and will not re-allocate this back
          // to us anymore. So we need to ask for this again. If there is no
          // outstanding request at that priority then its fine to not ask again.
          // See TEZ-915 for more details
          for (Map.Entry<Object, CookieContainerRequest> entry : taskRequests.entrySet()) {
            Object task = entry.getKey();
            CookieContainerRequest request = entry.getValue();
            if (request.getPriority().equals(lowestPriNewContainer.getPriority())) {
              LOG.info("Resending request for task again: " + task);
              deallocateTask(task, true);
              allocateTask(task, request.getCapability(), 
                  (request.getNodes() == null ? null : 
                    request.getNodes().toArray(new String[request.getNodes().size()])), 
                    (request.getRacks() == null ? null : 
                      request.getRacks().toArray(new String[request.getRacks().size()])), 
                    request.getPriority(), 
                    request.getCookie().getContainerSignature(),
                    request.getCookie().getAppCookie());
              break;
            }
          }
          // come back and free more new containers if needed
          continue;
        }
      }
      
      if (numPendingRequestsToService < 1) {
        return;
      }

      // there are no reused or new containers to release. try to preempt running containers
      // this assert will be a no-op in production but can help identify 
      // invalid assumptions during testing
      assert delayedContainerManager.delayedContainers.isEmpty();
      
      if ((numHeartbeats - heartbeatAtLastPreemption) <= numHeartbeatsBetweenPreemptions) {
        return;
      }
        
      Priority preemptedTaskPriority = null;
      int numEntriesAtPreemptedPriority = 0;
      for(Map.Entry<Object, Container> entry : taskAllocations.entrySet()) {
        HeldContainer heldContainer = heldContainers.get(entry.getValue().getId());
        CookieContainerRequest lastTaskInfo = heldContainer.getLastTaskInfo();
        Priority taskPriority = lastTaskInfo.getPriority();
        Object signature = lastTaskInfo.getCookie().getContainerSignature();
        if(!isHigherPriority(highestPriRequest.getPriority(), taskPriority)) {
          // higher or same priority
          continue;
        }
        if (containerSignatureMatcher.isExactMatch(
            highestPriRequest.getCookie().getContainerSignature(),
            signature)) {
          // exact match with different priorities
          continue;
        }
        if (preemptedTaskPriority == null ||
            !isHigherPriority(taskPriority, preemptedTaskPriority)) {
          // keep the lower priority
          if (taskPriority.equals(preemptedTaskPriority)) {
            numEntriesAtPreemptedPriority++;
          } else {
            // this is at a lower priority than existing
            numEntriesAtPreemptedPriority = 1;
          }
          preemptedTaskPriority = taskPriority;
        }
      }
      if(preemptedTaskPriority != null) {
        int newNumPendingRequestsToService = scaleDownByPreemptionPercentage(Math.min(
            numEntriesAtPreemptedPriority, numHighestPriRequests), preemptionPercentage);
        numPendingRequestsToService = Math.min(newNumPendingRequestsToService,
            numPendingRequestsToService);
        if (numPendingRequestsToService < 1) {
          return;
        }
        LOG.info("Trying to service " + numPendingRequestsToService + " out of total "
            + numHighestPriRequests + " pending requests at pri: "
            + highestPriRequest.getPriority() + " by preempting from "
            + numEntriesAtPreemptedPriority + " running tasks at priority: " + preemptedTaskPriority);
        // found something to preempt. get others of the same priority
        preemptedContainers = new ContainerId[numPendingRequestsToService];
        int currIndex = 0;
        for (Map.Entry<Object, Container> entry : taskAllocations.entrySet()) {
          HeldContainer heldContainer = heldContainers.get(entry.getValue().getId());
          CookieContainerRequest lastTaskInfo = heldContainer.getLastTaskInfo();
          Priority taskPriority = lastTaskInfo.getPriority();
          Container container = entry.getValue();
          if (preemptedTaskPriority.equals(taskPriority)) {
            // taskAllocations map will iterate from oldest to newest assigned containers
            // keep the N newest containersIds with the matching priority
            preemptedContainers[currIndex++ % numPendingRequestsToService] = container.getId();
          }
        }
        // app client will be notified when after container is killed
        // and we get its completed container status
      }
    }
    
    // upcall outside locks
    if (preemptedContainers != null) {
      heartbeatAtLastPreemption = numHeartbeats;
      for(int i=0; i<numPendingRequestsToService; ++i) {
        ContainerId cId = preemptedContainers[i];
        if (cId != null) {
          LOG.info("Preempting container: " + cId + " currently allocated to a task.");
          appClientDelegate.preemptContainer(cId);
        }
      }
    }
  }

  private boolean fitsIn(Resource toFit, Resource resource) {
    // YARN-893 prevents using correct library code
    //return Resources.fitsIn(toFit, resource);
    return resource.getMemory() >= toFit.getMemory();
  }

  private CookieContainerRequest getMatchingRequestWithPriority(
      Container container,
      String location) {
    Priority priority = container.getPriority();
    Resource capability = container.getResource();
    List<? extends Collection<CookieContainerRequest>> requestsList =
        amRmClient.getMatchingRequests(priority, location, capability);

    if (!requestsList.isEmpty()) {
      // pick first one
      for (Collection<CookieContainerRequest> requests : requestsList) {
        for (CookieContainerRequest cookieContainerRequest : requests) {
          if (canAssignTaskToContainer(cookieContainerRequest, container)) {
            return cookieContainerRequest;
          }
        }
      }
    }

    return null;
  }

  private CookieContainerRequest getMatchingRequestWithoutPriority(
      Container container,
      String location,
      boolean considerContainerAffinity) {
    Resource capability = container.getResource();
    List<? extends Collection<CookieContainerRequest>> pRequestsList =
      amRmClient.getMatchingRequestsForTopPriority(location, capability);
    if (considerContainerAffinity && 
        !priorityHasAffinity.contains(amRmClient.getTopPriority())) {
      considerContainerAffinity = false;
    }
    if (pRequestsList == null || pRequestsList.isEmpty()) {
      return null;
    }
    CookieContainerRequest firstMatch = null;
    for (Collection<CookieContainerRequest> requests : pRequestsList) {
      for (CookieContainerRequest cookieContainerRequest : requests) {
        if (firstMatch == null || // we dont have a match. So look for one 
            // we have a match but are looking for a better container level match.
            // skip the expensive canAssignTaskToContainer() if the request is 
            // not affinitized to the container
            container.getId().equals(cookieContainerRequest.getAffinitizedContainer())
            ) {
          if (canAssignTaskToContainer(cookieContainerRequest, container)) {
            // request matched to container
            if (!considerContainerAffinity) {
              return cookieContainerRequest;
            }
            ContainerId affCId = cookieContainerRequest.getAffinitizedContainer();
            boolean canMatchTaskWithAffinity = true;
            if (affCId == null || 
                !heldContainers.containsKey(affCId) ||
                inUseContainers.contains(affCId)) {
              // affinity not specified
              // affinitized container is no longer held
              // affinitized container is in use
              canMatchTaskWithAffinity = false;
            }
            if (canMatchTaskWithAffinity) {
              if (container.getId().equals(
                  cookieContainerRequest.getAffinitizedContainer())) {
                // container level match
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Matching with affinity for request: "
                      + cookieContainerRequest + " container: " + affCId);
                }
                return cookieContainerRequest;
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug("Skipping request for container " + container.getId()
                    + " due to affinity. Request: " + cookieContainerRequest
                    + " affContainer: " + affCId);
              }
            } else {
              firstMatch = cookieContainerRequest;
            }
          }
        }
      }
    }
    
    return firstMatch;
  }

  private boolean canAssignTaskToContainer(
      CookieContainerRequest cookieContainerRequest, Container container) {
    HeldContainer heldContainer = heldContainers.get(container.getId());
    if (heldContainer == null || heldContainer.isNew()) { // New container.
      return true;
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to match task to a held container, "
            + " containerId=" + heldContainer.container.getId());
      }
      if (containerSignatureMatcher.isSuperSet(heldContainer
          .getFirstContainerSignature(), cookieContainerRequest.getCookie()
          .getContainerSignature())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Matched delayed container to task"
            + " containerId=" + heldContainer.container.getId());
        }
        return true;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Failed to match delayed container to task"
        + " containerId=" + heldContainer.container.getId());
    }
    return false;
  }

  private Object getTask(CookieContainerRequest request) {
    return request.getCookie().getTask();
  }

  private void releaseContainer(ContainerId containerId) {
    Object assignedTask = containerAssignments.remove(containerId);
    if (assignedTask != null) {
      // A task was assigned to this container at some point. Inform the app.
      appClientDelegate.containerBeingReleased(containerId);
    }
    HeldContainer delayedContainer = heldContainers.remove(containerId);
    if (delayedContainer != null) {
      Resources.subtractFrom(allocatedResources,
          delayedContainer.getContainer().getResource());
    }
    if (delayedContainer != null || !shouldReuseContainers) {
      amRmClient.releaseAssignedContainer(containerId);
    }
    if (assignedTask != null) {
      // A task was assigned at some point. Add to release list since we are
      // releasing the container.
      releasedContainers.put(containerId, assignedTask);
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
    inUseContainers.add(container.getId());
    containerAssignments.put(container.getId(), task);
    HeldContainer heldContainer = heldContainers.get(container.getId()); 
    if (!shouldReuseContainers && heldContainer == null) {
      heldContainers.put(container.getId(), new HeldContainer(container,
        -1, -1, assigned));
      Resources.addTo(allocatedResources, container.getResource());
    } else {
      if (heldContainer.isNew()) {
        // check for existence before adding since the first container potentially
        // has the broadest signature as subsequent uses dont expand any dimension.
        // This will need to be enhanced to track other signatures too when we
        // think about preferring within vertex matching etc.
        heldContainers.put(container.getId(),
            new HeldContainer(container, heldContainer.getNextScheduleTime(),
                heldContainer.getContainerExpiryTime(), assigned));
      }
      heldContainer.setLastTaskInfo(assigned);
    }
  }
  
  private void pushNewContainerToDelayed(List<Container> containers){
    long expireTime = -1;
    if (idleContainerTimeoutMin > 0) {
      long currentTime = System.currentTimeMillis();
      expireTime = currentTime + idleContainerTimeoutMin;
    }

    synchronized (delayedContainerManager) {
      for (Container container : containers) {
        if (heldContainers.put(container.getId(), new HeldContainer(container,
            -1, expireTime, null)) != null) {
          throw new TezUncheckedException("New container " + container.getId()
              + " is already held.");
        }
        long nextScheduleTime = delayedContainerManager.maxScheduleTimeSeen;
        if (delayedContainerManager.maxScheduleTimeSeen == -1) {
          nextScheduleTime = System.currentTimeMillis();
        }
        Resources.addTo(allocatedResources, container.getResource());
        delayedContainerManager.addDelayedContainer(container,
          nextScheduleTime + 1);
      }
    }
    delayedContainerManager.triggerScheduling(false);      
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
    CookieContainerRequest oldRequest = taskRequests.put(task, request);
    if (oldRequest != null) {
      // remove all references of the request from AMRMClient
      amRmClient.removeContainerRequest(oldRequest);
    }
    amRmClient.addContainerRequest(request);
  }

  private Container doBookKeepingForTaskDeallocate(Object task) {
    Container container = taskAllocations.remove(task);
    if (container == null) {
      return null;
    }
    inUseContainers.remove(container.getId());
    return container;
  }

  private Object unAssignContainer(ContainerId containerId,
                                    boolean releaseIfFound) {
    // Not removing. containerAssignments tracks the last task run on a
    // container.
    Object task = containerAssignments.get(containerId);
    if(task == null) {
      return null;
    }
    Container container = taskAllocations.remove(task);
    assert container != null;
    inUseContainers.remove(containerId);
    if(releaseIfFound) {
      releaseContainer(containerId);
    }
    return task;
  }

  private boolean isHigherPriority(Priority lhs, Priority rhs) {
    return lhs.getPriority() < rhs.getPriority();
  }

  private synchronized void assignNewContainersWithLocation(
      Iterable<Container> containers,
      ContainerAssigner assigner,
      Map<CookieContainerRequest, Container> assignedContainers) {

    Iterator<Container> containerIterator = containers.iterator();
    while (containerIterator.hasNext()) {
      Container container = containerIterator.next();
      CookieContainerRequest assigned =
        assigner.assignNewContainer(container);
      if (assigned != null) {
        assignedContainers.put(assigned, container);
        containerIterator.remove();
      }
    }
  }

  private synchronized void assignReUsedContainersWithLocation(
      Iterable<Container> containers,
      ContainerAssigner assigner,
      Map<CookieContainerRequest, Container> assignedContainers,
      boolean honorLocality) {

    Iterator<Container> containerIterator = containers.iterator();
    while (containerIterator.hasNext()) {
      Container container = containerIterator.next();
      if (assignReUsedContainerWithLocation(container, assigner,
          assignedContainers, honorLocality)) {
        containerIterator.remove();
      }
    }
  }

  private synchronized boolean assignReUsedContainerWithLocation(
    Container container,
    ContainerAssigner assigner,
    Map<CookieContainerRequest, Container> assignedContainers,
    boolean honorLocality) {

    Priority containerPriority = container.getPriority();
    Priority topPendingTaskPriority = amRmClient.getTopPriority();
    if (topPendingTaskPriority == null) {
      // nothing left to assign
      return false;
    }
    
    if (topPendingTaskPriority.compareTo(containerPriority) > 0 && 
        heldContainers.get(container.getId()).isNew()) {
      // if the next task to assign is higher priority than the container then 
      // dont assign this container to that task.
      // if task and container are equal priority - then its first use or reuse
      // within the same priority - safe to use
      // if task is lower priority than container then if we use a container that
      // is no longer needed by higher priority tasks All those higher pri tasks 
      // has been assigned resources - safe to use (first use or reuse)
      // if task is higher priority than container then we may end up using a 
      // container that was assigned by the RM for a lower priority pending task 
      // that will be assigned after this higher priority task is assigned. If we
      // use that task's container now then we may not be able to match this 
      // container to that task later on. However the RM has already assigned us 
      // all containers and is not going to give us new containers. We will get 
      // stuck for resources.
      // the above applies for new containers. If a container has already been 
      // re-used then this is not relevant
      return false;
    }
    
    CookieContainerRequest assigned =
      assigner.assignReUsedContainer(container, honorLocality);
    if (assigned != null) {
      assignedContainers.put(assigned, container);
      return true;
    }
    return false;
  }

  private void releaseUnassignedContainers(Iterable<Container> containers) {
    for (Container container : containers) {
      LOG.info("Releasing unused container: "
          + container.getId());
      releaseContainer(container.getId());
    }
  }

  private void informAppAboutAssignment(CookieContainerRequest assigned,
      Container container) {
    appClientDelegate.taskAllocated(getTask(assigned),
        assigned.getCookie().getAppCookie(), container);
  }

  private void informAppAboutAssignments(
      Map<CookieContainerRequest, Container> assignedContainers) {
    if (assignedContainers == null || assignedContainers.isEmpty()) {
      return;
    }
    for (Entry<CookieContainerRequest, Container> entry : assignedContainers
        .entrySet()) {
      Container container = entry.getValue();
      // check for blacklisted nodes. There may be race conditions between
      // setting blacklist and receiving allocations
      if (blacklistedNodes.contains(container.getNodeId())) {
        CookieContainerRequest request = entry.getKey();
        Object task = getTask(request);
        LOG.info("Container: " + container.getId() + 
            " allocated on blacklisted node: " + container.getNodeId() + 
            " for task: " + task);
        Object deAllocTask = deallocateContainer(container.getId());
        assert deAllocTask.equals(task);
        // its ok to submit the same request again because the RM will not give us
        // the bad/unhealthy nodes again. The nodes may become healthy/unblacklisted
        // and so its better to give the RM the full information.
        allocateTask(task, request.getCapability(), 
            (request.getNodes() == null ? null : 
            request.getNodes().toArray(new String[request.getNodes().size()])), 
            (request.getRacks() == null ? null : 
              request.getRacks().toArray(new String[request.getRacks().size()])), 
            request.getPriority(), 
            request.getCookie().getContainerSignature(), 
            request.getCookie().getAppCookie());
      } else {
        informAppAboutAssignment(entry.getKey(), container);
      }
    }
  }

  private abstract class ContainerAssigner {

    protected final String locality;

    protected ContainerAssigner(String locality) {
      this.locality = locality;
    }

    public abstract CookieContainerRequest assignNewContainer(
        Container container);

    public abstract CookieContainerRequest assignReUsedContainer(
      Container container, boolean honorLocality);

    public void doBookKeepingForAssignedContainer(
        CookieContainerRequest assigned, Container container,
        String matchedLocation, boolean honorLocalityFlags) {
      if (assigned == null) {
        return;
      }
      Object task = getTask(assigned);
      assert task != null;

      LOG.info("Assigning container to task"
        + ", container=" + container
        + ", task=" + task
        + ", containerHost=" + container.getNodeId().getHost()
        + ", localityMatchType=" + locality
        + ", matchedLocation=" + matchedLocation
        + ", honorLocalityFlags=" + honorLocalityFlags
        + ", reusedContainer="
        + containerAssignments.containsKey(container.getId())
        + ", delayedContainers=" + delayedContainerManager.delayedContainers.size()
        + ", containerResourceMemory=" + container.getResource().getMemory()
        + ", containerResourceVCores="
        + container.getResource().getVirtualCores());

      assignContainer(task, container, assigned);
    }
  }
  
  private class NodeLocalContainerAssigner extends ContainerAssigner {

    NodeLocalContainerAssigner() {
      super("NodeLocal");
    }

    @Override
    public CookieContainerRequest assignNewContainer(Container container) {
      String location = container.getNodeId().getHost();
      CookieContainerRequest assigned = getMatchingRequestWithPriority(
          container, location);
      doBookKeepingForAssignedContainer(assigned, container, location, false);
      return assigned;
    }

    @Override
    public CookieContainerRequest assignReUsedContainer(Container container,
        boolean honorLocality) {
      String location = container.getNodeId().getHost();
      CookieContainerRequest assigned = getMatchingRequestWithoutPriority(
        container, location, true);
      doBookKeepingForAssignedContainer(assigned, container, location, true);
      return assigned;

    }
  }

  private class RackLocalContainerAssigner extends ContainerAssigner {

    RackLocalContainerAssigner() {
      super("RackLocal");
    }

    @Override
    public CookieContainerRequest assignNewContainer(Container container) {
      String location = RackResolver.resolve(container.getNodeId().getHost())
          .getNetworkLocation();
      CookieContainerRequest assigned = getMatchingRequestWithPriority(container,
          location);
      doBookKeepingForAssignedContainer(assigned, container, location, false);
      return assigned;
    }

    @Override
    public CookieContainerRequest assignReUsedContainer(
      Container container, boolean honorLocality) {
      // TEZ-586 this is not match an actual rackLocal request unless honorLocality
      // is false. This method is useless if honorLocality=true
      if (!honorLocality) {
        String location = heldContainers.get(container.getId()).getRack();
        CookieContainerRequest assigned = getMatchingRequestWithoutPriority(
            container, location, false);
        doBookKeepingForAssignedContainer(assigned, container, location,
            honorLocality);
        return assigned;
      }
      return null;
    }
  }

  private class NonLocalContainerAssigner extends ContainerAssigner {

    NonLocalContainerAssigner() {
      super("NonLocal");
    }

    @Override
    public CookieContainerRequest assignNewContainer(Container container) {
      String location = ResourceRequest.ANY;
      CookieContainerRequest assigned = getMatchingRequestWithPriority(container,
          location);
      doBookKeepingForAssignedContainer(assigned, container, location, false);
      return assigned;
    }

    @Override
    public CookieContainerRequest assignReUsedContainer(Container container,
        boolean honorLocality) {
      if (!honorLocality) {
        String location = ResourceRequest.ANY;
        CookieContainerRequest assigned = getMatchingRequestWithoutPriority(
          container, location, false);
        doBookKeepingForAssignedContainer(assigned, container, location,
            honorLocality);
        return assigned;
      }
      return null;
    }

  }
  
  
  @VisibleForTesting
  class DelayedContainerManager extends Thread {

    class HeldContainerTimerComparator implements Comparator<HeldContainer> {

      @Override
      public int compare(HeldContainer c1,
          HeldContainer c2) {
        return (int) (c1.getNextScheduleTime() - c2.getNextScheduleTime());
      }
    }

    PriorityBlockingQueue<HeldContainer> delayedContainers =
      new PriorityBlockingQueue<HeldContainer>(20,
        new HeldContainerTimerComparator());

    private volatile boolean tryAssigningAll = false;
    private volatile boolean running = true;
    private long maxScheduleTimeSeen = -1;
    
    // used for testing only
    @VisibleForTesting
    volatile AtomicBoolean drainedDelayedContainersForTest = null;

    DelayedContainerManager() {
      super.setName("DelayedContainerManager");
    }
    
    @Override
    public void run() {
      while(running) {
        // Try assigning all containers if there's a request to do so.
        if (tryAssigningAll) {
          doAssignAll();
          tryAssigningAll = false;
        }

        // Try allocating containers which have timed out.
        // Required since these containers may get assigned without
        // locality at this point.
        synchronized(this) {
          if (delayedContainers.peek() == null) {
            try {
              // test only signaling to make TestTaskScheduler work
              if (drainedDelayedContainersForTest != null) {
                drainedDelayedContainersForTest.set(true);
                synchronized (drainedDelayedContainersForTest) {
                  drainedDelayedContainersForTest.notifyAll();
                }
              }
              this.wait();
              // Re-loop to see if tryAssignAll is set.
              continue;
            } catch (InterruptedException e) {
              LOG.info("AllocatedContainerManager Thread interrupted");
            }
          }
        }
        // test only sleep to prevent tight loop cycling that makes tests stall
        if (drainedDelayedContainersForTest != null) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        HeldContainer delayedContainer = delayedContainers.peek();
        if (delayedContainer == null) {
          continue;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Considering HeldContainer: "
              + delayedContainer + " for assignment");
        }
        long currentTs = System.currentTimeMillis();
        long nextScheduleTs = delayedContainer.getNextScheduleTime();
        if (currentTs >= nextScheduleTs) {
          // Remove the container and try scheduling it.
          // TEZ-587 what if container is released by RM after this
          // in onContainerCompleted()
          delayedContainer = delayedContainers.poll();
          if (delayedContainer == null) {
            continue;
          }
          Map<CookieContainerRequest, Container> assignedContainers = null;
          synchronized(YarnTaskSchedulerService.this) {
            if (null !=
                heldContainers.get(delayedContainer.getContainer().getId())) {
              assignedContainers = assignDelayedContainer(delayedContainer);
            } else {
              LOG.info("Skipping delayed container as container is no longer"
                  + " running, containerId="
                  + delayedContainer.getContainer().getId());
            }
          }
          // Inform App should be done outside of the lock
          informAppAboutAssignments(assignedContainers);
        } else {
          synchronized(this) {
            try {
              // Wait for the next container to be assignable
              delayedContainer = delayedContainers.peek();
              long diff = localitySchedulingDelay;
              if (delayedContainer != null) {
                diff = delayedContainer.getNextScheduleTime() - currentTs;
              }
              if (diff > 0) {
                this.wait(diff);
              }
            } catch (InterruptedException e) {
              LOG.info("AllocatedContainerManager Thread interrupted");
            }
          }
        }
      }
      releasePendingContainers();
    }
    
    private void doAssignAll() {
      // The allocatedContainers queue should not be modified in the middle of an iteration over it.
      // Synchronizing here on TaskScheduler.this to prevent this from happening.
      // The call to assignAll from within this method should NOT add any
      // elements back to the allocatedContainers list. Since they're all
      // delayed elements, de-allocation should not happen either - leaving the
      // list of delayed containers intact, except for the contaienrs which end
      // up getting assigned.
      if (delayedContainers.isEmpty()) {
        return;
      }

      Map<CookieContainerRequest, Container> assignedContainers;
      synchronized(YarnTaskSchedulerService.this) {
        // honor reuse-locality flags (container not timed out yet), Don't queue
        // (already in queue), don't release (release happens when containers
        // time-out)
        if (LOG.isDebugEnabled()) {
          LOG.debug("Trying to assign all delayed containers to newly received"
            + " tasks");
        }
        Iterator<HeldContainer> iter = delayedContainers.iterator();
        while(iter.hasNext()) {
          HeldContainer delayedContainer = iter.next();
          if (!heldContainers.containsKey(delayedContainer.getContainer().getId())) {
            // this container is no longer held by us
            LOG.info("AssignAll - Skipping delayed container as container is no longer"
                + " running, containerId="
                + delayedContainer.getContainer().getId());
            iter.remove();
          }
        }
        assignedContainers = tryAssignReUsedContainers(
          new ContainerIterable(delayedContainers));
      }
      // Inform app
      informAppAboutAssignments(assignedContainers);
    }
    
    /**
     * Indicate that an attempt should be made to allocate all available containers.
     * Intended to be used in cases where new Container requests come in 
     */
    public void triggerScheduling(boolean scheduleAll) {
      synchronized(this) {
        this.tryAssigningAll = scheduleAll;
        this.notify();
      }
    }

    public void shutdown() {
      this.running = false;
      this.interrupt();
    }
    
    private void releasePendingContainers() {
      List<HeldContainer> pendingContainers = Lists.newArrayListWithCapacity(
        delayedContainers.size());
      delayedContainers.drainTo(pendingContainers);
      releaseUnassignedContainers(new ContainerIterable(pendingContainers));
    }

    private void addDelayedContainer(Container container,
        long nextScheduleTime) {
      HeldContainer delayedContainer = heldContainers.get(container.getId());
      if (delayedContainer == null) {
        LOG.warn("Attempting to add a non-running container to the"
            + " delayed container list, containerId=" + container.getId());
        return;
      } else {
        delayedContainer.setNextScheduleTime(nextScheduleTime);
      }
      if (maxScheduleTimeSeen < nextScheduleTime) {
        maxScheduleTimeSeen = nextScheduleTime;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding container to delayed queue"
          + ", containerId=" + delayedContainer.getContainer().getId()
          + ", nextScheduleTime=" + delayedContainer.getNextScheduleTime()
          + ", containerExpiry=" + delayedContainer.getContainerExpiryTime());
      }
      boolean added =  false;
      synchronized(this) {
        added = delayedContainers.offer(delayedContainer);
        this.notify();
      }
      if (!added) {
        releaseUnassignedContainers(Lists.newArrayList(container));
      }
    }

  }
  
  synchronized void determineMinHeldContainers() {
    sessionMinHeldContainers.clear();
    if (sessionNumMinHeldContainers <= 0) {
      return;
    }
    
    if (heldContainers.size() <= sessionNumMinHeldContainers) {
      sessionMinHeldContainers.addAll(heldContainers.keySet());
    }
    
    Map<String, AtomicInteger> rackHeldNumber = Maps.newHashMap();
    Map<String, List<HeldContainer>> nodeHeldContainers = Maps.newHashMap();
    for(HeldContainer heldContainer : heldContainers.values()) {
      AtomicInteger count = rackHeldNumber.get(heldContainer.getRack());
      if (count == null) {
        count = new AtomicInteger(0);
        rackHeldNumber.put(heldContainer.getRack(), count);
      }
      count.incrementAndGet();
      List<HeldContainer> nodeContainers = nodeHeldContainers.get(heldContainer.getNode());
      if (nodeContainers == null) {
        nodeContainers = Lists.newLinkedList();
        nodeHeldContainers.put(heldContainer.getNode(), nodeContainers);
      }
      nodeContainers.add(heldContainer);
    }
    Map<String, AtomicInteger> rackToHoldNumber = Maps.newHashMap();
    for (String rack : rackHeldNumber.keySet()) {
      rackToHoldNumber.put(rack, new AtomicInteger(0));
    }
    
    // distribute evenly across nodes
    // the loop assigns 1 container per rack over all racks
    int containerCount = 0;
    while (containerCount < sessionNumMinHeldContainers && !rackHeldNumber.isEmpty()) {
      Iterator<Entry<String, AtomicInteger>> iter = rackHeldNumber.entrySet().iterator();
      while (containerCount < sessionNumMinHeldContainers && iter.hasNext()) {
        Entry<String, AtomicInteger> entry = iter.next();
        if (entry.getValue().decrementAndGet() >=0) {
          containerCount++;
          rackToHoldNumber.get(entry.getKey()).incrementAndGet();
        } else {
          iter.remove();
        }
      }
    }
    
    // distribute containers evenly across nodes while not exceeding rack limit
    // the loop assigns 1 container per node over all nodes
    containerCount = 0;
    while (containerCount < sessionNumMinHeldContainers && !nodeHeldContainers.isEmpty()) {
      Iterator<Entry<String, List<HeldContainer>>> iter = nodeHeldContainers.entrySet().iterator();
      while (containerCount < sessionNumMinHeldContainers && iter.hasNext()) {
        List<HeldContainer> nodeContainers = iter.next().getValue();
        if (nodeContainers.isEmpty()) {
          // node is empty. remove it.
          iter.remove();
          continue;
        }
        HeldContainer heldContainer = nodeContainers.remove(nodeContainers.size() - 1);
        if (rackToHoldNumber.get(heldContainer.getRack()).decrementAndGet() >= 0) {
          // rack can hold a container
          containerCount++;
          sessionMinHeldContainers.add(heldContainer.getContainer().getId());
        } else {
          // rack limit reached. remove node.
          iter.remove();
        }
      }
    }
    
    LOG.info("Holding on to " + sessionMinHeldContainers.size() + " containers");
  }

  private static class ContainerIterable implements Iterable<Container> {

    private final Iterable<HeldContainer> delayedContainers;

    ContainerIterable(Iterable<HeldContainer> delayedContainers) {
      this.delayedContainers = delayedContainers;
    }

    @Override
    public Iterator<Container> iterator() {

      final Iterator<HeldContainer> delayedContainerIterator = delayedContainers
          .iterator();

      return new Iterator<Container>() {

        @Override
        public boolean hasNext() {
          return delayedContainerIterator.hasNext();
        }

        @Override
        public Container next() {
          return delayedContainerIterator.next().getContainer();
        }

        @Override
        public void remove() {
          delayedContainerIterator.remove();
        }
      };
    }
  }

  static class HeldContainer {

    enum LocalityMatchLevel {
      NEW,
      NODE,
      RACK,
      NON_LOCAL
    }

    Container container;
    private String rack;
    private long nextScheduleTime;
    private Object firstContainerSignature;
    private LocalityMatchLevel localityMatchLevel;
    private long containerExpiryTime;
    private CookieContainerRequest lastTaskInfo;
    private int numAssignmentAttempts = 0;
    
    HeldContainer(Container container,
        long nextScheduleTime,
        long containerExpiryTime,
        CookieContainerRequest firstTaskInfo) {
      this.container = container;
      this.nextScheduleTime = nextScheduleTime;
      if (firstTaskInfo != null) {
        this.lastTaskInfo = firstTaskInfo;
        this.firstContainerSignature = firstTaskInfo.getCookie().getContainerSignature();
      }
      this.localityMatchLevel = LocalityMatchLevel.NODE;
      this.containerExpiryTime = containerExpiryTime;
      this.rack = RackResolver.resolve(container.getNodeId().getHost())
          .getNetworkLocation();
    }
    
    boolean isNew() {
      return firstContainerSignature == null;
    }
    
    String getRack() {
      return this.rack;
    }
    
    String getNode() {
      return this.container.getNodeId().getHost();
    }
    
    int geNumAssignmentAttempts() {
      return numAssignmentAttempts;
    }
    
    void incrementAssignmentAttempts() {
      numAssignmentAttempts++;
    }
    
    public Container getContainer() {
      return this.container;
    }
    
    public long getNextScheduleTime() {
      return this.nextScheduleTime;
    }
    
    public void setNextScheduleTime(long nextScheduleTime) {
      this.nextScheduleTime = nextScheduleTime;
    }

    public long getContainerExpiryTime() {
      return this.containerExpiryTime;
    }

    public void setContainerExpiryTime(long containerExpiryTime) {
      this.containerExpiryTime = containerExpiryTime;
    }

    public Object getFirstContainerSignature() {
      return this.firstContainerSignature;
    }
    
    public CookieContainerRequest getLastTaskInfo() {
      return this.lastTaskInfo;
    }
    
    public void setLastTaskInfo(CookieContainerRequest taskInfo) {
      lastTaskInfo = taskInfo;
    }

    public synchronized void resetLocalityMatchLevel() {
      localityMatchLevel = LocalityMatchLevel.NEW;
    }

    public synchronized void incrementLocalityMatchLevel() {
      if (localityMatchLevel.equals(LocalityMatchLevel.NEW)) {
        localityMatchLevel = LocalityMatchLevel.NODE;
      } else if (localityMatchLevel.equals(LocalityMatchLevel.NODE)) {
        localityMatchLevel = LocalityMatchLevel.RACK;
      } else if (localityMatchLevel.equals(LocalityMatchLevel.RACK)) {
        localityMatchLevel = LocalityMatchLevel.NON_LOCAL;
      } else if (localityMatchLevel.equals(LocalityMatchLevel.NON_LOCAL)) {
        throw new TezUncheckedException("Cannot increment locality level "
          + " from current NON_LOCAL for container: " + container.getId());
      }
    }

    public LocalityMatchLevel getLocalityMatchLevel() {
      return this.localityMatchLevel;
    }

    @Override
    public String toString() {
      return "HeldContainer: id: " + container.getId()
          + ", nextScheduleTime: " + nextScheduleTime
          + ", localityMatchLevel=" + localityMatchLevel
          + ", signature: "
          + (firstContainerSignature != null? firstContainerSignature.toString():"null");
    }
  }
}
