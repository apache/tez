/*
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.tez.common.ContainerSignatureMatcher;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.serviceplugins.api.DagInfo;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.serviceplugins.api.TaskScheduler;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext.AMState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A YARN task scheduler that is aware of the dependencies between vertices
 * in the DAG and takes them into account when deciding how to schedule
 * and preempt tasks.
 *
 * This scheduler makes the assumption that vertex IDs start at 0 and are
 * densely allocated (i.e.: there are no "gaps" in the vertex ID space).
  */
public class DagAwareYarnTaskScheduler extends TaskScheduler
    implements AMRMClientAsync.CallbackHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DagAwareYarnTaskScheduler.class);
  private static final Comparator<HeldContainer> PREEMPT_ORDER_COMPARATOR = new PreemptOrderComparator();

  private final RandomDataGenerator random = new RandomDataGenerator();
  private AMRMClientAsyncWrapper client;
  private ScheduledExecutorService reuseExecutor;
  private ResourceCalculator resourceCalculator;
  private int numHeartbeats = 0;
  private Resource totalResources = Resource.newInstance(0, 0);
  @GuardedBy("this")
  private Resource allocatedResources = Resource.newInstance(0, 0);
  private final Set<NodeId> blacklistedNodes = Collections.newSetFromMap(new ConcurrentHashMap<NodeId, Boolean>());
  private final ContainerSignatureMatcher signatureMatcher;
  @GuardedBy("this")
  private final RequestTracker requestTracker = new RequestTracker();
  @GuardedBy("this")
  private final Map<ContainerId, HeldContainer> heldContainers = new HashMap<>();
  @GuardedBy("this")
  private final IdleContainerTracker idleTracker = new IdleContainerTracker();
  @GuardedBy("this")
  private final Map<Object, HeldContainer> taskAssignments = new HashMap<>();

  /** A mapping from the vertex ID to the set of containers assigned to tasks for that vertex */
  @GuardedBy("this")
  private final Map<Integer, Set<HeldContainer>> vertexAssignments = new HashMap<>();

  /** If vertex N has at least one task assigned to a container then the corresponding bit at index N is set */
  @GuardedBy("this")
  private final BitSet assignedVertices = new BitSet();

  /**
   * Tracks assigned tasks for released containers so the app can be notified properly when the
   * container completion event finally arrives.
   */
  @GuardedBy("this")
  private final Map<ContainerId, Object> releasedContainers = new HashMap<>();

  @GuardedBy("this")
  private final Set<HeldContainer> sessionContainers = new HashSet<>();

  /**
   * Tracks the set of descendant vertices in the DAG for each vertex.  The BitSet for descendants of vertex N
   * are at array index N.  If a bit is set at index X in the descendants BitSet then vertex X is a descendant
   * of vertex N in the DAG.
   */
  @GuardedBy("this")
  private ArrayList<BitSet> vertexDescendants = null;

  private volatile boolean stopRequested = false;
  private volatile boolean shouldUnregister = false;
  private volatile boolean hasUnregistered = false;

  // cached configuration parameters
  private boolean shouldReuseContainers;
  private boolean reuseRackLocal;
  private boolean reuseNonLocal;
  private long localitySchedulingDelay;
  private long idleContainerTimeoutMin;
  private long idleContainerTimeoutMax;
  private int sessionNumMinHeldContainers;
  private int preemptionPercentage;
  private int numHeartbeatsBetweenPreemptions;
  private int lastPreemptionHeartbeat = 0;
  private long preemptionMaxWaitTime;

  public DagAwareYarnTaskScheduler(TaskSchedulerContext taskSchedulerContext) {
    super(taskSchedulerContext);
    signatureMatcher = taskSchedulerContext.getContainerSignatureMatcher();
  }

  @Override
  public void initialize() throws Exception {
    initialize(new AMRMClientAsyncWrapper(new AMRMClientImpl<TaskRequest>(), 1000, this));
  }

  void initialize(AMRMClientAsyncWrapper client) throws Exception {
    super.initialize();
    this.client = client;
    Configuration conf = TezUtils.createConfFromUserPayload(getContext().getInitialUserPayload());
    client.init(conf);

    int heartbeatIntervalMax = conf.getInt(
        TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX,
        TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX_DEFAULT);
    client.setHeartbeatInterval(heartbeatIntervalMax);

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

    preemptionMaxWaitTime = conf.getInt(TezConfiguration.TEZ_AM_PREEMPTION_MAX_WAIT_TIME_MS,
        TezConfiguration.TEZ_AM_PREEMPTION_MAX_WAIT_TIME_MS_DEFAULT);
    Preconditions.checkArgument(preemptionMaxWaitTime >=0, "Preemption max wait time must be >=0");

    LOG.info("scheduler initialized with maxRMHeartbeatInterval:" + heartbeatIntervalMax +
            " reuseEnabled:" + shouldReuseContainers +
            " reuseRack:" + reuseRackLocal +
            " reuseAny:" + reuseNonLocal +
            " localityDelay:" + localitySchedulingDelay +
            " preemptPercentage:" + preemptionPercentage +
            " preemptMaxWaitTime:" + preemptionMaxWaitTime +
            " numHeartbeatsBetweenPreemptions:" + numHeartbeatsBetweenPreemptions +
            " idleContainerMinTimeout:" + idleContainerTimeoutMin +
            " idleContainerMaxTimeout:" + idleContainerTimeoutMax +
            " sessionMinHeldContainers:" + sessionNumMinHeldContainers);
  }

  @Override
  public void start() throws Exception {
    super.start();
    client.start();
    if (shouldReuseContainers) {
      reuseExecutor = createExecutor();
    }
    TaskSchedulerContext ctx = getContext();
    RegisterApplicationMasterResponse response = client.registerApplicationMaster(
        ctx.getAppHostName(), ctx.getAppClientPort(), ctx.getAppTrackingUrl());
    ctx.setApplicationRegistrationData(response.getMaximumResourceCapability(),
        response.getApplicationACLs(), response.getClientToAMTokenMasterKey(),
        response.getQueue());
    if (response.getSchedulerResourceTypes().contains(SchedulerResourceTypes.CPU)) {
      resourceCalculator = new MemCpuResourceCalculator();
    } else {
      resourceCalculator = new MemResourceCalculator();
    }
  }

  protected ScheduledExecutorService createExecutor() {
    return new ReuseContainerExecutor();
  }

  protected long now() {
    return Time.monotonicNow();
  }

  @Override
  public void initiateStop() {
    super.initiateStop();
    LOG.debug("Initiating stop of task scheduler");
    stopRequested = true;
    List<ContainerId> releasedLaunchedContainers;
    synchronized (this) {
      releasedLaunchedContainers = new ArrayList<>(heldContainers.size());
      List<HeldContainer> heldList = new ArrayList<>(heldContainers.values());
      for (HeldContainer hc : heldList) {
        if (releaseContainer(hc)) {
          releasedLaunchedContainers.add(hc.getId());
        }
      }

      List<Object> tasks = requestTracker.getTasks();
      for (Object task : tasks) {
        removeTaskRequest(task);
      }
    }

    // perform app callback outside of locks
    for (ContainerId id : releasedLaunchedContainers) {
      getContext().containerBeingReleased(id);
    }
  }

  @Override
  public void shutdown() throws Exception {
    super.shutdown();
    if (reuseExecutor != null) {
      reuseExecutor.shutdown();
      reuseExecutor.awaitTermination(2, TimeUnit.SECONDS);
    }
    synchronized (this) {
      if (shouldUnregister && !hasUnregistered) {
          TaskSchedulerContext.AppFinalStatus status = getContext().getFinalAppStatus();
          LOG.info("Unregistering from RM, exitStatus={} exitMessage={} trackingURL={}",
              status.exitStatus, status.exitMessage, status.postCompletionTrackingUrl);
          client.unregisterApplicationMaster(status.exitStatus,
              status.exitMessage,
              status.postCompletionTrackingUrl);
          hasUnregistered = true;
      }
    }
    client.stop();
  }

  @Override
  public void onContainersAllocated(List<Container> containers) {
    AMState appState = getContext().getAMState();
    if (stopRequested || appState == AMState.COMPLETED) {
      LOG.info("Ignoring {} allocations since app is terminating", containers.size());
      for (Container c : containers) {
        client.releaseAssignedContainer(c.getId());
      }
      return;
    }
    List<Assignment> assignments = assignNewContainers(containers, getContext().getAMState(), getContext().isSession());
    informAppAboutAssignments(assignments);
  }

  private synchronized List<Assignment> assignNewContainers(List<Container> newContainers,
      AMState appState, boolean isSession) {
    // try to assign the containers as node-local
    List<Assignment> assignments = new ArrayList<>(newContainers.size());
    List<HeldContainer> unassigned = new ArrayList<>(newContainers.size());
    for (Container c : newContainers) {
      HeldContainer hc = new HeldContainer(c);
      heldContainers.put(hc.getId(), hc);
      Resources.addTo(allocatedResources, c.getResource());
      tryAssignNewContainer(hc, hc.getHost(), assignments, unassigned);
    }

    // try to assign the remaining containers as rack-local
    List<HeldContainer> containers = unassigned;
    unassigned = new ArrayList<>(containers.size());
    for (HeldContainer hc : containers) {
      tryAssignNewContainer(hc, hc.getRack(), assignments, unassigned);
    }

    // try to assign the remaining containers without locality
    containers = unassigned;
    unassigned = new ArrayList<>(containers.size());
    for (HeldContainer hc : containers) {
      tryAssignNewContainer(hc, ResourceRequest.ANY, assignments, unassigned);
    }

    for (HeldContainer hc : unassigned) {
      if (shouldReuseContainers) {
        idleTracker.add(hc);
        TaskRequest assigned = tryAssignReuseContainer(hc, appState, isSession);
        if (assigned != null) {
          assignments.add(new Assignment(assigned, hc.getContainer()));
        }
      } else {
        releaseContainer(hc);
      }
    }

    return assignments;
  }

  /**
   * Try to assign a newly acquired container to a task of the same priority.
   *
   * @param hc the container to assign
   * @param location the locality to consider for assignment
   * @param assignments list to update if container is assigned
   * @param unassigned list to update if container is not assigned
   */
  @GuardedBy("this")
  private void tryAssignNewContainer(HeldContainer hc, String location,
      List<Assignment> assignments, List<HeldContainer> unassigned) {
    List<? extends Collection<TaskRequest>> results = client.getMatchingRequests(hc.getPriority(),
        location, hc.getCapability());
    if (!results.isEmpty()) {
      for (Collection<TaskRequest> requests : results) {
        if (!requests.isEmpty()) {
          TaskRequest request = requests.iterator().next();
          assignContainer(request, hc, location);
          assignments.add(new Assignment(request, hc.getContainer()));
          return;
        }
      }
    }

    unassigned.add(hc);
  }

  @GuardedBy("this")
  @Nullable
  private TaskRequest tryAssignReuseContainer(HeldContainer hc,
      AMState appState, boolean isSession) {
    if (stopRequested) {
      return null;
    }

    TaskRequest assignedRequest = null;
    switch (appState) {
    case IDLE:
      handleReuseContainerWhenIdle(hc, isSession);
      break;
    case RUNNING_APP:
      if (requestTracker.isEmpty()) {
        // treat no requests as if app is idle
        handleReuseContainerWhenIdle(hc, isSession);
      } else {
        assignedRequest = tryAssignReuseContainerAppRunning(hc);
        if (assignedRequest == null) {
          if (hc.atMaxMatchLevel()) {
            LOG.info("Releasing idle container {} due to pending requests", hc.getId());
            releaseContainer(hc);
          } else {
            hc.scheduleForReuse(localitySchedulingDelay);
          }
        }
      }
      break;
    case COMPLETED:
      LOG.info("Releasing container {} because app has completed", hc.getId());
      releaseContainer(hc);
      break;
    default:
      throw new IllegalStateException("Unexpected app state " + appState);
    }

    return assignedRequest;
  }

  @GuardedBy("this")
  private void handleReuseContainerWhenIdle(HeldContainer hc, boolean isSession) {
    if (isSession && sessionContainers.isEmpty() && sessionNumMinHeldContainers > 0) {
      computeSessionContainers();
    }

    if (sessionContainers.contains(hc)) {
      LOG.info("Retaining container {} since it is a session container");
      hc.resetMatchingLevel();
    } else {
      long now = now();
      long expiration = hc.getIdleExpirationTimestamp(now);
      if (now >= expiration) {
        LOG.info("Releasing expired idle container {}", hc.getId());
        releaseContainer(hc);
      } else {
        hc.scheduleForReuse(expiration - now);
      }
    }
  }

  @GuardedBy("this")
  @Nullable
  private TaskRequest tryAssignReuseContainerAppRunning(HeldContainer hc) {
    if (!hc.isAssignable()) {
      LOG.debug("Skipping scheduling of container {} because it state is {}", hc.getId(), hc.getState());
      return null;
    }

    TaskRequest assignedRequest = tryAssignReuseContainerForAffinity(hc);
    if (assignedRequest != null) {
      return assignedRequest;
    }

    for (Entry<Priority,RequestPriorityStats> entry : requestTracker.getStatsEntries()) {
      Priority priority = entry.getKey();
      RequestPriorityStats stats = entry.getValue();
      if (!stats.allowedVertices.intersects(stats.vertices)) {
        LOG.debug("Skipping requests at priority {} because all requesting vertices are blocked by higher priority requests",
            priority);
        continue;
      }

      String matchLocation = hc.getMatchingLocation();
      if (stats.localityCount <= 0) {
        LOG.debug("Overriding locality match of container {} to ANY since there are no locality requests at priority {}",
            hc.getId(), priority);
        matchLocation = ResourceRequest.ANY;
      }
      assignedRequest = tryAssignReuseContainerForPriority(hc, matchLocation,
          priority, stats.allowedVertices);
      if (assignedRequest != null) {
        break;
      }
    }
    return assignedRequest;
  }

  @GuardedBy("this")
  @Nullable
  private TaskRequest tryAssignReuseContainerForAffinity(HeldContainer hc) {
    Collection<TaskRequest> affinities = hc.getAffinities();
    if (affinities != null) {
      for (TaskRequest request : affinities) {
        if (requestTracker.isRequestBlocked(request)) {
          LOG.debug("Cannot assign task {} to container {} since vertex {} is a descendant of pending tasks",
              request.getTask(), hc.getId(), request.getVertexIndex());
        } else {
          assignContainer(request, hc, hc.getId());
          return request;
        }
      }
    }
    return null;
  }

  @GuardedBy("this")
  @Nullable
  private TaskRequest tryAssignReuseContainerForPriority(HeldContainer hc, String matchLocation,
      Priority priority, BitSet allowedVertices) {
    List<? extends Collection<TaskRequest>> results = client.getMatchingRequests(priority, matchLocation, hc.getCapability());
    if (results.isEmpty()) {
      return null;
    }

    for (Collection<TaskRequest> requests : results) {
      for (TaskRequest request : requests) {
        final int vertexIndex = request.getVertexIndex();
        if (!allowedVertices.get(vertexIndex)) {
          LOG.debug("Not assigning task {} since it is a descendant of a pending vertex", request.getTask());
          continue;
        }

        Object signature = hc.getSignature();
        if (signature == null || signatureMatcher.isSuperSet(signature, request.getContainerSignature())) {
          assignContainer(request, hc, matchLocation);
          return request;
        }
      }
    }
    return null;
  }

  private void informAppAboutAssignments(List<Assignment> assignments) {
    if (!assignments.isEmpty()) {
      for (Assignment a : assignments) {
        informAppAboutAssignment(a.request, a.container);
      }
    }
  }

  /**
   * Inform the app about a task assignment.  This should not be called with
   * any locks held.
   *
   * @param request the corresponding task request
   * @param container the container assigned to the task
   */
  private void informAppAboutAssignment(TaskRequest request, Container container) {
    if (blacklistedNodes.contains(container.getNodeId())) {
      Object task = request.getTask();
      LOG.info("Container {} allocated for task {} on blacklisted node {}",
          container.getId(), container.getNodeId(), task);
      deallocateContainer(container.getId());
      // its ok to submit the same request again because the RM will not give us
      // the bad/unhealthy nodes again. The nodes may become healthy/unblacklisted
      // and so its better to give the RM the full information.
      allocateTask(task, request.getCapability(),
          (request.getNodes() == null ? null :
              request.getNodes().toArray(new String[request.getNodes().size()])),
          (request.getRacks() == null ? null :
              request.getRacks().toArray(new String[request.getRacks().size()])),
          request.getPriority(),
          request.getContainerSignature(),
          request.getCookie());
    } else {
      getContext().taskAllocated(request.getTask(), request.getCookie(), container);
    }
  }

  @GuardedBy("this")
  private void computeSessionContainers() {
    Map<String, MutableInt> rackHeldNumber = new HashMap<>();
    Map<String, List<HeldContainer>> nodeHeldContainers = new HashMap<>();
    for(HeldContainer heldContainer : heldContainers.values()) {
      if (heldContainer.getSignature() == null) {
        // skip containers that have not been launched as there is no process to reuse
        continue;
      }
      MutableInt count = rackHeldNumber.get(heldContainer.getRack());
      if (count == null) {
        count = new MutableInt(0);
        rackHeldNumber.put(heldContainer.getRack(), count);
      }
      count.increment();
      String host = heldContainer.getHost();
      List<HeldContainer> nodeContainers = nodeHeldContainers.get(host);
      if (nodeContainers == null) {
        nodeContainers = new LinkedList<>();
        nodeHeldContainers.put(host, nodeContainers);
      }
      nodeContainers.add(heldContainer);
    }

    Map<String, MutableInt> rackToHoldNumber = new HashMap<>();
    for (String rack : rackHeldNumber.keySet()) {
      rackToHoldNumber.put(rack, new MutableInt(0));
    }

    // distribute evenly across nodes
    // the loop assigns 1 container per rack over all racks
    int containerCount = 0;
    while (containerCount < sessionNumMinHeldContainers && !rackHeldNumber.isEmpty()) {
      Iterator<Entry<String, MutableInt>> iter = rackHeldNumber.entrySet().iterator();
      while (containerCount < sessionNumMinHeldContainers && iter.hasNext()) {
        Entry<String, MutableInt> entry = iter.next();
        MutableInt rackCount = entry.getValue();
        rackCount.decrement();
        if (rackCount.intValue() >=0) {
          containerCount++;
          rackToHoldNumber.get(entry.getKey()).increment();
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
        MutableInt holdCount = rackToHoldNumber.get(heldContainer.getRack());
        holdCount.decrement();
        if (holdCount.intValue() >= 0) {
          // rack can hold a container
          containerCount++;
          sessionContainers.add(heldContainer);
        } else {
          // rack limit reached. remove node.
          iter.remove();
        }
      }
    }

    LOG.info("Identified {} session containers out of {} total containers",
        sessionContainers.size(), heldContainers.size());
  }

  @GuardedBy("this")
  private void activateSessionContainers() {
    if (!sessionContainers.isEmpty()) {
      for (HeldContainer hc : sessionContainers) {
        if (hc.isAssignable()) {
          hc.scheduleForReuse(localitySchedulingDelay);
        }
      }
      sessionContainers.clear();
    }
  }

  @Override
  public void onContainersCompleted(List<ContainerStatus> statuses) {
    if (stopRequested) {
      return;
    }

    List<TaskStatus> taskStatusList = new ArrayList<>(statuses.size());
    synchronized (this) {
      for (ContainerStatus status : statuses) {
        ContainerId cid = status.getContainerId();
        LOG.info("Container {} completed with status {}", cid, status);
        Object task = releasedContainers.remove(cid);
        if (task == null) {
          HeldContainer hc = heldContainers.get(cid);
          if (hc != null) {
            task = containerCompleted(hc);
          }
        }
        if (task != null) {
          taskStatusList.add(new TaskStatus(task, status));
        }
      }
    }

    // perform app callback outside of locks
    for (TaskStatus taskStatus : taskStatusList) {
      getContext().containerCompleted(taskStatus.task, taskStatus.status);
    }
  }

  @Override
  public void onNodesUpdated(List<NodeReport> updatedNodes) {
    if (!stopRequested) {
      getContext().nodesUpdated(updatedNodes);
    }
  }

  @Override
  public float getProgress() {
    if (stopRequested) {
      return 1;
    }

    Collection<ContainerId> preemptedContainers;
    synchronized (this) {
      Resource freeResources = getAvailableResources();
      if (totalResources.getMemory() == 0) {
        // assume this is the first allocate callback. nothing is allocated.
        // available resource = totalResource
        // TODO this will not handle dynamic changes in resources
        totalResources = Resources.clone(freeResources);
        LOG.info("App total resource memory: {} cpu: {} activeAssignments: {}",
            totalResources.getMemory(), totalResources.getVirtualCores(), taskAssignments.size());
      }

      ++numHeartbeats;
      if (LOG.isDebugEnabled() || numHeartbeats % 50 == 1) {
        LOG.info(constructPeriodicLog(freeResources));
      }

      preemptedContainers = maybePreempt(freeResources);
      if (preemptedContainers != null && !preemptedContainers.isEmpty()) {
        lastPreemptionHeartbeat = numHeartbeats;
      }
    }

    // perform app callback outside of locks
    if (preemptedContainers != null && !preemptedContainers.isEmpty()) {
      for (ContainerId cid : preemptedContainers) {
        LOG.info("Preempting container {} currently allocated to a task", cid);
        getContext().preemptContainer(cid);
      }
    }

    return getContext().getProgress();
  }

  @Override
  public void onShutdownRequest() {
    if (!stopRequested) {
      getContext().appShutdownRequested();
    }
  }

  @Override
  public void onError(Throwable e) {
    LOG.error("Error from ARMRMClient", e);
    if (!stopRequested) {
      getContext().reportError(YarnTaskSchedulerServiceError.RESOURCEMANAGER_ERROR,
          StringUtils.stringifyException(e), null);
    }
  }

  @Override
  public Resource getAvailableResources() {
    return client.getAvailableResources();
  }

  @Override
  public Resource getTotalResources() {
    return totalResources;
  }

  @Override
  public int getClusterNodeCount() {
    return client.getClusterNodeCount();
  }

  @Override
  public synchronized void blacklistNode(NodeId nodeId) {
    LOG.info("Blacklisting node: {}", nodeId);
    blacklistedNodes.add(nodeId);
    client.updateBlacklist(Collections.singletonList(nodeId.getHost()), null);
  }

  @Override
  public synchronized void unblacklistNode(NodeId nodeId) {
    if (blacklistedNodes.remove(nodeId)) {
      LOG.info("Removing blacklist for node: {}", nodeId);
      client.updateBlacklist(null, Collections.singletonList(nodeId.getHost()));
    }
  }

  @Override
  public void allocateTask(Object task, Resource capability, String[] hosts, String[] racks,
      Priority priority, Object containerSignature, Object clientCookie) {
    int vertexIndex = getContext().getVertexIndexForTask(task);
    TaskRequest request = new TaskRequest(task, vertexIndex, capability, hosts, racks,
        priority, containerSignature, clientCookie);
    addTaskRequest(request);
  }

  @Override
  public void allocateTask(Object task, Resource capability, ContainerId containerId,
      Priority priority, Object containerSignature, Object clientCookie) {
    String[] hosts = null;
    synchronized (this) {
      HeldContainer held = heldContainers.get(containerId);
      if (held != null) {
        if (held.canFit(capability)) {
          hosts = new String[]{held.getHost()};
        } else {
          LOG.warn("Match request to container {} but {} does not fit in {}",
              containerId, capability, held.getCapability());
          containerId = null;
        }
      } else {
        LOG.info("Ignoring match request to unknown container {}", containerId);
        containerId = null;
      }
    }
    int vertexIndex = getContext().getVertexIndexForTask(task);
    TaskRequest request = new TaskRequest(task, vertexIndex, capability, hosts, null,
        priority, containerSignature, clientCookie, containerId);
    addTaskRequest(request);
  }

  @Override
  public boolean deallocateTask(Object task, boolean taskSucceeded,
      TaskAttemptEndReason endReason, String diagnostics) {
    ContainerId releasedLaunchedContainer = null;
    AMState appState = getContext().getAMState();
    boolean isSession = getContext().isSession();
    TaskRequest newAssignment = null;
    HeldContainer hc;
    synchronized (this) {
      TaskRequest request = removeTaskRequest(task);
      if (request != null) {
        LOG.debug("Deallocating task {} before it was allocated", task);
        return false;
      }

      hc = removeTaskAssignment(task);
      if (hc != null) {
        if (taskSucceeded && shouldReuseContainers) {
          idleTracker.add(hc);
          newAssignment = tryAssignReuseContainer(hc, appState, isSession);
          if (newAssignment == null && hc.isReleasedAndUsed()) {
            releasedLaunchedContainer = hc.getId();
          }
        } else {
          if (releaseContainer(hc)) {
            releasedLaunchedContainer = hc.getId();
          }
        }
      }
    }

    // perform app callback outside of locks
    if (newAssignment != null) {
      informAppAboutAssignment(newAssignment, hc.getContainer());
      return true;
    }
    if (releasedLaunchedContainer != null) {
      getContext().containerBeingReleased(releasedLaunchedContainer);
      return true;
    }
    return hc != null;
  }

  @Override
  public Object deallocateContainer(ContainerId containerId) {
    Object task = null;
    ContainerId releasedLaunchedContainer = null;
    synchronized (this) {
      HeldContainer hc = heldContainers.remove(containerId);
      if (hc != null) {
        task = hc.getAssignedTask();
        if (task != null) {
          LOG.info("Deallocated container {} from task {}", containerId, task);
        }
        if (releaseContainer(hc)) {
          releasedLaunchedContainer = hc.getId();
        }
      } else {
        LOG.info("Ignoring deallocation of unknown container {}", containerId);
      }
    }

    // perform app callback outside of locks
    if (releasedLaunchedContainer != null) {
      getContext().containerBeingReleased(releasedLaunchedContainer);
    }
    return task;
  }

  @GuardedBy("this")
  private void assignContainer(TaskRequest request, HeldContainer hc, Object match) {
    LOG.info("Assigning container {} to task {} host={} priority={} capability={} match={} lastTask={}",
        hc.getId(), request.getTask(), hc.getHost(), hc.getPriority(), hc.getCapability(), match, hc.getLastTask());
    removeTaskRequest(request.getTask());
    addTaskAssignment(request, hc);
    idleTracker.remove(hc);
  }

  private synchronized boolean releaseContainer(HeldContainer hc) {
    Object task = containerCompleted(hc);
    client.releaseAssignedContainer(hc.getId());
    if (task != null) {
      releasedContainers.put(hc.getId(), task);
      return true;
    }
    return false;
  }

  @GuardedBy("this")
  private void addTaskAssignment(TaskRequest request, HeldContainer hc) {
    HeldContainer oldContainer = taskAssignments.put(request.getTask(), hc);
    if (oldContainer != null) {
      LOG.error("Task {} being assigned to container {} but was already assigned to container {}",
          request.getTask(), hc.getId(), oldContainer.getId());
    }
    Integer vertexIndex = request.vertexIndex;
    Set<HeldContainer> cset = vertexAssignments.get(vertexIndex);
    if (cset == null) {
      cset = new HashSet<>();
      vertexAssignments.put(vertexIndex, cset);
      assignedVertices.set(vertexIndex);
    }
    cset.add(hc);
    hc.assignTask(request);
  }

  @GuardedBy("this")
  private HeldContainer removeTaskAssignment(Object task) {
    HeldContainer hc = taskAssignments.remove(task);
    if (hc != null) {
      TaskRequest request = hc.removeAssignment();
      if (request != null) {
        Integer vertexIndex = request.vertexIndex;
        Set<HeldContainer> cset = vertexAssignments.get(vertexIndex);
        if (cset != null && cset.remove(hc) && cset.isEmpty()) {
          vertexAssignments.remove(vertexIndex);
          assignedVertices.clear(vertexIndex);
        }
      } else {
        LOG.error("Container {} had assigned task {} but no request?!?", hc.getId(), task);
      }
    }
    return hc;
  }

  @GuardedBy("this")
  @Nullable
  private Object containerCompleted(HeldContainer hc) {
    idleTracker.remove(hc);
    heldContainers.remove(hc.getId());
    Resources.subtractFrom(allocatedResources, hc.getCapability());
    removeTaskAssignment(hc.getAssignedTask());
    hc.released();
    return hc.getLastTask();
  }

  @GuardedBy("this")
  private void ensureVertexDescendants() {
    if (vertexDescendants == null) {
      DagInfo info = getContext().getCurrentDagInfo();
      if (info == null) {
        throw new IllegalStateException("Scheduling tasks but no current DAG info?");
      }
      int numVertices = info.getTotalVertices();
      ArrayList<BitSet> descendants = new ArrayList<>(numVertices);
      for (int i = 0; i < numVertices; ++i) {
        descendants.add(info.getVertexDescendants(i));
      }
      vertexDescendants = descendants;
    }
  }

  private void addTaskRequest(TaskRequest request) {
    Container assignedContainer = null;
    synchronized (this) {
      if (shouldReuseContainers && !stopRequested && getContext().getAMState() != AMState.COMPLETED) {
        ensureVertexDescendants();
        activateSessionContainers();
        HeldContainer hc = tryAssignTaskToIdleContainer(request);
        if (hc != null) {
          assignedContainer = hc.getContainer();
        }
      }

      if (assignedContainer == null) {
        ensureVertexDescendants();
        TaskRequest old = requestTracker.add(request);
        if (old != null) {
          removeTaskRequestByRequest(request);
        }
        client.addContainerRequest(request);

        HeldContainer hc = heldContainers.get(request.getAffinity());
        if (hc != null) {
          hc.addAffinity(request);
        }
      }
    }

    // perform app callback outside of locks
    if (assignedContainer != null) {
      informAppAboutAssignment(request, assignedContainer);
    }
  }

  @Nullable
  private synchronized TaskRequest removeTaskRequest(Object task) {
    TaskRequest request = requestTracker.remove(task);
    if (request != null) {
      removeTaskRequestByRequest(request);
    }
    return request;
  }

  @GuardedBy("this")
  private void removeTaskRequestByRequest(TaskRequest request) {
    client.removeContainerRequest(request);
    HeldContainer hc = heldContainers.get(request.getAffinity());
    if (hc != null) {
      hc.removeAffinity(request);
    }
  }

  @GuardedBy("this")
  @Nullable
  private HeldContainer tryAssignTaskToIdleContainer(TaskRequest request) {
    if (requestTracker.isRequestBlocked(request)) {
      LOG.debug("Cannot assign task {} to an idle container since vertex {} is a descendant of pending tasks",
          request.getTask(), request.getVertexIndex());
      return null;
    }

    // check if container affinity can be satisfied immediately
    ContainerId affinity = request.getAffinity();
    if (affinity != null) {
      HeldContainer hc = heldContainers.get(affinity);
      if (hc != null && hc.isAssignable()) {
        assignContainer(request, hc, affinity);
        return hc;
      }
    }

    // try to match the task against idle containers in order from best locality to worst
    HeldContainer hc;
    if (request.hasLocality()) {
      hc = tryAssignTaskToIdleContainer(request, request.getNodes(), HeldContainerState.MATCHES_LOCAL_STATES);
      if (hc == null) {
        hc = tryAssignTaskToIdleContainer(request, request.getRacks(), HeldContainerState.MATCHES_RACK_STATES);
        if (hc == null) {
          hc = tryAssignTaskToIdleContainer(request, ResourceRequest.ANY, HeldContainerState.MATCHES_ANY_STATES);
        }
      }
    } else {
      hc = tryAssignTaskToIdleContainer(request, ResourceRequest.ANY, HeldContainerState.MATCHES_LOCAL_STATES);
    }

    return hc;
  }

  @GuardedBy("this")
  @Nullable
  private HeldContainer tryAssignTaskToIdleContainer(TaskRequest request,
      List<String> locations, EnumSet<HeldContainerState> eligibleStates) {
    if (locations != null && !locations.isEmpty()) {
      for (String location : locations) {
        HeldContainer hc = tryAssignTaskToIdleContainer(request, location, eligibleStates);
        if (hc != null) {
          return hc;
        }
      }
    }
    return null;
  }

  @GuardedBy("this")
  @Nullable
  private HeldContainer tryAssignTaskToIdleContainer(TaskRequest request,
      String location, EnumSet<HeldContainerState> eligibleStates) {
    Set<HeldContainer> containers = idleTracker.getByLocation(location);
    HeldContainer bestMatch = null;
    if (containers != null && !containers.isEmpty()) {
      for (HeldContainer hc : containers) {
        if (eligibleStates.contains(hc.getState())) {
          Object csig = hc.getSignature();
          if (csig == null || signatureMatcher.isSuperSet(csig, request.getContainerSignature())) {
            int numAffinities = hc.getNumAffinities();
            if (numAffinities == 0) {
              bestMatch = hc;
              break;
            }
            if (bestMatch == null || numAffinities < bestMatch.getNumAffinities()) {
              bestMatch = hc;
            }
          } else {
            LOG.debug("Unable to assign task {} to container {} due to signature mismatch", request.getTask(), hc.getId());
          }
        }
      }
    }
    if (bestMatch != null) {
      assignContainer(request, bestMatch, location);
    }
    return bestMatch;
  }

  @Override
  public void setShouldUnregister() {
    shouldUnregister = true;
  }

  @Override
  public boolean hasUnregistered() {
    return hasUnregistered;
  }

  @Override
  public synchronized void dagComplete() {
    for (HeldContainer hc : sessionContainers) {
      hc.resetMatchingLevel();
    }
    vertexDescendants = null;
  }

  @GuardedBy("this")
  @Nullable
  private Collection<ContainerId> maybePreempt(Resource freeResources) {
    if (preemptionPercentage == 0 || numHeartbeats - lastPreemptionHeartbeat < numHeartbeatsBetweenPreemptions) {
      return null;
    }
    if (!requestTracker.isPreemptionDeadlineExpired() && requestTracker.fitsHighestPriorityRequest(freeResources)) {
      if (numHeartbeats % 50 == 1) {
        LOG.info("Highest priority request fits in free resources {}", freeResources);
      }
      return null;
    }

    int numIdleContainers = idleTracker.getNumContainers();
    if (numIdleContainers > 0) {
      if (numHeartbeats % 50 == 1) {
        LOG.info("Avoiding preemption since there are {} idle containers", numIdleContainers);
      }
      return null;
    }

    BitSet blocked = requestTracker.createVertexBlockedSet();
    if (!blocked.intersects(assignedVertices)) {
      if (numHeartbeats % 50 == 1) {
        LOG.info("Avoiding preemption since there are no descendants of the highest priority requests running");
      }
      return null;
    }

    Resource preemptLeft = requestTracker.getAmountToPreempt(preemptionPercentage);
    if (!resourceCalculator.anyAvailable(preemptLeft)) {
      if (numHeartbeats % 50 == 1) {
        LOG.info("Avoiding preemption since amount to preempt is {}", preemptLeft);
      }
      return null;
    }

    PriorityQueue<HeldContainer> candidates = new PriorityQueue<>(11, PREEMPT_ORDER_COMPARATOR);
    blocked.and(assignedVertices);
    for (int i = blocked.nextSetBit(0); i >= 0; i = blocked.nextSetBit(i + 1)) {
      Collection<HeldContainer> containers = vertexAssignments.get(i);
      if (containers != null) {
        candidates.addAll(containers);
      } else {
        LOG.error("Vertex {} in assignedVertices but no assignments?", i);
      }
    }

    ArrayList<ContainerId> preemptedContainers = new ArrayList<>();
    HeldContainer hc;
    while ((hc = candidates.poll()) != null) {
      LOG.info("Preempting container {} currently allocated to task {}", hc.getId(), hc.getAssignedTask());
      preemptedContainers.add(hc.getId());
      resourceCalculator.deductFrom(preemptLeft, hc.getCapability());
      if (!resourceCalculator.anyAvailable(preemptLeft)) {
        break;
      }
    }

    return preemptedContainers;
  }

  @GuardedBy("this")
  private String constructPeriodicLog(Resource freeResource) {
    Priority highestPriority = requestTracker.getHighestPriority();
    return "Allocated: " + allocatedResources +
        " Free: " + freeResource +
        " pendingRequests: " + requestTracker.getNumRequests() +
        " heldContainers: " + heldContainers.size() +
        " heartbeats: " + numHeartbeats +
        " lastPreemptionHeartbeat: " + lastPreemptionHeartbeat +
        ((highestPriority != null) ?
            (" highestWaitingRequestWaitStartTime: " + requestTracker.getHighestPriorityWaitTimestamp() +
                " highestWaitingRequestPriority: " + highestPriority) : "");
  }

  @VisibleForTesting
  int getNumBlacklistedNodes() {
    return blacklistedNodes.size();
  }

  @VisibleForTesting
  Collection<HeldContainer> getSessionContainers() {
    return sessionContainers;
  }

  // Wrapper class to work around lack of blacklisting APIs in async client.
  // This can be removed once Tez requires YARN >= 2.7.0
  static class AMRMClientAsyncWrapper extends AMRMClientAsyncImpl<TaskRequest> {
    AMRMClientAsyncWrapper(AMRMClient<TaskRequest> syncClient, int intervalMs, CallbackHandler handler) {
      super(syncClient, intervalMs, handler);
    }

    public void updateBlacklist(List<String> additions, List<String> removals) {
      client.updateBlacklist(additions, removals);
    }
  }

  /**
   * A utility class to track a task allocation.
   */
  static class TaskRequest extends AMRMClient.ContainerRequest {
    final Object task;
    final int vertexIndex;
    final Object signature;
    final Object cookie;
    final ContainerId affinityContainerId;

    TaskRequest(Object task, int vertexIndex, Resource capability, String[] hosts, String[] racks,
        Priority priority, Object signature, Object cookie) {
      this(task, vertexIndex, capability, hosts, racks, priority, signature, cookie,  null);
    }

    TaskRequest(Object task, int vertexIndex, Resource capability, String[] hosts, String[] racks,
        Priority priority, Object signature, Object cookie, ContainerId affinityContainerId) {
      super(capability, hosts, racks, priority);
      this.task = task;
      this.vertexIndex = vertexIndex;
      this.signature = signature;
      this.cookie = cookie;
      this.affinityContainerId = affinityContainerId;
    }

    Object getTask() {
      return task;
    }

    int getVertexIndex() {
      return vertexIndex;
    }

    Object getContainerSignature() {
      return signature;
    }

    Object getCookie() {
      return cookie;
    }

    @Nullable
    ContainerId getAffinity() {
      return affinityContainerId;
    }

    boolean hasLocality() {
      List<String> nodes = getNodes();
      List<String> racks = getRacks();
      return (nodes != null && !nodes.isEmpty()) || (racks != null && !racks.isEmpty());
    }
  }

  private enum HeldContainerState {
    MATCHING_LOCAL(true),
    MATCHING_RACK(true),
    MATCHING_ANY(true),
    ASSIGNED(false),
    RELEASED(false);

    private static final EnumSet<HeldContainerState> MATCHES_LOCAL_STATES = EnumSet.of(
        HeldContainerState.MATCHING_LOCAL, HeldContainerState.MATCHING_RACK, HeldContainerState.MATCHING_ANY);
    private static final EnumSet<HeldContainerState> MATCHES_RACK_STATES = EnumSet.of(
        HeldContainerState.MATCHING_RACK, HeldContainerState.MATCHING_ANY);
    private static final EnumSet<HeldContainerState> MATCHES_ANY_STATES = EnumSet.of(HeldContainerState.MATCHING_ANY);

    private final boolean assignable;

    HeldContainerState(boolean assignable) {
      this.assignable = assignable;
    }

    boolean isAssignable() {
      return assignable;
    }
  }

  /**
   * Tracking for an allocated container.
   */
  @VisibleForTesting
  class HeldContainer implements Callable<Void> {
    final Container container;
    final String rack;
    @GuardedBy("DagAwareYarnTaskScheduler.this")
    HeldContainerState state = HeldContainerState.MATCHING_LOCAL;

    /** The Future received when scheduling an idle container for re-allocation at a later time. */
    @GuardedBy("DagAwareYarnTaskScheduler.this")
    Future<Void> future = null;

    /** The collection of task requests that have specified this container as a scheduling affinity. */
    @GuardedBy("DagAwareYarnTaskScheduler.this")
    Collection<TaskRequest> affinities = null;

    /**
     * The task request corresponding to the currently assigned task to this container.
     * This field is null when the container is not currently assigned.
     */
    @GuardedBy("DagAwareYarnTaskScheduler.this")
    TaskRequest assignedRequest = null;

    /** The task request corresponding to the last task that was assigned to this container. */
    @GuardedBy("DagAwareYarnTaskScheduler.this")
    TaskRequest lastRequest = null;

    /** The timestamp when the idle container will expire. 0 if the container is not idle. */
    @GuardedBy("DagAwareYarnTaskScheduler.this")
    long idleExpirationTimestamp = 0;

    /** The timestamp when this container was assigned. 0 if the container is not assigned. */
    @GuardedBy("DagAwareYarnTaskScheduler.this")
    long assignmentTimestamp = 0;

    HeldContainer(Container container) {
      this.container = container;
      this.rack = RackResolver.resolve(container.getNodeId().getHost()).getNetworkLocation();
    }

    HeldContainerState getState() {
      return state;
    }

    boolean isAssignable() {
      return state.isAssignable();
    }

    boolean isReleasedAndUsed() {
      return state == HeldContainerState.RELEASED && getLastTask() != null;
    }

    Container getContainer() {
      return container;
    }

    ContainerId getId() {
      return container.getId();
    }

    String getHost() {
      return container.getNodeId().getHost();
    }

    String getRack() {
      return rack;
    }

    Priority getPriority() {
      return container.getPriority();
    }

    Resource getCapability() {
      return container.getResource();
    }

    @Nullable
    Object getAssignedTask() {
      return assignedRequest != null ? assignedRequest.getTask() : null;
    }

    void assignTask(TaskRequest request) {
      assert state != HeldContainerState.ASSIGNED && state != HeldContainerState.RELEASED;
      if (assignedRequest != null) {
        LOG.error("Container {} assigned task {} but already running task {}",
            getId(), request.getTask(), assignedRequest.getTask());
      }
      assignedRequest = request;
      lastRequest = request;
      state = HeldContainerState.ASSIGNED;
      idleExpirationTimestamp = 0;
      assignmentTimestamp = now();
      if (future != null) {
        future.cancel(false);
        future = null;
      }
    }

    TaskRequest removeAssignment() {
      assert state == HeldContainerState.ASSIGNED;
      TaskRequest result = assignedRequest;
      assignedRequest = null;
      assignmentTimestamp = 0;
      state = HeldContainerState.MATCHING_LOCAL;
      return result;
    }

    void addAffinity(TaskRequest request) {
      if (affinities == null) {
        affinities = new HashSet<>();
      }
      affinities.add(request);
    }

    void removeAffinity(TaskRequest request) {
      if (affinities != null && affinities.remove(request) && affinities.isEmpty()) {
        affinities = null;
      }
    }

    int getNumAffinities() {
      return affinities != null ? affinities.size() : 0;
    }

    @Nullable
    Collection<TaskRequest> getAffinities() {
      return affinities;
    }

    void scheduleForReuse(long delayMillis) {
      assert state != HeldContainerState.ASSIGNED && state != HeldContainerState.RELEASED;
      try {
        if (future != null) {
          future.cancel(false);
        }
        future = reuseExecutor.schedule(this, delayMillis, TimeUnit.MILLISECONDS);
      } catch (RejectedExecutionException e) {
        if (!stopRequested) {
          LOG.error("Container {} could not be scheduled for reuse!", getId(), e);
        }
      }
    }

    @Nullable
    Object getSignature() {
      return lastRequest != null ? lastRequest.getContainerSignature() : null;
    }

    @Nullable
    Object getLastTask() {
      return lastRequest != null ? lastRequest.getTask() : null;
    }

    String getMatchingLocation() {
      switch (state) {
      case MATCHING_LOCAL:
        return getHost();
      case MATCHING_RACK:
        return getRack();
      case MATCHING_ANY:
        return ResourceRequest.ANY;
      default:
        throw new IllegalStateException("Container " + getId() + " trying to match in state " + state);
      }
    }

    void moveToNextMatchingLevel() {
      switch (state) {
      case MATCHING_LOCAL:
        if (reuseRackLocal) {
          state = HeldContainerState.MATCHING_RACK;
        }
        break;
      case MATCHING_RACK:
        if (reuseNonLocal) {
          state = HeldContainerState.MATCHING_ANY;
        }
        break;
      case MATCHING_ANY:
        break;
      default:
        throw new IllegalStateException("Container " + getId() + " trying to match in state " + state);
      }
    }

    boolean atMaxMatchLevel() {
      switch (state) {
      case MATCHING_LOCAL:
        return !reuseRackLocal;
      case MATCHING_RACK:
        return !reuseNonLocal;
      case MATCHING_ANY:
        return true;
      default:
        throw new IllegalStateException("Container " + getId() + " trying to match in state " + state);
      }
    }

    void resetMatchingLevel() {
      if (isAssignable()) {
        state = HeldContainerState.MATCHING_LOCAL;
      }
    }

    long getIdleExpirationTimestamp(long now) {
      if (idleExpirationTimestamp == 0) {
        if (idleContainerTimeoutMin > 0) {
          idleExpirationTimestamp = now + random.nextLong(idleContainerTimeoutMin, idleContainerTimeoutMax);
        } else {
          idleExpirationTimestamp = Long.MAX_VALUE;
        }
      }
      return idleExpirationTimestamp;
    }

    long getAssignmentTimestamp() {
      return assignmentTimestamp;
    }

    boolean canFit(Resource capability) {
      Resource cr = container.getResource();
      return cr.getMemory() >= capability.getMemory() && cr.getVirtualCores() >= capability.getVirtualCores();
    }

    @Override
    public Void call() throws Exception {
      AMState appState = getContext().getAMState();
      boolean isSession = getContext().isSession();
      TaskRequest assigned = null;
      ContainerId released = null;
      synchronized (DagAwareYarnTaskScheduler.this) {
        future = null;
        if (isAssignable()) {
          moveToNextMatchingLevel();
          assigned = tryAssignReuseContainer(this, appState, isSession);
          if (assigned == null && isReleasedAndUsed()) {
            released = getId();
          }
        }
      }
      if (assigned != null) {
        informAppAboutAssignment(assigned, container);
      }
      if (released != null) {
        getContext().containerBeingReleased(released);
      }
      return null;
    }

    void released() {
      assert state != HeldContainerState.RELEASED;
      state = HeldContainerState.RELEASED;
      if (future != null) {
        future.cancel(false);
      }
      future = null;
    }
  }

  /**
   * Utility comparator to order containers by assignment timestamp from
   * most recent to least recent.
   */
  private static class PreemptOrderComparator implements Comparator<HeldContainer> {
    @Override
    public int compare(HeldContainer o1, HeldContainer o2) {
      long timestamp1 = o1.getAssignmentTimestamp();
      if (timestamp1 == 0) {
        timestamp1 = Long.MAX_VALUE;
      }
      long timestamp2 = o2.getAssignmentTimestamp();
      if (timestamp2 == 0) {
        timestamp2 = Long.MAX_VALUE;
      }
      return Long.compare(timestamp2, timestamp1);
    }
  }

  /**
   * Utility class for a request, container pair
   */
  private static class Assignment {
    final TaskRequest request;
    final Container container;

    Assignment(TaskRequest request, Container container) {
      this.request = request;
      this.container = container;
    }
  }

  /**
   * Utility class for a task, container exit status pair
   */
  private static class TaskStatus {
    final Object task;
    final ContainerStatus status;

    TaskStatus(Object task, ContainerStatus status) {
      this.task = task;
      this.status = status;
    }
  }

  /**
   * The task allocation request tracker tracks task allocations
   * and keeps statistics on which priorities have requests and which vertices
   * should be blocked from container reuse due to DAG topology.
   */
  private class RequestTracker {
    private final Map<Object, TaskRequest> requests = new HashMap<>();
    /** request map ordered by priority with highest priority first */
    private final NavigableMap<Priority, RequestPriorityStats> priorityStats =
        new TreeMap<>(Collections.reverseOrder());
    private Priority highestPriority = null;
    private long highestPriorityWaitTimestamp = 0;

    @GuardedBy("DagAwareYarnTaskScheduler.this")
    @Nullable
    TaskRequest add(TaskRequest request) {
      TaskRequest oldRequest = requests.put(request.getTask(), request);
      Priority priority = request.getPriority();
      RequestPriorityStats stats = priorityStats.get(priority);
      if (stats == null) {
        stats = addStatsForPriority(priority);
      }
      ++stats.requestCount;
      if (request.hasLocality()) {
        ++stats.localityCount;
      }
      incrVertexTaskCount(priority, stats, request.getVertexIndex());

      if (oldRequest != null) {
        updateStatsForRemoval(oldRequest);
      }
      return oldRequest;
    }

    @GuardedBy("DagAwareYarnTaskScheduler.this")
    @Nullable
    TaskRequest remove(Object task) {
      TaskRequest request = requests.remove(task);
      if (request != null) {
        updateStatsForRemoval(request);
        return request;
      }
      return null;
    }

    private RequestPriorityStats addStatsForPriority(Priority priority) {
      BitSet allowedVerts = new BitSet(vertexDescendants.size());
      Entry<Priority,RequestPriorityStats> lowerEntry = priorityStats.lowerEntry(priority);
      if (lowerEntry != null) {
        // initialize the allowed vertices BitSet using the information derived
        // from the next higher priority entry
        RequestPriorityStats priorStats = lowerEntry.getValue();
        allowedVerts.or(priorStats.allowedVertices);
        allowedVerts.andNot(priorStats.descendants);
      } else {
        // no higher priority entry so this priority is currently the highest
        highestPriority = priority;
        highestPriorityWaitTimestamp = now();
        allowedVerts.set(0, vertexDescendants.size());
      }
      RequestPriorityStats stats = new RequestPriorityStats(vertexDescendants.size(), allowedVerts);
      priorityStats.put(priority, stats);
      return stats;
    }

    private void updateStatsForRemoval(TaskRequest request) {
      Priority priority = request.getPriority();
      RequestPriorityStats stats = priorityStats.get(priority);
      decrVertexTaskCount(priority, stats, request.getVertexIndex());
      --stats.requestCount;
      if (request.hasLocality()) {
        --stats.localityCount;
      }
      if (stats.requestCount == 0) {
        priorityStats.remove(priority);
        if (highestPriority.equals(priority)) {
          if (priorityStats.isEmpty()) {
            highestPriority = null;
            highestPriorityWaitTimestamp = 0;
          } else {
            highestPriority = priorityStats.firstKey();
            highestPriorityWaitTimestamp = now();
          }
        }
      }
    }

    @GuardedBy("DagAwareYarnTaskScheduler.this")
    boolean isEmpty() {
      return requests.isEmpty();
    }

    @GuardedBy("DagAwareYarnTaskScheduler.this")
    int getNumRequests() {
      return requests.size();
    }

    @GuardedBy("DagAwareYarnTaskScheduler.this")
    List<Object> getTasks() {
      return new ArrayList<>(requests.keySet());
    }

    @GuardedBy("DagAwareYarnTaskScheduler.this")
    Collection<Entry<Priority, RequestPriorityStats>> getStatsEntries() {
      return priorityStats.entrySet();
    }

    @GuardedBy("DagAwareYarnTaskScheduler.this")
    @Nullable
    Priority getHighestPriority() {
      if (priorityStats.isEmpty()) {
        return null;
      }
      return priorityStats.firstKey();
    }

    @GuardedBy("DagAwareYarnTaskScheduler.this")
    long getHighestPriorityWaitTimestamp() {
      return highestPriorityWaitTimestamp;
    }

    @GuardedBy("DagAwareYarnTaskScheduler.this")
    boolean isRequestBlocked(TaskRequest request) {
      Entry<Priority, RequestPriorityStats> entry = priorityStats.floorEntry(request.getPriority());
      if (entry != null) {
        RequestPriorityStats stats = entry.getValue();
        int vertexIndex = request.getVertexIndex();
        return !stats.allowedVertices.get(vertexIndex) || stats.descendants.get(vertexIndex);
      }
      return false;
    }

    private void incrVertexTaskCount(Priority priority, RequestPriorityStats stats, int vertexIndex) {
      Integer vertexIndexInt = vertexIndex;
      MutableInt taskCount = stats.vertexTaskCount.get(vertexIndexInt);
      if (taskCount != null) {
        taskCount.increment();
      } else {
        addVertexToRequestStats(priority, stats, vertexIndexInt);
      }
    }

    private void decrVertexTaskCount(Priority priority, RequestPriorityStats stats, int vertexIndex) {
      Integer vertexIndexInt = vertexIndex;
      MutableInt taskCount = stats.vertexTaskCount.get(vertexIndexInt);
      taskCount.decrement();
      if (taskCount.intValue() <= 0) {
        removeVertexFromRequestStats(priority, stats, vertexIndexInt);
      }
    }

    /**
     * Add a new vertex to a RequestPriorityStats.
     *
     * Adding a vertex to the request stats requires updating the stats descendants bitmask to include the descendants
     * of the new vertex and also updating the allowedVertices bitmask for all lower priority requests to prevent any
     * task request from a descendant vertex in the DAG from being allocated. This avoids assigning allocations to
     * lower priority requests when a higher priority request of an ancestor is still pending, but it allows lower
     * priority requests to be satisfied if higher priority requests are not ancestors. This is particularly useful
     * for DAGs that have independent trees of vertices or significant, parallel branches within a tree.
     *
     * Requests are blocked by taking the specified vertex's full descendant vertex bitmask in vertexDescendants and
     * clearing those bits for all lower priority requests. For the following example DAG where each vertex index
     * corresponds to its letter position (i.e.: A=0, B=1, C=2, etc.)
     *
     *       A
     *       |
     *   C---B----E
     *   |        |
     *   D        F
     *            |
     *          G---H
     *
     * Vertices F, G, and H are descendants of E but all other vertices are not. The vertexDescendants bitmask for
     * vertex E is therefore 11100000b or 0xE0. When the first vertex E task request arrives we need to disallow
     * requests for all descendants of E. That is accomplished by iterating through the request stats for all lower
     * priority requests and clearing the allowedVertex bits corresponding to the descendants,
     * i.e: allowedVertices = allowedVertices & ~descendants
     */
    private void addVertexToRequestStats(Priority priority, RequestPriorityStats stats, Integer vertexIndexInt) {
      // Creating a new vertex entry for this priority, so the allowed vertices for all
      // lower priorities need to be updated based on the descendants of the new vertex.
      stats.vertexTaskCount.put(vertexIndexInt, new MutableInt(1));
      int vertexIndex = vertexIndexInt;
      stats.vertices.set(vertexIndex);
      BitSet d = vertexDescendants.get(vertexIndex);
      stats.descendants.or(d);
      for (RequestPriorityStats lowerStat : priorityStats.tailMap(priority, false).values()) {
        lowerStat.allowedVertices.andNot(d);
      }
    }

    /**
     * Removes a vertex from a RequestPriorityStats.
     *
     * Removing a vertex is more expensive than adding a vertex. The stats contain bitmasks which only store on/off
     * values rather than reference counts. Therefore we must rebuild the descendants bitmasks from the remaining
     * vertices in the request stats. Once the new descendants mask is computed we then need to rebuild the
     * allowedVertices BitSet for all lower priority request stats in case the removal of this vertex unblocks lower
     * priority requests of a descendant vertex.
     *
     * Rebuilding allowedVertices for the lower priorities involves starting with the allowedVertices mask at the
     * current priority then masking off the descendants at each priority level encountered, accumulating the results.
     * Any descendants of a level will be blocked at all lower levels. See the addVertexToRequestStats documentation
     * for details on how vertices map to the descendants and allowedVertices bit masks.
     */
    private void removeVertexFromRequestStats(Priority priority, RequestPriorityStats stats, Integer vertexIndexInt) {
      stats.vertexTaskCount.remove(vertexIndexInt);
      int vertexIndex = vertexIndexInt;
      stats.vertices.clear(vertexIndex);

      // Rebuild the descendants BitSet for the remaining vertices at this priority.
      stats.descendants.clear();
      for (Integer vIndex : stats.vertexTaskCount.keySet()) {
        stats.descendants.or(vertexDescendants.get(vIndex));
      }

      // The allowedVertices for all lower priorities need to be recalculated where the vertex descendants at each
      // level are removed from the list of allowed vertices at all subsequent levels.
      Collection<RequestPriorityStats> tailStats = priorityStats.tailMap(priority, false).values();
      if (!tailStats.isEmpty()) {
        BitSet cumulativeAllowed = new BitSet(vertexDescendants.size());
        cumulativeAllowed.or(stats.allowedVertices);
        cumulativeAllowed.andNot(stats.descendants);
        for (RequestPriorityStats s : tailStats) {
          s.allowedVertices.clear();
          s.allowedVertices.or(cumulativeAllowed);
          cumulativeAllowed.andNot(s.descendants);
        }
      }
    }

    @GuardedBy("DagAwareYarnTaskScheduler.this")
    boolean isPreemptionDeadlineExpired() {
      return highestPriorityWaitTimestamp != 0
          && now() - highestPriorityWaitTimestamp > preemptionMaxWaitTime;
    }

    @GuardedBy("DagAwareYarnTaskScheduler.this")
    boolean fitsHighestPriorityRequest(Resource freeResources) {
      if (priorityStats.isEmpty()) {
        return true;
      }
      Priority priority = priorityStats.firstKey();
      List<? extends Collection> requestsList = client.getMatchingRequests(
          priority, ResourceRequest.ANY, freeResources);
      return !requestsList.isEmpty();
    }

    @GuardedBy("DagAwareYarnTaskScheduler.this")
    Resource getAmountToPreempt(int preemptionPercentage) {
      if (priorityStats.isEmpty()) {
        return Resources.none();
      }
      Priority priority = priorityStats.firstKey();
      List<? extends Collection<TaskRequest>> requestsList = client.getMatchingRequests(
          priority, ResourceRequest.ANY, Resources.unbounded());
      int numRequests = 0;
      for (Collection<TaskRequest> requests : requestsList) {
        numRequests += requests.size();
      }
      numRequests = (int) Math.ceil(numRequests * (preemptionPercentage / 100.f));
      Resource toPreempt = Resource.newInstance(0, 0);
      if (numRequests != 0) {
        outer_loop:
        for (Collection<TaskRequest> requests : requestsList) {
          for (TaskRequest request : requests) {
            Resources.addTo(toPreempt, request.getCapability());
            if (--numRequests == 0) {
              break outer_loop;
            }
          }
        }
      }
      return toPreempt;
    }

    // Create a new BitSet that represents all of the vertices that should not be
    // scheduled due to outstanding requests from higher priority predecessor vertices.
    @GuardedBy("DagAwareYarnTaskScheduler.this")
    BitSet createVertexBlockedSet() {
      BitSet blocked = new BitSet();
      Entry<Priority, RequestPriorityStats> entry = priorityStats.lastEntry();
      if (entry != null) {
        RequestPriorityStats stats = entry.getValue();
        blocked.or(stats.allowedVertices);
        blocked.flip(0, blocked.length());
        blocked.or(stats.descendants);
      }
      return blocked;
    }
  }

  /**
   * Tracks statistics on vertices that are requesting tasks at a particular priority
   */
  private static class RequestPriorityStats {
    /** Map from vertex ID to number of task requests for that vertex */
    final Map<Integer, MutableInt> vertexTaskCount = new HashMap<>();
    /** BitSet of vertices that have oustanding requests at this priority */
    final BitSet vertices;
    /** BitSet of vertices that are descendants of this vertex */
    final BitSet descendants;
    /**
     * BitSet of vertices that are allowed to be scheduled at this priority
     * (i.e.: no oustanding predecessors requesting at higher priorities)
     */
    final BitSet allowedVertices;
    int requestCount = 0;
    int localityCount = 0;

    RequestPriorityStats(int numTotalVertices, BitSet allowedVertices) {
      this.vertices = new BitSet(numTotalVertices);
      this.descendants = new BitSet(numTotalVertices);
      this.allowedVertices = allowedVertices;
    }
  }

  /**
   * Tracks idle containers and facilitates faster matching of task requests
   * against those containers given a desired location.
   */
  private static class IdleContainerTracker {
    /**
     * Map of location ID (e.g.: a specific host, rack, or ANY) to set of
     * idle containers matching that location
     */
    final Map<String, Set<HeldContainer>> containersByLocation = new HashMap<>();
    int numContainers = 0;

    @GuardedBy("DagAwareYarnTaskScheduler.this")
    void add(HeldContainer hc) {
      add(hc, hc.getHost());
      add(hc, hc.getRack());
      add(hc, ResourceRequest.ANY);
      ++numContainers;
    }

    @GuardedBy("DagAwareYarnTaskScheduler.this")
    void remove(HeldContainer hc) {
      remove(hc, hc.getHost());
      remove(hc, hc.getRack());
      remove(hc, ResourceRequest.ANY);
      --numContainers;
    }

    @GuardedBy("DagAwareYarnTaskScheduler.this")
    int getNumContainers() {
      return numContainers;
    }

    private void add(HeldContainer hc, String location) {
      Set<HeldContainer> containers = containersByLocation.get(location);
      if (containers == null) {
        containers = new HashSet<>();
        containersByLocation.put(location, containers);
      }
      containers.add(hc);
    }

    private void remove(HeldContainer hc, String location) {
      Set<HeldContainer> containers = containersByLocation.get(location);
      if (containers != null) {
        if (containers.remove(hc) && containers.isEmpty()) {
          containersByLocation.remove(location);
        }
      }
    }

    @GuardedBy("DagAwareYarnTaskScheduler.this")
    @Nullable
    Set<HeldContainer> getByLocation(String location) {
      return containersByLocation.get(location);
    }
  }

  private interface ResourceCalculator {
    boolean anyAvailable(Resource rsrc);
    void deductFrom(Resource total, Resource toSubtract);
  }

  /**
   * ResourceCalculator for memory-only allocation
   */
  private static class MemResourceCalculator implements ResourceCalculator {

    @Override
    public boolean anyAvailable(Resource rsrc) {
      return rsrc.getMemory() > 0;
    }

    @Override
    public void deductFrom(Resource total, Resource toSubtract) {
      total.setMemory(total.getMemory() - toSubtract.getMemory());
    }
  }

  /**
   * ResourceCalculator for memory and vcore allocation
   */
  private static class MemCpuResourceCalculator extends MemResourceCalculator {

    @Override
    public boolean anyAvailable(Resource rsrc) {
      return super.anyAvailable(rsrc) || rsrc.getVirtualCores() > 0;
    }

    @Override
    public void deductFrom(Resource total, Resource toSubtract) {
      super.deductFrom(total, toSubtract);
      total.setVirtualCores(total.getVirtualCores() - toSubtract.getVirtualCores());
    }
  }

  /**
   * Scheduled thread pool executor that logs any errors that escape the worker thread.
   * This can be replaced with HadoopThreadPoolExecutor once Tez requires Hadoop 2.8 or later.
   */
  static class ReuseContainerExecutor extends ScheduledThreadPoolExecutor {
    ReuseContainerExecutor() {
      super(1, new ThreadFactoryBuilder().setNameFormat("ReuseContainerExecutor #%d").build());
      setRemoveOnCancelPolicy(true);
      setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
      super.afterExecute(r, t);

      if (t == null && r instanceof Future<?>) {
        try {
          ((Future<?>) r).get();
        } catch (ExecutionException ee) {
          LOG.warn("Execution exception when running task in {}",  Thread.currentThread().getName());
          t = ee.getCause();
        } catch (InterruptedException ie) {
          LOG.warn("Thread ({}) interrupted: ", Thread.currentThread(), ie);
          Thread.currentThread().interrupt();
        } catch (Throwable throwable) {
          t = throwable;
        }
      }

      if (t != null) {
        LOG.warn("Caught exception in thread {}", Thread.currentThread().getName(), t);
      }
    }
  }
}
