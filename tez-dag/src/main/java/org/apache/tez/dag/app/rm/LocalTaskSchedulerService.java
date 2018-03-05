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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

import com.google.common.primitives.Ints;

import org.apache.tez.common.TezUtils;
import org.apache.tez.serviceplugins.api.DagInfo;
import org.apache.tez.serviceplugins.api.TaskScheduler;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.common.ContainerSignatureMatcher;

public class LocalTaskSchedulerService extends TaskScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(LocalTaskSchedulerService.class);

  final ContainerSignatureMatcher containerSignatureMatcher;
  final LinkedBlockingQueue<SchedulerRequest> taskRequestQueue;
  final Configuration conf;
  AsyncDelegateRequestHandler taskRequestHandler;
  Thread asyncDelegateRequestThread;

  final HashMap<Object, AllocatedTask> taskAllocations;
  final String appTrackingUrl;
  final long customContainerAppId;

  public LocalTaskSchedulerService(TaskSchedulerContext taskSchedulerContext) {
    super(taskSchedulerContext);
    taskRequestQueue = new LinkedBlockingQueue<>();
    taskAllocations = new LinkedHashMap<>();
    this.appTrackingUrl = taskSchedulerContext.getAppTrackingUrl();
    this.containerSignatureMatcher = taskSchedulerContext.getContainerSignatureMatcher();
    this.customContainerAppId = taskSchedulerContext.getCustomClusterIdentifier();
    try {
      this.conf = TezUtils.createConfFromUserPayload(taskSchedulerContext.getInitialUserPayload());
    } catch (IOException e) {
      throw new TezUncheckedException(
          "Failed to deserialize payload for " + LocalTaskSchedulerService.class.getSimpleName(),
          e);
    }
  }

  @Override
  public Resource getAvailableResources() {
    long memory = Runtime.getRuntime().freeMemory();
    int cores = Runtime.getRuntime().availableProcessors();
    return createResource(memory, cores);
  }

  static Resource createResource(long runtimeMemory, int core) {
    if (runtimeMemory < 0 || core < 0) {
      throw new IllegalArgumentException("Negative Memory or Core provided!"
          + "mem: "+runtimeMemory+" core:"+core);
    }
    return Resource.newInstance(Ints.checkedCast(runtimeMemory/(1024*1024)), core);
  }

  @Override
  public int getClusterNodeCount() {
    return 1;
  }

  @Override
  public void dagComplete() {
    taskRequestHandler.dagComplete();
  }

  @Override
  public Resource getTotalResources() {
    long memory = Runtime.getRuntime().maxMemory();
    int cores = Runtime.getRuntime().availableProcessors();
    return createResource(memory, cores);
  }

  @Override
  public void blacklistNode(NodeId nodeId) {
  }

  @Override
  public void unblacklistNode(NodeId nodeId) {
  }

  @Override
  public void allocateTask(Object task, Resource capability, String[] hosts,
      String[] racks, Priority priority, Object containerSignature,
      Object clientCookie) {
    taskRequestHandler.addAllocateTaskRequest(task, capability, priority, clientCookie);
  }

  @Override
  public synchronized void allocateTask(Object task, Resource capability,
      ContainerId containerId, Priority priority, Object containerSignature,
      Object clientCookie) {
    // in local mode every task is already container level local
    taskRequestHandler.addAllocateTaskRequest(task, capability, priority, clientCookie);
  }

  @Override
  public boolean deallocateTask(Object task, boolean taskSucceeded, TaskAttemptEndReason endReason, String diagnostics) {
    return taskRequestHandler.addDeallocateTaskRequest(task);
  }

  @Override
  public Object deallocateContainer(ContainerId containerId) {
    taskRequestHandler.addDeallocateContainerRequest(containerId);
    return null;
  }

  @Override
  public void initialize() {
    taskRequestHandler = createRequestHandler(conf);
    asyncDelegateRequestThread = new Thread(taskRequestHandler);
    asyncDelegateRequestThread.setName(LocalTaskSchedulerService.class.getSimpleName() + "RequestHandler");
    asyncDelegateRequestThread.setDaemon(true);
  }

  protected AsyncDelegateRequestHandler createRequestHandler(Configuration conf) {
    return new AsyncDelegateRequestHandler(taskRequestQueue,
        new LocalContainerFactory(getContext().getApplicationAttemptId(), customContainerAppId),
        taskAllocations,
        getContext(),
        conf);
  }

  @Override
  public void start() {
    asyncDelegateRequestThread.start();
  }

  @Override
  public void shutdown() throws InterruptedException {
    if (asyncDelegateRequestThread != null) {
      asyncDelegateRequestThread.interrupt();
    }
  }

  @Override
  public void setShouldUnregister() {
  }

  @Override
  public boolean hasUnregistered() {
    // Should always return true as no multiple attempts in local mode
    return true;
  }

  @Override
  public void initiateStop() {

  }

  static class LocalContainerFactory {
    AtomicInteger nextId;
    final ApplicationAttemptId customAppAttemptId;

    public LocalContainerFactory(ApplicationAttemptId appAttemptId, long customAppId) {
      this.nextId = new AtomicInteger(1);
      ApplicationId appId = ApplicationId
          .newInstance(customAppId, appAttemptId.getApplicationId().getId());
      this.customAppAttemptId = ApplicationAttemptId
          .newInstance(appId, appAttemptId.getAttemptId());
    }

    @SuppressWarnings("deprecation")
    public Container createContainer(Resource capability, Priority priority) {
      ContainerId containerId = ContainerId.newInstance(customAppAttemptId, nextId.getAndIncrement());
      NodeId nodeId = NodeId.newInstance("127.0.0.1", 0);
      String nodeHttpAddress = "127.0.0.1:0";

      Container container = Container.newInstance(containerId,
          nodeId,
          nodeHttpAddress,
          capability,
          priority,
          null);

      return container;
    }
  }

  static class SchedulerRequest {
  }

  static class TaskRequest extends SchedulerRequest {
    final Object task;

    public TaskRequest(Object task) {
      this.task = task;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TaskRequest that = (TaskRequest) o;

      if (task != null ? !task.equals(that.task) : that.task != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 7841 + (task != null ? task.hashCode() : 0);
    }

  }

  static class AllocateTaskRequest extends TaskRequest implements Comparable<AllocateTaskRequest> {
    final Priority priority;
    final Resource capability;
    final Object clientCookie;
    final int vertexIndex;

    public AllocateTaskRequest(Object task, int vertexIndex, Resource capability, Priority priority,
                               Object clientCookie) {
      super(task);
      this.priority = priority;
      this.capability = capability;
      this.clientCookie = clientCookie;
      this.vertexIndex = vertexIndex;
    }

    @Override
    public int compareTo(AllocateTaskRequest request) {
      return request.priority.compareTo(this.priority);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      AllocateTaskRequest that = (AllocateTaskRequest) o;

      if (priority != null ? !priority.equals(that.priority) : that.priority != null) {
        return false;
      }

      if (capability != null ? !capability.equals(that.capability) : that.capability != null) {
        return false;
      }
      if (clientCookie != null ? !clientCookie.equals(that.clientCookie) :
          that.clientCookie != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 12329 * result + (priority != null ? priority.hashCode() : 0);
      result = 12329 * result + (capability != null ? capability.hashCode() : 0);
      result = 12329 * result + (clientCookie != null ? clientCookie.hashCode() : 0);
      return result;
    }
  }

  static class DeallocateTaskRequest extends TaskRequest {

    public DeallocateTaskRequest(Object task) {
      super(task);
    }
  }

  static class DeallocateContainerRequest extends SchedulerRequest {
    final ContainerId containerId;

    public DeallocateContainerRequest(ContainerId containerId) {
      this.containerId = containerId;
    }
  }

  static class AllocatedTask {
    final AllocateTaskRequest request;
    final Container container;

    AllocatedTask(AllocateTaskRequest request, Container container) {
      this.request = request;
      this.container = container;
    }
  }

  static class AsyncDelegateRequestHandler implements Runnable {
    final LinkedBlockingQueue<SchedulerRequest> clientRequestQueue;
    final PriorityBlockingQueue<AllocateTaskRequest> taskRequestQueue;
    final LocalContainerFactory localContainerFactory;
    final HashMap<Object, AllocatedTask> taskAllocations;
    final TaskSchedulerContext taskSchedulerContext;
    private final Object descendantsLock = new Object();
    private ArrayList<BitSet> vertexDescendants = null;
    final int MAX_TASKS;

    AsyncDelegateRequestHandler(LinkedBlockingQueue<SchedulerRequest> clientRequestQueue,
        LocalContainerFactory localContainerFactory,
        HashMap<Object, AllocatedTask> taskAllocations,
        TaskSchedulerContext taskSchedulerContext,
        Configuration conf) {
      this.clientRequestQueue = clientRequestQueue;
      this.localContainerFactory = localContainerFactory;
      this.taskAllocations = taskAllocations;
      this.taskSchedulerContext = taskSchedulerContext;
      this.MAX_TASKS = conf.getInt(TezConfiguration.TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS,
          TezConfiguration.TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS_DEFAULT);
      this.taskRequestQueue = new PriorityBlockingQueue<>();
    }

    void dagComplete() {
      synchronized (descendantsLock) {
        vertexDescendants = null;
      }
    }
    private void ensureVertexDescendants() {
      synchronized (descendantsLock) {
        if (vertexDescendants == null) {
          DagInfo info = taskSchedulerContext.getCurrentDagInfo();
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
    }

    public void addAllocateTaskRequest(Object task, Resource capability, Priority priority,
        Object clientCookie) {
      try {
        int vertexIndex = taskSchedulerContext.getVertexIndexForTask(task);
        clientRequestQueue.put(new AllocateTaskRequest(task, vertexIndex, capability, priority, clientCookie));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    public boolean addDeallocateTaskRequest(Object task) {
      try {
        clientRequestQueue.put(new DeallocateTaskRequest(task));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return true;
    }

    public void addDeallocateContainerRequest(ContainerId containerId) {
      try {
        clientRequestQueue.put(new DeallocateContainerRequest(containerId));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    boolean shouldProcess() {
      return !taskRequestQueue.isEmpty() && taskAllocations.size() < MAX_TASKS;
    }

    boolean shouldPreempt() {
      return !taskRequestQueue.isEmpty() && taskAllocations.size() >= MAX_TASKS;
    }

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        dispatchRequest();
        while (shouldProcess()) {
          allocateTask();
        }
      }
    }

    void dispatchRequest() {
      try {
        SchedulerRequest request = clientRequestQueue.take();
        if (request instanceof AllocateTaskRequest) {
          taskRequestQueue.put((AllocateTaskRequest)request);
          if (shouldPreempt()) {
            maybePreempt((AllocateTaskRequest) request);
          }
        }
        else if (request instanceof DeallocateTaskRequest) {
          deallocateTask((DeallocateTaskRequest)request);
        }
        else if (request instanceof DeallocateContainerRequest) {
          preemptTask((DeallocateContainerRequest)request);
        }
        else {
          LOG.error("Unknown task request message: " + request);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    void maybePreempt(AllocateTaskRequest request) {
      Priority priority = request.priority;
      for (Map.Entry<Object, AllocatedTask> entry : taskAllocations.entrySet()) {
        AllocatedTask allocatedTask = entry.getValue();
        Container container = allocatedTask.container;
        if (priority.compareTo(allocatedTask.container.getPriority()) > 0) {
          Object task = entry.getKey();
          ensureVertexDescendants();
          if (vertexDescendants.get(request.vertexIndex).get(allocatedTask.request.vertexIndex)) {
            LOG.info("Preempting task/container for task/priority:"  + task + "/" + container
                + " for " + request.task + "/" + priority);
            taskSchedulerContext.preemptContainer(allocatedTask.container.getId());
          }
        }
      }
    }

    void allocateTask() {
      try {
        AllocateTaskRequest request = taskRequestQueue.take();
        Container container = localContainerFactory.createContainer(request.capability,
            request.priority);
        taskAllocations.put(request.task, new AllocatedTask(request, container));
        taskSchedulerContext.taskAllocated(request.task, request.clientCookie, container);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    void deallocateTask(DeallocateTaskRequest request) {
      AllocatedTask allocatedTask = taskAllocations.remove(request.task);
      if (allocatedTask != null) {
        taskSchedulerContext.containerBeingReleased(allocatedTask.container.getId());
      }
      else {
        Iterator<AllocateTaskRequest> iter = taskRequestQueue.iterator();
        while (iter.hasNext()) {
          TaskRequest taskRequest = iter.next();
          if (taskRequest.task.equals(request.task)) {
            iter.remove();
            LOG.info("Deallocation request before allocation for task:" + request.task);
            break;
          }
        }
      }
    }

    void preemptTask(DeallocateContainerRequest request) {
      LOG.info("Trying to preempt: " + request.containerId);
      Iterator<Map.Entry<Object, AllocatedTask>> entries = taskAllocations.entrySet().iterator();
      while (entries.hasNext()) {
        Map.Entry<Object, AllocatedTask> entry = entries.next();
        Container container = entry.getValue().container;
        if (container.getId().equals(request.containerId)) {
          entries.remove();
          Object task = entry.getKey();
          LOG.info("Preempting task/container:" + task + "/" + container);
          taskSchedulerContext.containerBeingReleased(container.getId());
        }
      }
    }
  }
}
