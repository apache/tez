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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.google.common.primitives.Ints;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.rm.container.ContainerSignatureMatcher;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LocalTaskSchedulerService extends TaskSchedulerService {

  private static final Log LOG = LogFactory.getLog(LocalTaskSchedulerService.class);

  final TaskSchedulerAppCallback realAppClient;
  final TaskSchedulerAppCallback appClientDelegate;
  final ContainerSignatureMatcher containerSignatureMatcher;
  final PriorityBlockingQueue<TaskRequest> taskRequestQueue;
  AsyncDelegateRequestHandler taskRequestHandler;
  Thread asyncDelegateRequestThread;
  final ExecutorService appCallbackExecutor;

  final HashMap<Object, Container> taskAllocations;
  final String appHostName;
  final int appHostPort;
  final String appTrackingUrl;
  final AppContext appContext;

  public LocalTaskSchedulerService(TaskSchedulerAppCallback appClient,
      ContainerSignatureMatcher containerSignatureMatcher, String appHostName,
      int appHostPort, String appTrackingUrl, AppContext appContext) {
    super(LocalTaskSchedulerService.class.getName());
    this.realAppClient = appClient;
    this.appCallbackExecutor = createAppCallbackExecutorService();
    this.containerSignatureMatcher = containerSignatureMatcher;
    this.appClientDelegate = createAppCallbackDelegate(appClient);
    this.appHostName = appHostName;
    this.appHostPort = appHostPort;
    this.appTrackingUrl = appTrackingUrl;
    this.appContext = appContext;
    taskRequestQueue = new PriorityBlockingQueue<TaskRequest>();
    taskAllocations = new LinkedHashMap<Object, Container>();
  }

  private ExecutorService createAppCallbackExecutorService() {
    return Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("TaskSchedulerAppCaller #%d").setDaemon(true).build());
  }

  private TaskSchedulerAppCallback createAppCallbackDelegate(
      TaskSchedulerAppCallback realAppClient) {
    return new TaskSchedulerAppCallbackWrapper(realAppClient,
        appCallbackExecutor);
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
  public void resetMatchLocalityForAllHeldContainers() {
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
  public boolean deallocateTask(Object task, boolean taskSucceeded) {
    return taskRequestHandler.addDeallocateTaskRequest(task);
  }

  @Override
  public Object deallocateContainer(ContainerId containerId) {
    return null;
  }

  @Override
  public void serviceInit(Configuration conf) {
      taskRequestHandler = new AsyncDelegateRequestHandler(taskRequestQueue,
        new LocalContainerFactory(appContext),
        taskAllocations,
        appClientDelegate,
        conf);
    asyncDelegateRequestThread = new Thread(taskRequestHandler);
  }

  @Override
  public void serviceStart() {
    asyncDelegateRequestThread.start();
  }

  @Override
  public void serviceStop() throws InterruptedException {
    if (asyncDelegateRequestThread != null) {
      asyncDelegateRequestThread.interrupt();
    }
    appCallbackExecutor.shutdownNow();
    appCallbackExecutor.awaitTermination(1000l, TimeUnit.MILLISECONDS);
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
    final AppContext appContext;
    AtomicInteger nextId;

    public LocalContainerFactory(AppContext appContext) {
      this.appContext = appContext;
      this.nextId = new AtomicInteger(1);
    }

    public Container createContainer(Resource capability, Priority priority) {
      ApplicationAttemptId appAttemptId = appContext.getApplicationAttemptId();
      ContainerId containerId = ContainerId.newInstance(appAttemptId, nextId.getAndIncrement());
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

  static class TaskRequest implements Comparable<TaskRequest> {
    // Higher prority than Priority.UNDEFINED
    static final int HIGHEST_PRIORITY = -2;
    Object task;
    Priority priority;

    public TaskRequest(Object task, Priority priority) {
      this.task = task;
      this.priority = priority;
    }

    @Override
    public int compareTo(TaskRequest request) {
      return request.priority.compareTo(this.priority);
    }
  }

  static class AllocateTaskRequest extends TaskRequest {
    Resource capability;
    Object clientCookie;

    public AllocateTaskRequest(Object task, Resource capability, Priority priority,
        Object clientCookie) {
      super(task, priority);
      this.capability = capability;
      this.clientCookie = clientCookie;
    }
  }

  static class DeallocateTaskRequest extends TaskRequest {
    static final Priority DEALLOCATE_PRIORITY = Priority.newInstance(HIGHEST_PRIORITY);

    public DeallocateTaskRequest(Object task) {
      super(task, DEALLOCATE_PRIORITY);
    }
  }

  static class AsyncDelegateRequestHandler implements Runnable {
    final BlockingQueue<TaskRequest> taskRequestQueue;
    final LocalContainerFactory localContainerFactory;
    final HashMap<Object, Container> taskAllocations;
    final TaskSchedulerAppCallback appClientDelegate;
    final int MAX_TASKS;

    AsyncDelegateRequestHandler(BlockingQueue<TaskRequest> taskRequestQueue,
        LocalContainerFactory localContainerFactory,
        HashMap<Object, Container> taskAllocations,
        TaskSchedulerAppCallback appClientDelegate,
        Configuration conf) {
      this.taskRequestQueue = taskRequestQueue;
      this.localContainerFactory = localContainerFactory;
      this.taskAllocations = taskAllocations;
      this.appClientDelegate = appClientDelegate;
      this.MAX_TASKS = conf.getInt(TezConfiguration.TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS,
          TezConfiguration.TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS_DEFAULT);
    }

    public void addAllocateTaskRequest(Object task, Resource capability, Priority priority,
        Object clientCookie) {
      try {
        taskRequestQueue.put(new AllocateTaskRequest(task, capability, priority, clientCookie));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    public boolean addDeallocateTaskRequest(Object task) {
      try {
        taskRequestQueue.put(new DeallocateTaskRequest(task));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      synchronized(taskRequestQueue) {
        taskRequestQueue.notify();
      }
      return true;
    }

    boolean shouldWait() {
      return taskAllocations.size() >= MAX_TASKS;
    }

    @Override
    public void run() {
      while(true) {
        synchronized(taskRequestQueue) {
          try {
            if (shouldWait()) {
              taskRequestQueue.wait();
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        processRequest();
      }
    }

    void processRequest() {
        try {
          TaskRequest request = taskRequestQueue.take();
          if (request instanceof AllocateTaskRequest) {
            allocateTask((AllocateTaskRequest)request);
          }
          else if (request instanceof DeallocateTaskRequest) {
            deallocateTask((DeallocateTaskRequest)request);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (NullPointerException e) {
          LOG.warn("Task request was badly constructed");
        }
    }

    void allocateTask(AllocateTaskRequest request) {
      Container container = localContainerFactory.createContainer(request.capability,
          request.priority);
      taskAllocations.put(request.task, container);
      appClientDelegate.taskAllocated(request.task, request.clientCookie, container);
    }

    void deallocateTask(DeallocateTaskRequest request) {
      Container container = taskAllocations.remove(request.task);
      if (container != null) {
        appClientDelegate.containerBeingReleased(container.getId());
      }
      else {
        LOG.warn("Unable to find and remove task " + request.task + " from task allocations");
      }
    }
  }
}
