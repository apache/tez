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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.rm.YarnTaskSchedulerService.CookieContainerRequest;
import org.apache.tez.dag.app.rm.TaskSchedulerService.TaskSchedulerAppCallback;
import org.apache.tez.dag.app.rm.container.ContainerSignatureMatcher;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

class TestTaskSchedulerHelpers {

  // Mocking AMRMClientImpl to make use of getMatchingRequest
  static class AMRMClientForTest extends AMRMClientImpl<CookieContainerRequest> {

    @Override
    protected void serviceStart() {
    }

    @Override
    protected void serviceStop() {
    }
  }


  // Mocking AMRMClientAsyncImpl to make use of getMatchingRequest
  static class AMRMClientAsyncForTest extends
      TezAMRMClientAsync<CookieContainerRequest> {

    public AMRMClientAsyncForTest(
        AMRMClient<CookieContainerRequest> client,
        int intervalMs) {
      // CallbackHandler is not needed - will be called independently in the test.
      super(client, intervalMs, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public RegisterApplicationMasterResponse registerApplicationMaster(
        String appHostName, int appHostPort, String appTrackingUrl) {
      RegisterApplicationMasterResponse mockRegResponse = mock(RegisterApplicationMasterResponse.class);
      Resource mockMaxResource = mock(Resource.class);
      Map<ApplicationAccessType, String> mockAcls = mock(Map.class);
      when(mockRegResponse.getMaximumResourceCapability()).thenReturn(
          mockMaxResource);
      when(mockRegResponse.getApplicationACLs()).thenReturn(mockAcls);
      return mockRegResponse;
    }

    @Override
    public void unregisterApplicationMaster(FinalApplicationStatus appStatus,
        String appMessage, String appTrackingUrl) {
    }

    @Override
    protected void serviceStart() {
    }

    @Override
    protected void serviceStop() {
    }
  }
  
  // Overrides start / stop. Will be controlled without the extra event handling thread.
  static class TaskSchedulerEventHandlerForTest extends
      TaskSchedulerEventHandler {

    private TezAMRMClientAsync<CookieContainerRequest> amrmClientAsync;
    private ContainerSignatureMatcher containerSignatureMatcher;

    @SuppressWarnings("rawtypes")
    public TaskSchedulerEventHandlerForTest(AppContext appContext,
        EventHandler eventHandler,
        TezAMRMClientAsync<CookieContainerRequest> amrmClientAsync,
        ContainerSignatureMatcher containerSignatureMatcher) {
      super(appContext, null, eventHandler, containerSignatureMatcher, null);
      this.amrmClientAsync = amrmClientAsync;
      this.containerSignatureMatcher = containerSignatureMatcher;
    }

    @Override
    public TaskSchedulerService createTaskScheduler(String host, int port,
        String trackingUrl, AppContext appContext) {
      return new TaskSchedulerWithDrainableAppCallback(this,
          containerSignatureMatcher, host, port, trackingUrl, amrmClientAsync,
          appContext);
    }

    public TaskSchedulerService getSpyTaskScheduler() {
      return this.taskScheduler;
    }

    @Override
    public void serviceStart() {
      TaskSchedulerService taskSchedulerReal = createTaskScheduler("host", 0, "",
        appContext);
      // Init the service so that reuse configuration is picked up.
      ((AbstractService)taskSchedulerReal).init(getConfig());
      ((AbstractService)taskSchedulerReal).start();
      taskScheduler = spy(taskSchedulerReal);
    }

    @Override
    public void serviceStop() {
    }
  }

  @SuppressWarnings("rawtypes")
  static class CapturingEventHandler implements EventHandler {

    private List<Event> events = new LinkedList<Event>();


    public void handle(Event event) {
      events.add(event);
    }

    public void reset() {
      events.clear();
    }

    public void verifyNoInvocations(Class<? extends Event> eventClass) {
      for (Event e : events) {
        assertFalse(e.getClass().getName().equals(eventClass.getName()));
      }
    }

    public Event verifyInvocation(Class<? extends Event> eventClass) {
      for (Event e : events) {
        if (e.getClass().getName().equals(eventClass.getName())) {
          return e;
        }
      }
      fail("Expected Event: " + eventClass.getName() + " not sent");
      return null;
    }
  }

  static class TaskSchedulerWithDrainableAppCallback extends YarnTaskSchedulerService {

    private TaskSchedulerAppCallbackDrainable drainableAppCallback;

    public TaskSchedulerWithDrainableAppCallback(
        TaskSchedulerAppCallback appClient,
        ContainerSignatureMatcher containerSignatureMatcher,
        String appHostName, int appHostPort, String appTrackingUrl,
        AppContext appContext) {
      super(appClient, containerSignatureMatcher, appHostName, appHostPort,
          appTrackingUrl, appContext);
      shouldUnregister.set(true);
    }

    public TaskSchedulerWithDrainableAppCallback(
        TaskSchedulerAppCallback appClient,
        ContainerSignatureMatcher containerSignatureMatcher,
        String appHostName, int appHostPort, String appTrackingUrl,
        TezAMRMClientAsync<CookieContainerRequest> client,
        AppContext appContext) {
      super(appClient, containerSignatureMatcher, appHostName, appHostPort,
          appTrackingUrl, client, appContext);
      shouldUnregister.set(true);
    }

    @Override
    TaskSchedulerAppCallback createAppCallbackDelegate(
        TaskSchedulerAppCallback realAppClient) {
      drainableAppCallback = new TaskSchedulerAppCallbackDrainable(
          new TaskSchedulerAppCallbackWrapper(realAppClient,
              appCallbackExecutor));
      return drainableAppCallback;
    }
    
    @Override
    ExecutorService createAppCallbackExecutorService() {
      ExecutorService real = super.createAppCallbackExecutorService();
      return new CountingExecutorService(real);
    }

    public TaskSchedulerAppCallbackDrainable getDrainableAppCallback() {
      return drainableAppCallback;
    }
  }

  @SuppressWarnings("rawtypes")
  static class TaskSchedulerAppCallbackDrainable implements TaskSchedulerAppCallback {
    int completedEvents;
    int invocations;
    private TaskSchedulerAppCallback real;
    private CountingExecutorService countingExecutorService;
    final AtomicInteger count = new AtomicInteger(0);
    
    public TaskSchedulerAppCallbackDrainable(TaskSchedulerAppCallbackWrapper real) {
      countingExecutorService = (CountingExecutorService) real.executorService;
      this.real = real;
    }

    @Override
    public void taskAllocated(Object task, Object appCookie, Container container) {
      count.incrementAndGet();
      invocations++;
      real.taskAllocated(task, appCookie, container);
    }

    @Override
    public void containerCompleted(Object taskLastAllocated,
        ContainerStatus containerStatus) {
      invocations++;
      real.containerCompleted(taskLastAllocated, containerStatus);
    }

    @Override
    public void containerBeingReleased(ContainerId containerId) {
      invocations++;
      real.containerBeingReleased(containerId);
    }

    @Override
    public void nodesUpdated(List<NodeReport> updatedNodes) {
      invocations++;
      real.nodesUpdated(updatedNodes);
    }

    @Override
    public void appShutdownRequested() {
      invocations++;
      real.appShutdownRequested();
    }

    @Override
    public void setApplicationRegistrationData(Resource maxContainerCapability,
        Map<ApplicationAccessType, String> appAcls, ByteBuffer key) {
      invocations++;
      real.setApplicationRegistrationData(maxContainerCapability, appAcls, key);
    }

    @Override
    public void onError(Throwable t) {
      invocations++;
      real.onError(t);
    }

    @Override
    public float getProgress() {
      invocations++;
      return real.getProgress();
    }

    @Override
    public AppFinalStatus getFinalAppStatus() {
      invocations++;
      return real.getFinalAppStatus();
    }

    @Override
    public void preemptContainer(ContainerId cId) {
      invocations++;
      real.preemptContainer(cId);
    }

    public void drain() throws InterruptedException, ExecutionException {
      while (completedEvents < invocations) {
        Future f = countingExecutorService.completionService.poll(5000l, TimeUnit.MILLISECONDS);
        if (f != null) {
          completedEvents++;
        } else {
          fail("Timed out while trying to drain queue");
        }
      }
    }
  }

  static class AlwaysMatchesContainerMatcher implements ContainerSignatureMatcher {

    @Override
    public boolean isSuperSet(Object cs1, Object cs2) {
      Preconditions.checkNotNull(cs1, "Arguments cannot be null");
      Preconditions.checkNotNull(cs2, "Arguments cannot be null");
      return true;
    }

    @Override
    public boolean isExactMatch(Object cs1, Object cs2) {
      return true;
    }

    @Override
    public Map<String, LocalResource> getAdditionalResources(Map<String, LocalResource> lr1,
        Map<String, LocalResource> lr2) {
      return Maps.newHashMap();
    }
  }
  
  static class PreemptionMatcher implements ContainerSignatureMatcher {
    @Override
    public boolean isSuperSet(Object cs1, Object cs2) {
      Preconditions.checkNotNull(cs1, "Arguments cannot be null");
      Preconditions.checkNotNull(cs2, "Arguments cannot be null");
      return true;
    }

    @Override
    public boolean isExactMatch(Object cs1, Object cs2) {
      if (cs1 == cs2 && cs1 != null) {
        return true;
      }
      return false;
    }

    @Override
    public Map<String, LocalResource> getAdditionalResources(Map<String, LocalResource> lr1,
        Map<String, LocalResource> lr2) {
      return Maps.newHashMap();
    }
  }
  

  static void waitForDelayedDrainNotify(AtomicBoolean drainNotifier)
      throws InterruptedException {
    synchronized (drainNotifier) {
      while (!drainNotifier.get()) {
        drainNotifier.wait();
      }
    }
  }
  
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static class CountingExecutorService implements ExecutorService {

    final ExecutorService real;
    final CompletionService completionService;

    CountingExecutorService(ExecutorService real) {
      this.real = real;
      completionService = new ExecutorCompletionService(real);
    }

    @Override
    public void execute(Runnable command) {
      throw new UnsupportedOperationException("Not expected to be used");
    }

    @Override
    public void shutdown() {
      real.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      return real.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
      return real.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return real.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return real.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
      return completionService.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
      return completionService.submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
      throw new UnsupportedOperationException("Not expected to be used");
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
        throws InterruptedException {
      throw new UnsupportedOperationException("Not expected to be used");
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout,
        TimeUnit unit) throws InterruptedException {
      throw new UnsupportedOperationException("Not expected to be used");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException,
        ExecutionException {
      throw new UnsupportedOperationException("Not expected to be used");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      throw new UnsupportedOperationException("Not expected to be used");
    }
    
  }

}
