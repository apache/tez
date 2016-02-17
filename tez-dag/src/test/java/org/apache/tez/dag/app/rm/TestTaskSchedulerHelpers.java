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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
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
import org.apache.tez.common.ContainerSignatureMatcher;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ServicePluginLifecycleAbstractService;
import org.apache.tez.dag.app.rm.YarnTaskSchedulerService.CookieContainerRequest;
import org.apache.tez.serviceplugins.api.DagInfo;
import org.apache.tez.serviceplugins.api.ServicePluginError;
import org.apache.tez.serviceplugins.api.TaskScheduler;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;

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
  static class TaskSchedulerManagerForTest extends
      TaskSchedulerManager {

    private TezAMRMClientAsync<CookieContainerRequest> amrmClientAsync;
    private ContainerSignatureMatcher containerSignatureMatcher;
    private UserPayload defaultPayload;

    @SuppressWarnings("rawtypes")
    public TaskSchedulerManagerForTest(AppContext appContext,
                                       EventHandler eventHandler,
                                       TezAMRMClientAsync<CookieContainerRequest> amrmClientAsync,
                                       ContainerSignatureMatcher containerSignatureMatcher,
                                       UserPayload defaultPayload) {
      super(appContext, null, eventHandler, containerSignatureMatcher, null,
          Lists.newArrayList(new NamedEntityDescriptor("FakeScheduler", null)),
          false);
      this.amrmClientAsync = amrmClientAsync;
      this.containerSignatureMatcher = containerSignatureMatcher;
      this.defaultPayload = defaultPayload;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void instantiateSchedulers(String host, int port, String trackingUrl,
                                      AppContext appContext) {
      TaskSchedulerContext taskSchedulerContext =
          new TaskSchedulerContextImpl(this, appContext, 0, trackingUrl, 1000, host, port,
              defaultPayload);
      TaskSchedulerContextImplWrapper wrapper =
          new TaskSchedulerContextImplWrapper(taskSchedulerContext,
              new CountingExecutorService(appCallbackExecutor));
      TaskSchedulerContextDrainable drainable = new TaskSchedulerContextDrainable(wrapper);

      taskSchedulers[0] = new TaskSchedulerWrapper(
          new TaskSchedulerWithDrainableContext(drainable, amrmClientAsync));
      taskSchedulerServiceWrappers[0] =
          new ServicePluginLifecycleAbstractService(taskSchedulers[0].getTaskScheduler());
    }

    public TaskScheduler getSpyTaskScheduler() {
      return taskSchedulers[0].getTaskScheduler();
    }

    @Override
    public void serviceStart() {
      instantiateSchedulers("host", 0, "", appContext);
      // Init the service so that reuse configuration is picked up.
      ((AbstractService)taskSchedulerServiceWrappers[0]).init(getConfig());
      ((AbstractService)taskSchedulerServiceWrappers[0]).start();
      // For some reason, the spy needs to be setup after sertvice startup.
      taskSchedulers[0] = new TaskSchedulerWrapper(spy(taskSchedulers[0].getTaskScheduler()));

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

  static class TaskSchedulerWithDrainableContext extends YarnTaskSchedulerService {


    public TaskSchedulerWithDrainableContext(
        TaskSchedulerContextDrainable appClient,
        TezAMRMClientAsync<CookieContainerRequest> client) {
      super(appClient, client);
      shouldUnregister.set(true);
    }

    public TaskSchedulerContextDrainable getDrainableAppCallback() {
      return (TaskSchedulerContextDrainable)getContext();
    }
  }

  @SuppressWarnings("rawtypes")
  static class TaskSchedulerContextDrainable implements TaskSchedulerContext {
    int completedEvents;
    int invocations;
    private TaskSchedulerContext real;
    private CountingExecutorService countingExecutorService;
    final AtomicInteger count = new AtomicInteger(0);
    
    public TaskSchedulerContextDrainable(TaskSchedulerContextImplWrapper real) {
      countingExecutorService = (CountingExecutorService) real.getExecutorService();
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
    public void reportError(@Nonnull ServicePluginError servicePluginError, String message,
                            DagInfo dagInfo) {
      invocations++;
      real.reportError(servicePluginError, message, dagInfo);
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

    // Not incrementing invocations for methods which to not obtain locks,
    // and do not go via the executor service.
    @Override
    public UserPayload getInitialUserPayload() {
      return real.getInitialUserPayload();
    }

    @Override
    public String getAppTrackingUrl() {
      return real.getAppTrackingUrl();
    }

    @Override
    public long getCustomClusterIdentifier() {
      return real.getCustomClusterIdentifier();
    }

    @Override
    public ContainerSignatureMatcher getContainerSignatureMatcher() {
      return real.getContainerSignatureMatcher();
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
      return real.getApplicationAttemptId();
    }

    @Nullable
    @Override
    public DagInfo getCurrentDagInfo() {
      return real.getCurrentDagInfo();
    }

    @Override
    public String getAppHostName() {
      return real.getAppHostName();
    }

    @Override
    public int getAppClientPort() {
      return real.getAppClientPort();
    }

    @Override
    public boolean isSession() {
      return real.isSession();
    }

    @Override
    public AMState getAMState() {
      return real.getAMState();
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

    @Override
    public Object union(Object cs1, Object cs2) {
      return cs1;
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

    @Override
    public Object union(Object cs1, Object cs2) {
      return cs1;
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

  static CountingExecutorService createCountingExecutingService(ExecutorService rawExecutor) {
    return new CountingExecutorService(rawExecutor);
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

  static TaskSchedulerContext setupMockTaskSchedulerContext(String appHost, int appPort,
                                                            String appUrl, Configuration conf) {
    return setupMockTaskSchedulerContext(appHost, appPort, appUrl, false, conf);
  }

  static TaskSchedulerContext setupMockTaskSchedulerContext(String appHost, int appPort,
                                                            String appUrl, boolean isSession,
                                                            Configuration conf) {
    return setupMockTaskSchedulerContext(appHost, appPort, appUrl, isSession, null, null, null,
        conf);
  }

  static TaskSchedulerContext setupMockTaskSchedulerContext(String appHost, int appPort,
                                                            String appUrl, boolean isSession,
                                                            ApplicationAttemptId appAttemptId,
                                                            Long customAppIdentifier,
                                                            ContainerSignatureMatcher containerSignatureMatcher,
                                                            Configuration conf) {

    TaskSchedulerContext mockContext = mock(TaskSchedulerContext.class);
    when(mockContext.getAppHostName()).thenReturn(appHost);
    when(mockContext.getAppClientPort()).thenReturn(appPort);
    when(mockContext.getAppTrackingUrl()).thenReturn(appUrl);

    when(mockContext.getAMState()).thenReturn(TaskSchedulerContext.AMState.RUNNING_APP);
    UserPayload userPayload;
    try {
      userPayload = TezUtils.createUserPayloadFromConf(conf);
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    when(mockContext.getInitialUserPayload()).thenReturn(userPayload);
    when(mockContext.isSession()).thenReturn(isSession);
    if (containerSignatureMatcher != null) {
      when(mockContext.getContainerSignatureMatcher())
          .thenReturn(containerSignatureMatcher);
    } else {
      when(mockContext.getContainerSignatureMatcher())
          .thenReturn(new AlwaysMatchesContainerMatcher());
    }
    if (appAttemptId != null) {
      when(mockContext.getApplicationAttemptId()).thenReturn(appAttemptId);
    }
    if (customAppIdentifier != null) {
      when(mockContext.getCustomClusterIdentifier()).thenReturn(customAppIdentifier);
    }

    return mockContext;
  }

}
