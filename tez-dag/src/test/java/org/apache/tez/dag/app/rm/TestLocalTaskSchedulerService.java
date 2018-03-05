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

import java.util.BitSet;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.rm.TestLocalTaskSchedulerService.MockLocalTaskSchedulerSerivce.MockAsyncDelegateRequestHandler;
import org.apache.tez.serviceplugins.api.DagInfo;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestLocalTaskSchedulerService {

  LocalTaskSchedulerService ltss ;
  int core =10;

  @Test(timeout = 5000)
  public void testCreateResource() {
    Resource resource;
    //value in integer
    long value = 4*1024*1024;
    resource = ltss.createResource(value,core);
    Assert.assertEquals((int)(value/(1024*1024)),resource.getMemory());
  }

  @Test(timeout = 5000)
  public void testCreateResourceLargerThanIntMax() {
    //value beyond integer but within Long.MAX_VALUE
    try {
      ltss.createResource(Long.MAX_VALUE, core);
      fail("No exception thrown.");
    } catch (Exception ex) {
      assertTrue(ex instanceof IllegalArgumentException);
      assertTrue(ex.getMessage().contains("Out of range:"));
    }
  }

  @Test(timeout = 5000)
  public void testCreateResourceWithNegativeValue() {
    //value is Long.MAX_VALUE*1024*1024,
    // it will be negative after it is passed to createResource

    try {
      ltss.createResource((Long.MAX_VALUE*1024*1024), core);
      fail("No exception thrown.");
    } catch (Exception ex) {
      assertTrue(ex instanceof IllegalArgumentException);
      assertTrue(ex.getMessage().contains("Negative Memory or Core provided!"));
    }
  }

  /**
   * Normal flow of TaskAttempt
   */
  @Test(timeout = 5000)
  public void testDeallocationBeforeAllocation() throws InterruptedException {
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(10000l, 1), 1);

    TaskSchedulerContext mockContext = TestTaskSchedulerHelpers
        .setupMockTaskSchedulerContext("", 0, "", false, appAttemptId, 10000l, null, new Configuration());

    MockLocalTaskSchedulerSerivce taskSchedulerService = new MockLocalTaskSchedulerSerivce(mockContext);
    taskSchedulerService.initialize();
    taskSchedulerService.start();

    // create a task that fills the task allocation queue
    Task dummy_task = mock(Task.class);
    taskSchedulerService.allocateTask(dummy_task, Resource.newInstance(1024, 1), null, null, Priority.newInstance(1), null, null);
    Task task = mock(Task.class);
    taskSchedulerService.allocateTask(task, Resource.newInstance(1024, 1), null, null, Priority.newInstance(1), null, null);
    taskSchedulerService.deallocateTask(task, false, null, null);
    // start the RequestHandler, DeallocateTaskRequest has higher priority, so will be processed first
    taskSchedulerService.startRequestHandlerThread();

    MockAsyncDelegateRequestHandler requestHandler = taskSchedulerService.getRequestHandler();
    requestHandler.drainRequest(3);
    assertEquals(1, requestHandler.deallocateCount);
    // The corresponding AllocateTaskRequest will be removed, so won't been processed.
    assertEquals(1, requestHandler.allocateCount);
    taskSchedulerService.shutdown();
  }

  /**
   * TaskAttempt Killed from START_WAIT
   */
  @Test(timeout = 5000)
  public void testDeallocationAfterAllocation() throws InterruptedException {
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(10000l, 1), 1);

    TaskSchedulerContext mockContext = TestTaskSchedulerHelpers
        .setupMockTaskSchedulerContext("", 0, "", false, appAttemptId, 10000l, null, new Configuration());

    MockLocalTaskSchedulerSerivce taskSchedulerService =
        new MockLocalTaskSchedulerSerivce(mockContext);

    taskSchedulerService.initialize();
    taskSchedulerService.start();

    Task task = mock(Task.class);
    taskSchedulerService.allocateTask(task, Resource.newInstance(1024, 1), null, null, Priority.newInstance(1), null, null);
    taskSchedulerService.startRequestHandlerThread();

    MockAsyncDelegateRequestHandler requestHandler = taskSchedulerService.getRequestHandler();
    requestHandler.drainRequest(1);
    taskSchedulerService.deallocateTask(task, false, null, null);
    requestHandler.drainRequest(2);
    assertEquals(1, requestHandler.deallocateCount);
    assertEquals(1, requestHandler.allocateCount);
    taskSchedulerService.shutdown();
  }

  @Test
  public void preemptDescendantsOnly() {

    final int MAX_TASKS = 2;
    TezConfiguration tezConf = new TezConfiguration();
    tezConf.setInt(TezConfiguration.TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS, MAX_TASKS);

    ApplicationId appId = ApplicationId.newInstance(2000, 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    Long parentTask1 = new Long(1);
    Long parentTask2 = new Long(2);
    Long childTask1 = new Long(3);
    Long grandchildTask1 = new Long(4);

    TaskSchedulerContext
        mockContext = TestTaskSchedulerHelpers.setupMockTaskSchedulerContext("", 0, "", true,
        appAttemptId, 1000l, null, tezConf);
    when(mockContext.getVertexIndexForTask(parentTask1)).thenReturn(0);
    when(mockContext.getVertexIndexForTask(parentTask2)).thenReturn(0);
    when(mockContext.getVertexIndexForTask(childTask1)).thenReturn(1);
    when(mockContext.getVertexIndexForTask(grandchildTask1)).thenReturn(2);

    DagInfo mockDagInfo = mock(DagInfo.class);
    when(mockDagInfo.getTotalVertices()).thenReturn(3);
    BitSet vertex1Descendants = new BitSet();
    vertex1Descendants.set(1);
    vertex1Descendants.set(2);
    BitSet vertex2Descendants = new BitSet();
    vertex2Descendants.set(2);
    BitSet vertex3Descendants = new BitSet();
    when(mockDagInfo.getVertexDescendants(0)).thenReturn(vertex1Descendants);
    when(mockDagInfo.getVertexDescendants(1)).thenReturn(vertex2Descendants);
    when(mockDagInfo.getVertexDescendants(2)).thenReturn(vertex3Descendants);
    when(mockContext.getCurrentDagInfo()).thenReturn(mockDagInfo);

    Priority priority1 = Priority.newInstance(1);
    Priority priority2 = Priority.newInstance(2);
    Priority priority3 = Priority.newInstance(3);
    Priority priority4 = Priority.newInstance(4);
    Resource resource = Resource.newInstance(1024, 1);

    MockLocalTaskSchedulerSerivce taskSchedulerService = new MockLocalTaskSchedulerSerivce(mockContext);

    // The mock context need to send a deallocate container request to the scheduler service
    Answer<Void> answer = new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        ContainerId containerId = invocation.getArgumentAt(0, ContainerId.class);
        taskSchedulerService.deallocateContainer(containerId);
        return null;
      }
    };
    doAnswer(answer).when(mockContext).preemptContainer(any(ContainerId.class));

    taskSchedulerService.initialize();
    taskSchedulerService.start();
    taskSchedulerService.startRequestHandlerThread();

    MockAsyncDelegateRequestHandler requestHandler = taskSchedulerService.getRequestHandler();
    taskSchedulerService.allocateTask(parentTask1, resource, null, null, priority1, null, null);
    taskSchedulerService.allocateTask(childTask1, resource, null, null, priority3, null, null);
    taskSchedulerService.allocateTask(grandchildTask1, resource, null, null, priority4, null, null);
    requestHandler.drainRequest(3);

    // We should not preempt if we have not reached max task allocations
    Assert.assertEquals("Wrong number of allocate tasks", MAX_TASKS, requestHandler.allocateCount);
    Assert.assertTrue("Another allocation should not fit", !requestHandler.shouldProcess());

    // Next task allocation should preempt
    taskSchedulerService.allocateTask(parentTask2, Resource.newInstance(1024, 1), null, null, priority2, null, null);
    requestHandler.drainRequest(5);

    // All allocated tasks should have been removed
    Assert.assertEquals("Wrong number of preempted tasks", 1, requestHandler.preemptCount);
  }

  static class MockLocalTaskSchedulerSerivce extends LocalTaskSchedulerService {

    private MockAsyncDelegateRequestHandler requestHandler;

    public MockLocalTaskSchedulerSerivce(TaskSchedulerContext appClient) {
      super(appClient);
    }

    @Override
    public AsyncDelegateRequestHandler createRequestHandler(Configuration conf) {
      requestHandler = new MockAsyncDelegateRequestHandler(taskRequestQueue,
          new LocalContainerFactory(getContext().getApplicationAttemptId(), customContainerAppId),
          taskAllocations,
          getContext(),
          conf);
      return requestHandler;
    }

    @Override
    public void start() {
      // don't start RequestHandler thread, control it in unit test
    }

    public void startRequestHandlerThread() {
      asyncDelegateRequestThread.start();
    }

    public MockAsyncDelegateRequestHandler getRequestHandler() {
      return requestHandler;
    }

    static class MockAsyncDelegateRequestHandler extends AsyncDelegateRequestHandler {

      public int allocateCount = 0;
      public int deallocateCount = 0;
      public int preemptCount = 0;
      public int dispatchCount = 0;

      MockAsyncDelegateRequestHandler(
          LinkedBlockingQueue<SchedulerRequest> taskRequestQueue,
          LocalContainerFactory localContainerFactory,
          HashMap<Object, AllocatedTask> taskAllocations,
          TaskSchedulerContext appClientDelegate, Configuration conf) {
        super(taskRequestQueue, localContainerFactory, taskAllocations,
            appClientDelegate, conf);
      }

      @Override
      void dispatchRequest() {
        super.dispatchRequest();
        dispatchCount++;
      }

      @Override
      void allocateTask() {
        super.allocateTask();
        allocateCount++;
      }

      public void drainRequest(int count) {
        while(dispatchCount != count || !clientRequestQueue.isEmpty()) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }

      @Override
      void deallocateTask(DeallocateTaskRequest request) {
        super.deallocateTask(request);
        deallocateCount++;
      }

      @Override
      void preemptTask(DeallocateContainerRequest request) {
        super.preemptTask(request);
        preemptCount++;
      }
    }
  }
}
