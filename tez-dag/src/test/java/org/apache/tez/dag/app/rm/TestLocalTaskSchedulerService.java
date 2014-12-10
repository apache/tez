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

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.rm.TaskSchedulerService.TaskSchedulerAppCallback;
import org.apache.tez.dag.app.rm.TestLocalTaskSchedulerService.MockLocalTaskSchedulerSerivce.MockAsyncDelegateRequestHandler;
import org.apache.tez.dag.app.rm.container.ContainerSignatureMatcher;
import org.junit.Assert;
import org.junit.Test;

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
  public void testDeallocationBeforeAllocation() {
    MockLocalTaskSchedulerSerivce taskSchedulerService = new MockLocalTaskSchedulerSerivce
        (mock(TaskSchedulerAppCallback.class), mock(ContainerSignatureMatcher.class), "", 0, "", mock(AppContext.class));
    taskSchedulerService.init(new Configuration());
    taskSchedulerService.start();

    Task task = mock(Task.class);
    taskSchedulerService.allocateTask(task, Resource.newInstance(1024, 1), null, null, Priority.newInstance(1), null, null);
    taskSchedulerService.deallocateTask(task, false);
    // start the RequestHandler, DeallocateTaskRequest has higher priority, so will be processed first
    taskSchedulerService.startRequestHandlerThread();

    MockAsyncDelegateRequestHandler requestHandler = taskSchedulerService.getRequestHandler();
    requestHandler.drainRequest(1);
    assertEquals(1, requestHandler.deallocateCount);
    // The corresponding AllocateTaskRequest will be removed, so won't been processed.
    assertEquals(0, requestHandler.allocateCount);
    taskSchedulerService.stop();
  }

  /**
   * TaskAttempt Killed from START_WAIT
   */
  @Test(timeout = 5000)
  public void testDeallocationAfterAllocation() {
    MockLocalTaskSchedulerSerivce taskSchedulerService = new MockLocalTaskSchedulerSerivce
        (mock(TaskSchedulerAppCallback.class), mock(ContainerSignatureMatcher.class), "", 0, "", mock(AppContext.class));
    taskSchedulerService.init(new Configuration());
    taskSchedulerService.start();

    Task task = mock(Task.class);
    taskSchedulerService.allocateTask(task, Resource.newInstance(1024, 1), null, null, Priority.newInstance(1), null, null);
    taskSchedulerService.startRequestHandlerThread();

    MockAsyncDelegateRequestHandler requestHandler = taskSchedulerService.getRequestHandler();
    requestHandler.drainRequest(1);
    taskSchedulerService.deallocateTask(task, false);
    requestHandler.drainRequest(2);
    assertEquals(1, requestHandler.deallocateCount);
    assertEquals(1, requestHandler.allocateCount);
    taskSchedulerService.stop();
  }

  static class MockLocalTaskSchedulerSerivce extends LocalTaskSchedulerService {

    private MockAsyncDelegateRequestHandler requestHandler;

    public MockLocalTaskSchedulerSerivce(TaskSchedulerAppCallback appClient,
        ContainerSignatureMatcher containerSignatureMatcher,
        String appHostName, int appHostPort, String appTrackingUrl,
        AppContext appContext) {
      super(appClient, containerSignatureMatcher, appHostName, appHostPort,
          appTrackingUrl, appContext);
    }

    @Override
    public AsyncDelegateRequestHandler createRequestHandler(Configuration conf) {
      requestHandler = new MockAsyncDelegateRequestHandler(taskRequestQueue,
          new LocalContainerFactory(appContext),
          taskAllocations,
          appClientDelegate,
          conf);
      return requestHandler;
    }

    @Override
    public void serviceStart() {
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
      public int processedCount =0;

      MockAsyncDelegateRequestHandler(
          BlockingQueue<TaskRequest> taskRequestQueue,
          LocalContainerFactory localContainerFactory,
          HashMap<Object, Container> taskAllocations,
          TaskSchedulerAppCallback appClientDelegate, Configuration conf) {
        super(taskRequestQueue, localContainerFactory, taskAllocations,
            appClientDelegate, conf);
      }

      @Override
      void processRequest() {
        super.processRequest();
        processedCount ++;
      }

      public void drainRequest(int count) {
        while(processedCount != count || !taskRequestQueue.isEmpty()) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }

      @Override
      void allocateTask(AllocateTaskRequest request) {
        super.allocateTask(request);
        allocateCount ++;
      }

      @Override
      void deallocateTask(DeallocateTaskRequest request) {
        super.deallocateTask(request);
        deallocateCount ++;
      }
    }
  }
}
