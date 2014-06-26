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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;

import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.rm.LocalTaskSchedulerService.AsyncDelegateRequestHandler;
import org.apache.tez.dag.app.rm.LocalTaskSchedulerService.LocalContainerFactory;
import org.apache.tez.dag.app.rm.LocalTaskSchedulerService.TaskRequest;
import org.apache.tez.dag.app.rm.TaskSchedulerService.TaskSchedulerAppCallback;

public class TestLocalTaskScheduler {

  public AppContext createMockAppContext() {

    ApplicationId appId = ApplicationId.newInstance(2000, 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);

    AppContext appContext = mock(AppContext.class);
    doReturn(appAttemptId).when(appContext).getApplicationAttemptId();

    return appContext;
  }

  @Test
  public void maxTasksAllocationsCannotBeExceeded() {

    final int MAX_TASKS = 4;
    TezConfiguration tezConf = new TezConfiguration();
    tezConf.setInt(TezConfiguration.TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS, MAX_TASKS);

    LocalContainerFactory containerFactory = new LocalContainerFactory(createMockAppContext());
    HashMap<Object, Container> taskAllocations = new LinkedHashMap<Object, Container>();
    PriorityBlockingQueue<TaskRequest> taskRequestQueue = new PriorityBlockingQueue<TaskRequest>();
    TaskSchedulerAppCallback appClientDelegate = mock(TaskSchedulerAppCallback.class);

    // Object under test
    AsyncDelegateRequestHandler requestHandler =
      new AsyncDelegateRequestHandler(taskRequestQueue,
          containerFactory,
          taskAllocations,
          appClientDelegate,
          tezConf);

    // Allocate up to max tasks
    for (int i = 0; i < MAX_TASKS; i++) {
      Priority priority = Priority.newInstance(20);
      requestHandler.addAllocateTaskRequest(new Long(i), null, priority, null);
      requestHandler.processRequest();
    }

    // Only MAX_TASKS number of tasks should have been allocated
    Assert.assertEquals("Wrong number of allocate tasks", MAX_TASKS, taskAllocations.size());
    Assert.assertTrue("Another allocation should not fit", requestHandler.shouldWait());

    // Deallocate down to zero
    for (int i = 0; i < MAX_TASKS; i++) {
      requestHandler.addDeallocateTaskRequest(new Long(i));
      requestHandler.processRequest();
    }

    // All allocated tasks should have been removed
    Assert.assertEquals("Wrong number of allocate tasks", 0, taskAllocations.size());
  }
}
