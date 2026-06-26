/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.dag.app.rm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.rm.LocalTaskSchedulerService.AllocatedTask;
import org.apache.tez.dag.app.rm.LocalTaskSchedulerService.AsyncDelegateRequestHandler;
import org.apache.tez.dag.app.rm.LocalTaskSchedulerService.LocalContainerFactory;
import org.apache.tez.dag.app.rm.LocalTaskSchedulerService.SchedulerRequest;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestLocalTaskScheduler {

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void maxTasksAllocationsCannotBeExceeded() {

    final int MAX_TASKS = 4;
    TezConfiguration tezConf = new TezConfiguration();
    tezConf.setInt(TezConfiguration.TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS, MAX_TASKS);

    ApplicationId appId = ApplicationId.newInstance(2000, 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);

    TaskSchedulerContext mockContext =
        TestTaskSchedulerHelpers.setupMockTaskSchedulerContext(
            "", 0, "", true, appAttemptId, 1000L, null, new Configuration());

    LocalContainerFactory containerFactory = new LocalContainerFactory(appAttemptId, 1000);

    HashMap<Object, AllocatedTask> taskAllocations = new LinkedHashMap<>();
    LinkedBlockingQueue<SchedulerRequest> clientRequestQueue = new LinkedBlockingQueue<>();

    // Object under test
    AsyncDelegateRequestHandler requestHandler =
        new AsyncDelegateRequestHandler(
            clientRequestQueue, containerFactory, taskAllocations, mockContext, tezConf);

    // Allocate up to max tasks
    for (int i = 0; i < MAX_TASKS; i++) {
      Priority priority = Priority.newInstance(20);
      requestHandler.addAllocateTaskRequest(new Long(i), null, priority, null);
      requestHandler.dispatchRequest();
      requestHandler.allocateTask();
    }

    // Only MAX_TASKS number of tasks should have been allocated
    assertEquals(MAX_TASKS, taskAllocations.size(), "Wrong number of allocate tasks");
    assertFalse(requestHandler.shouldProcess(), "Another allocation should not fit");

    // Deallocate down to zero
    for (int i = 0; i < MAX_TASKS; i++) {
      requestHandler.addDeallocateTaskRequest(new Long(i));
      requestHandler.dispatchRequest();
    }

    // All allocated tasks should have been removed
    assertEquals(0, taskAllocations.size(), "Wrong number of allocate tasks");
  }
}
