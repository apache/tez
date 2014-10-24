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

package org.apache.tez.common;

import static org.junit.Assert.assertEquals;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestEnvironmentUpdateUtils {

  @Test
  public void testMultipleUpdateEnvironment() {
    EnvironmentUpdateUtils.put("test.environment1", "test.value1");
    EnvironmentUpdateUtils.put("test.environment2", "test.value2");
    assertEquals("Environment was not set propertly", "test.value1", System.getenv("test.environment1"));
    assertEquals("Environment was not set propertly", "test.value2", System.getenv("test.environment2"));
  }

  @Test
  public void testConcurrentRequests() throws InterruptedException {
    int timeoutSecond = 5;
    int concurThread = 10;
    int exceptionCount = 0;
    List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();
    List<ListenableFuture<Object>> pendingTasks = new ArrayList<ListenableFuture<Object>>();
    final ExecutorService callbackExecutor = Executors.newFixedThreadPool(concurThread,
        new ThreadFactoryBuilder().setDaemon(false).setNameFormat("CallbackExecutor").build());
    ListeningExecutorService taskExecutorService =
        MoreExecutors.listeningDecorator(callbackExecutor);
    while(concurThread > 0){
      ListenableFuture<Object> runningTaskFuture =
          taskExecutorService.submit(new EnvironmentRequest());
      pendingTasks.add(runningTaskFuture);
      concurThread--;
    }

    //waiting for all threads submitted to thread pool
    for (ListenableFuture<Object> future : pendingTasks) {
     try {
        future.get();
      } catch (ExecutionException e) {
        exceptionCount++;
      }
    }

    //stop accepting new threads and shutdown threadpool
    taskExecutorService.shutdown();
    try {
      if(!taskExecutorService.awaitTermination(timeoutSecond, TimeUnit.SECONDS)) {
        taskExecutorService.shutdownNow();
      }
    } catch (InterruptedException ie) {
      taskExecutorService.shutdownNow();
    }

    assertEquals(0, exceptionCount);
  }

  private class EnvironmentRequest implements Callable<Object> {

    @Override
    public Object call() throws Exception {
      EnvironmentUpdateUtils.put("test.environment.concurrent"
          +Thread.currentThread().getId(), "test.evironment.concurrent");
      return null;
    }
  }
 }
