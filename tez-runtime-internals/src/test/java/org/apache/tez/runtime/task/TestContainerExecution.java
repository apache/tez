/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.task;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.junit.Test;

public class TestContainerExecution {

  @Test(timeout = 5000)
  public void testGetTaskShouldDie() throws InterruptedException, ExecutionException {
    ListeningExecutorService executor = null;
    try {
      ExecutorService rawExecutor = Executors.newFixedThreadPool(1);
      executor = MoreExecutors.listeningDecorator(rawExecutor);
      ApplicationId appId = ApplicationId.newInstance(10000, 1);
      ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
      @SuppressWarnings("deprecation")
      ContainerId containerId = ContainerId.newInstance(appAttemptId, 1);

      TaskExecutionTestHelpers.TezTaskUmbilicalForTest
          umbilical = new TaskExecutionTestHelpers.TezTaskUmbilicalForTest();
      ContainerContext containerContext = new ContainerContext(containerId.toString());

      ContainerReporter containerReporter = new ContainerReporter(umbilical, containerContext, 100);
      ListenableFuture<ContainerTask> getTaskFuture = executor.submit(containerReporter);

      getTaskFuture.get();
      assertEquals(1, umbilical.getTaskInvocations);

    } finally {
      executor.shutdownNow();
    }
  }
}
