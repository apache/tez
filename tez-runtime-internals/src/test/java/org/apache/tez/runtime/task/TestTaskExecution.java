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

package org.apache.tez.runtime.task;

import static org.apache.tez.runtime.task.TaskExecutionTestHelpers.createTaskReporter;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.common.resources.ScalingAllocator;
import org.apache.tez.runtime.task.TaskExecutionTestHelpers.TestProcessor;
import org.apache.tez.runtime.task.TaskExecutionTestHelpers.TezTaskUmbilicalForTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

// Tests in this class cannot be run in parallel.
public class TestTaskExecution {

  private static final Logger LOG = LoggerFactory.getLogger(TestTaskExecution.class);



  private static final Configuration defaultConf = new Configuration();
  private static final FileSystem localFs;
  private static final Path workDir;

  private static final ExecutorService taskExecutor = Executors.newFixedThreadPool(1);

  static {
    defaultConf.set("fs.defaultFS", "file:///");
    defaultConf.set(TezConfiguration.TEZ_TASK_SCALE_MEMORY_ALLOCATOR_CLASS,
        ScalingAllocator.class.getName());
    try {
      localFs = FileSystem.getLocal(defaultConf);
      Path wd = new Path(System.getProperty("test.build.data", "/tmp"),
          TestTaskExecution.class.getSimpleName());
      workDir = localFs.makeQualified(wd);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void reset() {
    TestProcessor.reset();
  }

  @AfterClass
  public static void shutdown() {
    taskExecutor.shutdownNow();
  }

  @Test(timeout = 5000)
  public void testSingleSuccessfulTask() throws IOException, InterruptedException, TezException,
      ExecutionException {
    ListeningExecutorService executor = null;
    try {
      ExecutorService rawExecutor = Executors.newFixedThreadPool(1);
      executor = MoreExecutors.listeningDecorator(rawExecutor);
      ApplicationId appId = ApplicationId.newInstance(10000, 1);
      TezTaskUmbilicalForTest umbilical = new TezTaskUmbilicalForTest();
      TaskReporter taskReporter = createTaskReporter(appId, umbilical);

      TezTaskRunner taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_EMPTY);
      // Setup the executor
      Future<Boolean> taskRunnerFuture = taskExecutor.submit(new TaskRunnerCallable1ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.signal();
      boolean result = taskRunnerFuture.get();
      assertTrue(result);
      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskSuccessEvent();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(timeout = 5000)
  public void testMultipleSuccessfulTasks() throws IOException, InterruptedException, TezException,
      ExecutionException {

    ListeningExecutorService executor = null;
    try {
      ExecutorService rawExecutor = Executors.newFixedThreadPool(1);
      executor = MoreExecutors.listeningDecorator(rawExecutor);
      ApplicationId appId = ApplicationId.newInstance(10000, 1);
      TezTaskUmbilicalForTest umbilical = new TezTaskUmbilicalForTest();
      TaskReporter taskReporter = createTaskReporter(appId, umbilical);

      TezTaskRunner taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_EMPTY);
      // Setup the executor
      Future<Boolean> taskRunnerFuture = taskExecutor.submit(new TaskRunnerCallable1ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.signal();
      boolean result = taskRunnerFuture.get();
      assertTrue(result);
      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskSuccessEvent();
      umbilical.resetTrackedEvents();

      taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_EMPTY);
      // Setup the executor
      taskRunnerFuture = taskExecutor.submit(new TaskRunnerCallable1ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.signal();
      result = taskRunnerFuture.get();
      assertTrue(result);
      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskSuccessEvent();
    } finally {
      executor.shutdownNow();
    }
  }

  // test task failed due to exception in Processor
  @Test(timeout = 5000)
  public void testFailedTask() throws IOException, InterruptedException, TezException {

    ListeningExecutorService executor = null;
    try {
      ExecutorService rawExecutor = Executors.newFixedThreadPool(1);
      executor = MoreExecutors.listeningDecorator(rawExecutor);
      ApplicationId appId = ApplicationId.newInstance(10000, 1);
      TezTaskUmbilicalForTest umbilical = new TezTaskUmbilicalForTest();
      TaskReporter taskReporter = createTaskReporter(appId, umbilical);

      TezTaskRunner taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_THROW_TEZ_EXCEPTION);
      // Setup the executor
      Future<Boolean> taskRunnerFuture = taskExecutor.submit(new TaskRunnerCallable1ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.awaitStart();
      TestProcessor.signal();
      try {
        taskRunnerFuture.get();
        fail("Expecting the task to fail");
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        LOG.info(cause.getClass().getName());
        assertTrue(cause instanceof TezException);
      }

      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskFailedEvent("Failure while running task:org.apache.tez.dag.api.TezException: TezException");
    } finally {
      executor.shutdownNow();
    }
  }

  // Test task failed due to Processor class not found
  @Test(timeout = 5000)
  public void testFailedTask2() throws IOException, InterruptedException, TezException {

    ListeningExecutorService executor = null;
    try {
      ExecutorService rawExecutor = Executors.newFixedThreadPool(1);
      executor = MoreExecutors.listeningDecorator(rawExecutor);
      ApplicationId appId = ApplicationId.newInstance(10000, 1);
      TezTaskUmbilicalForTest umbilical = new TezTaskUmbilicalForTest();
      TaskReporter taskReporter = createTaskReporter(appId, umbilical);

      TezTaskRunner taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          "NotExitedProcessor", TestProcessor.CONF_THROW_TEZ_EXCEPTION);
      // Setup the executor
      Future<Boolean> taskRunnerFuture = taskExecutor.submit(new TaskRunnerCallable1ForTest(taskRunner));
      try {
        taskRunnerFuture.get();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        LOG.info(cause.getClass().getName());
        assertTrue(cause instanceof TezException);
      }
      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskFailedEvent("Failure while running task:org.apache.tez.dag.api.TezUncheckedException: "
            + "Unable to load class: NotExitedProcessor");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(timeout = 5000)
  public void testHeartbeatException() throws IOException, InterruptedException, TezException {

    ListeningExecutorService executor = null;
    try {
      ExecutorService rawExecutor = Executors.newFixedThreadPool(1);
      executor = MoreExecutors.listeningDecorator(rawExecutor);
      ApplicationId appId = ApplicationId.newInstance(10000, 1);
      TezTaskUmbilicalForTest umbilical = new TezTaskUmbilicalForTest();
      TaskReporter taskReporter = createTaskReporter(appId, umbilical);

      TezTaskRunner taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_EMPTY);
      // Setup the executor
      Future<Boolean> taskRunnerFuture = taskExecutor.submit(new TaskRunnerCallable1ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.awaitStart();
      umbilical.signalThrowException();
      umbilical.awaitRegisteredEvent();
      // Not signaling an actual start to verify task interruption
      try {
        taskRunnerFuture.get();
        fail("Expecting the task to fail");
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        assertTrue(cause instanceof IOException);
        assertTrue(cause.getMessage().contains(TaskExecutionTestHelpers.HEARTBEAT_EXCEPTION_STRING));
      }
      TestProcessor.awaitCompletion();
      assertTrue(TestProcessor.wasInterrupted());
      assertNull(taskReporter.currentCallable);
      // No completion events since umbilical communication already failed.
      umbilical.verifyNoCompletionEvents();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(timeout = 5000)
  public void testHeartbeatShouldDie() throws IOException, InterruptedException, TezException,
      ExecutionException {

    ListeningExecutorService executor = null;
    try {
      ExecutorService rawExecutor = Executors.newFixedThreadPool(1);
      executor = MoreExecutors.listeningDecorator(rawExecutor);
      ApplicationId appId = ApplicationId.newInstance(10000, 1);
      TezTaskUmbilicalForTest umbilical = new TezTaskUmbilicalForTest();
      TaskReporter taskReporter = createTaskReporter(appId, umbilical);

      TezTaskRunner taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_EMPTY);
      // Setup the executor
      Future<Boolean> taskRunnerFuture = taskExecutor.submit(new TaskRunnerCallable1ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.awaitStart();
      umbilical.signalSendShouldDie();
      umbilical.awaitRegisteredEvent();
      // Not signaling an actual start to verify task interruption

      boolean result = taskRunnerFuture.get();
      assertFalse(result);

      TestProcessor.awaitCompletion();
      assertTrue(TestProcessor.wasInterrupted());
      assertNull(taskReporter.currentCallable);
      // TODO Is this statement correct ?
      // No completion events since shouldDie was requested by the AM, which should have killed the
      // task.
      umbilical.verifyNoCompletionEvents();
    } finally {
      executor.shutdownNow();
    }
  }

  // Potential new tests
  // Different states - initialization failure, close failure
  // getTask states

  private static class TaskRunnerCallable1ForTest implements Callable<Boolean> {
    private final TezTaskRunner taskRunner;

    public TaskRunnerCallable1ForTest(TezTaskRunner taskRunner) {
      this.taskRunner = taskRunner;
    }

    @Override
    public Boolean call() throws Exception {
      return taskRunner.run();
    }
  }





  private TezTaskRunner createTaskRunner(ApplicationId appId, TezTaskUmbilicalForTest umbilical,
      TaskReporter taskReporter, ListeningExecutorService executor, byte[] processorConf)
      throws IOException {
    return createTaskRunner(appId, umbilical, taskReporter, executor, TestProcessor.class.getName(),
        processorConf);
  }

  private TezTaskRunner createTaskRunner(ApplicationId appId, TezTaskUmbilicalForTest umbilical,
      TaskReporter taskReporter, ListeningExecutorService executor, String processorClass, byte[] processorConf) throws IOException{
    TezConfiguration tezConf = new TezConfiguration(defaultConf);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    Path testDir = new Path(workDir, UUID.randomUUID().toString());
    String[] localDirs = new String[] { testDir.toString() };

    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexId = TezVertexID.getInstance(dagId, 1);
    TezTaskID taskId = TezTaskID.getInstance(vertexId, 1);
    TezTaskAttemptID taskAttemptId = TezTaskAttemptID.getInstance(taskId, 1);
    ProcessorDescriptor processorDescriptor = ProcessorDescriptor.create(processorClass)
        .setUserPayload(UserPayload.create(ByteBuffer.wrap(processorConf)));
    TaskSpec taskSpec = new TaskSpec(taskAttemptId, "dagName", "vertexName", -1, processorDescriptor,
        new ArrayList<InputSpec>(), new ArrayList<OutputSpec>(), null);

    TezTaskRunner taskRunner = new TezTaskRunner(tezConf, ugi, localDirs, taskSpec, 1,
        new HashMap<String, ByteBuffer>(), new HashMap<String, String>(), HashMultimap.<String, String> create(), taskReporter,
        executor, null, "", new ExecutionContextImpl("localhost"), Runtime.getRuntime().maxMemory());
    return taskRunner;
  }


}
