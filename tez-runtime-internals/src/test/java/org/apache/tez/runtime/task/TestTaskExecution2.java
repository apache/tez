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

import static org.apache.tez.runtime.task.TaskExecutionTestHelpers.createProcessorIOException;
import static org.apache.tez.runtime.task.TaskExecutionTestHelpers.createProcessorTezException;
import static org.apache.tez.runtime.task.TaskExecutionTestHelpers.createTaskReporter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.common.resources.ScalingAllocator;
import org.apache.tez.runtime.internals.api.TaskReporterInterface;
import org.apache.tez.runtime.task.TaskExecutionTestHelpers.TestProcessor;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTaskExecution2 {

  private static final Logger LOG = LoggerFactory.getLogger(TestTaskExecution2.class);

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
      TaskExecutionTestHelpers.TezTaskUmbilicalForTest
          umbilical = new TaskExecutionTestHelpers.TezTaskUmbilicalForTest();
      TaskReporter taskReporter = createTaskReporter(appId, umbilical);

      TezTaskRunner2 taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_EMPTY);
      // Setup the executor
      Future<TaskRunner2Result> taskRunnerFuture = taskExecutor.submit(
          new TaskRunnerCallable2ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.signal();
      TaskRunner2Result result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.SUCCESS, null, false);
      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskSuccessEvent();
      assertFalse(TestProcessor.wasAborted());
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
      TaskExecutionTestHelpers.TezTaskUmbilicalForTest
          umbilical = new TaskExecutionTestHelpers.TezTaskUmbilicalForTest();
      TaskReporter taskReporter = createTaskReporter(appId, umbilical);

      TezTaskRunner2 taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_EMPTY);
      // Setup the executor
      Future<TaskRunner2Result> taskRunnerFuture = taskExecutor.submit(
          new TaskRunnerCallable2ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.signal();
      TaskRunner2Result result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.SUCCESS, null, false);
      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskSuccessEvent();
      assertFalse(TestProcessor.wasAborted());
      umbilical.resetTrackedEvents();

      taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_EMPTY);
      // Setup the executor
      taskRunnerFuture = taskExecutor.submit(new TaskRunnerCallable2ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.signal();
      result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.SUCCESS, null, false);
      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskSuccessEvent();
      assertFalse(TestProcessor.wasAborted());
    } finally {
      executor.shutdownNow();
    }
  }

  // test task failed due to exception in Processor
  @Test(timeout = 5000)
  public void testFailedTaskTezException() throws IOException, InterruptedException, TezException,
      ExecutionException {

    ListeningExecutorService executor = null;
    try {
      ExecutorService rawExecutor = Executors.newFixedThreadPool(1);
      executor = MoreExecutors.listeningDecorator(rawExecutor);
      ApplicationId appId = ApplicationId.newInstance(10000, 1);
      TaskExecutionTestHelpers.TezTaskUmbilicalForTest
          umbilical = new TaskExecutionTestHelpers.TezTaskUmbilicalForTest();
      TaskReporter taskReporter = createTaskReporter(appId, umbilical);

      TezTaskRunner2 taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_THROW_TEZ_EXCEPTION);
      // Setup the executor
      Future<TaskRunner2Result> taskRunnerFuture =
          taskExecutor.submit(new TaskRunnerCallable2ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.awaitStart();
      TestProcessor.signal();
      TaskRunner2Result result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.TASK_ERROR, createProcessorTezException(), false);

      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskFailedEvent(
          "Failure while running task",
          TezException.class.getName() + ": " + TezException.class.getSimpleName());
      // Failure detected as a result of fall off from the run method. abort isn't required.
      assertFalse(TestProcessor.wasAborted());
    } finally {
      executor.shutdownNow();
    }
  }


  // Test task failed due to Processor class not found
  @Test(timeout = 5000)
  public void testFailedTask2() throws IOException, InterruptedException, TezException,
      ExecutionException {

    ListeningExecutorService executor = null;
    try {
      ExecutorService rawExecutor = Executors.newFixedThreadPool(1);
      executor = MoreExecutors.listeningDecorator(rawExecutor);
      ApplicationId appId = ApplicationId.newInstance(10000, 1);
      TaskExecutionTestHelpers.TezTaskUmbilicalForTest
          umbilical = new TaskExecutionTestHelpers.TezTaskUmbilicalForTest();
      TaskReporter taskReporter = createTaskReporter(appId, umbilical);

      TezTaskRunner2 taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          "NotExitedProcessor", TestProcessor.CONF_EMPTY, false);
      // Setup the executor
      Future<TaskRunner2Result> taskRunnerFuture =
          taskExecutor.submit(new TaskRunnerCallable2ForTest(taskRunner));

      TaskRunner2Result result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.TASK_ERROR,
          new TezUncheckedException("Unchecked exception"), false);

      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskFailedEvent("Failure while running task",
          ":org.apache.tez.dag.api.TezUncheckedException: "
              + "Unable to load class: NotExitedProcessor");
      // Failure detected as a result of fall off from the run method. abort isn't required.
      assertFalse(TestProcessor.wasAborted());
    } finally {
      executor.shutdownNow();
    }
  }

  // test task failed due to exception in Processor
  @Test(timeout = 5000)
  public void testFailedTaskIOException() throws IOException, InterruptedException, TezException,
      ExecutionException {

    ListeningExecutorService executor = null;
    try {
      ExecutorService rawExecutor = Executors.newFixedThreadPool(1);
      executor = MoreExecutors.listeningDecorator(rawExecutor);
      ApplicationId appId = ApplicationId.newInstance(10000, 1);
      TaskExecutionTestHelpers.TezTaskUmbilicalForTest
          umbilical = new TaskExecutionTestHelpers.TezTaskUmbilicalForTest();
      TaskReporter taskReporter = createTaskReporter(appId, umbilical);

      TezTaskRunner2 taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_THROW_IO_EXCEPTION);
      // Setup the executor
      Future<TaskRunner2Result> taskRunnerFuture =
          taskExecutor.submit(new TaskRunnerCallable2ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.awaitStart();
      TestProcessor.signal();
      TaskRunner2Result result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.TASK_ERROR, createProcessorIOException(), false);


      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskFailedEvent(
          "Failure while running task",
          IOException.class.getName() + ": " + IOException.class.getSimpleName());
      // Failure detected as a result of fall off from the run method. abort isn't required.
      assertFalse(TestProcessor.wasAborted());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(timeout = 5000)
  public void testHeartbeatException() throws IOException, InterruptedException, TezException,
      ExecutionException {

    ListeningExecutorService executor = null;
    try {
      ExecutorService rawExecutor = Executors.newFixedThreadPool(1);
      executor = MoreExecutors.listeningDecorator(rawExecutor);
      ApplicationId appId = ApplicationId.newInstance(10000, 1);
      TaskExecutionTestHelpers.TezTaskUmbilicalForTest
          umbilical = new TaskExecutionTestHelpers.TezTaskUmbilicalForTest();
      TaskReporter taskReporter = createTaskReporter(appId, umbilical);

      TezTaskRunner2 taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_EMPTY);
      // Setup the executor
      Future<TaskRunner2Result> taskRunnerFuture =
          taskExecutor.submit(new TaskRunnerCallable2ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.awaitStart();
      umbilical.signalThrowException();
      umbilical.awaitRegisteredEvent();
      // Not signaling an actual start to verify task interruption

      TaskRunner2Result result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.COMMUNICATION_FAILURE,
          new IOException("IOException"),
          TaskExecutionTestHelpers.HEARTBEAT_EXCEPTION_STRING, false);

      TestProcessor.awaitCompletion();
      assertTrue(TestProcessor.wasInterrupted());
      assertNull(taskReporter.currentCallable);
      // No completion events since umbilical communication already failed.
      umbilical.verifyNoCompletionEvents();
      assertTrue(TestProcessor.wasAborted());
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
      TaskExecutionTestHelpers.TezTaskUmbilicalForTest
          umbilical = new TaskExecutionTestHelpers.TezTaskUmbilicalForTest();
      TaskReporter taskReporter = createTaskReporter(appId, umbilical);

      TezTaskRunner2 taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_EMPTY);
      // Setup the executor
      Future<TaskRunner2Result> taskRunnerFuture =
          taskExecutor.submit(new TaskRunnerCallable2ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.awaitStart();
      umbilical.signalSendShouldDie();
      umbilical.awaitRegisteredEvent();
      // Not signaling an actual start to verify task interruption

      TaskRunner2Result result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.CONTAINER_STOP_REQUESTED, null, true);


      TestProcessor.awaitCompletion();
      assertTrue(TestProcessor.wasInterrupted());
      assertNull(taskReporter.currentCallable);
      // TODO Is this statement correct ?
      // No completion events since shouldDie was requested by the AM, which should have killed the
      // task.
      umbilical.verifyNoCompletionEvents();
      assertTrue(TestProcessor.wasAborted());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(timeout = 5000)
  public void testSignalFatalErrorAndLoop() throws IOException, InterruptedException, TezException,
      ExecutionException {

    ListeningExecutorService executor = null;
    try {
      ExecutorService rawExecutor = Executors.newFixedThreadPool(1);
      executor = MoreExecutors.listeningDecorator(rawExecutor);
      ApplicationId appId = ApplicationId.newInstance(10000, 1);
      TaskExecutionTestHelpers.TezTaskUmbilicalForTest
          umbilical = new TaskExecutionTestHelpers.TezTaskUmbilicalForTest();
      TaskReporter taskReporter = createTaskReporter(appId, umbilical);

      TezTaskRunner2 taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_SIGNAL_FATAL_AND_LOOP);
      // Setup the executor
      Future<TaskRunner2Result> taskRunnerFuture =
          taskExecutor.submit(new TaskRunnerCallable2ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.awaitStart();
      TestProcessor.signal();

      TestProcessor.awaitLoop();
      // The fatal error should have caused an interrupt.

      TaskRunner2Result result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.TASK_ERROR, createProcessorIOException(), false);

      TestProcessor.awaitCompletion();
      assertTrue(TestProcessor.wasInterrupted());
      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskFailedEvent(
          "Failure while running task",
          IOException.class.getName() + ": " + IOException.class.getSimpleName());
      // Signal fatal error should cause the processor to fail.
      assertTrue(TestProcessor.wasAborted());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(timeout = 5000)
  public void testTaskKilled() throws IOException, InterruptedException, TezException,
      ExecutionException {

    ListeningExecutorService executor = null;
    try {
      ExecutorService rawExecutor = Executors.newFixedThreadPool(1);
      executor = MoreExecutors.listeningDecorator(rawExecutor);
      ApplicationId appId = ApplicationId.newInstance(10000, 1);
      TaskExecutionTestHelpers.TezTaskUmbilicalForTest
          umbilical = new TaskExecutionTestHelpers.TezTaskUmbilicalForTest();
      TaskReporter taskReporter = createTaskReporter(appId, umbilical);

      TezTaskRunner2 taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_EMPTY);
      // Setup the executor
      Future<TaskRunner2Result> taskRunnerFuture =
          taskExecutor.submit(new TaskRunnerCallable2ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.awaitStart();

      taskRunner.killTask();

      TaskRunner2Result result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.KILL_REQUESTED, null, false);

      TestProcessor.awaitCompletion();
      assertTrue(TestProcessor.wasInterrupted());
      assertNull(taskReporter.currentCallable);
      // Kill events are not sent over the umbilical at the moment.
      umbilical.verifyNoCompletionEvents();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(timeout = 5000)
  public void testKilledAfterComplete() throws IOException, InterruptedException, TezException,
      ExecutionException {

    ListeningExecutorService executor = null;
    try {
      ExecutorService rawExecutor = Executors.newFixedThreadPool(1);
      executor = MoreExecutors.listeningDecorator(rawExecutor);
      ApplicationId appId = ApplicationId.newInstance(10000, 1);
      TaskExecutionTestHelpers.TezTaskUmbilicalForTest
          umbilical = new TaskExecutionTestHelpers.TezTaskUmbilicalForTest();
      TaskReporter taskReporter = createTaskReporter(appId, umbilical);

      TezTaskRunner2ForTest taskRunner =
          createTaskRunnerForTest(appId, umbilical, taskReporter, executor,
              TestProcessor.CONF_EMPTY);
      // Setup the executor
      Future<TaskRunner2Result> taskRunnerFuture =
          taskExecutor.submit(new TaskRunnerCallable2ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.awaitStart();
      TestProcessor.signal();
      TestProcessor.awaitCompletion();

      taskRunner.awaitCallableCompletion();

      taskRunner.killTask();
      TaskRunner2Result result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.SUCCESS, null, false);

      assertFalse(TestProcessor.wasInterrupted());
      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskSuccessEvent();
    } finally {
      executor.shutdownNow();
    }
  }


  private void verifyTaskRunnerResult(TaskRunner2Result taskRunner2Result,
                                      EndReason expectedEndReason, Throwable expectedThrowable,
                                      boolean wasShutdownRequested) {
    verifyTaskRunnerResult(taskRunner2Result, expectedEndReason, expectedThrowable, null,
        wasShutdownRequested);
  }

  private void verifyTaskRunnerResult(TaskRunner2Result taskRunner2Result,
                                      EndReason expectedEndReason, Throwable expectedThrowable,
                                      String expectedExceptionMessage,
                                      boolean wasShutdownRequested) {
    assertEquals(expectedEndReason, taskRunner2Result.getEndReason());
    if (expectedThrowable == null) {
      assertNull(taskRunner2Result.getError());
    } else {
      assertNotNull(taskRunner2Result.getError());
      Throwable cause = taskRunner2Result.getError();
      LOG.info(cause.getClass().getName());
      assertTrue(cause.getClass().isAssignableFrom(expectedThrowable.getClass()));

      if (expectedExceptionMessage != null) {
        assertTrue(cause.getMessage().contains(expectedExceptionMessage));
      }

    }
    assertEquals(wasShutdownRequested, taskRunner2Result.isContainerShutdownRequested());
  }


  private static class TaskRunnerCallable2ForTest implements Callable<TaskRunner2Result> {
    private final TezTaskRunner2 taskRunner;

    public TaskRunnerCallable2ForTest(TezTaskRunner2 taskRunner) {
      this.taskRunner = taskRunner;
    }

    @Override
    public TaskRunner2Result call() throws Exception {
      return taskRunner.run();
    }
  }

  private TezTaskRunner2 createTaskRunner(ApplicationId appId,
                                          TaskExecutionTestHelpers.TezTaskUmbilicalForTest umbilical,
                                          TaskReporter taskReporter,
                                          ListeningExecutorService executor, byte[] processorConf)
      throws IOException {
    return createTaskRunner(appId, umbilical, taskReporter, executor, TestProcessor.class.getName(),
        processorConf, false);
  }

  private TezTaskRunner2ForTest createTaskRunnerForTest(ApplicationId appId,
                                                        TaskExecutionTestHelpers.TezTaskUmbilicalForTest umbilical,
                                                        TaskReporter taskReporter,
                                                        ListeningExecutorService executor,
                                                        byte[] processorConf)
      throws IOException {
    return (TezTaskRunner2ForTest) createTaskRunner(appId, umbilical, taskReporter, executor,
        TestProcessor.class.getName(),
        processorConf, true);
  }

  private TezTaskRunner2 createTaskRunner(ApplicationId appId,
                                          TaskExecutionTestHelpers.TezTaskUmbilicalForTest umbilical,
                                          TaskReporter taskReporter,
                                          ListeningExecutorService executor, String processorClass,
                                          byte[] processorConf, boolean testRunner) throws
      IOException {
    TezConfiguration tezConf = new TezConfiguration(defaultConf);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    Path testDir = new Path(workDir, UUID.randomUUID().toString());
    String[] localDirs = new String[]{testDir.toString()};

    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexId = TezVertexID.getInstance(dagId, 1);
    TezTaskID taskId = TezTaskID.getInstance(vertexId, 1);
    TezTaskAttemptID taskAttemptId = TezTaskAttemptID.getInstance(taskId, 1);
    ProcessorDescriptor processorDescriptor = ProcessorDescriptor.create(processorClass)
        .setUserPayload(UserPayload.create(ByteBuffer.wrap(processorConf)));
    TaskSpec taskSpec =
        new TaskSpec(taskAttemptId, "dagName", "vertexName", -1, processorDescriptor,
            new ArrayList<InputSpec>(), new ArrayList<OutputSpec>(), null);

    TezTaskRunner2 taskRunner;
    if (testRunner) {
      taskRunner = new TezTaskRunner2ForTest(tezConf, ugi, localDirs, taskSpec, 1,
          new HashMap<String, ByteBuffer>(), new HashMap<String, String>(),
          HashMultimap.<String, String>create(), taskReporter,
          executor, null, "", new ExecutionContextImpl("localhost"),
          Runtime.getRuntime().maxMemory());
    } else {
      taskRunner = new TezTaskRunner2(tezConf, ugi, localDirs, taskSpec, 1,
          new HashMap<String, ByteBuffer>(), new HashMap<String, String>(),
          HashMultimap.<String, String>create(), taskReporter,
          executor, null, "", new ExecutionContextImpl("localhost"),
          Runtime.getRuntime().maxMemory());
    }

    return taskRunner;
  }

  public static class TezTaskRunner2ForTest extends TezTaskRunner2 {

    private final ReentrantLock testLock = new ReentrantLock();
    private final Condition callableCompletionCondition = testLock.newCondition();

    private final AtomicBoolean isCallableComplete = new AtomicBoolean(false);

    public TezTaskRunner2ForTest(Configuration tezConf, UserGroupInformation ugi,
                                 String[] localDirs,
                                 TaskSpec taskSpec, int appAttemptNumber,
                                 Map<String, ByteBuffer> serviceConsumerMetadata,
                                 Map<String, String> serviceProviderEnvMap,
                                 Multimap<String, String> startedInputsMap,
                                 TaskReporterInterface taskReporter,
                                 ListeningExecutorService executor,
                                 ObjectRegistry objectRegistry,
                                 String pid,
                                 ExecutionContext executionContext,
                                 long memAvailable) throws IOException {
      super(tezConf, ugi, localDirs, taskSpec, appAttemptNumber, serviceConsumerMetadata,
          serviceProviderEnvMap, startedInputsMap, taskReporter, executor, objectRegistry, pid,
          executionContext, memAvailable);
    }


    @Override
    @VisibleForTesting
    void processCallableResult(TaskRunner2Callable.TaskRunner2CallableResult executionResult) {
      testLock.lock();
      try {
        super.processCallableResult(executionResult);
        isCallableComplete.set(true);
        callableCompletionCondition.signal();
      } finally {
        testLock.unlock();
      }
    }

    void awaitCallableCompletion() throws InterruptedException {
      testLock.lock();
      try {
        while (!isCallableComplete.get()) {
          callableCompletionCondition.await();
        }
      } finally {
        testLock.unlock();
      }
    }
  }

}
