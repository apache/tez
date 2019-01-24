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

import static org.apache.tez.runtime.task.TaskExecutionTestHelpers.*;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezExecutors;
import org.apache.tez.common.TezSharedExecutor;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.FileSystemCounter;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezReflectionException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.hadoop.shim.DefaultHadoopShim;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.TaskFailureType;
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

  private static final String FAILURE_START_STRING = "Error while running task ( failure )";
  private static final String KILL_START_STRING = "Error while running task ( kill )";

  private static final ExecutorService taskExecutor = Executors.newFixedThreadPool(1);

  static {
    defaultConf.set("fs.defaultFS", "file:///");
    defaultConf.set(TezConfiguration.TEZ_TASK_SCALE_MEMORY_ALLOCATOR_CLASS,
        ScalingAllocator.class.getName());
    try {
      localFs = FileSystem.getLocal(defaultConf);
      Path wd = new Path(System.getProperty("test.build.data", "/tmp"),
          TestTaskExecution2.class.getSimpleName());
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
      verifyTaskRunnerResult(result, EndReason.SUCCESS, null, false, null);
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
          TestProcessor.CONF_EMPTY, true);
      LogicalIOProcessorRuntimeTask runtimeTask = taskRunner.task;
      // Setup the executor
      Future<TaskRunner2Result> taskRunnerFuture = taskExecutor.submit(
          new TaskRunnerCallable2ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.signal();
      TaskRunner2Result result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.SUCCESS, null, false, null);
      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskSuccessEvent();
      assertFalse(TestProcessor.wasAborted());
      umbilical.resetTrackedEvents();
      TezCounters tezCounters = runtimeTask.getCounters();
      verifySysCounters(tezCounters, 5, 5);

      taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_EMPTY, false);
      runtimeTask = taskRunner.task;
      // Setup the executor
      taskRunnerFuture = taskExecutor.submit(new TaskRunnerCallable2ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.signal();
      result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.SUCCESS, null, false, null);
      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskSuccessEvent();
      assertFalse(TestProcessor.wasAborted());
      tezCounters = runtimeTask.getCounters();
      verifySysCounters(tezCounters, -1, -1);
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
      verifyTaskRunnerResult(result, EndReason.TASK_ERROR, createProcessorTezException(), false, TaskFailureType.NON_FATAL);

      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskFailedEvent(
          FAILURE_START_STRING,
          TezException.class.getName() + ": " + TezException.class.getSimpleName());
      // Failure detected as a result of fall off from the run method. abort isn't required.
      assertFalse(TestProcessor.wasAborted());
      assertTrue(taskRunner.task.getCounters().countCounters() != 0);
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
          "NotExitedProcessor", TestProcessor.CONF_EMPTY, false, true);
      // Setup the executor
      Future<TaskRunner2Result> taskRunnerFuture =
          taskExecutor.submit(new TaskRunnerCallable2ForTest(taskRunner));

      TaskRunner2Result result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.TASK_ERROR,
          new TezReflectionException("TezReflectionException"), false, TaskFailureType.NON_FATAL);

      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskFailedEvent(FAILURE_START_STRING,
          ":org.apache.tez.dag.api.TezReflectionException: "
              + "Unable to load class: NotExitedProcessor");
      // Failure detected as a result of fall off from the run method. abort isn't required.
      assertFalse(TestProcessor.wasAborted());
      assertTrue(taskRunner.task.getCounters().countCounters() != 0);
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
      verifyTaskRunnerResult(result, EndReason.TASK_ERROR, createProcessorIOException(), false, TaskFailureType.NON_FATAL);


      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskFailedEvent(
          FAILURE_START_STRING,
          IOException.class.getName() + ": " + IOException.class.getSimpleName());
      // Failure detected as a result of fall off from the run method. abort isn't required.
      assertFalse(TestProcessor.wasAborted());
      assertTrue(taskRunner.task.getCounters().countCounters() != 0);
    } finally {
      executor.shutdownNow();
    }
  }

  // test that makes sure errors aren't reported when the container is already failing
  @Test(timeout = 5000)
  public void testIgnoreErrorsDuringFailure() throws IOException, InterruptedException, TezException,
      ExecutionException {

    ListeningExecutorService executor = null;
    try {
      ExecutorService rawExecutor = Executors.newFixedThreadPool(1);
      executor = MoreExecutors.listeningDecorator(rawExecutor);
      ApplicationId appId = ApplicationId.newInstance(10000, 1);
      TaskExecutionTestHelpers.TezTaskUmbilicalForTest
          umbilical = new TaskExecutionTestHelpers.TezTaskUmbilicalForTest();

      TaskReporter taskReporter = new TaskReporter(umbilical, 100, 1000, 100, new AtomicLong(0),
        createContainerId(appId).toString()) {
        @Override
        protected boolean isShuttingDown() {
          return true;
        }
      };

      TezTaskRunner2 taskRunner = createTaskRunner(appId, umbilical, taskReporter, executor,
          TestProcessor.CONF_THROW_IO_EXCEPTION);
      // Setup the executor

      taskExecutor.submit(new TaskRunnerCallable2ForTest(taskRunner));

      // Signal the processor to go through
      TestProcessor.awaitStart();
      TestProcessor.signal();

      umbilical.verifyNoCompletionEvents();
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
          TaskExecutionTestHelpers.HEARTBEAT_EXCEPTION_STRING, false, TaskFailureType.NON_FATAL);

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
      verifyTaskRunnerResult(result, EndReason.CONTAINER_STOP_REQUESTED, null, true, null);


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
  public void testSignalDeprecatedFatalErrorAndLoop() throws IOException, InterruptedException, TezException,
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
          TestProcessor.CONF_SIGNAL_DEPRECATEDFATAL_AND_LOOP);
      // Setup the executor
      Future<TaskRunner2Result> taskRunnerFuture =
          taskExecutor.submit(new TaskRunnerCallable2ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.awaitStart();
      TestProcessor.signal();

      TestProcessor.awaitLoop();
      // The fatal error should have caused an interrupt.

      TaskRunner2Result result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.TASK_ERROR, createProcessorIOException(), false, TaskFailureType.NON_FATAL);

      TestProcessor.awaitCompletion();
      assertTrue(TestProcessor.wasInterrupted());
      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskFailedEvent(
          FAILURE_START_STRING,
          IOException.class.getName() + ": " + IOException.class.getSimpleName());
      // Signal fatal error should cause the processor to fail.
      assertTrue(TestProcessor.wasAborted());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(timeout = 5000)
  public void testSignalFatalAndThrow() throws IOException, InterruptedException, TezException,
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
          TestProcessor.CONF_SIGNAL_FATAL_AND_THROW);
      // Setup the executor
      Future<TaskRunner2Result> taskRunnerFuture =
          taskExecutor.submit(new TaskRunnerCallable2ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.awaitStart();
      TestProcessor.signal();

      TaskRunner2Result result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.TASK_ERROR, createProcessorIOException(), false, TaskFailureType.FATAL);

      TestProcessor.awaitCompletion();
      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskFailedEvent(
          FAILURE_START_STRING,
          IOException.class.getName() + ": " + IOException.class.getSimpleName(), TaskFailureType.FATAL);
      assertTrue(TestProcessor.wasAborted());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(timeout = 5000)
  public void testSignalNonFatalAndThrow() throws IOException, InterruptedException, TezException,
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
          TestProcessor.CONF_SIGNAL_NON_FATAL_AND_THROW);
      // Setup the executor
      Future<TaskRunner2Result> taskRunnerFuture =
          taskExecutor.submit(new TaskRunnerCallable2ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.awaitStart();
      TestProcessor.signal();

      TaskRunner2Result result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.TASK_ERROR, createProcessorIOException(), false, TaskFailureType.NON_FATAL);

      TestProcessor.awaitCompletion();
      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskFailedEvent(
          FAILURE_START_STRING,
          IOException.class.getName() + ": " + IOException.class.getSimpleName(), TaskFailureType.NON_FATAL);
      assertTrue(TestProcessor.wasAborted());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(timeout = 5000)
  public void testTaskSelfKill() throws IOException, InterruptedException, TezException,
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
          TestProcessor.CONF_SELF_KILL_AND_COMPLETE);
      // Setup the executor
      Future<TaskRunner2Result> taskRunnerFuture =
          taskExecutor.submit(new TaskRunnerCallable2ForTest(taskRunner));
      // Signal the processor to go through
      TestProcessor.awaitStart();
      TestProcessor.signal();

      TaskRunner2Result result = taskRunnerFuture.get();
      verifyTaskRunnerResult(result, EndReason.TASK_KILL_REQUEST, createProcessorIOException(), false,
          null);

      TestProcessor.awaitCompletion();
      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskKilledEvent(
          KILL_START_STRING,
          IOException.class.getName() + ": " + IOException.class.getSimpleName());
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
      verifyTaskRunnerResult(result, EndReason.KILL_REQUESTED, null, false, null);

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
      verifyTaskRunnerResult(result, EndReason.SUCCESS, null, false, null);

      assertFalse(TestProcessor.wasInterrupted());
      assertNull(taskReporter.currentCallable);
      umbilical.verifyTaskSuccessEvent();
    } finally {
      executor.shutdownNow();
    }
  }

  private void verifySysCounters(TezCounters tezCounters, int minTaskCounterCount, int minFsCounterCount) {

    Preconditions.checkArgument((minTaskCounterCount > 0 && minFsCounterCount > 0) ||
        (minTaskCounterCount <= 0 && minFsCounterCount <= 0),
        "Both targetCounter counts should be postitive or negative. A mix is not expected");

    int numTaskCounters = 0;
    int numFsCounters = 0;
    for (CounterGroup counterGroup : tezCounters) {
      if (counterGroup.getName().equals(TaskCounter.class.getName())) {
        for (TezCounter ignored : counterGroup) {
          numTaskCounters++;
        }
      } else if (counterGroup.getName().equals(FileSystemCounter.class.getName())) {
        for (TezCounter ignored : counterGroup) {
          numFsCounters++;
        }
      }
    }

    // If Target <=0, assert counter count is exactly 0
    if (minTaskCounterCount <= 0) {
      assertEquals(tezCounters.toString(), 0, numTaskCounters);
      assertEquals(tezCounters.toString(), 0, numFsCounters);
    } else {
      assertTrue(numTaskCounters >= minTaskCounterCount);
      assertTrue(numFsCounters >= minFsCounterCount);
    }
  }

  private void verifyTaskRunnerResult(TaskRunner2Result taskRunner2Result,
                                      EndReason expectedEndReason, Throwable expectedThrowable,
                                      boolean wasShutdownRequested,
                                      TaskFailureType taskFailureType) {
    verifyTaskRunnerResult(taskRunner2Result, expectedEndReason, expectedThrowable, null,
        wasShutdownRequested, taskFailureType);
  }

  private void verifyTaskRunnerResult(TaskRunner2Result taskRunner2Result,
                                      EndReason expectedEndReason, Throwable expectedThrowable,
                                      String expectedExceptionMessage,
                                      boolean wasShutdownRequested,
                                      TaskFailureType taskFailureType) {
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
    assertEquals(taskFailureType, taskRunner2Result.getTaskFailureType());
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
                                          ListeningExecutorService executor, byte[] processorConf) throws
      IOException {
    return createTaskRunner(appId, umbilical, taskReporter, executor, processorConf, true);

  }

  private TezTaskRunner2 createTaskRunner(ApplicationId appId,
                                          TaskExecutionTestHelpers.TezTaskUmbilicalForTest umbilical,
                                          TaskReporter taskReporter,
                                          ListeningExecutorService executor, byte[] processorConf,
                                          boolean updateSysCounters)
      throws IOException {
    return createTaskRunner(appId, umbilical, taskReporter, executor, TestProcessor.class.getName(),
        processorConf, false, updateSysCounters);
  }

  private TezTaskRunner2ForTest createTaskRunnerForTest(ApplicationId appId,
                                                        TaskExecutionTestHelpers.TezTaskUmbilicalForTest umbilical,
                                                        TaskReporter taskReporter,
                                                        ListeningExecutorService executor,
                                                        byte[] processorConf)
      throws IOException {
    return (TezTaskRunner2ForTest) createTaskRunner(appId, umbilical, taskReporter, executor,
        TestProcessor.class.getName(),
        processorConf, true, true);
  }

  private TezTaskRunner2 createTaskRunner(ApplicationId appId,
                                          TaskExecutionTestHelpers.TezTaskUmbilicalForTest umbilical,
                                          TaskReporter taskReporter,
                                          ListeningExecutorService executor, String processorClass,
                                          byte[] processorConf, boolean testRunner,
                                          boolean updateSysCounters) throws
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
            new ArrayList<InputSpec>(), new ArrayList<OutputSpec>(), null, null);

    TezExecutors sharedExecutor = new TezSharedExecutor(tezConf);
    TezTaskRunner2 taskRunner;
    if (testRunner) {
      taskRunner = new TezTaskRunner2ForTest(tezConf, ugi, localDirs, taskSpec, 1,
          new HashMap<String, ByteBuffer>(), new HashMap<String, String>(),
          HashMultimap.<String, String>create(), taskReporter,
          executor, null, "", new ExecutionContextImpl("localhost"),
          Runtime.getRuntime().maxMemory(), updateSysCounters, sharedExecutor);
    } else {
      taskRunner = new TezTaskRunner2(tezConf, ugi, localDirs, taskSpec, 1,
          new HashMap<String, ByteBuffer>(), new HashMap<String, String>(),
          HashMultimap.<String, String>create(), taskReporter,
          executor, null, "", new ExecutionContextImpl("localhost"),
          Runtime.getRuntime().maxMemory(), updateSysCounters, new DefaultHadoopShim(),
          sharedExecutor);
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
                                 long memAvailable,
                                 boolean updateSysCounters,
                                 TezExecutors sharedExecutor) throws IOException {
      super(tezConf, ugi, localDirs, taskSpec, appAttemptNumber, serviceConsumerMetadata,
          serviceProviderEnvMap, startedInputsMap, taskReporter, executor, objectRegistry, pid,
          executionContext, memAvailable, updateSysCounters, new DefaultHadoopShim(),
          sharedExecutor);
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
