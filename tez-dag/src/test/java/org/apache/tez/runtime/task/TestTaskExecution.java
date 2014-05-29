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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.log4j.Logger;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.events.TaskAttemptCompletedEvent;
import org.apache.tez.runtime.api.events.TaskAttemptFailedEvent;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

// Tests in this class cannot be run in parallel.
public class TestTaskExecution {

  private static final Logger LOG = Logger.getLogger(TestTaskExecution.class);

  private static final String HEARTBEAT_EXCEPTION_STRING = "HeartbeatException";

  private static final Configuration defaultConf = new Configuration();
  private static final FileSystem localFs;
  private static final Path workDir;

  private static final ExecutorService taskExecutor = Executors.newFixedThreadPool(1);

  static {
    defaultConf.set("fs.defaultFS", "file:///");
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

  @Test
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
      Future<Boolean> taskRunnerFuture = taskExecutor.submit(new TaskRunnerCallable(taskRunner));
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

  @Test
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
      Future<Boolean> taskRunnerFuture = taskExecutor.submit(new TaskRunnerCallable(taskRunner));
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
      taskRunnerFuture = taskExecutor.submit(new TaskRunnerCallable(taskRunner));
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

  @Test
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
      Future<Boolean> taskRunnerFuture = taskExecutor.submit(new TaskRunnerCallable(taskRunner));
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
      umbilical.verifyTaskFailedEvent();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
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
      Future<Boolean> taskRunnerFuture = taskExecutor.submit(new TaskRunnerCallable(taskRunner));
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
        assertTrue(cause.getMessage().contains(HEARTBEAT_EXCEPTION_STRING));
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

  @Test
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
      Future<Boolean> taskRunnerFuture = taskExecutor.submit(new TaskRunnerCallable(taskRunner));
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

  @Test
  public void testGetTaskShouldDie() throws InterruptedException, ExecutionException {
    ListeningExecutorService executor = null;
    try {
      ExecutorService rawExecutor = Executors.newFixedThreadPool(1);
      executor = MoreExecutors.listeningDecorator(rawExecutor);
      ApplicationId appId = ApplicationId.newInstance(10000, 1);
      ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
      ContainerId containerId = ContainerId.newInstance(appAttemptId, 1);

      TezTaskUmbilicalForTest umbilical = new TezTaskUmbilicalForTest();
      ContainerContext containerContext = new ContainerContext(containerId.toString());

      ContainerReporter containerReporter = new ContainerReporter(umbilical, containerContext, 100);
      ListenableFuture<ContainerTask> getTaskFuture = executor.submit(containerReporter);

      getTaskFuture.get();
      assertEquals(1, umbilical.getTaskInvocations);

    } finally {
      executor.shutdownNow();
    }
  }

  // Potential new tests
  // Different states - initialization failure, close failure
  // getTask states

  private static class TaskRunnerCallable implements Callable<Boolean> {
    private final TezTaskRunner taskRunner;

    public TaskRunnerCallable(TezTaskRunner taskRunner) {
      this.taskRunner = taskRunner;
    }

    @Override
    public Boolean call() throws Exception {
      return taskRunner.run();
    }
  }

  // Uses static fields for signaling. Ensure only used by one test at a time.
  public static class TestProcessor extends SimpleProcessor {

    public static final byte[] CONF_EMPTY = new byte[] { 0 };
    public static final byte[] CONF_THROW_IO_EXCEPTION = new byte[] { 1 };
    public static final byte[] CONF_THROW_TEZ_EXCEPTION = new byte[] { 2 };
    public static final byte[] CONF_SIGNAL_FATAL_AND_THROW = new byte[] { 4 };
    public static final byte[] CONF_SIGNAL_FATAL_AND_LOOP = new byte[] { 8 };
    public static final byte[] CONF_SIGNAL_FATAL_AND_COMPLETE = new byte[] { 16 };

    private static final Logger LOG = Logger.getLogger(TestProcessor.class);

    private static final ReentrantLock processorLock = new ReentrantLock();
    private static final Condition processorCondition = processorLock.newCondition();
    private static final Condition completionCondition = processorLock.newCondition();
    private static final Condition runningCondition = processorLock.newCondition();
    private static boolean completed = false;
    private static boolean running = false;
    private static boolean signalled = false;

    public static boolean receivedInterrupt = false;

    private boolean throwIOException = false;
    private boolean throwTezException = false;
    private boolean signalFatalAndThrow = false;
    private boolean signalFatalAndLoop = false;
    private boolean signalFatalAndComplete = false;

    @Override
    public void initialize() throws Exception {
      parseConf(getContext().getUserPayload());
    }

    private void parseConf(byte[] bytes) {
      byte b = bytes[0];
      throwIOException = (b & 1) > 1;
      throwTezException = (b & 2) > 1;
      signalFatalAndThrow = (b & 4) > 1;
      signalFatalAndLoop = (b & 8) > 1;
      signalFatalAndComplete = (b & 16) > 1;
    }

    public static void reset() {
      signalled = false;
      receivedInterrupt = false;
      completed = false;
      running = false;
    }

    public static void signal() {
      LOG.info("Signalled");
      processorLock.lock();
      try {
        signalled = true;
        processorCondition.signal();
      } finally {
        processorLock.unlock();
      }
    }

    public static void awaitStart() throws InterruptedException {
      LOG.info("Awaiting Process run");
      processorLock.lock();
      try {
        if (running) {
          return;
        }
        runningCondition.await();
      } finally {
        processorLock.unlock();
      }
    }

    public static void awaitCompletion() throws InterruptedException {
      LOG.info("Await completion");
      processorLock.lock();
      try {
        if (completed) {
          return;
        } else {
          completionCondition.await();
        }
      } finally {
        processorLock.unlock();
      }
    }

    public static boolean wasInterrupted() {
      processorLock.lock();
      try {
        return receivedInterrupt;
      } finally {
        processorLock.unlock();
      }
    }

    @Override
    public void run() throws Exception {
      processorLock.lock();
      running = true;
      runningCondition.signal();
      try {
        try {
          LOG.info("Signal is: " + signalled);
          if (!signalled) {
            LOG.info("Waiting for processor signal");
            processorCondition.await();
          }
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
          }
          LOG.info("Received processor signal");
          if (throwIOException) {
            throw new IOException();
          } else if (throwTezException) {
            throw new TezException("TezException");
          } else if (signalFatalAndThrow) {
            IOException io = new IOException("FATALERROR");
            getContext().fatalError(io, "FATALERROR");
            throw io;
          } else if (signalFatalAndComplete) {
            IOException io = new IOException("FATALERROR");
            getContext().fatalError(io, "FATALERROR");
            return;
          } else if (signalFatalAndLoop) {
            IOException io = new IOException("FATALERROR");
            getContext().fatalError(io, "FATALERROR");
            LOG.info("Waiting for Processor signal again");
            processorCondition.await();
            LOG.info("Received second processor signal");
          }
        } catch (InterruptedException e) {
          receivedInterrupt = true;
        }
      } finally {
        completed = true;
        completionCondition.signal();
        processorLock.unlock();
      }
    }
  }

  private static class TezTaskUmbilicalForTest implements TezTaskUmbilicalProtocol {

    private static final Logger LOG = Logger.getLogger(TezTaskUmbilicalForTest.class);

    private final List<TezEvent> requestEvents = new LinkedList<TezEvent>();

    private final ReentrantLock umbilicalLock = new ReentrantLock();
    private final Condition eventCondition = umbilicalLock.newCondition();
    private boolean pendingEvent = false;
    private boolean eventEnacted = false;
    
    volatile int getTaskInvocations = 0;

    private boolean shouldThrowException = false;
    private boolean shouldSendDieSignal = false;

    public void signalThrowException() {
      umbilicalLock.lock();
      try {
        shouldThrowException = true;
        pendingEvent = true;
      } finally {
        umbilicalLock.unlock();
      }
    }

    public void signalSendShouldDie() {
      umbilicalLock.lock();
      try {
        shouldSendDieSignal = true;
        pendingEvent = true;
      } finally {
        umbilicalLock.unlock();
      }
    }

    public void awaitRegisteredEvent() throws InterruptedException {
      umbilicalLock.lock();
      try {
        if (eventEnacted) {
          return;
        }
        LOG.info("Awaiting event");
        eventCondition.await();
      } finally {
        umbilicalLock.unlock();
      }
    }

    public void resetTrackedEvents() {
      umbilicalLock.lock();
      try {
        requestEvents.clear();
      } finally {
        umbilicalLock.unlock();
      }
    }

    public void verifyNoCompletionEvents() {
      umbilicalLock.lock();
      try {
        for (TezEvent event : requestEvents) {
          if (event.getEvent() instanceof TaskAttemptFailedEvent) {
            fail("Found a TaskAttemptFailedEvent when not expected");
          }
          if (event.getEvent() instanceof TaskAttemptCompletedEvent) {
            fail("Found a TaskAttemptCompletedvent when not expected");
          }
        }
      } finally {
        umbilicalLock.unlock();
      }
    }

    public void verifyTaskFailedEvent() {
      umbilicalLock.lock();
      try {
        for (TezEvent event : requestEvents) {
          if (event.getEvent() instanceof TaskAttemptFailedEvent) {
            return;
          }
        }
        fail("No TaskAttemptFailedEvents sent over umbilical");
      } finally {
        umbilicalLock.unlock();
      }
    }

    public void verifyTaskSuccessEvent() {
      umbilicalLock.lock();
      try {
        for (TezEvent event : requestEvents) {
          if (event.getEvent() instanceof TaskAttemptCompletedEvent) {
            return;
          }
        }
        fail("No TaskAttemptFailedEvents sent over umbilical");
      } finally {
        umbilicalLock.unlock();
      }
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
      return 0;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
        int clientMethodsHash) throws IOException {
      return null;
    }

    @Override
    public ContainerTask getTask(ContainerContext containerContext) throws IOException {
      // Return shouldDie = true
      getTaskInvocations++;
      return new ContainerTask(null, true, null, null, false);
    }

    @Override
    public boolean canCommit(TezTaskAttemptID taskid) throws IOException {
      return true;
    }

    @Override
    public TezHeartbeatResponse heartbeat(TezHeartbeatRequest request) throws IOException,
        TezException {
      umbilicalLock.lock();
      if (request.getEvents() != null) {
        requestEvents.addAll(request.getEvents());
      }
      try {
        if (shouldThrowException) {
          LOG.info("TestUmbilical throwing Exception");
          throw new IOException(HEARTBEAT_EXCEPTION_STRING);
        }
        TezHeartbeatResponse response = new TezHeartbeatResponse();
        response.setLastRequestId(request.getRequestId());
        if (shouldSendDieSignal) {
          LOG.info("TestUmbilical returning shouldDie=true");
          response.setShouldDie();
        }
        return response;
      } finally {
        if (pendingEvent) {
          eventEnacted = true;
          LOG.info("Signalling Event");
          eventCondition.signal();
        }
        umbilicalLock.unlock();
      }
    }
  }

  private TaskReporter createTaskReporter(ApplicationId appId, TezTaskUmbilicalForTest umbilical) {
    TaskReporter taskReporter = new TaskReporter(umbilical, 100, 1000, 100, new AtomicLong(0),
        createContainerId(appId).toString());
    return taskReporter;
  }

  private TezTaskRunner createTaskRunner(ApplicationId appId, TezTaskUmbilicalForTest umbilical,
      TaskReporter taskReporter, ListeningExecutorService executor, byte[] processorConf)
      throws IOException {
    TezConfiguration tezConf = new TezConfiguration(defaultConf);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    Path testDir = new Path(workDir, UUID.randomUUID().toString());
    String[] localDirs = new String[] { testDir.toString() };

    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexId = TezVertexID.getInstance(dagId, 1);
    TezTaskID taskId = TezTaskID.getInstance(vertexId, 1);
    TezTaskAttemptID taskAttemptId = TezTaskAttemptID.getInstance(taskId, 1);
    ProcessorDescriptor processorDescriptor = new ProcessorDescriptor(TestProcessor.class.getName())
        .setUserPayload(processorConf);
    TaskSpec taskSpec = new TaskSpec(taskAttemptId, "dagName", "vertexName", processorDescriptor,
        new ArrayList<InputSpec>(), new ArrayList<OutputSpec>(), null);

    TezTaskRunner taskRunner = new TezTaskRunner(tezConf, ugi, localDirs, taskSpec, umbilical, 1,
        new HashMap<String, ByteBuffer>(), HashMultimap.<String, String> create(), taskReporter,
        executor);
    return taskRunner;
  }

  private ContainerId createContainerId(ApplicationId appId) {
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newInstance(appAttemptId, 1);
    return containerId;
  }
}
