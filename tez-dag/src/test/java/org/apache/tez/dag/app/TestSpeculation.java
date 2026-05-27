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
package org.apache.tez.dag.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.DAGStatus.State;
import org.apache.tez.dag.app.MockDAGAppMaster.MockContainerLauncher;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.app.dag.speculation.legacy.LegacySpeculator;
import org.apache.tez.dag.app.dag.speculation.legacy.LegacyTaskRuntimeEstimator;
import org.apache.tez.dag.app.dag.speculation.legacy.SimpleExponentialTaskRuntimeEstimator;
import org.apache.tez.dag.app.dag.speculation.legacy.TaskRuntimeEstimator;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;

import com.google.common.base.Joiner;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * test speculation behavior given the list of estimator classes.
 */
public class TestSpeculation {
  private final static Logger LOG = LoggerFactory.getLogger(TezConfiguration.class);

  private static final String ASSERT_SPECULATIONS_COUNT_MSG =
      "Number of attempts after Speculation should be two";
  private static final String UNIT_EXCEPTION_MESSAGE =
      "test timed out after";

  /**
   * {@link MockDAGAppMaster#launcherSleepTime} advances tasks every 1 millisecond.
   * We want our test task to take at least slightly more than 1 second. This is because
   * MockDAGAppMaster's mock clock advances clock 1 second at each tick. If we are unlucky
   * this may cause speculator to wait 1 second between each evaluation. If we are really
   * unlucky, our test tasks finish before speculator has a chance to evaluate and speculate
   * them. That is why we want the tasks to take at least one second.
   */
  private static final int NUM_UPDATES_FOR_TEST_TASK = 1200;
  private static final int ASSERT_SPECULATIONS_COUNT_RETRIES = 3;
  private Configuration defaultConf;
  private FileSystem localFs;

  /**
   * The Mock app.
   */
  MockDAGAppMaster mockApp;

  /**
   * The Mock launcher.
   */
  MockContainerLauncher mockLauncher;

  private interface TestTask {
    void run() throws Exception;
  }

  private void runWithRetry(TestTask task) throws Exception {
    int retries = ASSERT_SPECULATIONS_COUNT_RETRIES;
    while (retries-- > 0) {
      try {
        task.run();
        return;
      } catch (Throwable t) {
        if (retries > 0 &&
            ((t instanceof AssertionError && t.getMessage().contains(ASSERT_SPECULATIONS_COUNT_MSG))
                || (t instanceof Exception && t.getMessage().contains(UNIT_EXCEPTION_MESSAGE)))) {
          LOG.warn("Test failed. Retries remaining: {}", retries);
        } else {
          if (t instanceof Exception) {
            throw (Exception) t;
          }
          throw new RuntimeException(t);
        }
      }
    }
  }

  private Class<? extends TaskRuntimeEstimator> estimatorClass;

  public void setup(Class<? extends TaskRuntimeEstimator> estimatorClass) {
    this.estimatorClass = estimatorClass;
    setDefaultConf();
  }

  /**
   * Sets default conf.
   */
  public void setDefaultConf() {
    try {
      defaultConf = new Configuration(false);
      defaultConf.set("fs.defaultFS", "file:///");
      defaultConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
      defaultConf.setBoolean(TezConfiguration.TEZ_AM_SPECULATION_ENABLED, true);
      defaultConf.setFloat(
          ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION, 1);
      defaultConf.setFloat(
          ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION, 1);
      localFs = FileSystem.getLocal(defaultConf);
      String stagingDir =
          "target" + Path.SEPARATOR + TestSpeculation.class.getName()
              + "-tmpDir";
      defaultConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDir);
      defaultConf.setClass(TezConfiguration.TEZ_AM_TASK_ESTIMATOR_CLASS,
          estimatorClass,
          TaskRuntimeEstimator.class);
      defaultConf.setInt(TezConfiguration.TEZ_AM_MINIMUM_ALLOWED_SPECULATIVE_TASKS, 20);
      defaultConf.setDouble(TezConfiguration.TEZ_AM_PROPORTION_TOTAL_TASKS_SPECULATABLE, 0.2);
      defaultConf.setDouble(TezConfiguration.TEZ_AM_PROPORTION_RUNNING_TASKS_SPECULATABLE, 0.25);
      defaultConf.setLong(TezConfiguration.TEZ_AM_SOONEST_RETRY_AFTER_NO_SPECULATE, 25);
      defaultConf.setLong(TezConfiguration.TEZ_AM_SOONEST_RETRY_AFTER_SPECULATE, 50);
      defaultConf.setInt(TezConfiguration.TEZ_AM_ESTIMATOR_EXPONENTIAL_SKIP_INITIALS, 2);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  /**
   * Tear down.
   */
  @AfterEach
  public void tearDown() {
    defaultConf = null;
    try {
      if (localFs != null) {
        localFs.close();
      }
      if (mockLauncher != null) {
        mockLauncher.shutdown();
      }
      if (mockApp != null) {
        mockApp.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Gets test parameters.
   *
   * @return the test parameters
   */
  public static Stream<Class<? extends TaskRuntimeEstimator>> getTestParameters() {
    return Stream.of(
        SimpleExponentialTaskRuntimeEstimator.class,
        LegacyTaskRuntimeEstimator.class
    );
  }

  /**
   * Create tez session mock tez client.
   *
   * @return the mock tez client
   * @throws Exception the exception
   */
  MockTezClient createTezSession() throws Exception {
    TezConfiguration tezconf = new TezConfiguration(defaultConf);
    AtomicBoolean mockAppLauncherGoFlag = new AtomicBoolean(false);
    MockTezClient tezClient = new MockTezClient("testspeculation", tezconf, true, null, null,
        new MockClock(), mockAppLauncherGoFlag, false, false, 1, 2);
    tezClient.start();
    syncWithMockAppLauncher(false, mockAppLauncherGoFlag, tezClient);
    return tezClient;
  }

  /**
   * Sync with mock app launcher.
   *
   * @param allowScheduling the allow scheduling
   * @param mockAppLauncherGoFlag the mock app launcher go flag
   * @param tezClient the tez client
   * @throws Exception the exception
   */
  void syncWithMockAppLauncher(boolean allowScheduling, AtomicBoolean mockAppLauncherGoFlag,
      MockTezClient tezClient) throws Exception {
    synchronized (mockAppLauncherGoFlag) {
      while (!mockAppLauncherGoFlag.get()) {
        mockAppLauncherGoFlag.wait();
      }
      mockApp = tezClient.getLocalClient().getMockApp();
      mockLauncher = mockApp.getContainerLauncher();
      mockLauncher.startScheduling(allowScheduling);
      mockAppLauncherGoFlag.notify();
    }
  }

  /**
   * Test single task speculation.
   *
   * @throws Exception the exception
   */
  @ParameterizedTest(name = "{index}: TaskEstimator(EstimatorClass {0})")
  @MethodSource("getTestParameters")
  @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
  public void testSingleTaskSpeculation(Class<? extends TaskRuntimeEstimator> estimatorClass) throws Exception {
    setup(estimatorClass);
    runWithRetry(() -> {
      // Map<Timeout conf value, expected number of tasks>
      Map<Long, Integer> confToExpected = new HashMap<Long, Integer>();
      confToExpected.put(Long.MAX_VALUE >> 1, 1); // Really long time to speculate
      confToExpected.put(100L, 2);
      confToExpected.put(-1L, 1); // Don't speculate
      defaultConf.setLong(TezConfiguration.TEZ_AM_SOONEST_RETRY_AFTER_NO_SPECULATE, 50);
      for(Map.Entry<Long, Integer> entry : confToExpected.entrySet()) {
        defaultConf.setLong(
                TezConfiguration.TEZ_AM_LEGACY_SPECULATIVE_SINGLE_TASK_VERTEX_TIMEOUT,
                entry.getKey());

        DAG dag = DAG.create("DAG-testSingleTaskSpeculation");
        Vertex vA = Vertex.create("A",
                ProcessorDescriptor.create("Proc.class"),
                1);
        dag.addVertex(vA);

        MockTezClient tezClient = createTezSession();

        DAGClient dagClient = tezClient.submitDAG(dag);
        DAGImpl dagImpl = (DAGImpl) mockApp.getContext().getCurrentDAG();
        TezVertexID vertexId = TezVertexID.getInstance(dagImpl.getID(), 0);
        // original attempt is killed and speculative one is successful
        TezTaskAttemptID killedTaId =
            TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), 0);
        TezTaskAttemptID successTaId =
            TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), 1);
        Thread.sleep(200);
        // cause speculation trigger
        mockLauncher.setStatusUpdatesForTask(killedTaId, NUM_UPDATES_FOR_TEST_TASK);

        mockLauncher.startScheduling(true);
        dagClient.waitForCompletion();
        assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
        Task task = dagImpl.getTask(killedTaId.getTaskID());
        assertEquals(entry.getValue().intValue(), task.getAttempts().size());
        if (entry.getValue() > 1) {
          assertEquals(successTaId, task.getSuccessfulAttempt().getTaskAttemptID());
          TaskAttempt killedAttempt = task.getAttempt(killedTaId);
          Joiner.on(",").join(killedAttempt.getDiagnostics()).contains("Killed as speculative attempt");
          assertEquals(TaskAttemptTerminationCause.TERMINATED_EFFECTIVE_SPECULATION,
                  killedAttempt.getTerminationCause());
        }
        tezClient.stop();
      }
    });
  }

  /**
   * Test basic speculation.
   *
   * @param withProgress the with progress
   * @throws Exception the exception
   */
  public void testBasicSpeculation(boolean withProgress) throws Exception {
    DAG dag = DAG.create("DAG-testBasicSpeculation");
    Vertex vA = Vertex.create("A",
        ProcessorDescriptor.create("Proc.class"), 5);
    dag.addVertex(vA);

    MockTezClient tezClient = createTezSession();
    DAGClient dagClient = tezClient.submitDAG(dag);
    DAGImpl dagImpl = (DAGImpl) mockApp.getContext().getCurrentDAG();
    TezVertexID vertexId = TezVertexID.getInstance(dagImpl.getID(), 0);
    // original attempt is killed and speculative one is successful
    TezTaskAttemptID killedTaId =
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), 0);
    TezTaskAttemptID successTaId =
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), 1);

    mockLauncher.updateProgress(withProgress);
    // cause speculation trigger
    mockLauncher.setStatusUpdatesForTask(killedTaId, NUM_UPDATES_FOR_TEST_TASK);

    mockLauncher.startScheduling(true);
    dagClient.waitForCompletion();
    assertEquals(State.SUCCEEDED,
        dagClient.getDAGStatus(null).getState());
    Task task = dagImpl.getTask(killedTaId.getTaskID());
    assertEquals(2, task.getAttempts().size(), ASSERT_SPECULATIONS_COUNT_MSG);
    assertEquals(successTaId, task.getSuccessfulAttempt().getTaskAttemptID());
    TaskAttempt killedAttempt = task.getAttempt(killedTaId);
    Joiner.on(",").join(killedAttempt.getDiagnostics()).contains("Killed as speculative attempt");
    assertEquals(TaskAttemptTerminationCause.TERMINATED_EFFECTIVE_SPECULATION,
        killedAttempt.getTerminationCause());
    if (withProgress) {
      // without progress updates occasionally more than 1 task speculates
      assertEquals(1, task.getCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
          .getValue());
      assertEquals(1, dagImpl.getAllCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
          .getValue());
      org.apache.tez.dag.app.dag.Vertex v = dagImpl.getVertex(killedTaId.getVertexID());
      assertEquals(1, v.getAllCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
          .getValue());
    }

    LegacySpeculator speculator =
        (LegacySpeculator)(dagImpl.getVertex(vA.getName())).getSpeculator();
    assertEquals(20, speculator.getMinimumAllowedSpeculativeTasks());
    assertEquals(.2, speculator.getProportionTotalTasksSpeculatable(), 0);
    assertEquals(.25, speculator.getProportionRunningTasksSpeculatable(), 0);
    assertEquals(25, speculator.getSoonestRetryAfterNoSpeculate());
    assertEquals(50, speculator.getSoonestRetryAfterSpeculate());

    tezClient.stop();
  }

  /**
   * Test basic speculation with progress.
   *
   * @throws Exception the exception
   */
  @ParameterizedTest(name = "{index}: TaskEstimator(EstimatorClass {0})")
  @MethodSource("getTestParameters")
  @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
  public void testBasicSpeculationWithProgress(Class<? extends TaskRuntimeEstimator> estimatorClass) throws Exception {
    setup(estimatorClass);
    runWithRetry(() -> testBasicSpeculation(true));
  }

  /**
   * Test basic speculation without progress.
   *
   * @throws Exception the exception
   */
  @ParameterizedTest(name = "{index}: TaskEstimator(EstimatorClass {0})")
  @MethodSource("getTestParameters")
  @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
  public void testBasicSpeculationWithoutProgress(Class<? extends TaskRuntimeEstimator> estimatorClass) throws Exception {
    setup(estimatorClass);
    runWithRetry(() -> testBasicSpeculation(false));
  }

  /**
   * Test basic speculation per vertex conf.
   *
   * @throws Exception the exception
   */
  @ParameterizedTest(name = "{index}: TaskEstimator(EstimatorClass {0})")
  @MethodSource("getTestParameters")
  @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
  public void testBasicSpeculationPerVertexConf(Class<? extends TaskRuntimeEstimator> estimatorClass) throws Exception {
    setup(estimatorClass);
    runWithRetry(() -> {
      DAG dag = DAG.create("DAG-testBasicSpeculationPerVertexConf");
      String vNameNoSpec = "A";
      String vNameSpec = "B";
      Vertex vA = Vertex.create(vNameNoSpec, ProcessorDescriptor.create("Proc.class"), 5);
      Vertex vB = Vertex.create(vNameSpec, ProcessorDescriptor.create("Proc.class"), 5);
      vA.setConf(TezConfiguration.TEZ_AM_SPECULATION_ENABLED, "false");
      dag.addVertex(vA);
      dag.addVertex(vB);
      // min/max src fraction is set to 1. So vertices will run sequentially
      dag.addEdge(
          Edge.create(vA, vB,
              EdgeProperty.create(DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
                  SchedulingType.SEQUENTIAL, OutputDescriptor.create("O"),
                  InputDescriptor.create("I"))));

      MockTezClient tezClient = createTezSession();

      DAGClient dagClient = tezClient.submitDAG(dag);
      DAGImpl dagImpl = (DAGImpl) mockApp.getContext().getCurrentDAG();
      TezVertexID vertexIdSpec = dagImpl.getVertex(vNameSpec).getVertexId();
      TezVertexID vertexIdNoSpec = dagImpl.getVertex(vNameNoSpec).getVertexId();
      // original attempt is killed and speculative one is successful
      TezTaskAttemptID killedTaId =
          TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexIdSpec, 0), 0);
      TezTaskAttemptID successfulTaId = TezTaskAttemptID
          .getInstance(TezTaskID.getInstance(vertexIdNoSpec, 0), 0);

      // cause speculation trigger for both
      mockLauncher.setStatusUpdatesForTask(killedTaId, NUM_UPDATES_FOR_TEST_TASK);
      mockLauncher.setStatusUpdatesForTask(successfulTaId, NUM_UPDATES_FOR_TEST_TASK);

      mockLauncher.startScheduling(true);
      org.apache.tez.dag.app.dag.Vertex vSpec = dagImpl.getVertex(vertexIdSpec);
      org.apache.tez.dag.app.dag.Vertex vNoSpec = dagImpl.getVertex(vertexIdNoSpec);
      // Wait enough time to give chance for the speculator to trigger
      // speculation on VB.
      // This would fail because of JUnit time out.
      do {
        Thread.sleep(100);
      } while (vSpec.getAllCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
          .getValue() <= 0);
      dagClient.waitForCompletion();
      // speculation for vA but not for vB
      assertTrue(vSpec.getAllCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
              .getValue() > 0, "Num Speculations is not higher than 0");
      assertEquals(0,
          vNoSpec.getAllCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
              .getValue());

      tezClient.stop();
    });
  }

  /**
   * Test basic speculation not useful.
   *
   * @throws Exception the exception
   */
  @ParameterizedTest(name = "{index}: TaskEstimator(EstimatorClass {0})")
  @MethodSource("getTestParameters")
  @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
  public void testBasicSpeculationNotUseful(Class<? extends TaskRuntimeEstimator> estimatorClass) throws Exception {
    setup(estimatorClass);
    runWithRetry(() -> {
      DAG dag = DAG.create("DAG-testBasicSpeculationNotUseful");
      Vertex vA = Vertex.create("A", ProcessorDescriptor.create("Proc.class"), 5);
      dag.addVertex(vA);

      MockTezClient tezClient = createTezSession();

      DAGClient dagClient = tezClient.submitDAG(dag);
      DAGImpl dagImpl = (DAGImpl) mockApp.getContext().getCurrentDAG();
      TezVertexID vertexId = TezVertexID.getInstance(dagImpl.getID(), 0);
      // original attempt is successful and speculative one is killed
      TezTaskAttemptID successTaId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), 0);
      TezTaskAttemptID killedTaId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), 1);

      mockLauncher.setStatusUpdatesForTask(successTaId, NUM_UPDATES_FOR_TEST_TASK);
      mockLauncher.setStatusUpdatesForTask(killedTaId, NUM_UPDATES_FOR_TEST_TASK);

      mockLauncher.startScheduling(true);
      dagClient.waitForCompletion();
      assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
      Task task = dagImpl.getTask(killedTaId.getTaskID());
      assertEquals(2, task.getAttempts().size());
      assertEquals(successTaId, task.getSuccessfulAttempt().getTaskAttemptID());
      TaskAttempt killedAttempt = task.getAttempt(killedTaId);
      Joiner.on(",").join(killedAttempt.getDiagnostics()).contains("Killed speculative attempt as");
      assertEquals(TaskAttemptTerminationCause.TERMINATED_INEFFECTIVE_SPECULATION,
          killedAttempt.getTerminationCause());
      assertEquals(1, task.getCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
          .getValue());
      assertEquals(1, dagImpl.getAllCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
          .getValue());
      org.apache.tez.dag.app.dag.Vertex v = dagImpl.getVertex(killedTaId.getVertexID());
      assertEquals(1, v.getAllCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
          .getValue());
      tezClient.stop();
    });
  }
}
