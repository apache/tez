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

package org.apache.tez.dag.app;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.apache.tez.dag.app.MockDAGAppMaster.MockContainerLauncher;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.app.dag.impl.VertexImpl;
import org.apache.tez.dag.app.dag.speculation.legacy.LegacySpeculator;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Joiner;

public class TestSpeculation {
  static Configuration defaultConf;
  static FileSystem localFs;
  
  MockDAGAppMaster mockApp;
  MockContainerLauncher mockLauncher;
  
  static {
    try {
      defaultConf = new Configuration(false);
      defaultConf.set("fs.defaultFS", "file:///");
      defaultConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
      defaultConf.setBoolean(TezConfiguration.TEZ_AM_SPECULATION_ENABLED, true);
      defaultConf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION, 1);
      defaultConf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION, 1);
      localFs = FileSystem.getLocal(defaultConf);
      String stagingDir = "target" + Path.SEPARATOR + TestSpeculation.class.getName() + "-tmpDir";
      defaultConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDir);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }
  
  MockTezClient createTezSession() throws Exception {
    TezConfiguration tezconf = new TezConfiguration(defaultConf);
    AtomicBoolean mockAppLauncherGoFlag = new AtomicBoolean(false);
    MockTezClient tezClient = new MockTezClient("testspeculation", tezconf, true, null, null,
        new MockClock(), mockAppLauncherGoFlag, false, false, 1, 2);
    tezClient.start();
    syncWithMockAppLauncher(false, mockAppLauncherGoFlag, tezClient);
    return tezClient;
  }
  
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

  @Test (timeout = 10000)
  public void testSingleTaskSpeculation() throws Exception {
    // Map<Timeout conf value, expected number of tasks>
    Map<Long, Integer> confToExpected = new HashMap<Long, Integer>();
    confToExpected.put(Long.MAX_VALUE >> 1, 1); // Really long time to speculate
    confToExpected.put(100L, 2);
    confToExpected.put(-1L, 1); // Don't speculate

    for(Map.Entry<Long, Integer> entry : confToExpected.entrySet()) {
      defaultConf.setLong(
              TezConfiguration.TEZ_AM_LEGACY_SPECULATIVE_SINGLE_TASK_VERTEX_TIMEOUT,
              entry.getKey());
      DAG dag = DAG.create("test");
      Vertex vA = Vertex.create("A",
              ProcessorDescriptor.create("Proc.class"),
              1);
      dag.addVertex(vA);

      MockTezClient tezClient = createTezSession();

      DAGClient dagClient = tezClient.submitDAG(dag);
      DAGImpl dagImpl = (DAGImpl) mockApp.getContext().getCurrentDAG();
      TezVertexID vertexId = TezVertexID.getInstance(dagImpl.getID(), 0);
      // original attempt is killed and speculative one is successful
      TezTaskAttemptID killedTaId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), 0);
      TezTaskAttemptID successTaId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), 1);

      Thread.sleep(200);
      // cause speculation trigger
      mockLauncher.setStatusUpdatesForTask(killedTaId, 100);

      mockLauncher.startScheduling(true);
      dagClient.waitForCompletion();
      Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
      Task task = dagImpl.getTask(killedTaId.getTaskID());
      Assert.assertEquals(entry.getValue().intValue(), task.getAttempts().size());
      if (entry.getValue() > 1) {
        Assert.assertEquals(successTaId, task.getSuccessfulAttempt().getID());
        TaskAttempt killedAttempt = task.getAttempt(killedTaId);
        Joiner.on(",").join(killedAttempt.getDiagnostics()).contains("Killed as speculative attempt");
        Assert.assertEquals(TaskAttemptTerminationCause.TERMINATED_EFFECTIVE_SPECULATION,
                killedAttempt.getTerminationCause());
      }
      tezClient.stop();
    }
  }

  public void testBasicSpeculation(boolean withProgress) throws Exception {

    defaultConf.setInt(TezConfiguration.TEZ_AM_MINIMUM_ALLOWED_SPECULATIVE_TASKS, 20);
    defaultConf.setDouble(TezConfiguration.TEZ_AM_PROPORTION_TOTAL_TASKS_SPECULATABLE, 0.2);
    defaultConf.setDouble(TezConfiguration.TEZ_AM_PROPORTION_RUNNING_TASKS_SPECULATABLE, 0.25);
    defaultConf.setLong(TezConfiguration.TEZ_AM_SOONEST_RETRY_AFTER_NO_SPECULATE, 2000);
    defaultConf.setLong(TezConfiguration.TEZ_AM_SOONEST_RETRY_AFTER_SPECULATE, 10000);

    DAG dag = DAG.create("test");
    Vertex vA = Vertex.create("A", ProcessorDescriptor.create("Proc.class"), 5);
    dag.addVertex(vA);

    MockTezClient tezClient = createTezSession();
    
    DAGClient dagClient = tezClient.submitDAG(dag);
    DAGImpl dagImpl = (DAGImpl) mockApp.getContext().getCurrentDAG();
    TezVertexID vertexId = TezVertexID.getInstance(dagImpl.getID(), 0);
    // original attempt is killed and speculative one is successful
    TezTaskAttemptID killedTaId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), 0);
    TezTaskAttemptID successTaId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), 1);

    mockLauncher.updateProgress(withProgress);
    // cause speculation trigger
    mockLauncher.setStatusUpdatesForTask(killedTaId, 100);

    mockLauncher.startScheduling(true);
    dagClient.waitForCompletion();
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
    Task task = dagImpl.getTask(killedTaId.getTaskID());
    Assert.assertEquals(2, task.getAttempts().size());
    Assert.assertEquals(successTaId, task.getSuccessfulAttempt().getID());
    TaskAttempt killedAttempt = task.getAttempt(killedTaId);
    Joiner.on(",").join(killedAttempt.getDiagnostics()).contains("Killed as speculative attempt");
    Assert.assertEquals(TaskAttemptTerminationCause.TERMINATED_EFFECTIVE_SPECULATION, 
        killedAttempt.getTerminationCause());
    if (withProgress) {
      // without progress updates occasionally more than 1 task speculates
      Assert.assertEquals(1, task.getCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
          .getValue());
      Assert.assertEquals(1, dagImpl.getAllCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
          .getValue());
      org.apache.tez.dag.app.dag.Vertex v = dagImpl.getVertex(killedTaId.getTaskID().getVertexID());
      Assert.assertEquals(1, v.getAllCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
          .getValue());
    }

    LegacySpeculator speculator = ((VertexImpl) dagImpl.getVertex(vA.getName())).getSpeculator();
    Assert.assertEquals(20, speculator.getMinimumAllowedSpeculativeTasks());
    Assert.assertEquals(.2, speculator.getProportionTotalTasksSpeculatable(), 0);
    Assert.assertEquals(.25, speculator.getProportionRunningTasksSpeculatable(), 0);
    Assert.assertEquals(2000, speculator.getSoonestRetryAfterNoSpeculate());
    Assert.assertEquals(10000, speculator.getSoonestRetryAfterSpeculate());

    tezClient.stop();
  }
  
  @Test (timeout=10000)
  public void testBasicSpeculationWithProgress() throws Exception {
    testBasicSpeculation(true);
  }

  @Test (timeout=10000)
  public void testBasicSpeculationWithoutProgress() throws Exception {
    testBasicSpeculation(false);
  }
  
  @Test (timeout=10000)
  public void testBasicSpeculationPerVertexConf() throws Exception {
    DAG dag = DAG.create("test");
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
    TezVertexID vertexId = dagImpl.getVertex(vNameSpec).getVertexId();
    TezVertexID vertexIdNoSpec = dagImpl.getVertex(vNameNoSpec).getVertexId();
    // original attempt is killed and speculative one is successful
    TezTaskAttemptID killedTaId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0),
        0);
    TezTaskAttemptID noSpecTaId = TezTaskAttemptID
        .getInstance(TezTaskID.getInstance(vertexIdNoSpec, 0), 0);

    // cause speculation trigger for both
    mockLauncher.setStatusUpdatesForTask(killedTaId, 100);
    mockLauncher.setStatusUpdatesForTask(noSpecTaId, 100);

    mockLauncher.startScheduling(true);
    dagClient.waitForCompletion();
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
    org.apache.tez.dag.app.dag.Vertex vSpec = dagImpl.getVertex(vertexId);
    org.apache.tez.dag.app.dag.Vertex vNoSpec = dagImpl.getVertex(vertexIdNoSpec);
    // speculation for vA but not for vB
    Assert.assertTrue(vSpec.getAllCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
        .getValue() > 0);
    Assert.assertEquals(0, vNoSpec.getAllCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
        .getValue());

    tezClient.stop();
  }

  @Test (timeout=10000)
  public void testBasicSpeculationNotUseful() throws Exception {
    DAG dag = DAG.create("test");
    Vertex vA = Vertex.create("A", ProcessorDescriptor.create("Proc.class"), 5);
    dag.addVertex(vA);

    MockTezClient tezClient = createTezSession();
    
    DAGClient dagClient = tezClient.submitDAG(dag);
    DAGImpl dagImpl = (DAGImpl) mockApp.getContext().getCurrentDAG();
    TezVertexID vertexId = TezVertexID.getInstance(dagImpl.getID(), 0);
    // original attempt is successful and speculative one is killed
    TezTaskAttemptID successTaId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), 0);
    TezTaskAttemptID killedTaId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), 1);

    mockLauncher.setStatusUpdatesForTask(successTaId, 100);
    mockLauncher.setStatusUpdatesForTask(killedTaId, 100);

    mockLauncher.startScheduling(true);
    dagClient.waitForCompletion();
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
    Task task = dagImpl.getTask(killedTaId.getTaskID());
    Assert.assertEquals(2, task.getAttempts().size());
    Assert.assertEquals(successTaId, task.getSuccessfulAttempt().getID());
    TaskAttempt killedAttempt = task.getAttempt(killedTaId);
    Joiner.on(",").join(killedAttempt.getDiagnostics()).contains("Killed speculative attempt as");
    Assert.assertEquals(TaskAttemptTerminationCause.TERMINATED_INEFFECTIVE_SPECULATION, 
        killedAttempt.getTerminationCause());
    Assert.assertEquals(1, task.getCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
        .getValue());
    Assert.assertEquals(1, dagImpl.getAllCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
        .getValue());
    org.apache.tez.dag.app.dag.Vertex v = dagImpl.getVertex(killedTaId.getTaskID().getVertexID());
    Assert.assertEquals(1, v.getAllCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
        .getValue());
    tezClient.stop();
  }

}
