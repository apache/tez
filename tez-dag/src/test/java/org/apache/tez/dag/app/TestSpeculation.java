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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.app.MockDAGAppMaster.MockContainerLauncher;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
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
  
  public void testBasicSpeculation(boolean withProgress) throws Exception {
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
      // without progress updates occasionally more than 1 task specualates
      Assert.assertEquals(1, task.getCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
          .getValue());
      Assert.assertEquals(1, dagImpl.getAllCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
          .getValue());
      org.apache.tez.dag.app.dag.Vertex v = dagImpl.getVertex(killedTaId.getTaskID().getVertexID());
      Assert.assertEquals(1, v.getAllCounters().findCounter(TaskCounter.NUM_SPECULATIONS)
          .getValue());
    }
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
