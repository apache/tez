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
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.app.dag.impl.TaskAttemptImpl;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestPreemption {
  
  static Configuration defaultConf;
  static FileSystem localFs;
  static Path workDir;
  
  static {
    try {
      defaultConf = new Configuration(false);
      defaultConf.set("fs.defaultFS", "file:///");
      defaultConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
      localFs = FileSystem.getLocal(defaultConf);
      workDir = new Path(new Path(System.getProperty("test.build.data", "/tmp")),
          "TestDAGAppMaster").makeQualified(localFs);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }
  
  MockDAGAppMaster mockApp;    
  MockContainerLauncher mockLauncher;
  
  int dagCount = 0;
  
  DAG createDAG(DataMovementType dmType) {
    DAG dag = DAG.create("test-" + dagCount++);
    Vertex vA = Vertex.create("A", ProcessorDescriptor.create("Proc.class"), 5);
    Vertex vB = Vertex.create("B", ProcessorDescriptor.create("Proc.class"), 5);
    Edge eAB = Edge.create(vA, vB, 
    EdgeProperty.create(dmType, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, OutputDescriptor.create("O.class"),
        InputDescriptor.create("I.class")));
    
    dag.addVertex(vA).addVertex(vB).addEdge(eAB);
    return dag;
  }
  
  @Test (timeout = 5000)
  public void testPreemptionWithoutSession() throws Exception {
    System.out.println("TestPreemptionWithoutSession");
    TezConfiguration tezconf = new TezConfiguration(defaultConf);
    tezconf.setInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS, 0);
    AtomicBoolean mockAppLauncherGoFlag = new AtomicBoolean(false);
    MockTezClient tezClient = new MockTezClient("testPreemption", tezconf, false, null, null,
        mockAppLauncherGoFlag);
    tezClient.start();
    
    DAGClient dagClient = tezClient.submitDAG(createDAG(DataMovementType.SCATTER_GATHER));
    // now the MockApp has been started. sync with it to get the launcher
    syncWithMockAppLauncher(false, mockAppLauncherGoFlag, tezClient);

    DAGImpl dagImpl;
    do {
      Thread.sleep(100); // usually needs to sleep 2-3 times
    } while ((dagImpl = (DAGImpl) mockApp.getContext().getCurrentDAG()) == null);

    int vertexIndex = 0;
    int upToTaskVersion = 3;
    TezVertexID vertexId = TezVertexID.getInstance(dagImpl.getID(), vertexIndex);
    TezTaskAttemptID taId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), 0);

    mockLauncher.preemptContainerForTask(taId.getTaskID(), upToTaskVersion);
    mockLauncher.startScheduling(true);
    
    dagClient.waitForCompletion();
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());

    for (int i=0; i<=upToTaskVersion; ++i) {
      TezTaskAttemptID testTaId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), i);      
      TaskAttemptImpl taImpl = dagImpl.getTaskAttempt(testTaId);
      Assert.assertEquals(TaskAttemptStateInternal.KILLED, taImpl.getInternalState());
    }
    
    tezClient.stop();
  }
  
  @Test (timeout = 30000)
  public void testPreemptionWithSession() throws Exception {
    System.out.println("TestPreemptionWithSession");
    MockTezClient tezClient = createTezSession();
    testPreemptionSingle(tezClient, createDAG(DataMovementType.SCATTER_GATHER), 0, "Scatter-Gather");
    testPreemptionMultiple(tezClient, createDAG(DataMovementType.SCATTER_GATHER), 0, "Scatter-Gather");
    testPreemptionSingle(tezClient, createDAG(DataMovementType.BROADCAST), 0, "Broadcast");
    testPreemptionMultiple(tezClient, createDAG(DataMovementType.BROADCAST), 0, "Broadcast");
    testPreemptionSingle(tezClient, createDAG(DataMovementType.ONE_TO_ONE), 0, "1-1");
    testPreemptionMultiple(tezClient, createDAG(DataMovementType.ONE_TO_ONE), 0, "1-1");
    testPreemptionSingle(tezClient, createDAG(DataMovementType.SCATTER_GATHER), 1, "Scatter-Gather");
    testPreemptionMultiple(tezClient, createDAG(DataMovementType.SCATTER_GATHER), 1, "Scatter-Gather");
    testPreemptionSingle(tezClient, createDAG(DataMovementType.BROADCAST), 1, "Broadcast");
    testPreemptionMultiple(tezClient, createDAG(DataMovementType.BROADCAST), 1, "Broadcast");
    testPreemptionSingle(tezClient, createDAG(DataMovementType.ONE_TO_ONE), 1, "1-1");
    testPreemptionMultiple(tezClient, createDAG(DataMovementType.ONE_TO_ONE), 1, "1-1");
    tezClient.stop();
  }
  
  MockTezClient createTezSession() throws Exception {
    TezConfiguration tezconf = new TezConfiguration(defaultConf);
    tezconf.setInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS, 0);
    AtomicBoolean mockAppLauncherGoFlag = new AtomicBoolean(false);
    MockTezClient tezClient = new MockTezClient("testPreemption", tezconf, true, null, null,
        mockAppLauncherGoFlag);
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
  
  void testPreemptionSingle(MockTezClient tezClient, DAG dag, int vertexIndex, String info)
      throws Exception {
    testPreemptionJob(tezClient, dag, vertexIndex, 0, info + "-Single");
  }

  void testPreemptionMultiple(MockTezClient tezClient, DAG dag, int vertexIndex, String info)
      throws Exception {
    testPreemptionJob(tezClient, dag, vertexIndex, 3, info + "-Multiple");
  }

  void testPreemptionJob(MockTezClient tezClient, DAG dag, int vertexIndex,
      int upToTaskVersion, String info) throws Exception {
    System.out.println("TestPreemption - Running - " + info);
    TezConfiguration tezconf = new TezConfiguration(defaultConf);
    tezconf.setInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS, 0);
    
    mockLauncher.startScheduling(false); // turn off scheduling to block DAG before submitting it
    DAGClient dagClient = tezClient.submitDAG(dag);
    
    DAGImpl dagImpl = (DAGImpl) mockApp.getContext().getCurrentDAG();
    TezVertexID vertexId = TezVertexID.getInstance(dagImpl.getID(), vertexIndex);
    TezTaskAttemptID taId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), 0);

    mockLauncher.preemptContainerForTask(taId.getTaskID(), upToTaskVersion);
    mockLauncher.startScheduling(true);
    
    dagClient.waitForCompletion();
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
    
    for (int i=0; i<=upToTaskVersion; ++i) {
      TezTaskAttemptID testTaId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), i);      
      TaskAttemptImpl taImpl = dagImpl.getTaskAttempt(testTaId);
      Assert.assertEquals(TaskAttemptStateInternal.KILLED, taImpl.getInternalState());
      Assert.assertEquals(TaskAttemptTerminationCause.EXTERNAL_PREEMPTION, taImpl.getTerminationCause());
    }
    
    System.out.println("TestPreemption - Done running - " + info);
  }
}
