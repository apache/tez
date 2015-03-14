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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.MockDAGAppMaster.CountersDelegate;
import org.apache.tez.dag.app.MockDAGAppMaster.MockContainerLauncher;
import org.apache.tez.dag.app.MockDAGAppMaster.MockContainerLauncher.ContainerData;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventSchedulingServiceError;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.app.dag.impl.TaskImpl;
import org.apache.tez.dag.app.dag.impl.VertexImpl;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TestMockDAGAppMaster {
  static Configuration defaultConf;
  static FileSystem localFs;
  
  static {
    try {
      defaultConf = new Configuration(false);
      defaultConf.set("fs.defaultFS", "file:///");
      defaultConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
      localFs = FileSystem.getLocal(defaultConf);
      String stagingDir = "target" + Path.SEPARATOR + TestMockDAGAppMaster.class.getName() + "-tmpDir";
      defaultConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDir);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }
  
  @Test (timeout = 5000)
  public void testLocalResourceSetup() throws Exception {
    TezConfiguration tezconf = new TezConfiguration(defaultConf);
    
    MockTezClient tezClient = new MockTezClient("testMockAM", tezconf, true, null, null, null, null);
    tezClient.start();
    
    MockDAGAppMaster mockApp = tezClient.getLocalClient().getMockApp();
    MockContainerLauncher mockLauncher = mockApp.getContainerLauncher();
    mockLauncher.startScheduling(false);
    
    Map<String, LocalResource> lrDAG = Maps.newHashMap();
    String lrName1 = "LR1";
    lrDAG.put(lrName1, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    Map<String, LocalResource> lrVertex = Maps.newHashMap();
    String lrName2 = "LR2";
    lrVertex.put(lrName2, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test1"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));

    DAG dag = DAG.create("test").addTaskLocalFiles(lrDAG);
    Vertex vA = Vertex.create("A", ProcessorDescriptor.create("Proc.class"), 5).addTaskLocalFiles(lrVertex);
    dag.addVertex(vA);

    DAGClient dagClient = tezClient.submitDAG(dag);
    mockLauncher.waitTillContainersLaunched();
    ContainerData cData = mockLauncher.getContainers().values().iterator().next();
    ContainerLaunchContext launchContext = cData.launchContext;
    Map<String, LocalResource> taskLR = launchContext.getLocalResources();
    // verify tasks are launched with both DAG and task resources.
    Assert.assertTrue(taskLR.containsKey(lrName1));
    Assert.assertTrue(taskLR.containsKey(lrName2));
    
    mockLauncher.startScheduling(true);
    dagClient.waitForCompletion();
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
    tezClient.stop();
  }
  
  @Test (timeout = 5000)
  public void testInternalPreemption() throws Exception {
    TezConfiguration tezconf = new TezConfiguration(defaultConf);
    
    MockTezClient tezClient = new MockTezClient("testMockAM", tezconf, true, null, null, null, null);
    tezClient.start();
    
    MockDAGAppMaster mockApp = tezClient.getLocalClient().getMockApp();
    MockContainerLauncher mockLauncher = mockApp.getContainerLauncher();
    mockLauncher.startScheduling(false);
    // there is only 1 task whose first attempt will be preempted
    DAG dag = DAG.create("test");
    Vertex vA = Vertex.create("A", ProcessorDescriptor.create("Proc.class"), 1);
    dag.addVertex(vA);

    DAGClient dagClient = tezClient.submitDAG(dag);
    mockLauncher.waitTillContainersLaunched();
    ContainerData cData = mockLauncher.getContainers().values().iterator().next();
    DAGImpl dagImpl = (DAGImpl) mockApp.getContext().getCurrentDAG();
    mockApp.getTaskSchedulerEventHandler().preemptContainer(cData.cId);
    
    mockLauncher.startScheduling(true);
    dagClient.waitForCompletion();
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
    TezVertexID vertexId = TezVertexID.getInstance(dagImpl.getID(), 0);
    TezTaskAttemptID killedTaId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, 0), 0);
    TaskAttempt killedTa = dagImpl.getVertex(vA.getName()).getTask(0).getAttempt(killedTaId);
    Assert.assertEquals(TaskAttemptState.KILLED, killedTa.getState());
    tezClient.stop();
  }
  
  @Test (timeout = 5000)
  public void testBasicEvents() throws Exception {
    TezConfiguration tezconf = new TezConfiguration(defaultConf);
    
    MockTezClient tezClient = new MockTezClient("testMockAM", tezconf, true, null, null, null, null);
    tezClient.start();
    
    MockDAGAppMaster mockApp = tezClient.getLocalClient().getMockApp();
    MockContainerLauncher mockLauncher = mockApp.getContainerLauncher();
    mockLauncher.startScheduling(false);
    mockApp.sendDMEvents = true;
    DAG dag = DAG.create("test");
    Vertex vA = Vertex.create("A", ProcessorDescriptor.create("Proc.class"), 2);
    Vertex vB = Vertex.create("B", ProcessorDescriptor.create("Proc.class"), 2);
    Vertex vC = Vertex.create("C", ProcessorDescriptor.create("Proc.class"), 2);
    Vertex vD = Vertex.create("D", ProcessorDescriptor.create("Proc.class"), 2);
    dag.addVertex(vA)
        .addVertex(vB)
        .addVertex(vC)
        .addVertex(vD)
        .addEdge(
            Edge.create(vA, vB, EdgeProperty.create(DataMovementType.BROADCAST,
                DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
                OutputDescriptor.create("Out"), InputDescriptor.create("In"))))
        .addEdge(
            Edge.create(vA, vC, EdgeProperty.create(DataMovementType.SCATTER_GATHER,
                DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
                OutputDescriptor.create("Out"), InputDescriptor.create("In"))))
        .addEdge(
            Edge.create(vA, vD, EdgeProperty.create(DataMovementType.ONE_TO_ONE,
                DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
                OutputDescriptor.create("Out"), InputDescriptor.create("In"))));

    DAGClient dagClient = tezClient.submitDAG(dag);
    mockLauncher.waitTillContainersLaunched();
    DAGImpl dagImpl = (DAGImpl) mockApp.getContext().getCurrentDAG();
    mockLauncher.startScheduling(true);
    dagClient.waitForCompletion();
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
    VertexImpl vImpl = (VertexImpl) dagImpl.getVertex(vB.getName());
    TaskImpl tImpl = (TaskImpl) vImpl.getTask(1);
    List<TezEvent> tEvents = tImpl.getTaskEvents();
    Assert.assertEquals(2, tEvents.size()); // 2 from vA
    Assert.assertEquals(vA.getName(), tEvents.get(0).getDestinationInfo().getEdgeVertexName());
    Assert.assertEquals(0, ((DataMovementEvent)tEvents.get(0).getEvent()).getTargetIndex());
    Assert.assertEquals(0, ((DataMovementEvent)tEvents.get(0).getEvent()).getSourceIndex());
    Assert.assertEquals(vA.getName(), tEvents.get(1).getDestinationInfo().getEdgeVertexName());
    Assert.assertEquals(1, ((DataMovementEvent)tEvents.get(1).getEvent()).getTargetIndex());
    Assert.assertEquals(0, ((DataMovementEvent)tEvents.get(1).getEvent()).getSourceIndex());
    vImpl = (VertexImpl) dagImpl.getVertex(vC.getName());
    tImpl = (TaskImpl) vImpl.getTask(1);
    tEvents = tImpl.getTaskEvents();
    Assert.assertEquals(2, tEvents.size()); // 2 from vA
    Assert.assertEquals(vA.getName(), tEvents.get(0).getDestinationInfo().getEdgeVertexName());
    Assert.assertEquals(0, ((DataMovementEvent)tEvents.get(0).getEvent()).getTargetIndex());
    Assert.assertEquals(1, ((DataMovementEvent)tEvents.get(0).getEvent()).getSourceIndex());
    Assert.assertEquals(vA.getName(), tEvents.get(1).getDestinationInfo().getEdgeVertexName());
    Assert.assertEquals(1, ((DataMovementEvent)tEvents.get(1).getEvent()).getTargetIndex());
    Assert.assertEquals(1, ((DataMovementEvent)tEvents.get(1).getEvent()).getSourceIndex());
    vImpl = (VertexImpl) dagImpl.getVertex(vD.getName());
    tImpl = (TaskImpl) vImpl.getTask(1);
    tEvents = tImpl.getTaskEvents();
    Assert.assertEquals(1, tEvents.size()); // 1 from vA
    Assert.assertEquals(vA.getName(), tEvents.get(0).getDestinationInfo().getEdgeVertexName());
    Assert.assertEquals(0, ((DataMovementEvent)tEvents.get(0).getEvent()).getTargetIndex());
    Assert.assertEquals(0, ((DataMovementEvent)tEvents.get(0).getEvent()).getSourceIndex());

    tezClient.stop();
  }

  @Test (timeout = 10000)
  public void testBasicCounters() throws Exception {
    TezConfiguration tezconf = new TezConfiguration(defaultConf);
    MockTezClient tezClient = new MockTezClient("testMockAM", tezconf, true, null, null, null,
        null, false, false);
    tezClient.start();

    final String vAName = "A";
    final String vBName = "B";
    final String procCounterName = "Proc";
    final String globalCounterName = "Global";
    DAG dag = DAG.create("testBasicCounters");
    Vertex vA = Vertex.create(vAName, ProcessorDescriptor.create("Proc.class"), 10);
    Vertex vB = Vertex.create(vBName, ProcessorDescriptor.create("Proc.class"), 1);
    dag.addVertex(vA)
        .addVertex(vB)
        .addEdge(
            Edge.create(vA, vB, EdgeProperty.create(DataMovementType.SCATTER_GATHER,
                DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
                OutputDescriptor.create("Out"), InputDescriptor.create("In"))));

    MockDAGAppMaster mockApp = tezClient.getLocalClient().getMockApp();
    MockContainerLauncher mockLauncher = mockApp.getContainerLauncher();
    mockLauncher.startScheduling(false);
    mockApp.countersDelegate = new CountersDelegate() {
      @Override
      public TezCounters getCounters(TaskSpec taskSpec) {
        String vName = taskSpec.getVertexName();
        TezCounters counters = new TezCounters();
        counters.findCounter(globalCounterName, globalCounterName).increment(1);
        counters.findCounter(vName, procCounterName).increment(1);
        for (OutputSpec output : taskSpec.getOutputs()) {
          counters.findCounter(vName, output.getDestinationVertexName()).increment(1);
        }
        for (InputSpec input : taskSpec.getInputs()) {
          counters.findCounter(vName, input.getSourceVertexName()).increment(1);
        }
        return counters;
      }
    };
    mockApp.doSleep = false;
    DAGClient dagClient = tezClient.submitDAG(dag);
    mockLauncher.waitTillContainersLaunched();
    DAGImpl dagImpl = (DAGImpl) mockApp.getContext().getCurrentDAG();
    mockLauncher.startScheduling(true);
    DAGStatus status = dagClient.waitForCompletion();
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, status.getState());
    TezCounters counters = dagImpl.getAllCounters();
    // verify processor counters
    Assert.assertEquals(10, counters.findCounter(vAName, procCounterName).getValue());
    Assert.assertEquals(1, counters.findCounter(vBName, procCounterName).getValue());
    // verify edge counters
    Assert.assertEquals(10, counters.findCounter(vAName, vBName).getValue());
    Assert.assertEquals(1, counters.findCounter(vBName, vAName).getValue());
    // verify global counters
    Assert.assertEquals(11, counters.findCounter(globalCounterName, globalCounterName).getValue());
    
    tezClient.stop();
  }
  
  @Test (timeout = 10000)
  public void testMultipleSubmissions() throws Exception {
    Map<String, LocalResource> lrDAG = Maps.newHashMap();
    String lrName1 = "LR1";
    lrDAG.put(lrName1, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    Map<String, LocalResource> lrVertex = Maps.newHashMap();
    String lrName2 = "LR2";
    lrVertex.put(lrName2, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test1"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));

    DAG dag = DAG.create("test").addTaskLocalFiles(lrDAG);
    Vertex vA = Vertex.create("A", ProcessorDescriptor.create("Proc.class"), 5).addTaskLocalFiles(lrVertex);
    dag.addVertex(vA);

    TezConfiguration tezconf = new TezConfiguration(defaultConf);
    
    MockTezClient tezClient = new MockTezClient("testMockAM", tezconf, true, null, null, null, null);
    tezClient.start();
    DAGClient dagClient = tezClient.submitDAG(dag);
    dagClient.waitForCompletion();
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
    tezClient.stop();
    
    // submit the same DAG again to verify it can be done.
    tezClient = new MockTezClient("testMockAM", tezconf, true, null, null, null, null);
    tezClient.start();
    dagClient = tezClient.submitDAG(dag);
    dagClient.waitForCompletion();
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
    tezClient.stop();
  }

  @Test (timeout = 10000)
  public void testSchedulerErrorHandling() throws Exception {
    TezConfiguration tezconf = new TezConfiguration(defaultConf);

    MockTezClient tezClient = new MockTezClient("testMockAM", tezconf, true, null, null, null, null);
    tezClient.start();

    MockDAGAppMaster mockApp = tezClient.getLocalClient().getMockApp();
    MockContainerLauncher mockLauncher = mockApp.getContainerLauncher();
    mockLauncher.startScheduling(false);

    DAG dag = DAG.create("test");
    Vertex vA = Vertex.create("A", ProcessorDescriptor.create("Proc.class"), 5);
    dag.addVertex(vA);

    tezClient.submitDAG(dag);
    mockLauncher.waitTillContainersLaunched();
    mockApp.handle(new DAGAppMasterEventSchedulingServiceError(new RuntimeException("Mock error")));

    while(!mockApp.getShutdownHandler().wasShutdownInvoked()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(DAGState.RUNNING, mockApp.getContext().getCurrentDAG().getState());
  }

  @Test (timeout = 10000)
  public void testInitFailed() throws Exception {
    TezConfiguration tezconf = new TezConfiguration(defaultConf);
    MockTezClient tezClient = new MockTezClient("testMockAM", tezconf, true,
      null, null, null, new AtomicBoolean(false), true, false);
    try {
      tezClient.start();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals("FailInit", e.getCause().getCause().getMessage());
      MockDAGAppMaster mockApp = tezClient.getLocalClient().getMockApp();
      // will timeout if DAGAppMasterShutdownHook is not invoked
      mockApp.waitForServiceToStop(Integer.MAX_VALUE);
    }
  }

  @Test (timeout = 10000)
  public void testStartFailed() {
    TezConfiguration tezconf = new TezConfiguration(defaultConf);
    MockTezClient tezClient = new MockTezClient("testMockAM", tezconf, true,
      null, null, null, new AtomicBoolean(false), false, true);
    try {
      tezClient.start();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals("FailStart", e.getCause().getCause().getMessage());
      MockDAGAppMaster mockApp = tezClient.getLocalClient().getMockApp();
      // will timeout if DAGAppMasterShutdownHook is not invoked
      mockApp.waitForServiceToStop(Integer.MAX_VALUE);
    }
  }
}
