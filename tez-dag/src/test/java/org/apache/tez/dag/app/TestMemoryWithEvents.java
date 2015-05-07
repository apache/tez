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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.app.MockDAGAppMaster.MockContainerLauncher;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

// The objective of these tests is to make sure the large job simulations pass 
// within the memory limits set by the junit tests (1GB)
// For large jobs please increase memory limits to account for memory used by the 
// simulation code itself
public class TestMemoryWithEvents {
  static Configuration defaultConf;
  static FileSystem localFs;
  
  static {
    try {
      defaultConf = new Configuration(false);
      defaultConf.set("fs.defaultFS", "file:///");
      defaultConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
      localFs = FileSystem.getLocal(defaultConf);
      String stagingDir = "target" + Path.SEPARATOR + TestMemoryWithEvents.class.getName() + "-tmpDir";
      defaultConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDir);
      Logger.getRootLogger().setLevel(Level.WARN);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  final int numThreads = 30;
  final int numTasks = 10000;

  private void checkMemory(String name, MockDAGAppMaster mockApp) {
    long mb = 1024*1024;
    long microsPerMs = 1000;

    //Getting the runtime reference from system
    Runtime runtime = Runtime.getRuntime();

    System.out.println("##### Heap utilization statistics [MB] for " + name);

    runtime.gc();
    
    //Print used memory
    System.out.println("##### Used Memory:"
        + (runtime.totalMemory() - runtime.freeMemory()) / mb);

    //Print free memory
    System.out.println("##### Free Memory:"
        + runtime.freeMemory() / mb);
     
    //Print total available memory
    System.out.println("##### Total Memory:" + runtime.totalMemory() / mb);

    //Print Maximum available memory
    System.out.println("##### Max Memory:" + runtime.maxMemory() / mb);
    
    //Print Maximum heartbeat time
    long numHeartbeats = mockApp.numHearbeats.get();
    if (numHeartbeats == 0) {
      numHeartbeats = 1;
    }
    System.out.println("##### Heartbeat (ms) :" 
        + " latency avg: " + ((mockApp.heartbeatTime.get() / numHeartbeats) / microsPerMs) 
        + " cpu total: " + (mockApp.heartbeatCpu.get() / microsPerMs)
        + " cpu avg: " + ((mockApp.heartbeatCpu.get() / numHeartbeats) / microsPerMs)
        + " numHeartbeats: " + mockApp.numHearbeats.get());
  }
  
  private void testMemory(DAG dag, boolean sendDMEvents) throws Exception {
    TezConfiguration tezconf = new TezConfiguration(defaultConf);

    MockTezClient tezClient = new MockTezClient("testMockAM", tezconf, true, null, null, null,
        null, false, false, numThreads, 1000);
    tezClient.start();
    
    MockDAGAppMaster mockApp = tezClient.getLocalClient().getMockApp();
    MockContainerLauncher mockLauncher = mockApp.getContainerLauncher();
    mockLauncher.startScheduling(false);
    mockApp.eventsDelegate = new TestMockDAGAppMaster.TestEventsDelegate();
    mockApp.doSleep = false;
    DAGClient dagClient = tezClient.submitDAG(dag);
    mockLauncher.waitTillContainersLaunched();
    mockLauncher.startScheduling(true);
    DAGStatus status = dagClient.waitForCompletion();
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, status.getState());
    checkMemory(dag.getName(), mockApp);
    
    tezClient.stop();
  }
  
  public static class SimulationInitializer extends InputInitializer {
    public SimulationInitializer(InputInitializerContext initializerContext) {
      super(initializerContext);
    }

    @Override
    public List<Event> initialize() throws Exception {
      int numTasks = getContext().getNumTasks();
      List<Event> events = Lists.newArrayListWithCapacity(numTasks);
      for (int i=0; i<numTasks; ++i) {
        events.add(InputDataInformationEvent.createWithSerializedPayload(i, null));
      }
      return events;
    }

    @Override
    public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws Exception {
    }
  }

  @Ignore
  @Test (timeout = 600000)
  public void testMemoryRootInputEvents() throws Exception {
    DAG dag = DAG.create("testMemoryRootInputEvents");
    Vertex vA = Vertex.create("A", ProcessorDescriptor.create("Proc.class"), numTasks);
    Vertex vB = Vertex.create("B", ProcessorDescriptor.create("Proc.class"), numTasks);
    vA.addDataSource(
        "Input",
        DataSourceDescriptor.create(InputDescriptor.create("In"),
            InputInitializerDescriptor.create(SimulationInitializer.class.getName()), null));
    dag.addVertex(vA).addVertex(vB);
    testMemory(dag, false);
  }
  
  @Ignore
  @Test (timeout = 600000)
  public void testMemoryOneToOne() throws Exception {
    DAG dag = DAG.create("testMemoryOneToOne");
    Vertex vA = Vertex.create("A", ProcessorDescriptor.create("Proc.class"), numTasks);
    Vertex vB = Vertex.create("B", ProcessorDescriptor.create("Proc.class"), numTasks);
    dag.addVertex(vA)
        .addVertex(vB)
        .addEdge(
            Edge.create(vA, vB, EdgeProperty.create(DataMovementType.ONE_TO_ONE,
                DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
                OutputDescriptor.create("Out"), InputDescriptor.create("In"))));
    testMemory(dag, true);
  }

  @Ignore
  @Test (timeout = 600000)
  public void testMemoryBroadcast() throws Exception {
    DAG dag = DAG.create("testMemoryBroadcast");
    Vertex vA = Vertex.create("A", ProcessorDescriptor.create("Proc.class"), numTasks);
    Vertex vB = Vertex.create("B", ProcessorDescriptor.create("Proc.class"), numTasks);
    dag.addVertex(vA)
        .addVertex(vB)
        .addEdge(
            Edge.create(vA, vB, EdgeProperty.create(DataMovementType.BROADCAST,
                DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
                OutputDescriptor.create("Out"), InputDescriptor.create("In"))));
    testMemory(dag, true);
  }
  
  @Ignore
  @Test (timeout = 600000)
  public void testMemoryScatterGather() throws Exception {
    DAG dag = DAG.create("testMemoryScatterGather");
    Vertex vA = Vertex.create("A", ProcessorDescriptor.create("Proc.class"), numTasks);
    Vertex vB = Vertex.create("B", ProcessorDescriptor.create("Proc.class"), numTasks);
    dag.addVertex(vA)
        .addVertex(vB)
        .addEdge(
            Edge.create(vA, vB, EdgeProperty.create(DataMovementType.SCATTER_GATHER,
                DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
                OutputDescriptor.create("Out"), InputDescriptor.create("In"))));
    testMemory(dag, true);
  }

}
