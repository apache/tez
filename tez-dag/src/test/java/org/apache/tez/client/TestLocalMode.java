/*
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

package org.apache.tez.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.processor.SleepProcessor;
import org.junit.Test;

public class TestLocalMode {

  @Test(timeout = 30000)
  public void testMultipleClientsWithSession() throws TezException, InterruptedException,
      IOException {
    TezConfiguration tezConf1 = new TezConfiguration();
    tezConf1.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    tezConf1.set("fs.defaultFS", "file:///");
    tezConf1.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
    TezClient tezClient1 = TezClient.create("commonName", tezConf1, true);
    tezClient1.start();

    DAG dag1 = createSimpleDAG("dag1", SleepProcessor.class.getName());

    DAGClient dagClient1 = tezClient1.submitDAG(dag1);
    dagClient1.waitForCompletion();
    assertEquals(DAGStatus.State.SUCCEEDED, dagClient1.getDAGStatus(null).getState());

    dagClient1.close();
    tezClient1.stop();


    TezConfiguration tezConf2 = new TezConfiguration();
    tezConf2.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    tezConf2.set("fs.defaultFS", "file:///");
    tezConf2.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
    DAG dag2 = createSimpleDAG("dag2", SleepProcessor.class.getName());
    TezClient tezClient2 = TezClient.create("commonName", tezConf2, true);
    tezClient2.start();
    DAGClient dagClient2 = tezClient2.submitDAG(dag2);
    dagClient2.waitForCompletion();
    assertEquals(DAGStatus.State.SUCCEEDED, dagClient2.getDAGStatus(null).getState());
    assertFalse(dagClient1.getExecutionContext().equals(dagClient2.getExecutionContext()));
    dagClient2.close();
    tezClient2.stop();
  }

  @Test(timeout = 10000)
  public void testMultipleClientsWithoutSession() throws TezException, InterruptedException,
      IOException {
    TezConfiguration tezConf1 = new TezConfiguration();
    tezConf1.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    tezConf1.set("fs.defaultFS", "file:///");
    tezConf1.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
    TezClient tezClient1 = TezClient.create("commonName", tezConf1, false);
    tezClient1.start();

    DAG dag1 = createSimpleDAG("dag1", SleepProcessor.class.getName());

    DAGClient dagClient1 = tezClient1.submitDAG(dag1);
    dagClient1.waitForCompletion();
    assertEquals(DAGStatus.State.SUCCEEDED, dagClient1.getDAGStatus(null).getState());

    dagClient1.close();
    tezClient1.stop();


    TezConfiguration tezConf2 = new TezConfiguration();
    tezConf2.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    tezConf2.set("fs.defaultFS", "file:///");
    tezConf2.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
    DAG dag2 = createSimpleDAG("dag2", SleepProcessor.class.getName());
    TezClient tezClient2 = TezClient.create("commonName", tezConf2, false);
    tezClient2.start();
    DAGClient dagClient2 = tezClient2.submitDAG(dag2);
    dagClient2.waitForCompletion();
    assertEquals(DAGStatus.State.SUCCEEDED, dagClient2.getDAGStatus(null).getState());
    assertFalse(dagClient1.getExecutionContext().equals(dagClient2.getExecutionContext()));
    dagClient2.close();
    tezClient2.stop();
  }

  @Test(timeout = 20000)
  public void testNoSysExitOnSuccessfulDAG() throws TezException, InterruptedException,
      IOException {
    TezConfiguration tezConf1 = new TezConfiguration();
    tezConf1.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    tezConf1.set("fs.defaultFS", "file:///");
    tezConf1.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
    // Run in non-session mode so that the AM terminates
    TezClient tezClient1 = TezClient.create("commonName", tezConf1, false);
    tezClient1.start();

    DAG dag1 = createSimpleDAG("dag1", SleepProcessor.class.getName());

    DAGClient dagClient1 = tezClient1.submitDAG(dag1);
    dagClient1.waitForCompletion();
    assertEquals(DAGStatus.State.SUCCEEDED, dagClient1.getDAGStatus(null).getState());

    // Sleep for more time than is required for the DAG to complete.
    Thread.sleep((long) (TezConstants.TEZ_DAG_SLEEP_TIME_BEFORE_EXIT * 1.5));

    dagClient1.close();
    tezClient1.stop();
  }

  @Test(timeout = 20000)
  public void testNoSysExitOnFailinglDAG() throws TezException, InterruptedException,
      IOException {
    TezConfiguration tezConf1 = new TezConfiguration();
    tezConf1.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    tezConf1.set("fs.defaultFS", "file:///");
    tezConf1.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
    // Run in non-session mode so that the AM terminates
    TezClient tezClient1 = TezClient.create("commonName", tezConf1, false);
    tezClient1.start();

    DAG dag1 = createSimpleDAG("dag1", FailingProcessor.class.getName());

    DAGClient dagClient1 = tezClient1.submitDAG(dag1);
    dagClient1.waitForCompletion();
    assertEquals(DAGStatus.State.FAILED, dagClient1.getDAGStatus(null).getState());

    // Sleep for more time than is required for the DAG to complete.
    Thread.sleep((long) (TezConstants.TEZ_DAG_SLEEP_TIME_BEFORE_EXIT * 1.5));

    dagClient1.close();
    tezClient1.stop();
  }

  public static class FailingProcessor extends AbstractLogicalIOProcessor {

    public FailingProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void initialize() throws Exception {
    }

    @Override
    public void handleEvents(List<Event> processorEvents) {
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws
        Exception {
      throw new TezException("FailingProcessor");

    }
  }

  private DAG createSimpleDAG(String dagName, String processorName) {
    DAG dag = DAG.create(dagName).addVertex(Vertex.create("Sleep", ProcessorDescriptor.create(
        processorName).setUserPayload(
        new SleepProcessor.SleepProcessorConfig(1).toUserPayload()), 1));
    return dag;

  }
}
