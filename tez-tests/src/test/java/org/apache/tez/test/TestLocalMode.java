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

package org.apache.tez.test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.examples.OrderedWordCount;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.processor.SleepProcessor;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestLocalMode {

  private static final File TEST_DIR = new File(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")), "TestLocalMode-tez-localmode");

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
  @Test(timeout=30000)
  public void testMultiDAGsOnSession() throws IOException, TezException, InterruptedException {
    int dags = 2;//two dags will be submitted to session
    String[] inputPaths = new String[dags];
    String[] outputPaths =  new String[dags];
    DAGClient[] dagClients = new DAGClient[dags];

    TezConfiguration tezConf = new TezConfiguration();
    tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    tezConf.set("fs.defaultFS", "file:///");
    tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
    TezClient tezClient = TezClient.create("testMultiDAGOnSession", tezConf, true);
    tezClient.start();

    //create inputs and outputs
    FileSystem fs = FileSystem.get(tezConf);
    for(int i = 0; i < dags; i++) {
      inputPaths[i] = new Path(TEST_DIR.getAbsolutePath(),"in-"+i).toString();
      createInputFile(fs, inputPaths[i]);
      outputPaths[i] = new Path(TEST_DIR.getAbsolutePath(),"out-"+i).toString();
    }

    //start testing
    try {
      for (int i=0; i<inputPaths.length; ++i) {
        DAG dag = OrderedWordCount.createDAG(tezConf, inputPaths[i], outputPaths[i], 1,
            ("DAG-Iteration-" + i)); // the names of the DAGs must be unique in a session

        tezClient.waitTillReady();
        System.out.println("Running dag number " + i);
        dagClients[i] = tezClient.submitDAG(dag);

        // wait to finish
        DAGStatus dagStatus = dagClients[i].waitForCompletion();
        if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
          fail("Iteration " + i + " failed with diagnostics: "
              + dagStatus.getDiagnostics());
        }
        //verify all dags sharing the same execution context
        if(i>0) {
          assertTrue(dagClients[i-1].getExecutionContext().equals(dagClients[i].getExecutionContext()));
        }
      }
    } finally {
      tezClient.stop();
    }
  }

  private void createInputFile(FileSystem fs, String path) throws IOException {
    Path file = new Path(new Path(path), "input.txt");
    try {
      FSDataOutputStream fsdos = fs.create(file);
      fsdos.write("This is a small test file !".getBytes());
      fsdos.flush();
      fsdos.close();
    } catch (IOException ioe) {
      fail("Can not create input File!");
    }
  }
}
