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
package org.apache.tez.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.examples.OrderedWordCount;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.processor.SleepProcessor;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for running Tez in local execution mode (without YARN).
 */
public class TestLocalMode {

  /**
   * In order to be able to safely get VertexStatus from a running DAG,
   * the DAG needs to run for a certain amount of time, see TEZ-4475 for details.
   */
  private static final int SLEEP_PROCESSOR_TIME_TO_SLEEP_MS = 500;

  private static final File STAGING_DIR = new File(System.getProperty("test.build.data"),
      TestLocalMode.class.getName());

  private static MiniDFSCluster dfsCluster;
  private static FileSystem remoteFs;

  public static Stream<Arguments> params() {
    return Stream.of(Arguments.of(false, false), Arguments.of(true, false), Arguments.of(false, true),
        Arguments.of(true, true));
  }

  @BeforeAll
  public static void beforeClass() throws Exception {
    try {
      Configuration conf = new Configuration();
      dfsCluster =
          new MiniDFSCluster.Builder(conf).numDataNodes(3).format(true)
              .racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }
  }

  @AfterAll
  public static void afterClass() throws InterruptedException {
    if (dfsCluster != null) {
      try {
        dfsCluster.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private TezConfiguration createConf(boolean useDfs, boolean useLocalModeWithoutNetwork) {
    TezConfiguration conf = new TezConfiguration();
    conf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    conf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE_WITHOUT_NETWORK, useLocalModeWithoutNetwork);

    if (useDfs) {
      conf.set("fs.defaultFS", remoteFs.getUri().toString());
    } else {
      conf.set("fs.defaultFS", "file:///");
    }
    conf.set(TezConfiguration.TEZ_AM_STAGING_DIR, STAGING_DIR.getAbsolutePath());
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
    return conf;
  }

  @ParameterizedTest(name = "useDFS:{0} useLocalModeWithoutNetwork:{1}")
  @MethodSource("params")
  @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
  public void testMultipleClientsWithSession(boolean useDfs, boolean useLocalModeWithoutNetwork)
      throws TezException, InterruptedException, IOException {
    TezConfiguration tezConf1 = createConf(useDfs, useLocalModeWithoutNetwork);
    TezClient tezClient1 = TezClient.create("commonName", tezConf1, true);
    tezClient1.start();

    DAG dag1 = createSimpleDAG("testMultipleClientsWithSession", SleepProcessor.class.getName(), useDfs,
        useLocalModeWithoutNetwork);

    DAGClient dagClient1 = tezClient1.submitDAG(dag1);
    dagClient1.waitForCompletion();
    assertEquals(DAGStatus.State.SUCCEEDED, dagClient1.getDAGStatus(null).getState());
    assertEquals(VertexStatus.State.SUCCEEDED,
        dagClient1.getVertexStatus(SleepProcessor.SLEEP_VERTEX_NAME, null).getState());

    dagClient1.close();
    tezClient1.stop();

    TezConfiguration tezConf2 = createConf(useDfs, useLocalModeWithoutNetwork);
    DAG dag2 = createSimpleDAG("testMultipleClientsWithSession_2", SleepProcessor.class.getName(), useDfs,
        useLocalModeWithoutNetwork);
    TezClient tezClient2 = TezClient.create("commonName", tezConf2, true);
    tezClient2.start();
    DAGClient dagClient2 = tezClient2.submitDAG(dag2);
    dagClient2.waitForCompletion();
    assertEquals(DAGStatus.State.SUCCEEDED, dagClient2.getDAGStatus(null).getState());
    assertEquals(VertexStatus.State.SUCCEEDED,
        dagClient2.getVertexStatus(SleepProcessor.SLEEP_VERTEX_NAME, null).getState());
    assertNotEquals(dagClient1.getExecutionContext(), dagClient2.getExecutionContext());
    dagClient2.close();
    tezClient2.stop();
  }

  @ParameterizedTest(name = "useDFS:{0} useLocalModeWithoutNetwork:{1}")
  @MethodSource("params")
  @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
  public void testMultipleClientsWithoutSession(boolean useDfs, boolean useLocalModeWithoutNetwork)
      throws TezException, InterruptedException, IOException {
    TezConfiguration tezConf1 = createConf(useDfs, useLocalModeWithoutNetwork);
    TezClient tezClient1 = TezClient.create("commonName", tezConf1, false);
    tezClient1.start();

    DAG dag1 = createSimpleDAG("testMultipleClientsWithoutSession", SleepProcessor.class.getName(), useDfs,
        useLocalModeWithoutNetwork);

    DAGClient dagClient1 = tezClient1.submitDAG(dag1);
    dagClient1.waitForCompletion();
    assertEquals(DAGStatus.State.SUCCEEDED, dagClient1.getDAGStatus(null).getState());
    assertEquals(VertexStatus.State.SUCCEEDED,
        dagClient1.getVertexStatus(SleepProcessor.SLEEP_VERTEX_NAME, null).getState());
    dagClient1.close();
    tezClient1.stop();


    TezConfiguration tezConf2 = createConf(useDfs, useLocalModeWithoutNetwork);
    DAG dag2 = createSimpleDAG("testMultipleClientsWithoutSession_2", SleepProcessor.class.getName(), useDfs,
        useLocalModeWithoutNetwork);
    TezClient tezClient2 = TezClient.create("commonName", tezConf2, false);
    tezClient2.start();
    DAGClient dagClient2 = tezClient2.submitDAG(dag2);
    dagClient2.waitForCompletion();
    assertEquals(DAGStatus.State.SUCCEEDED, dagClient2.getDAGStatus(null).getState());
    assertEquals(VertexStatus.State.SUCCEEDED,
        dagClient2.getVertexStatus(SleepProcessor.SLEEP_VERTEX_NAME, null).getState());
    assertNotEquals(dagClient1.getExecutionContext(), dagClient2.getExecutionContext());
    dagClient2.close();
    tezClient2.stop();
  }

  @ParameterizedTest(name = "useDFS:{0} useLocalModeWithoutNetwork:{1}")
  @MethodSource("params")
  @Timeout(value = 20000, unit = TimeUnit.MILLISECONDS)
  public void testNoSysExitOnSuccessfulDAG(boolean useDfs, boolean useLocalModeWithoutNetwork)
      throws TezException, InterruptedException, IOException {
    TezConfiguration tezConf1 = createConf(useDfs, useLocalModeWithoutNetwork);
    // Run in non-session mode so that the AM terminates
    TezClient tezClient1 = TezClient.create("commonName", tezConf1, false);
    tezClient1.start();

    DAG dag1 = createSimpleDAG("testNoSysExitOnSuccessfulDAG", SleepProcessor.class.getName(), useDfs,
        useLocalModeWithoutNetwork);

    DAGClient dagClient1 = tezClient1.submitDAG(dag1);
    dagClient1.waitForCompletion();
    assertEquals(DAGStatus.State.SUCCEEDED, dagClient1.getDAGStatus(null).getState());
    assertEquals(VertexStatus.State.SUCCEEDED,
        dagClient1.getVertexStatus(SleepProcessor.SLEEP_VERTEX_NAME, null).getState());
    // Sleep for more time than is required for the DAG to complete.
    Thread.sleep((long) (TezConstants.TEZ_DAG_SLEEP_TIME_BEFORE_EXIT * 1.5));

    dagClient1.close();
    tezClient1.stop();
  }

  @ParameterizedTest(name = "useDFS:{0} useLocalModeWithoutNetwork:{1}")
  @MethodSource("params")
  @Timeout(value = 20000, unit = TimeUnit.MILLISECONDS)
  public void testNoSysExitOnFailingDAG(boolean useDfs, boolean useLocalModeWithoutNetwork)
      throws TezException, InterruptedException, IOException {
    TezConfiguration tezConf1 = createConf(useDfs, useLocalModeWithoutNetwork);
    // Run in non-session mode so that the AM terminates
    TezClient tezClient1 = TezClient.create("commonName", tezConf1, false);
    tezClient1.start();

    DAG dag1 = createSimpleDAG("testNoSysExitOnFailingDAG", FailingProcessor.class.getName(), useDfs,
        useLocalModeWithoutNetwork);

    DAGClient dagClient1 = tezClient1.submitDAG(dag1);
    dagClient1.waitForCompletion();
    assertEquals(DAGStatus.State.FAILED, dagClient1.getDAGStatus(null).getState());
    assertEquals(VertexStatus.State.FAILED,
        dagClient1.getVertexStatus(SleepProcessor.SLEEP_VERTEX_NAME, null).getState());
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

  private DAG createSimpleDAG(String dagName, String processorName, boolean useDfs,
                              boolean useLocalModeWithoutNetwork) {
    DAG dag = DAG.create(generateDagName("DAG-" + dagName, useDfs, useLocalModeWithoutNetwork)).addVertex(
        Vertex.create(SleepProcessor.SLEEP_VERTEX_NAME, ProcessorDescriptor.create(processorName)
                .setUserPayload(new SleepProcessor.SleepProcessorConfig(SLEEP_PROCESSOR_TIME_TO_SLEEP_MS).toUserPayload()),
            1));
    return dag;
  }
  private String generateDagName(String baseName, boolean useDfs, boolean useLocalModeWithoutNetwork) {
    return baseName + (useDfs ? "_useDfs" : "") + (useLocalModeWithoutNetwork ? "_useLocalModeWithoutNetwork" : "");
  }

  @ParameterizedTest(name = "useDFS:{0} useLocalModeWithoutNetwork:{1}")
  @MethodSource("params")
  @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
  public void testMultiDAGsOnSession(boolean useDfs, boolean useLocalModeWithoutNetwork)
      throws IOException, TezException, InterruptedException {
    int dags = 2;//two dags will be submitted to session
    String[] inputPaths = new String[dags];
    String[] outputPaths = new String[dags];
    DAGClient[] dagClients = new DAGClient[dags];

    TezConfiguration tezConf = createConf(useDfs, useLocalModeWithoutNetwork);
    TezClient tezClient = TezClient.create("testMultiDAGOnSession", tezConf, true);
    tezClient.start();

    //create inputs and outputs
    FileSystem fs = FileSystem.get(tezConf);
    for(int i = 0; i < dags; i++) {
      inputPaths[i] = new Path(STAGING_DIR.getAbsolutePath(), "in-" + i).toString();
      createInputFile(fs, inputPaths[i]);
      outputPaths[i] = new Path(STAGING_DIR.getAbsolutePath(), "out-" + i).toString();
    }

    //start testing
    try {
      for (int i=0; i<inputPaths.length; ++i) {
        DAG dag = OrderedWordCount.createDAG(tezConf, inputPaths[i], outputPaths[i], 1,
            false, false, ("DAG-Iteration-" + i)); // the names of the DAGs must be unique in a session

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
        if (i > 0) {
          assertEquals(dagClients[i - 1].getExecutionContext(), dagClients[i].getExecutionContext());
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
