/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.runtime.library.processor.SleepProcessor;
import org.apache.tez.runtime.library.processor.SleepProcessor.SleepProcessorConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAM {

  private static final Logger LOG = LoggerFactory.getLogger(TestAM.class);

  private static MiniTezCluster tezCluster;
  private static MiniDFSCluster dfsCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem remoteFs;

  private static final String TEST_ROOT_DIR = "target" + Path.SEPARATOR + TestAM.class.getName() + "-tmpDir";

  @BeforeClass
  public static void setup() throws IOException {
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }

    if (tezCluster == null) {
      tezCluster = new MiniTezCluster(TestAM.class.getName(), 1, 1, 1);
      Configuration tezClusterConf = new Configuration();
      tezClusterConf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
      tezClusterConf.setInt("yarn.nodemanager.delete.debug-delay-sec", 20000);
      tezClusterConf.setLong(TezConfiguration.TEZ_AM_SLEEP_TIME_BEFORE_EXIT_MILLIS, 1000);
      tezClusterConf.set(YarnConfiguration.PROXY_ADDRESS, "localhost");
      tezCluster.init(tezClusterConf);
      tezCluster.start();
    }
  }

  @AfterClass
  public static void tearDown() {
    if (tezCluster != null) {
      tezCluster.stop();
      tezCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  @Test(timeout = 60000)
  public void testAMWebUIService() throws TezException, IOException, InterruptedException {
    SleepProcessorConfig spConf = new SleepProcessorConfig(1);

    DAG dag = DAG.create("TezSleepProcessor");
    Vertex vertex = Vertex.create("SleepVertex",
        ProcessorDescriptor.create(SleepProcessor.class.getName()).setUserPayload(spConf.toUserPayload()), 1,
        Resource.newInstance(1024, 1));
    dag.addVertex(vertex);

    TezConfiguration tezConf = new TezConfiguration(tezCluster.getConfig());
    TezClient tezSession = TezClient.create("TezSleepProcessor", tezConf, false);
    tezSession.start();

    DAGClient dagClient = tezSession.submitDAG(dag);

    DAGStatus dagStatus = dagClient.getDAGStatus(null);
    while (!dagStatus.isCompleted()) {
      Thread.sleep(500L);
      dagStatus = dagClient.getDAGStatus(null);
    }

    String webUIAddress = dagClient.getWebUIAddress();
    assertNotNull("getWebUIAddress should return TezAM's web UI address", webUIAddress);
    LOG.info("TezAM webUI address: " + webUIAddress);

    checkAddress(webUIAddress + "/jmx");
    checkAddress(webUIAddress + "/conf");
    checkAddress(webUIAddress + "/stacks");

    URL url = new URL(webUIAddress);
    IntegerRanges portRange = conf.getRange(TezConfiguration.TEZ_AM_WEBSERVICE_PORT_RANGE,
        TezConfiguration.TEZ_AM_WEBSERVICE_PORT_RANGE_DEFAULT);
    assertTrue("WebUIService port should be in the defined range (got: " + url.getPort() + ")",
        portRange.getRangeStart() <= url.getPort());

    tezSession.stop();
  }

  private void checkAddress(String url) {
    boolean success = false;
    try {
      HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
      connection.connect();
      success = (connection.getResponseCode() == 200);
    } catch (Exception e) {
      LOG.error("Error while checking url: " + url, e);
    }
    assertTrue(url + " should be available", success);
  }
}
