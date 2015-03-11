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

package org.apache.tez.dag.history.logging.ats;

import java.io.IOException;
import java.util.Random;

import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.runtime.library.processor.SleepProcessor;
import org.apache.tez.runtime.library.processor.SleepProcessor.SleepProcessorConfig;
import org.apache.tez.tests.MiniTezClusterWithTimeline;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class TestATSHistoryWithMiniCluster {

  private static final Log LOG = LogFactory.getLog(TestATSHistoryWithMiniCluster.class);

  protected static MiniTezClusterWithTimeline mrrTezCluster = null;
  protected static MiniDFSCluster dfsCluster = null;
  private static String timelineAddress;
  private Random random = new Random();

  private static Configuration conf = new Configuration();
  private static FileSystem remoteFs;

  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestATSHistoryWithMiniCluster.class.getName() + "-tmpDir";

  @BeforeClass
  public static void setup() throws IOException {
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).racks(null)
          .build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }

    if (mrrTezCluster == null) {
      try {
        mrrTezCluster = new MiniTezClusterWithTimeline(TestATSHistoryWithMiniCluster.class.getName(),
            1, 1, 1, true);
        Configuration conf = new Configuration();
        conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
        conf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
        conf.setInt("yarn.nodemanager.delete.debug-delay-sec", 20000);
        mrrTezCluster.init(conf);
        mrrTezCluster.start();
      } catch (Throwable e) {
        LOG.info("Failed to start Mini Tez Cluster", e);
      }
    }
    timelineAddress = mrrTezCluster.getConfig().get(
        YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS);
    if (timelineAddress != null) {
      // Hack to handle bug in MiniYARNCluster handling of webapp address
      timelineAddress = timelineAddress.replace("0.0.0.0", "localhost");
    }
  }

  @AfterClass
  public static void tearDown() throws InterruptedException {
    LOG.info("Shutdown invoked");
    Thread.sleep(10000);
    if (mrrTezCluster != null) {
      mrrTezCluster.stop();
      mrrTezCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  // To be replaced after Timeline has java APIs for domains
  private <K> K getTimelineData(String url, Class<K> clazz) {
    Client client = new Client();
    WebResource resource = client.resource(url);

    ClientResponse response = resource.accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());

    K entity = response.getEntity(clazz);
    Assert.assertNotNull(entity);
    return entity;
  }

  @Test (timeout=50000)
  public void testDisabledACls() throws Exception {
    TezClient tezSession = null;
    try {
      SleepProcessorConfig spConf = new SleepProcessorConfig(1);

      DAG dag = DAG.create("TezSleepProcessor");
      Vertex vertex = Vertex.create("SleepVertex", ProcessorDescriptor.create(
              SleepProcessor.class.getName()).setUserPayload(spConf.toUserPayload()), 1,
          Resource.newInstance(256, 1));
      dag.addVertex(vertex);

      TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
      tezConf.setBoolean(TezConfiguration.TEZ_AM_ALLOW_DISABLED_TIMELINE_DOMAINS, true);
      tezConf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS,
          ATSHistoryLoggingService.class.getName());
      Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String.valueOf(random
          .nextInt(100000))));
      remoteFs.mkdirs(remoteStagingDir);
      tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());

      tezSession = TezClient.create("TezSleepProcessor", tezConf, true);
      tezSession.start();

      DAGClient dagClient = tezSession.submitDAG(dag);

      DAGStatus dagStatus = dagClient.getDAGStatus(null);
      while (!dagStatus.isCompleted()) {
        LOG.info("Waiting for job to complete. Sleeping for 500ms." + " Current state: "
            + dagStatus.getState());
        Thread.sleep(500l);
        dagStatus = dagClient.getDAGStatus(null);
      }
      Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagStatus.getState());
    } finally {
      if (tezSession != null) {
        tezSession.stop();
      }
    }
  }

}
