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

package org.apache.tez.auxservices;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.examples.OrderedWordCount;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import static org.apache.tez.test.TestTezJobs.generateOrderedWordCountInput;
import static org.apache.tez.test.TestTezJobs.verifyOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.test.MiniTezCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestShuffleHandlerJobs {

  private static final Logger LOG = LoggerFactory.getLogger(TestShuffleHandlerJobs.class);

  protected static MiniTezCluster tezCluster;
  protected static MiniDFSCluster dfsCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem remoteFs;
  private static int NUM_NMS = 5;
  private static int NUM_DNS = 5;
  @BeforeClass
  public static void setup() throws IOException {
    try {
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
      conf.setInt(YarnConfiguration.NM_CONTAINER_MGR_THREAD_COUNT, 22);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DNS)
          .format(true).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }

    if (!(new File(MiniTezCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniTezCluster.APPJAR
          + " not found. Not running test.");
      return;
    }

    if (tezCluster == null) {
      tezCluster = new MiniTezCluster(TestShuffleHandlerJobs.class.getName(), NUM_NMS,
          1, 1);
      Configuration conf = new Configuration();
      conf.set(YarnConfiguration.NM_AUX_SERVICES,
          ShuffleHandler.TEZ_SHUFFLE_SERVICEID);
      String serviceStr = String.format(YarnConfiguration.NM_AUX_SERVICE_FMT,
          ShuffleHandler.TEZ_SHUFFLE_SERVICEID);
      conf.set(serviceStr, ShuffleHandler.class.getName());
      conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
      conf.set("fs.defaultFS", remoteFs.getUri().toString());   // use HDFS
      conf.setLong(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 0l);
      tezCluster.init(conf);
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
  @Test(timeout = 300000)
  public void testOrderedWordCount() throws Exception {
    String inputDirStr = "/tmp/owc-input/";
    Path inputDir = new Path(inputDirStr);
    Path stagingDirPath = new Path("/tmp/owc-staging-dir");
    remoteFs.mkdirs(inputDir);
    remoteFs.mkdirs(stagingDirPath);
    generateOrderedWordCountInput(inputDir, remoteFs);

    String outputDirStr = "/tmp/owc-output/";
    Path outputDir = new Path(outputDirStr);

    TezConfiguration tezConf = new TezConfiguration(tezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    tezConf.set(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID, ShuffleHandler.TEZ_SHUFFLE_SERVICEID);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_DAG_CLEANUP_ON_COMPLETION, true);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true);
    tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, false);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);
    TezClient tezSession = TezClient.create("WordCountTest", tezConf);
    tezSession.start();
    try {
      final OrderedWordCount job = new OrderedWordCount();
      Assert.assertTrue("OrderedWordCount failed", job.run(tezConf, new String[]{"-counter",
              inputDirStr, outputDirStr, "10"}, tezSession)==0);
      verifyOutput(outputDir, remoteFs);
      tezSession.stop();
      ClientRMService rmService = tezCluster.getResourceManager().getClientRMService();
      boolean isAppComplete = false;
      while(!isAppComplete) {
        GetApplicationReportResponse resp = rmService.getApplicationReport(
            new GetApplicationReportRequest() {
              @Override
              public ApplicationId getApplicationId() {
                return job.getAppId();
              }

              @Override
              public void setApplicationId(ApplicationId applicationId) {
              }
            });
        if (resp.getApplicationReport().getYarnApplicationState() == YarnApplicationState.FINISHED) {
          isAppComplete = true;
        }
        Thread.sleep(100);
      }
      for(int i = 0; i < NUM_NMS; i++) {
        String appPath = tezCluster.getTestWorkDir() + "/" + this.getClass().getName()
            + "-localDir-nm-" + i + "_0/usercache/" + UserGroupInformation.getCurrentUser().getUserName()
            + "/appcache/" + job.getAppId();
        String dagPathStr = appPath + "/dag_1";

        File fs = new File(dagPathStr);
        Assert.assertFalse(fs.exists());
        fs = new File(appPath);
        Assert.assertTrue(fs.exists());
      }
    } finally {
      remoteFs.delete(stagingDirPath, true);
    }
  }
}
