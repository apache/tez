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

package org.apache.tez.common;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.tez.client.TestTezClientUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTezCommonUtils {
  private static final String STAGE_DIR = "/tmp/mystage";
  private static String RESOLVED_STAGE_DIR;
  private static Configuration conf = new Configuration();;
  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestTezCommonUtils.class.getName() + "-tmpDir";
  private static MiniDFSCluster dfsCluster = null;
  private static FileSystem remoteFs = null;
  private static final Log LOG = LogFactory.getLog(TestTezCommonUtils.class);

  @BeforeClass
  public static void setup() throws Exception {
    conf.set(TezConfiguration.TEZ_AM_STAGING_DIR, STAGE_DIR);
    LOG.info("Starting mini clusters");
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).format(true).racks(null)
          .build();
      remoteFs = dfsCluster.getFileSystem();
      RESOLVED_STAGE_DIR = remoteFs.getUri() + STAGE_DIR;
      conf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
    if (dfsCluster != null) {
      try {
        LOG.info("Stopping DFSCluster");
        dfsCluster.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  // Testing base staging dir
  @Test
  public void testTezBaseStagingPath() throws Exception {
    Configuration localConf = new Configuration();
    // Check if default works with localFS
    localConf.unset(TezConfiguration.TEZ_AM_STAGING_DIR);
    localConf.set("fs.defaultFS", "file:///");
    Path stageDir = TezCommonUtils.getTezBaseStagingPath(localConf);
    Assert.assertEquals(stageDir.toString(), "file:" + TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT);

    // check if user set something, indeed works
    conf.set(TezConfiguration.TEZ_AM_STAGING_DIR, STAGE_DIR);
    stageDir = TezCommonUtils.getTezBaseStagingPath(conf);
    Assert.assertEquals(stageDir.toString(), RESOLVED_STAGE_DIR);
  }

  // Testing System staging dir if createed
  @Test
  public void testCreateTezSysStagingPath() throws Exception {
    String strAppId = "testAppId";
    String expectedStageDir = RESOLVED_STAGE_DIR + Path.SEPARATOR
        + TezCommonUtils.TEZ_SYSTEM_SUB_DIR + Path.SEPARATOR + strAppId;
    String unResolvedStageDir = STAGE_DIR + Path.SEPARATOR + TezCommonUtils.TEZ_SYSTEM_SUB_DIR
        + Path.SEPARATOR + strAppId;

    Path stagePath = new Path(unResolvedStageDir);
    FileSystem fs = stagePath.getFileSystem(conf);
    if (fs.exists(stagePath)) {
      fs.delete(stagePath, true);
    }
    Assert.assertFalse(fs.exists(stagePath));
    Path stageDir = TezCommonUtils.createTezSystemStagingPath(conf, strAppId);
    Assert.assertEquals(stageDir.toString(), expectedStageDir);
    Assert.assertTrue(fs.exists(stagePath));
  }

  // Testing System staging dir
  @Test
  public void testTezSysStagingPath() throws Exception {
    String strAppId = "testAppId";
    Path stageDir = TezCommonUtils.getTezSystemStagingPath(conf, strAppId);
    String expectedStageDir = RESOLVED_STAGE_DIR + Path.SEPARATOR
        + TezCommonUtils.TEZ_SYSTEM_SUB_DIR + Path.SEPARATOR + strAppId;
    Assert.assertEquals(stageDir.toString(), expectedStageDir);
  }

  // Testing conf staging dir
  @Test
  public void testTezConfStagingPath() throws Exception {
    String strAppId = "testAppId";
    Path stageDir = TezCommonUtils.getTezSystemStagingPath(conf, strAppId);
    Path confStageDir = TezCommonUtils.getTezConfStagingPath(stageDir);
    String expectedDir = RESOLVED_STAGE_DIR + Path.SEPARATOR
        + TezCommonUtils.TEZ_SYSTEM_SUB_DIR + Path.SEPARATOR + strAppId + Path.SEPARATOR
        + TezConstants.TEZ_PB_BINARY_CONF_NAME;
    Assert.assertEquals(confStageDir.toString(), expectedDir);
  }

  // Testing session jars staging dir
  @Test
  public void testTezSessionJarStagingPath() throws Exception {
    String strAppId = "testAppId";
    Path stageDir = TezCommonUtils.getTezSystemStagingPath(conf, strAppId);
    Path confStageDir = TezCommonUtils.getTezAMJarStagingPath(stageDir);
    String expectedDir = RESOLVED_STAGE_DIR + Path.SEPARATOR
        + TezCommonUtils.TEZ_SYSTEM_SUB_DIR + Path.SEPARATOR + strAppId + Path.SEPARATOR
        + TezConstants.TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME;
    Assert.assertEquals(confStageDir.toString(), expectedDir);
  }

  // Testing bin plan staging dir
  @Test
  public void testTezBinPlanStagingPath() throws Exception {
    String strAppId = "testAppId";
    Path stageDir = TezCommonUtils.getTezSystemStagingPath(conf, strAppId);
    Path confStageDir = TezCommonUtils.getTezBinPlanStagingPath(stageDir);
    String expectedDir = RESOLVED_STAGE_DIR + Path.SEPARATOR
        + TezCommonUtils.TEZ_SYSTEM_SUB_DIR + Path.SEPARATOR + strAppId + Path.SEPARATOR
        + TezConstants.TEZ_PB_PLAN_BINARY_NAME;
    Assert.assertEquals(confStageDir.toString(), expectedDir);
  }

  // Testing text plan staging dir
  @Test
  public void testTezTextPlanStagingPath() throws Exception {
    String strAppId = "testAppId";
    String dagPBName = "testDagPBName";
    Path tezSysStagingPath = TezCommonUtils.getTezSystemStagingPath(conf, strAppId);
    Path confStageDir =
        TezCommonUtils.getTezTextPlanStagingPath(tezSysStagingPath, strAppId, dagPBName);
    String expectedDir = RESOLVED_STAGE_DIR + Path.SEPARATOR
        + TezCommonUtils.TEZ_SYSTEM_SUB_DIR + Path.SEPARATOR + strAppId + Path.SEPARATOR
        + strAppId + "-" + dagPBName + "-" + TezConstants.TEZ_PB_PLAN_TEXT_NAME;
    Assert.assertEquals(confStageDir.toString(), expectedDir);
  }

  // Testing recovery path staging dir
  @Test
  public void testTezRecoveryStagingPath() throws Exception {
    String strAppId = "testAppId";
    Path stageDir = TezCommonUtils.getTezSystemStagingPath(conf, strAppId);
    Path confStageDir = TezCommonUtils.getRecoveryPath(stageDir, conf);
    String expectedDir = RESOLVED_STAGE_DIR + Path.SEPARATOR
        + TezCommonUtils.TEZ_SYSTEM_SUB_DIR + Path.SEPARATOR + strAppId + Path.SEPARATOR
        + TezConstants.DAG_RECOVERY_DATA_DIR_NAME;
    Assert.assertEquals(confStageDir.toString(), expectedDir);
  }

  // Testing app attempt specific recovery path staging dir
  @Test
  public void testTezAttemptRecoveryStagingPath() throws Exception {
    String strAppId = "testAppId";
    Path stageDir = TezCommonUtils.getTezSystemStagingPath(conf, strAppId);
    Path recoveryPath = TezCommonUtils.getRecoveryPath(stageDir, conf);
    Path recoveryStageDir = TezCommonUtils.getAttemptRecoveryPath(recoveryPath, 2);

    String expectedDir = RESOLVED_STAGE_DIR + Path.SEPARATOR
        + TezCommonUtils.TEZ_SYSTEM_SUB_DIR + Path.SEPARATOR + strAppId + Path.SEPARATOR
        + TezConstants.DAG_RECOVERY_DATA_DIR_NAME + Path.SEPARATOR + "2";
    Assert.assertEquals(recoveryStageDir.toString(), expectedDir);
  }

  // Testing DAG specific recovery path staging dir
  @Test
  public void testTezDAGRecoveryStagingPath() throws Exception {
    String strAppId = "testAppId";
    Path stageDir = TezCommonUtils.getTezSystemStagingPath(conf, strAppId);
    Path recoveryPath = TezCommonUtils.getRecoveryPath(stageDir, conf);
    Path recoveryStageDir = TezCommonUtils.getAttemptRecoveryPath(recoveryPath, 2);

    Path dagRecoveryPathj = TezCommonUtils.getDAGRecoveryPath(recoveryStageDir, "dag_123");

    String expectedDir = RESOLVED_STAGE_DIR + Path.SEPARATOR
        + TezCommonUtils.TEZ_SYSTEM_SUB_DIR + Path.SEPARATOR + strAppId + Path.SEPARATOR
        + TezConstants.DAG_RECOVERY_DATA_DIR_NAME + Path.SEPARATOR + "2" + Path.SEPARATOR
        + "dag_123" + TezConstants.DAG_RECOVERY_RECOVER_FILE_SUFFIX;
    Assert.assertEquals(expectedDir, dagRecoveryPathj.toString());
  }

  // Testing Summary recovery path staging dir
  @Test
  public void testTezSummaryRecoveryStagingPath() throws Exception {
    String strAppId = "testAppId";
    Path stageDir = TezCommonUtils.getTezSystemStagingPath(conf, strAppId);
    Path recoveryPath = TezCommonUtils.getRecoveryPath(stageDir, conf);
    Path recoveryStageDir = TezCommonUtils.getAttemptRecoveryPath(recoveryPath, 2);
    Path summaryRecoveryPathj = TezCommonUtils.getSummaryRecoveryPath(recoveryStageDir);

    String expectedDir = RESOLVED_STAGE_DIR + Path.SEPARATOR
        + TezCommonUtils.TEZ_SYSTEM_SUB_DIR + Path.SEPARATOR + strAppId + Path.SEPARATOR
        + TezConstants.DAG_RECOVERY_DATA_DIR_NAME + Path.SEPARATOR + "2" + Path.SEPARATOR
        + TezConstants.DAG_RECOVERY_SUMMARY_FILE_SUFFIX;
    Assert.assertEquals(expectedDir, summaryRecoveryPathj.toString());
  }

  // This test is running here to leverage existing mini cluster
  @Test
  public void testLocalResourceVisibility() throws Exception {
    TestTezClientUtils.testLocalResourceVisibility(dfsCluster.getFileSystem(), conf);
  }

  @Test
  public void testStringTokenize() {
    String s = "foo:bar:xyz::too";
    String[] expectedTokens = { "foo", "bar" , "xyz" , "too"};
    String[] tokens = new String[4];
    TezCommonUtils.tokenizeString(s, ":").toArray(tokens);
    Assert.assertArrayEquals(expectedTokens, tokens);
  }

}
