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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.tez.client.TestTezClientUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezUncheckedException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTezCommonUtils {
  private static final String STAGE_DIR = "/tmp/mystage";

  private static final File LOCAL_STAGING_DIR = new File(System.getProperty("test.build.data"),
      TestTezCommonUtils.class.getSimpleName()).getAbsoluteFile();
  private static String RESOLVED_STAGE_DIR;
  private static Configuration conf = new Configuration();;
  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestTezCommonUtils.class.getName() + "-tmpDir";
  private static MiniDFSCluster dfsCluster = null;
  private static FileSystem remoteFs = null;
  private static final Logger LOG = LoggerFactory.getLogger(TestTezCommonUtils.class);

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
  @Test(timeout = 5000)
  public void testTezBaseStagingPath() throws Exception {
    Configuration localConf = new Configuration();
    // Check if default works with localFS
    localConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, LOCAL_STAGING_DIR.getAbsolutePath());
    localConf.set("fs.defaultFS", "file:///");
    Path stageDir = TezCommonUtils.getTezBaseStagingPath(localConf);
    Assert.assertEquals("file:" + LOCAL_STAGING_DIR, stageDir.toString());

    // check if user set something, indeed works
    conf.set(TezConfiguration.TEZ_AM_STAGING_DIR, STAGE_DIR);
    stageDir = TezCommonUtils.getTezBaseStagingPath(conf);
    Assert.assertEquals(stageDir.toString(), RESOLVED_STAGE_DIR);
  }

  // Testing System staging dir if createed
  @Test(timeout = 5000)
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
  @Test(timeout = 5000)
  public void testTezSysStagingPath() throws Exception {
    String strAppId = "testAppId";
    Path stageDir = TezCommonUtils.getTezSystemStagingPath(conf, strAppId);
    String expectedStageDir = RESOLVED_STAGE_DIR + Path.SEPARATOR
        + TezCommonUtils.TEZ_SYSTEM_SUB_DIR + Path.SEPARATOR + strAppId;
    Assert.assertEquals(stageDir.toString(), expectedStageDir);
  }

  // Testing conf staging dir
  @Test(timeout = 5000)
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
  @Test(timeout = 5000)
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
  @Test(timeout = 5000)
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
  @Test(timeout = 5000)
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
  @Test(timeout = 5000)
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
  @Test(timeout = 5000)
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
  @Test(timeout = 5000)
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
  @Test(timeout = 5000)
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
  @Test(timeout = 5000)
  public void testLocalResourceVisibility() throws Exception {
    TestTezClientUtils.testLocalResourceVisibility(dfsCluster.getFileSystem(), conf);
  }

  @Test(timeout = 5000)
  public void testStringTokenize() {
    String s = "foo:bar:xyz::too";
    String[] expectedTokens = { "foo", "bar" , "xyz" , "too"};
    String[] tokens = new String[4];
    TezCommonUtils.tokenizeString(s, ":").toArray(tokens);
    Assert.assertArrayEquals(expectedTokens, tokens);
  }


  @Test(timeout = 5000)
  public void testAddAdditionalLocalResources() {
    String lrName = "LR";
    Map<String, LocalResource> originalLrs;
    originalLrs= Maps.newHashMap();
    originalLrs.put(lrName, LocalResource.newInstance(
        URL.newInstance("file", "localhost", 0, "/test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));


    Map<String, LocalResource> additionalLrs;

    // Same path, same size.
    originalLrs= Maps.newHashMap();
    originalLrs.put(lrName, LocalResource.newInstance(
        URL.newInstance("file", "localhost", 0, "/test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    additionalLrs = Maps.newHashMap();
    additionalLrs.put(lrName, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    TezCommonUtils.addAdditionalLocalResources(additionalLrs, originalLrs, "");

    // Same path, different size.
    originalLrs= Maps.newHashMap();
    originalLrs.put(lrName, LocalResource.newInstance(
        URL.newInstance("file", "localhost", 0, "/test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    additionalLrs = Maps.newHashMap();
    additionalLrs.put(lrName, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 100, 1));
    try {
      TezCommonUtils.addAdditionalLocalResources(additionalLrs, originalLrs, "");
      Assert.fail("Duplicate LRs with different sizes expected to fail");
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains("Duplicate Resources found with different size"));
    }

    // Different path, same size, diff timestamp
    originalLrs= Maps.newHashMap();
    originalLrs.put(lrName, LocalResource.newInstance(
        URL.newInstance("file", "localhost", 0, "/test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    additionalLrs = Maps.newHashMap();
    additionalLrs.put(lrName, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test2"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 100));
    TezCommonUtils.addAdditionalLocalResources(additionalLrs, originalLrs, "");

    // Different path, different size
    originalLrs= Maps.newHashMap();
    originalLrs.put(lrName, LocalResource.newInstance(
        URL.newInstance("file", "localhost", 0, "/test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    additionalLrs = Maps.newHashMap();
    additionalLrs.put(lrName, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test2"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 100, 1));
    try {
      TezCommonUtils.addAdditionalLocalResources(additionalLrs, originalLrs, "");
      Assert.fail("Duplicate LRs with different sizes expected to fail");
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains("Duplicate Resources found with different size"));
    }

  }

  @Test (timeout = 5000)
  public void testAMClientHeartBeatTimeout() {
    TezConfiguration conf = new TezConfiguration(false);

    // -1 for any negative value
    Assert.assertEquals(-1,
        TezCommonUtils.getAMClientHeartBeatTimeoutMillis(conf));
    conf.setInt(TezConfiguration.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS, -2);
    Assert.assertEquals(-1,
        TezCommonUtils.getAMClientHeartBeatTimeoutMillis(conf));

    // For any value > 0 but less than min, revert to min
    conf.setInt(TezConfiguration.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS,
        TezConstants.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS_MINIMUM - 1);
    Assert.assertEquals(TezConstants.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS_MINIMUM * 1000,
        TezCommonUtils.getAMClientHeartBeatTimeoutMillis(conf));

    // For val > min, should remain val as configured
    conf.setInt(TezConfiguration.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS,
        TezConstants.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS_MINIMUM * 2);
    Assert.assertEquals(TezConstants.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS_MINIMUM * 2000,
        TezCommonUtils.getAMClientHeartBeatTimeoutMillis(conf));

    conf = new TezConfiguration(false);
    Assert.assertEquals(-1, TezCommonUtils.getAMClientHeartBeatPollIntervalMillis(conf, -1, 10));
    Assert.assertEquals(-1, TezCommonUtils.getAMClientHeartBeatPollIntervalMillis(conf, -123, 10));
    Assert.assertEquals(-1, TezCommonUtils.getAMClientHeartBeatPollIntervalMillis(conf, 0, 10));

    // min poll interval is 1000
    Assert.assertEquals(1000, TezCommonUtils.getAMClientHeartBeatPollIntervalMillis(conf, 600, 10));

    // Poll interval is heartbeat interval/10
    Assert.assertEquals(2000,
        TezCommonUtils.getAMClientHeartBeatPollIntervalMillis(conf, 20000, 10));

    // Configured poll interval ignored
    conf.setInt(TezConfiguration.TEZ_AM_CLIENT_HEARTBEAT_POLL_INTERVAL_MILLIS, -1);
    Assert.assertEquals(4000,
        TezCommonUtils.getAMClientHeartBeatPollIntervalMillis(conf, 20000, 5));

    // Positive poll interval is allowed
    conf.setInt(TezConfiguration.TEZ_AM_CLIENT_HEARTBEAT_POLL_INTERVAL_MILLIS, 2000);
    Assert.assertEquals(2000,
        TezCommonUtils.getAMClientHeartBeatPollIntervalMillis(conf, 20000, 5));


  }

  @Test
  public void testLogSystemProperties() throws Exception {
    Configuration conf = new Configuration();
    // test default logging
    conf.set(TezConfiguration.TEZ_JVM_SYSTEM_PROPERTIES_TO_LOG, " ");
    String value = TezCommonUtils.getSystemPropertiesToLog(conf);
    for(String key: TezConfiguration.TEZ_JVM_SYSTEM_PROPERTIES_TO_LOG_DEFAULT) {
      Assert.assertTrue(value.contains(key));
    }

    // test logging of selected keys
    String classpath = "java.class.path";
    String os = "os.name";
    String version = "java.version";
    conf.set(TezConfiguration.TEZ_JVM_SYSTEM_PROPERTIES_TO_LOG, classpath + ", " + os);
    value = TezCommonUtils.getSystemPropertiesToLog(conf);
    Assert.assertNotNull(value);
    Assert.assertTrue(value.contains(classpath));
    Assert.assertTrue(value.contains(os));
    Assert.assertFalse(value.contains(version));
  }

  @Test(timeout=5000)
  public void testGetDAGSessionTimeout() {
    Configuration conf = new Configuration(false);
    Assert.assertEquals(TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS_DEFAULT*1000,
        TezCommonUtils.getDAGSessionTimeout(conf));

    // set to 1 month - * 1000 guaranteed to cross positive integer boundary
    conf.setInt(TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS,
        24 * 60 * 60 * 30);
    Assert.assertEquals(86400l*1000*30,
        TezCommonUtils.getDAGSessionTimeout(conf));

    // set to negative val
    conf.setInt(TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS,
        -24 * 60 * 60 * 30);
    Assert.assertEquals(-1,
        TezCommonUtils.getDAGSessionTimeout(conf));

    conf.setInt(TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS, 0);
    Assert.assertEquals(1000,
        TezCommonUtils.getDAGSessionTimeout(conf));

  }


}
