/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.tests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.examples.HashJoinExample;
import org.apache.tez.examples.JoinDataGen;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.service.MiniTezTestServiceCluster;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
import org.apache.tez.test.MiniTezCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalTezServiceTestHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalTezServiceTestHelper.class);

  private volatile MiniTezCluster tezCluster;
  private volatile MiniDFSCluster dfsCluster;
  private volatile MiniTezTestServiceCluster tezTestServiceCluster;

  private volatile Configuration clusterConf = new Configuration();
  private volatile Configuration confForJobs;

  private volatile FileSystem remoteFs;

  private volatile TezClient sharedTezClient;

  /**
   * Current usage: Create. setupSharedTezClient - during setup (beforeClass). Invoke tearDownAll when done (afterClass)
   * Alternately tearDown the sharedTezClient independently
   */
  public ExternalTezServiceTestHelper(String testRootDir) throws
      IOException {
    try {
      clusterConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testRootDir);
      dfsCluster =
          new MiniDFSCluster.Builder(clusterConf).numDataNodes(1).format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
      LOG.info("MiniDFSCluster started");
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }

    tezCluster = new MiniTezCluster(TestExternalTezServices.class.getName(), 1, 1, 1);
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
    tezCluster.init(conf);
    tezCluster.start();
    LOG.info("MiniTezCluster started");

    clusterConf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
    for (Map.Entry<String, String> entry : tezCluster.getConfig()) {
      clusterConf.set(entry.getKey(), entry.getValue());
    }
    long jvmMax = Runtime.getRuntime().maxMemory();

    tezTestServiceCluster = MiniTezTestServiceCluster
        .create(TestExternalTezServices.class.getSimpleName(), 3, ((long) (jvmMax * 0.5d)), 1);
    tezTestServiceCluster.init(clusterConf);
    tezTestServiceCluster.start();
    LOG.info("MiniTezTestServer started");

    confForJobs = new Configuration(clusterConf);
    for (Map.Entry<String, String> entry : tezTestServiceCluster
        .getClusterSpecificConfiguration()) {
      confForJobs.set(entry.getKey(), entry.getValue());
    }

    Path stagingDirPath = new Path("/tmp/tez-staging-dir");
    remoteFs.mkdirs(stagingDirPath);
    // This is currently configured to push tasks into the Service, and then use the standard RPC
    confForJobs.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    confForJobs.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, false);
  }

  public void setupSharedTezClient(ServicePluginsDescriptor servicePluginsDescriptor) throws
      IOException, TezException, InterruptedException {
    // Create a session to use for all tests.
    TezConfiguration tezClientConf = new TezConfiguration(confForJobs);

    sharedTezClient = TezClient
        .newBuilder(TestExternalTezServices.class.getSimpleName() + "_session", tezClientConf)
        .setIsSession(true).setServicePluginDescriptor(servicePluginsDescriptor).build();

    sharedTezClient.start();
    LOG.info("Shared TezSession started");
    sharedTezClient.waitTillReady();
    LOG.info("Shared TezSession ready for submission");
  }

  public void tearDownAll() throws IOException, TezException {
    if (sharedTezClient != null) {
      sharedTezClient.stop();
      sharedTezClient = null;
    }

    if (tezTestServiceCluster != null) {
      tezTestServiceCluster.stop();
      tezTestServiceCluster = null;
    }

    if (tezCluster != null) {
      tezCluster.stop();
      tezCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  public void shutdownSharedTezClient() throws IOException, TezException {
    if (sharedTezClient != null) {
      sharedTezClient.stop();
      sharedTezClient = null;
    }
  }


  public void setupHashJoinData(Path srcDataDir, Path dataPath1, Path dataPath2,
                                Path expectedResultPath, Path outputPath) throws
      Exception {
    remoteFs.mkdirs(srcDataDir);
    TezConfiguration tezConf = new TezConfiguration(confForJobs);
    //   Generate join data - with 2 tasks.
    JoinDataGen dataGen = new JoinDataGen();
    String[] dataGenArgs = new String[]{
        dataPath1.toString(), "1048576", dataPath2.toString(), "524288",
        expectedResultPath.toString(), "2"};
    assertEquals(0, dataGen.run(tezConf, dataGenArgs, sharedTezClient));
    //    Run the actual join - with 2 reducers
    HashJoinExample joinExample = new HashJoinExample();
    String[] args = new String[]{
        dataPath1.toString(), dataPath2.toString(), "2", outputPath.toString()};
    assertEquals(0, joinExample.run(tezConf, args, sharedTezClient));
    LOG.info("Completed generating Data - Expected Hash Result and Actual Join Result");
  }


  public MiniTezCluster getTezCluster() {
    return tezCluster;
  }

  public MiniDFSCluster getDfsCluster() {
    return dfsCluster;
  }

  public MiniTezTestServiceCluster getTezTestServiceCluster() {
    return tezTestServiceCluster;
  }

  public Configuration getClusterConf() {
    return clusterConf;
  }

  public Configuration getConfForJobs() {
    return confForJobs;
  }

  public FileSystem getRemoteFs() {
    return remoteFs;
  }

  public TezClient getSharedTezClient() {
    Preconditions.checkNotNull(sharedTezClient);
    return sharedTezClient;
  }
}
