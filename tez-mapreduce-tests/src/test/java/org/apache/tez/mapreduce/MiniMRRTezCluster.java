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

package org.apache.tez.mapreduce;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.util.JarFinder;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.service.Service;
import org.apache.tez.dag.app.DAGAppMaster;
import org.apache.tez.mapreduce.hadoop.MRConfig;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;

/**
 * Configures and starts the Tez-specific components in the YARN cluster.
 *
 * When using this mini cluster, the user is expected to
 */
public class MiniMRRTezCluster extends MiniYARNCluster {

  public static final String APPJAR = JarFinder.getJar(DAGAppMaster.class);

  private static final Log LOG = LogFactory.getLog(MiniMRRTezCluster.class);

  private static final String YARN_CLUSTER_CONFIG = "yarn-site.xml";

  private Path confFilePath;

  public MiniMRRTezCluster(String testName) {
    this(testName, 1);
  }

  public MiniMRRTezCluster(String testName, int noOfNMs) {
    super(testName, noOfNMs, 4, 4);
  }

  public MiniMRRTezCluster(String testName, int noOfNMs,
      int numLocalDirs, int numLogDirs)  {
    super(testName, noOfNMs, numLocalDirs, numLogDirs);
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_TEZ_FRAMEWORK_NAME);
    if (conf.get(MRJobConfig.MR_AM_STAGING_DIR) == null) {
      conf.set(MRJobConfig.MR_AM_STAGING_DIR, new File(getTestWorkDir(),
          "apps_staging_dir" + Path.SEPARATOR).getAbsolutePath());
    }

    // VMEM monitoring disabled, PMEM monitoring enabled.
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);

    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY,  "000");

    try {
      Path stagingPath = FileContext.getFileContext(conf).makeQualified(
          new Path(conf.get(MRJobConfig.MR_AM_STAGING_DIR)));
      FileContext fc=FileContext.getFileContext(stagingPath.toUri(), conf);
      if (fc.util().exists(stagingPath)) {
        LOG.info(stagingPath + " exists! deleting...");
        fc.delete(stagingPath, true);
      }
      LOG.info("mkdir: " + stagingPath);
      fc.mkdir(stagingPath, null, true);

      //mkdir done directory as well
      String doneDir =
          JobHistoryUtils.getConfiguredHistoryServerDoneDirPrefix(conf);
      Path doneDirPath = fc.makeQualified(new Path(doneDir));
      fc.mkdir(doneDirPath, null, true);
    } catch (IOException e) {
      throw new TezUncheckedException("Could not create staging directory. ", e);
    }
    conf.set(MRConfig.MASTER_ADDRESS, "test");

    //configure the shuffle service in NM
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES,
        new String[] { ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID });
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT,
        ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID), ShuffleHandler.class,
        Service.class);

    // Non-standard shuffle port
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);

    conf.setClass(YarnConfiguration.NM_CONTAINER_EXECUTOR,
        DefaultContainerExecutor.class, ContainerExecutor.class);

    // TestMRJobs is for testing non-uberized operation only; see TestUberAM
    // for corresponding uberized tests.
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    super.serviceStart();
    File workDir = super.getTestWorkDir();
    Configuration conf = super.getConfig();

    confFilePath = new Path(workDir.getAbsolutePath(), YARN_CLUSTER_CONFIG);
    File confFile = new File(confFilePath.toString());
    try {
      confFile.createNewFile();
      conf.writeXml(new FileOutputStream(confFile));
      confFile.deleteOnExit();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    confFilePath = new Path(confFile.getAbsolutePath());
  }

  public Path getConfigFilePath() {
    return confFilePath;
  }

}
