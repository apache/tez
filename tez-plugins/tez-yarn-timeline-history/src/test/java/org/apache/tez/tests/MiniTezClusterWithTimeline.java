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

package org.apache.tez.tests;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.DAGAppMaster;
import org.apache.tez.mapreduce.hadoop.MRConfig;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

/**
 * Configures and starts the Tez-specific components in the YARN cluster.
 *
 */
public class MiniTezClusterWithTimeline extends MiniYARNCluster {

  public static final String APPJAR = JarFinder.getJar(DAGAppMaster.class);

  private static final Log LOG = LogFactory.getLog(MiniTezClusterWithTimeline.class);

  private static final String YARN_CLUSTER_CONFIG = "yarn-site.xml";

  private Path confFilePath;

  private long maxTimeToWaitForAppsOnShutdown;

  public MiniTezClusterWithTimeline(String testName) {
    this(testName, 1);
  }

  public MiniTezClusterWithTimeline(String testName, int noOfNMs) {
    super(testName, noOfNMs, 4, 4);
  }

  public MiniTezClusterWithTimeline(String testName, int noOfNMs,
      int numLocalDirs, int numLogDirs)  {
    super(testName, noOfNMs, numLocalDirs, numLogDirs);
  }

  public MiniTezClusterWithTimeline(String testName, int noOfNMs,
                        int numLocalDirs, int numLogDirs, boolean enableAHS)  {
    super(testName, 1, noOfNMs, numLocalDirs, numLogDirs, enableAHS);
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_TEZ_FRAMEWORK_NAME);
    // Use libs from cluster since no build is available
    conf.setBoolean(TezConfiguration.TEZ_USE_CLUSTER_HADOOP_LIBS, true);
    // blacklisting disabled to prevent scheduling issues
    conf.setBoolean(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, false);
    if (conf.get(MRJobConfig.MR_AM_STAGING_DIR) == null) {
      conf.set(MRJobConfig.MR_AM_STAGING_DIR, new File(getTestWorkDir(),
          "apps_staging_dir" + Path.SEPARATOR).getAbsolutePath());
    }
    
    if (conf.get(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC) == null) {
      // nothing defined. set quick delete value
      conf.setLong(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 0l);
    }

    maxTimeToWaitForAppsOnShutdown = conf.getLong(
        TezConfiguration.TEZ_TEST_MINI_CLUSTER_APP_WAIT_ON_SHUTDOWN_SECS,
        TezConfiguration.TEZ_TEST_MINI_CLUSTER_APP_WAIT_ON_SHUTDOWN_SECS_DEFAULT);

    File appJarLocalFile = new File(MiniTezClusterWithTimeline.APPJAR);

    if (!appJarLocalFile.exists()) {
      String message = "TezAppJar " + MiniTezClusterWithTimeline.APPJAR
          + " not found. Exiting.";
      LOG.info(message);
      throw new TezUncheckedException(message);
    } else {
      LOG.info("Using Tez AppJar: " + appJarLocalFile.getAbsolutePath());
    }
    
    FileSystem fs = FileSystem.get(conf);
    Path testRootDir = fs.makeQualified(new Path("target", getName() + "-tmpDir"));
    Path appRemoteJar = new Path(testRootDir, "TezAppJar.jar");
    // Copy AppJar and make it public.
    Path appMasterJar = new Path(MiniTezClusterWithTimeline.APPJAR);
    fs.copyFromLocalFile(appMasterJar, appRemoteJar);
    fs.setPermission(appRemoteJar, new FsPermission("777"));

    conf.set(TezConfiguration.TEZ_LIB_URIS, appRemoteJar.toUri().toString());
    LOG.info("Set TEZ-LIB-URI to: " + conf.get(TezConfiguration.TEZ_LIB_URIS));

    // VMEM monitoring disabled, PMEM monitoring enabled.
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);

    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY,  "000");

    try {
      Path stagingPath = FileContext.getFileContext(conf).makeQualified(
          new Path(conf.get(MRJobConfig.MR_AM_STAGING_DIR)));
      /*
       * Re-configure the staging path on Windows if the file system is localFs.
       * We need to use a absolute path that contains the drive letter. The unit
       * test could run on a different drive than the AM. We can run into the
       * issue that job files are localized to the drive where the test runs on,
       * while the AM starts on a different drive and fails to find the job
       * metafiles. Using absolute path can avoid this ambiguity.
       */
      if (Path.WINDOWS) {
        if (LocalFileSystem.class.isInstance(stagingPath.getFileSystem(conf))) {
          conf.set(MRJobConfig.MR_AM_STAGING_DIR,
              new File(conf.get(MRJobConfig.MR_AM_STAGING_DIR))
                  .getAbsolutePath());
        }
      }
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
    LOG.info("Starting MiniTezClusterWithTimeline");
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
    conf.setStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        workDir.getAbsolutePath(), System.getProperty("java.class.path"));
    LOG.info("Setting yarn-site.xml via YARN-APP-CP at: "
        + conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH));
  }

  @Override
  protected void serviceStop() throws Exception {
    waitForAppsToFinish();
    super.serviceStop();
  }

  private void waitForAppsToFinish() {
    long waitStartTime = System.currentTimeMillis();
    long waitEndTime = maxTimeToWaitForAppsOnShutdown == -1 ?
        -1 : (waitStartTime + (1000 * maxTimeToWaitForAppsOnShutdown));

    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(getConfig());
    yarnClient.start();
    Collection<ApplicationReport> unCompletedApps = null;
    try {
      do {
        List<ApplicationReport> appReports = yarnClient.getApplications();
        unCompletedApps = Collections2.filter(appReports, new Predicate<ApplicationReport>() {
          @Override
          public boolean apply(ApplicationReport appReport) {
            return EnumSet.of(YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
                YarnApplicationState.SUBMITTED, YarnApplicationState.ACCEPTED, YarnApplicationState.RUNNING)
                .contains(appReport.getYarnApplicationState());
          }
        });
        if (unCompletedApps.isEmpty()) {
          break;
        }
        LOG.info("Waiting for applications to finish in MiniTezClusterWithTimeline"
            + ", incompleteAppsCount=" + unCompletedApps.size());
        Thread.sleep(1000);
      } while (waitEndTime != -1 && waitEndTime > System.currentTimeMillis());

      if (unCompletedApps != null && !unCompletedApps.isEmpty()) {
        LOG.info("Killing incomplete applications in MiniTezCluster"
            + ", incompleteAppsCount=" + unCompletedApps.size());
        Set<ApplicationId> incompleteAppIds =
            new HashSet<ApplicationId>();
        for (ApplicationReport appReport : unCompletedApps) {
          try {
            LOG.info("Killing application, id=" + appReport.getApplicationId()
                + ", appName=" + appReport.getName());
            yarnClient.killApplication(appReport.getApplicationId());
            incompleteAppIds.add(appReport.getApplicationId());
          } catch (Exception e) {
            LOG.warn("Failed to kill app on MiniTezCluster shutdown"
                + ", appId=" + appReport.getApplicationId()
                + ", appName=" + appReport.getName());
          }
        }

        // Wait for RM to report back that incomplete apps are killed
        waitStartTime = System.currentTimeMillis();
        waitEndTime = maxTimeToWaitForAppsOnShutdown == -1 ?
            -1 : (waitStartTime + (1000 * maxTimeToWaitForAppsOnShutdown));
        do {
          Iterator<ApplicationId> iter = incompleteAppIds.iterator();
          while (iter.hasNext()) {
            ApplicationId applicationId = iter.next();
            ApplicationReport report = yarnClient.getApplicationReport(applicationId);
            if (EnumSet.of(YarnApplicationState.FINISHED, YarnApplicationState.FAILED,
                YarnApplicationState.KILLED).contains(report.getYarnApplicationState())) {
              iter.remove();
              LOG.info("Application completed, id=" + report.getApplicationId()
                  + ", yarnState=" + report.getYarnApplicationState());
            }
          }
          if (incompleteAppIds.isEmpty()) {
            break;
          }
        } while (waitEndTime != -1 && waitEndTime > System.currentTimeMillis());

      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      yarnClient.stop();
    }
  }
  
  public Path getConfigFilePath() {
    return confFilePath;
  }

}
