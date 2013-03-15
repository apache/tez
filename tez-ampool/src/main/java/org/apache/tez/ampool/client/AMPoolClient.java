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

package org.apache.tez.ampool.client;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.app2.lazy.LazyAMConfig;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.ampool.AMPoolService;
import org.apache.tez.ampool.AMPoolConfiguration;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;

public class AMPoolClient extends YarnClientImpl {

  private static final Log LOG = LogFactory.getLog(AMPoolClient.class);

  // Configuration
  private Configuration conf;

  private Options opts;

  private String appMasterJar;

  private final String appName = AMPoolConfiguration.APP_NAME;
  private static final String appMasterMainClass = AMPoolService.class.getName();

  private String tmpAppDirPath = null;

  public AMPoolClient() {
    opts = new Options();
    opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
    opts.addOption("jar", true, "Jar file containing the application master");
    opts.addOption("help", false, "Print usage");
  }

  /**
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    boolean result = false;
    AMPoolClient amPoolClient = null;
    try {
      amPoolClient = new AMPoolClient();
      LOG.info("Initializing AMPoolClient");
      try {
        boolean doRun = amPoolClient.init(args);
        if (!doRun) {
          System.exit(0);
        }
      } catch (IllegalArgumentException e) {
        System.err.println(e.getLocalizedMessage());
        amPoolClient.printUsage();
        System.exit(-1);
      }
      result = amPoolClient.run();
    } catch (Throwable t) {
      LOG.fatal("Error running CLient", t);
      System.exit(1);
    } finally {
      if (amPoolClient != null
          && amPoolClient.tmpAppDirPath != null) {
        LOG.info("Deleting working dir: " + amPoolClient.tmpAppDirPath);
        File dir = new File(amPoolClient.tmpAppDirPath);
        for (File f : dir.listFiles()) {
          f.delete();
        }
        dir.delete();
      }
    }
    if (result) {
      LOG.info("Application completed successfully");
      System.exit(0);
    }
    LOG.error("Application failed to complete successfully");
    System.exit(2);
  }

  public boolean init(String[] args) throws Exception {
    this.conf = new AMPoolConfiguration(new YarnConfiguration());
    // TODO Auto-generated method stub
    super.init(conf);

    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      throw new IllegalArgumentException("No args specified");
    }

    if (cliParser.hasOption("help")) {
      printUsage();
      return false;
    }

    if (!cliParser.hasOption("jar")) {
      throw new IllegalArgumentException("No jar file specified for application master");
    }


    if (!cliParser.hasOption("jar")) {
      throw new IllegalArgumentException(
          "No jar file specified for application master");
    }

    appMasterJar = cliParser.getOptionValue("jar");

    return true;
  }

  private void printUsage() {
    // TODO Auto-generated method stub
  }

  public boolean run() throws IOException {
    LOG.info("Running AMPoolClient");
    start();

    // Get a new application id
    GetNewApplicationResponse newApp = super.getNewApplication();
    ApplicationId appId = newApp.getApplicationId();
    System.out.println("Starting AMPoolMaster with Application ID: "
        + appId);

    int minMem = newApp.getMinimumResourceCapability().getMemory();
    int maxMem = newApp.getMaximumResourceCapability().getMemory();
    LOG.info("Min mem capabililty of resources in this cluster " + minMem);
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

    int amMemory = conf.getInt(AMPoolConfiguration.AM_MASTER_MEMORY_MB,
        AMPoolConfiguration.DEFAULT_AM_MASTER_MEMORY_MB);

    // Create launch context for app master
    LOG.info("Setting up application submission context for ASM");
    ApplicationSubmissionContext appContext =
        Records.newRecord(ApplicationSubmissionContext.class);

    appContext.setApplicationId(appId);
    appContext.setApplicationName(appName);

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records.newRecord(
        ContainerLaunchContext.class);

    // set local resources for the application master
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();

    LOG.info("Copy App Master jar from local filesystem"
        + " and add to local environment");
    FileSystem fs = FileSystem.get(conf);
    Path src = new Path(appMasterJar);
    String pathSuffix = appName + "/" + appId.toString() + "/AMPoolService.jar";
    Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
    fs.copyFromLocalFile(false, true, src, dst);
    FileStatus destStatus = fs.getFileStatus(dst);
    LocalResource amJarRsrc = Records.newRecord(LocalResource.class);

    amJarRsrc.setType(LocalResourceType.FILE);
    amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst));
    amJarRsrc.setTimestamp(destStatus.getModificationTime());
    amJarRsrc.setSize(destStatus.getLen());
    localResources.put("AMPoolService.jar",  amJarRsrc);

    tmpAppDirPath   = conf.get(AMPoolConfiguration.TMP_DIR_PATH,
        AMPoolConfiguration.DEFAULT_TMP_DIR_PATH)
      + File.separator + appId.toString() + File.separator;
    File appDir = new File(tmpAppDirPath);
    appDir.deleteOnExit();

    LOG.info("Writing contents of LazyAMConfig to local fs");

    Configuration lazyAMConfToUpload = new LazyAMConfig();
    File lazyAmConfFile = new File(tmpAppDirPath,
        LazyAMConfig.LAZY_AM_JOB_XML_FILE);
    LOG.info("Writing contents of LazyAMConfig to local fs"
        + ", path=" + lazyAmConfFile.getAbsolutePath());
    lazyAmConfFile.getParentFile().mkdirs();
    lazyAMConfToUpload.writeXml(new FileOutputStream(lazyAmConfFile));

    Path lazyAmConfPathOnDfs = new Path(
        fs.getHomeDirectory(),
        appName + "/" + appId.toString() + "/"
        + LazyAMConfig.LAZY_AM_JOB_XML_FILE);

    LOG.info("Uploading contents of LazyAMConfig to dfs"
        + ", srcpath=" + lazyAmConfFile.getAbsolutePath()
        + ", destpath=" + lazyAmConfPathOnDfs.toString());
    fs.copyFromLocalFile(false, true,
        new Path(lazyAmConfFile.getAbsolutePath()), lazyAmConfPathOnDfs);
    fs.getFileStatus(lazyAmConfPathOnDfs);

    Configuration amPoolConfToUpload = new AMPoolConfiguration();
    amPoolConfToUpload.set(
        AMPoolConfiguration.LAZY_AM_CONF_FILE_PATH,
        lazyAmConfPathOnDfs.toString());

    File amPoolConfFile = new File(tmpAppDirPath,
        AMPoolConfiguration.AMPOOL_APP_XML_FILE);
    amPoolConfFile.getParentFile().mkdirs();
    LOG.info("Writing contents of AmPoolConf to local fs"
        + ", path=" + amPoolConfFile.getAbsolutePath());
    amPoolConfToUpload.writeXml(new FileOutputStream(amPoolConfFile));

    Path amPoolConfPathOnDfs = new Path(
        fs.getHomeDirectory(),
        appName + "/" + appId.toString() + "/"
        + AMPoolConfiguration.AMPOOL_APP_XML_FILE);
    LOG.info("Uploading contents of AmPoolConfig to dfs"
        + ", srcpath=" + amPoolConfFile.getAbsolutePath()
        + ", destpath=" + amPoolConfPathOnDfs.toString());
    fs.copyFromLocalFile(false, true,
        new Path(amPoolConfFile.getAbsolutePath()),
        amPoolConfPathOnDfs);
    FileStatus amPoolConfFileStatus =
        fs.getFileStatus(amPoolConfPathOnDfs);

    LocalResource amPoolConfRsrc = Records.newRecord(LocalResource.class);
    amPoolConfRsrc.setType(LocalResourceType.FILE);
    amPoolConfRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    amPoolConfRsrc.setResource(ConverterUtils.getYarnUrlFromPath(
        amPoolConfPathOnDfs));
    amPoolConfRsrc.setTimestamp(amPoolConfFileStatus.getModificationTime());
    amPoolConfRsrc.setSize(amPoolConfFileStatus.getLen());
    LOG.info("Adding AMPoolConfig as a local resource for AMPoolServiceAM");
    localResources.put(AMPoolConfiguration.AMPOOL_APP_XML_FILE,
        amPoolConfRsrc);

    amContainer.setLocalResources(localResources);

    // Set the env variables to be setup in the env
    // where the application master will be run
    LOG.info("Set the environment for the application master");
    Map<String, String> env = new HashMap<String, String>();

    StringBuilder classPathEnv = new StringBuilder("${CLASSPATH}:.");
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      classPathEnv.append(':');
      classPathEnv.append(c.trim());
    }

    for (String c : conf.getStrings(
        MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
        MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH)) {
      classPathEnv.append(':');
      classPathEnv.append(c.trim());
    }

    for (String c : conf.getStrings(
        AMPoolConfiguration.AMPOOL_APPLICATION_CLASSPATH,
        AMPoolConfiguration.DEFAULT_AMPOOL_APPLICATION_CLASSPATH)) {
      classPathEnv.append(':');
      classPathEnv.append(c.trim());
    }

    // add the runtime classpath needed for tests to work
    if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      classPathEnv.append(':');
      classPathEnv.append(System.getProperty("java.class.path"));
    }

    env.put("CLASSPATH", classPathEnv.toString());

    amContainer.setEnvironment(env);

    // Set the necessary command to execute the application master
    Vector<CharSequence> vargs = new Vector<CharSequence>(30);

    // Set java executable command
    LOG.info("Setting up app master command");
    vargs.add("${JAVA_HOME}" + "/bin/java");
    // Set Xmx based on am memory size
    int normalizeMem = (int)(amMemory * 0.75);
    vargs.add("-Xmx" + normalizeMem + "m");
    // Set class name
    vargs.add(appMasterMainClass);

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
        + "/AMPoolService.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
        + "/AMPoolService.stderr");

    // Get final commmand
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    LOG.info("Completed setting up app master command " + command.toString());
    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());
    amContainer.setCommands(commands);

    // Set up resource type requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(amMemory);
    amContainer.setResource(capability);

    appContext.setAMContainerSpec(amContainer);

    // Set the queue to which this application is to be submitted in the RM
    String amQueue = conf.get(AMPoolConfiguration.AM_MASTER_QUEUE,
        AMPoolConfiguration.DEFAULT_AM_MASTER_QUEUE);
    appContext.setQueue(amQueue);

    // Submit the application to the applications manager
    LOG.info("Submitting application to ASM");
    super.submitApplication(appContext);

    LOG.info("Monitoring application, appId="
        + appId);
    while (true) {
      ApplicationReport report = super.getApplicationReport(appId);
      if (report.getFinalApplicationStatus().equals(
          FinalApplicationStatus.FAILED)
          || report.getFinalApplicationStatus().equals(
              FinalApplicationStatus.KILLED)) {
        LOG.fatal("AMPoolService failed to start up");
        return false;
      }
      else if (report.getYarnApplicationState().equals(
          YarnApplicationState.RUNNING)) {
        LOG.info("AMPoolService running on"
            + ", host=" + report.getHost()
            + ", trackingUrl=" + report.getTrackingUrl());
        break;
      }
      else if (report.getFinalApplicationStatus().equals(
          FinalApplicationStatus.SUCCEEDED)) {
        LOG.warn("AMPoolService shutdown");
        return false;
      }
    }
    return true;
  }

}
