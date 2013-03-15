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

package org.apache.tez.ampool.mr;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.app2.lazy.LazyAMConfig;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.ampool.AMLauncher;
import org.apache.tez.ampool.AMPoolConfiguration;
import org.apache.tez.ampool.manager.AMLaunchedEvent;

public class MRAMLauncher extends AbstractService
    implements AMLauncher {

  private static final Log LOG = LogFactory.getLog(MRAMLauncher.class);

  private final FileContext defaultFileContext;

  private final Dispatcher dispatcher;

  private YarnClient yarnClient;

  private ExecutorService executorService;

  private Configuration conf;

  private final String pollingUrl;

  private String lazyAMConfigPathStr;

  private final boolean inCLIMode;

  private final String tmpAppDirPath;

  private final String user;

  private Path lazyAmConfPathOnDfs;

  private FileSystem fs = null;

  public MRAMLauncher(Configuration conf, Dispatcher dispatcher,
      YarnClient yarnClient, String pollingUrl, String lazyAMConfigPath,
      boolean inCLIMode, String tmpAppDirPath)
      throws Exception {
    super(MRAMLauncher.class.getName());
    // TODO Auto-generated constructor stub
    this.defaultFileContext = FileContext.getFileContext(conf);
    this.dispatcher = dispatcher;
    this.yarnClient = yarnClient;
    this.pollingUrl = pollingUrl;
    this.lazyAMConfigPathStr = lazyAMConfigPath;
    this.inCLIMode = inCLIMode;
    this.tmpAppDirPath = tmpAppDirPath;
    this.user = UserGroupInformation.getCurrentUser().getShortUserName();
  }

  @Override
  public void init(Configuration conf) {
    executorService = Executors.newCachedThreadPool();
    this.conf = conf;
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    super.init(conf);
  }

  @Override
  public void start() {
    if (!inCLIMode) {
      return;
    }
    Configuration lazyAMConfToUpload = new LazyAMConfig();
    File lazyAmConfFile = new File(tmpAppDirPath,
        LazyAMConfig.LAZY_AM_JOB_XML_FILE);
    LOG.info("Writing contents of LazyAMConfig to local fs"
        + ", path=" + lazyAmConfFile.getAbsolutePath());
    lazyAmConfFile.getParentFile().mkdirs();
    try {
      lazyAMConfToUpload.writeXml(new FileOutputStream(lazyAmConfFile));

      String stagingDir = conf.get(AMPoolConfiguration.AM_STAGING_DIR,
          AMPoolConfiguration.DEFAULT_AM_STAGING_DIR)
          + Path.SEPARATOR + user + Path.SEPARATOR;

      lazyAmConfPathOnDfs = new Path(stagingDir,
          LazyAMConfig.LAZY_AM_JOB_XML_FILE);
      this.lazyAMConfigPathStr = lazyAmConfPathOnDfs.toString();

      FileSystem fs = FileSystem.get(conf);
      LOG.info("Uploading contents of LazyAMConfig to dfs"
          + ", srcpath=" + lazyAmConfFile.getAbsolutePath()
          + ", destpath=" + lazyAmConfPathOnDfs.toString());
      fs.copyFromLocalFile(false, true,
          new Path(lazyAmConfFile.getAbsolutePath()), lazyAmConfPathOnDfs);
      fs.getFileStatus(lazyAmConfPathOnDfs);
    } catch (IOException e) {
      throw new RuntimeException("Failed to setup lazy am config in DFS", e);
    }
  }

  @Override public void stop() {
    executorService.shutdownNow();
    if (inCLIMode && fs != null
        && lazyAmConfPathOnDfs != null) {
      try {
        fs.delete(lazyAmConfPathOnDfs, true);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public synchronized void launchAM() {
    LOG.info("Launching new AM");
    executorService.execute(
        new MRAMLauncherThread(dispatcher, yarnClient, conf));
  }

  private class MRAMLauncherThread implements Runnable {

    private final Dispatcher dispatcher;
    private final YarnClient yarnClient;
    private final Configuration conf;

    public MRAMLauncherThread(Dispatcher dispatcher,
        YarnClient yarnClient,
        Configuration conf) {
      this.dispatcher = dispatcher;
      this.yarnClient = yarnClient;
      this.conf = conf;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      ApplicationId newAppId = null;
      try {
        newAppId = yarnClient.getNewApplication().getApplicationId();
          ApplicationSubmissionContext submissionContext =
              createApplicationSubmissionContext(newAppId, conf);
          yarnClient.submitApplication(submissionContext);
          LOG.info("Launched new MR AM"
              + ", applicationId=" + newAppId);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      if (newAppId != null) {
        AMLaunchedEvent event = new AMLaunchedEvent(newAppId);
        this.dispatcher.getEventHandler().handle(event);
      }
    }
  }

  private LocalResource createApplicationResource(FileContext fs,
      Path p, LocalResourceType type) throws IOException {
    LocalResource rsrc = Records.newRecord(LocalResource.class);
    FileStatus rsrcStat = fs.getFileStatus(p);
    rsrc.setResource(ConverterUtils.getYarnUrlFromPath(fs
        .getDefaultFileSystem().resolvePath(rsrcStat.getPath())));
    rsrc.setSize(rsrcStat.getLen());
    rsrc.setTimestamp(rsrcStat.getModificationTime());
    rsrc.setType(type);
    rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    return rsrc;
  }

  public ApplicationSubmissionContext createApplicationSubmissionContext(
      ApplicationId applicationId,
      Configuration conf) throws IOException {

    // Setup resource requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(
        conf.getInt(
            AMPoolConfiguration.MR_AM_MEMORY_ALLOCATION_MB,
            AMPoolConfiguration.DEFAULT_MR_AM_MEMORY_ALLOCATION_MB
            )
        );
    // TODO for now use default of 1 for virtual cores
    capability.setVirtualCores(
        conf.getInt(
            MRJobConfig.MR_AM_CPU_VCORES, MRJobConfig.DEFAULT_MR_AM_CPU_VCORES
            )
        );
    LOG.info("LazyMRMAM capability = " + capability);

    // Setup LocalResources
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();

    if (lazyAMConfigPathStr == null
        || lazyAMConfigPathStr.isEmpty()) {
      LOG.error("Invalid path to lazy am config provided");
      // TODO throw error?
    } else {
      LOG.info("Using lazyAMConf as a local resource for LazyAM"
          + ", confPath=" + lazyAMConfigPathStr);
      Path lazyAMConfPath = new Path(lazyAMConfigPathStr);
      localResources.put(LazyAMConfig.LAZY_AM_JOB_XML_FILE,
          createApplicationResource(defaultFileContext, lazyAMConfPath,
              LocalResourceType.FILE));
    }

    String[] jarPaths = conf.getStrings(AMPoolConfiguration.MR_AM_JOB_JAR_PATH,
        AMPoolConfiguration.DEFAULT_MR_AM_JOB_JAR_PATH);
    if (jarPaths.length == 1) {
      String jarPath = jarPaths[0];
      if (jarPath != null) {
        jarPath = jarPath.trim();
        if (!jarPath.isEmpty()) {
          Path jobJarPath = new Path(jarPath);
          LocalResource rc = createApplicationResource(defaultFileContext,
              jobJarPath, LocalResourceType.FILE);
          localResources.put(MRJobConfig.JOB_JAR, rc);
        }
      }
    } else {
      for (String jarPath : jarPaths) {
        jarPath = jarPath.trim();
        if (jarPath == null || jarPath.isEmpty()) {
          continue;
        }
        Path jobJarPath = new Path(jarPath);
        String jarName = jobJarPath.getName();
        LocalResource rc = createApplicationResource(defaultFileContext,
            jobJarPath, LocalResourceType.FILE);
        localResources.put(jarName, rc);
      }
    }

    // Setup the command to run the AM
    List<String> vargs = new ArrayList<String>(8);
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");

    // TODO: why do we use 'conf' some places and 'jobConf' others?
    long logSize = TaskLog.getTaskLogLength(new JobConf(conf));
    String logLevel = conf.get(
        MRJobConfig.MR_AM_LOG_LEVEL, MRJobConfig.DEFAULT_MR_AM_LOG_LEVEL);
    MRApps.addLog4jSystemProperties(logLevel, logSize, vargs);

    // Add AM admin command opts before user command opts
    // so that it can be overridden by user
    vargs.add(conf.get(MRJobConfig.MR_AM_ADMIN_COMMAND_OPTS,
        MRJobConfig.DEFAULT_MR_AM_ADMIN_COMMAND_OPTS));

    vargs.add(conf.get(MRJobConfig.MR_AM_COMMAND_OPTS,
        MRJobConfig.DEFAULT_MR_AM_COMMAND_OPTS));

    vargs.add(conf.get(AMPoolConfiguration.APPLICATION_MASTER_CLASS,
        AMPoolConfiguration.DEFAULT_APPLICATION_MASTER_CLASS));
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
        Path.SEPARATOR + ApplicationConstants.STDOUT);
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
        Path.SEPARATOR + ApplicationConstants.STDERR);


    Vector<String> vargsFinal = new Vector<String>(8);
    // Final command
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    vargsFinal.add(mergedCommand.toString());

    LOG.debug("Command to launch container for ApplicationMaster is : "
        + mergedCommand);

    // Setup the CLASSPATH in environment
    // i.e. add { Hadoop jars, job jar, CWD } to classpath.
    Map<String, String> environment = new HashMap<String, String>();
    MRApps.setClasspath(environment, conf);

    // Setup the environment variables (LD_LIBRARY_PATH, etc)
    MRApps.setEnvFromInputString(environment,
        conf.get(MRJobConfig.MR_AM_ENV));

    environment.put(AMPoolConfiguration.LAZY_AM_POLLING_URL_ENV, pollingUrl);

    // TODO distributed cache ?
    // TODO acls ?

    // Setup ContainerLaunchContext for AM container
    ContainerLaunchContext amContainer = BuilderUtils
        .newContainerLaunchContext(null, UserGroupInformation
            .getCurrentUser().getShortUserName(), capability, localResources,
            environment, vargsFinal, null, null, null);

    // Set up the ApplicationSubmissionContext
    ApplicationSubmissionContext appContext =
       Records.newRecord(ApplicationSubmissionContext.class);
    appContext.setApplicationId(applicationId);
    appContext.setUser(
        UserGroupInformation.getCurrentUser().getShortUserName());

    appContext.setQueue(
        conf.get(AMPoolConfiguration.MR_AM_QUEUE_NAME,
        YarnConfiguration.DEFAULT_QUEUE_NAME));
    appContext.setApplicationName("MRAMLaunchedbyAMPoolService");
    appContext.setCancelTokensWhenComplete(
        conf.getBoolean(MRJobConfig.JOB_CANCEL_DELEGATION_TOKEN, true));
    appContext.setAMContainerSpec(amContainer);

    return appContext;
  }

}
