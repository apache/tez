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

package org.apache.tez.ampool;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.client.AMRMClient;
import org.apache.hadoop.yarn.client.AMRMClientImpl;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.ampool.manager.AMPoolEventType;
import org.apache.tez.ampool.manager.AMPoolManager;
import org.apache.tez.ampool.rest.AMPoolStatusService;
import org.apache.tez.ampool.rest.ApplicationPollService;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import com.sun.jersey.spi.container.servlet.ServletContainer;

public class AMPoolService extends CompositeService {

  private static final Log LOG = LogFactory.getLog(AMPoolService.class);

  private final long startTime = System.currentTimeMillis();
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private AMPoolContext context;
  private AMPoolClientRMProxy clientRMProxy;
  private AMPoolManager amPoolManager;
  private RMHeartbeatService rmHeartbeatService;
  private AMMonitorService amMonitorService;
  private Server webAppServer = null;
  private Configuration conf;

  private YarnClient yarnRMClient;
  private AMRMClient amRMClient;

  private Dispatcher dispatcher;

  private final ApplicationAttemptId applicationAttemptId;
  private final ContainerId containerId;
  private final String nmHost;
  private final int nmPort;
  private int webAppServerPort;
  private String trackerUrl;

  private final boolean inCLIMode;

  public String tmpAppDirPath = null;

  private FileSystem localFs;

  private static final Options opts;

  static {
    opts = new Options();
    opts.addOption("cli", false, "Run via command-line in non-AM mode");
  }

  public AMPoolService(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort,
      boolean inCLIMode) {
    super(AMPoolService.class.getName());
    this.applicationAttemptId = applicationAttemptId;
    this.containerId = containerId;
    this.nmHost = nmHost;
    this.nmPort = nmPort;
    this.inCLIMode = inCLIMode;
  }

  public AMPoolService(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort) {
    this(applicationAttemptId, containerId, nmHost, nmPort, false);
    LOG.info("Created AMPoolService"
        + ", applicationAttemptId=" + applicationAttemptId
        + ", containerId=" + containerId
        + ", nmHost=" + nmHost
        + ", nmPort=" + nmPort
        + ", launchTime=" + startTime
        + ", inCLIMode=" + inCLIMode);
  }

  public AMPoolService(String localhost) {
    this(null, null, localhost, -1, true);
    LOG.info("Created AMPoolService"
        + ", host=" + nmHost
        + ", launchTime=" + startTime
        + ", inCLIMode=" + inCLIMode);
  }

  @Override
  public void init(final Configuration conf) {
    this.conf = conf;
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    dispatcher = new AsyncDispatcher();
    addIfService(dispatcher);

    yarnRMClient = new YarnClientImpl();
    addIfService(yarnRMClient);

    if (inCLIMode) {
      amRMClient = null;
    } else {
      amRMClient = new AMRMClientImpl(applicationAttemptId);
      addIfService(amRMClient);
    }

    tmpAppDirPath   = conf.get(AMPoolConfiguration.TMP_DIR_PATH,
        AMPoolConfiguration.DEFAULT_TMP_DIR_PATH)
      + File.separator;

    amPoolManager = new AMPoolManager(conf, dispatcher, yarnRMClient, nmHost,
        inCLIMode, tmpAppDirPath);
    dispatcher.register(AMPoolEventType.class, amPoolManager);
    addIfService(amPoolManager);

    context = new AMPoolContext(conf, nmHost, dispatcher,
        yarnRMClient, amRMClient, amPoolManager);

    amMonitorService = new AMMonitorService(context);
    dispatcher.register(AMMonitorEventType.class, amMonitorService);
    addIfService(amMonitorService);

    clientRMProxy = new AMPoolClientRMProxy(context);
    addIfService(clientRMProxy);

    webAppServerPort = conf.getInt(AMPoolConfiguration.WS_PORT,
        AMPoolConfiguration.DEFAULT_WS_PORT);
    trackerUrl = "http://" + nmHost
        + ":" + webAppServerPort
        + "/master/status";

    if (!inCLIMode) {
      rmHeartbeatService = new RMHeartbeatService(context, trackerUrl,
          webAppServerPort);
      addIfService(rmHeartbeatService);
    }

    webAppServer = new Server(webAppServerPort);
    webAppServer.setThreadPool(new QueuedThreadPool(50));
    webAppServer.setStopAtShutdown(true);

    ServletContextHandler root = new ServletContextHandler(webAppServer, "/");
    ServletHolder rootServlet = root.addServlet(DefaultServlet.class, "/");
    rootServlet.setInitOrder(1);

    ServletHolder restHandler = new ServletHolder(ServletContainer.class);
    restHandler.setInitParameter(
        "com.sun.jersey.config.property.resourceConfigClass",
        "com.sun.jersey.api.core.PackagesResourceConfig");
    restHandler.setInitParameter(
        "com.sun.jersey.config.property.packages",
        "org.apache.tez.ampool.rest");
    restHandler.setInitParameter("com.sun.jersey.api.json.POJOMappingFeature",
        "true");
    root.addServlet(restHandler, "/*");
    restHandler.setInitOrder(2);

    ApplicationPollService.init(amPoolManager);
    AMPoolStatusService.init(amPoolManager);

    super.init(conf);
  }

  @Override
  public void start() {
    if (!inCLIMode) {
      LOG.debug("Starting AMPoolService"
          + ", applicationAttemptId=" + applicationAttemptId
          + ", containerId=" + containerId
          + ", nmHost=" + nmHost
          + ", nmPort=" + nmPort
          + ", launchTime=" + startTime
          + ", startingTime=" + System.currentTimeMillis()
          + ", inCLIMode=" + inCLIMode);
    }
    super.start();
    try {
      webAppServer.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (!inCLIMode) {
      LOG.info("Started AMPoolService"
          + ", applicationAttemptId=" + applicationAttemptId
          + ", containerId=" + containerId
          + ", nmHost=" + nmHost
          + ", nmPort=" + nmPort
          + ", launchTime=" + startTime
          + ", startedTime=" + System.currentTimeMillis()
          + ", inCLIMode=" + inCLIMode);
    } else {
      LOG.info("Started AMPoolService. Use " + trackerUrl
          + " to monitor the AMPoolService");
    }
  }

  @Override
  public void stop() {
    if (webAppServer != null) {
      try {
        webAppServer.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    super.stop();
  }

  protected void addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
    }
  }

  private static void validateInputParam(String value, String param)
      throws IOException {
    if (value == null) {
      String msg = param + " is null";
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  static class AMPoolServiceShutdownHook implements Runnable {
    AMPoolService appMaster;
    AMPoolServiceShutdownHook(AMPoolService appMaster) {
      this.appMaster = appMaster;
    }
    public void run() {
      LOG.info("AMPoolService received a signal. Shutting down now.");
      Configuration conf = appMaster.conf;
      appMaster.stop();
      // TODO fix - this is a problem with multiple attempts of this AM
      // Client does not remain running till end of AM
      FileSystem fs;
      try {
        if (!appMaster.inCLIMode) {
          fs = FileSystem.get(conf);
          Path appDirOnDFS = new Path(
              fs.getHomeDirectory(),
              AMPoolConfiguration.APP_NAME + "/"
              + appMaster.applicationAttemptId.getApplicationId().toString()
              + "/");
          LOG.info("Deleting app dir on dfs"
              + ", path=" + appDirOnDFS.toString());
          fs.delete(appDirOnDFS, true);
        } else {
          if (appMaster.tmpAppDirPath != null) {
            LOG.info("Deleting working dir: " + appMaster.tmpAppDirPath);
            appMaster.localFs.delete(new Path(appMaster.tmpAppDirPath), true);
          }
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

    }
  }

  private static AMPoolService initNonCLIAM(Configuration conf)
      throws IOException {
    String containerIdStr =
        System.getenv(ApplicationConstants.AM_CONTAINER_ID_ENV);
    String nodeHostString = System.getenv(ApplicationConstants.NM_HOST_ENV);
    String nodePortString = System.getenv(ApplicationConstants.NM_PORT_ENV);

    validateInputParam(containerIdStr,
        ApplicationConstants.AM_CONTAINER_ID_ENV);
    validateInputParam(nodeHostString, ApplicationConstants.NM_HOST_ENV);
    validateInputParam(nodePortString, ApplicationConstants.NM_PORT_ENV);

    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    ApplicationAttemptId applicationAttemptId =
          containerId.getApplicationAttemptId();

    AMPoolService appMaster =
          new AMPoolService(applicationAttemptId, containerId, nodeHostString,
              Integer.parseInt(nodePortString));
    return appMaster;
  }

  public static void main(String[] args) throws IOException, ParseException {

    Configuration conf = new AMPoolConfiguration(new YarnConfiguration());
    conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

    CommandLine cliParser = new GnuParser().parse(opts, args);
    AMPoolService appMaster = null;

    String localhost = InetAddress.getLocalHost().getCanonicalHostName();

    if (!cliParser.hasOption("cli")) {
      appMaster = initNonCLIAM(conf);
    } else {
      appMaster = new AMPoolService(localhost);
    }

    ShutdownHookManager.get().addShutdownHook(
        new AMPoolServiceShutdownHook(appMaster), SHUTDOWN_HOOK_PRIORITY);

    appMaster.init(conf);
    appMaster.start();

  }
}
