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

package org.apache.tez.ampool.manager;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.tez.ampool.AMContext;
import org.apache.tez.ampool.AMLauncher;
import org.apache.tez.ampool.AMMonitorEvent;
import org.apache.tez.ampool.AMMonitorEventType;
import org.apache.tez.ampool.AMPoolConfiguration;
import org.apache.tez.ampool.mr.MRAMLauncher;

public class AMPoolManager extends CompositeService
    implements EventHandler<AMPoolEvent> {

  private static final Log LOG = LogFactory.getLog(AMPoolManager.class);

  private AMLauncher amLauncher;

  private final Dispatcher dispatcher;

  private int numAMs;

  private int maxNumAMs;

  private AtomicInteger pendingLaunches;
  private AtomicInteger failedLaunches;

  private ConcurrentHashMap<ApplicationId, AMContext> appContexts;

  private LinkedList<AMContext> unassignedAppContexts;

  private YarnClient yarnClient;
  private final String nmHost;

  private String pollingUrl;
  private boolean launchAMOnCompletion = true;
  private int maxLaunchFailures = 100;
  private final String lazyAMConfigPath;
  private final String tmpAppDirPath;
  private final boolean inCLIMode;

  public AMPoolManager(Configuration conf, Dispatcher dispatcher, YarnClient yarnClient,
      String nmHost, boolean inCLIMode, String tmpAppDirPath) {
    super(AMPoolManager.class.getName());
    this.dispatcher = dispatcher;
    this.nmHost = nmHost;
    appContexts = new ConcurrentHashMap<ApplicationId, AMContext>();
    unassignedAppContexts = new LinkedList<AMContext>();
    pendingLaunches = new AtomicInteger();
    failedLaunches = new AtomicInteger();
    this.yarnClient = yarnClient;
    this.lazyAMConfigPath = conf.get(
        AMPoolConfiguration.LAZY_AM_CONF_FILE_PATH);
    this.inCLIMode = inCLIMode;
    this.tmpAppDirPath = tmpAppDirPath;
  }

  @Override
  public void init(Configuration conf) {
    LOG.info("Initializing AMPoolManager");
    numAMs = conf.getInt(AMPoolConfiguration.AM_POOL_SIZE,
        AMPoolConfiguration.DEFAULT_AM_POOL_SIZE);
    maxNumAMs = conf.getInt(AMPoolConfiguration.MAX_AM_POOL_SIZE,
        AMPoolConfiguration.DEFAULT_MAX_AM_POOL_SIZE);
    launchAMOnCompletion = conf.getBoolean(
        AMPoolConfiguration.AM_LAUNCH_NEW_AM_AFTER_APP_COMPLETION,
        AMPoolConfiguration.DEFAULT_AM_LAUNCH_NEW_AM_AFTER_APP_COMPLETION);
    maxLaunchFailures = conf.getInt(
        AMPoolConfiguration.MAX_AM_LAUNCH_FAILURES,
        AMPoolConfiguration.DEFAULT_MAX_AM_LAUNCH_FAILURES);

    this.pollingUrl = "http://" + nmHost
        + ":" + conf.getInt(AMPoolConfiguration.WS_PORT,
            AMPoolConfiguration.DEFAULT_WS_PORT)
        + "/applications/poll/";

    LOG.info("Initializing AMPoolManager"
        + ", poolSize=" + numAMs
        + ", maxPoolSize=" + maxNumAMs
        + ", launchAMOnCompletion=" + launchAMOnCompletion
        + ", pollingUrlForAMs=" + pollingUrl
        + ", lazyMRAMConfig=" + this.lazyAMConfigPath);

    if (numAMs <= 0
        || maxNumAMs <= 0
        || numAMs > maxNumAMs
        || maxLaunchFailures < 0) {
      throw new IllegalArgumentException("Invalid configuration values"
          + " specified"
          + ", poolSize=" + numAMs
          + ", maxPoolSize=" + maxNumAMs
          + ", maxLaunchFailures" + maxLaunchFailures);
    }

    try {
      amLauncher = new MRAMLauncher(conf, dispatcher, yarnClient,
          pollingUrl, lazyAMConfigPath, inCLIMode, tmpAppDirPath);
      if (amLauncher instanceof Service) {
        addService((Service) amLauncher);
      }
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException("Could not initialize launcher");
    }
    super.init(conf);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void start() {
    LOG.info("Starting AMPoolManager");
    super.start();
    for (int i = 0; i < numAMs; ++i) {
      AMPoolEvent e = new AMPoolEvent(AMPoolEventType.LAUNCH_AM);
      this.dispatcher.getEventHandler().handle(e);
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping AMPoolManager");
    super.stop();
  }

  @SuppressWarnings("unchecked")
  public synchronized ApplicationId getAM() {
    if (unassignedAppContexts.isEmpty()) {
      LOG.warn("Did not find any unassigned AMs to allocate for new job"
          + ", pendingLaunchCount=" + pendingLaunches.get()
          + ", currentApplicationCount=" + appContexts.size()
          + ", unassignedAMsCount=" + unassignedAppContexts.size());
      return null;
    }
    AMContext popped = unassignedAppContexts.removeFirst();
    LOG.info("Assigning new application id to new application"
        + ", applicationId=" + popped.getApplicationId());
    appContexts.put(popped.getApplicationId(), popped);
    if (!launchAMOnCompletion) {
      LOG.info("Launching new AM as assigned app to new application"
        + ", assignedAppId=" + popped.getApplicationId());
      AMPoolEvent e = new AMPoolEvent(AMPoolEventType.LAUNCH_AM);
      this.dispatcher.getEventHandler().handle(e);
    }
    return popped.getApplicationId();
  }

  public synchronized void killAM(ApplicationId applicationId) {
    // TODO Auto-generated method stub
  }

  public synchronized void submitApplication(SubmitApplicationRequest request) {
    LOG.info("Received an ApplicationSubmissionContext from client for"
        + " application="
        + request.getApplicationSubmissionContext().getApplicationId()
            .toString());
    AMContext amContext = appContexts.get(
        request.getApplicationSubmissionContext().getApplicationId());
    amContext.setSubmissionContext(request.getApplicationSubmissionContext());
    amContext.setApplicationSubmissionTime(System.currentTimeMillis());
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized void handle(AMPoolEvent event) {
    switch (event.getType()) {
      case LAUNCH_AM:
        // TODO
        if ((pendingLaunches.get() +
            appContexts.size() +
            unassignedAppContexts.size()) >= maxNumAMs) {
          LOG.info("Hitting AM pool limits, ignoring launch event"
              + ", pendingLaunchCount=" + pendingLaunches.get()
              + ", currentApplicationCount=" + appContexts.size()
              + ", unassignedAMsCount=" + unassignedAppContexts.size());
          break;
        }
        pendingLaunches.incrementAndGet();
        amLauncher.launchAM();
        break;
      case AM_FINISHED:
        // TODO
        // if unassigned we should launch a new AM
        AMFinishedEvent fEvent = (AMFinishedEvent) event;
        ApplicationId appId = fEvent.getApplicationId();
        boolean isManaged = isManagedApp(appId);
        LOG.info("Received an AM finished event for application"
            + ", application=" + appId
            + ", isManaged=" + isManaged);
        if (!isManaged) {
          if (!fEvent.getFinalApplicationStatus().equals(
              FinalApplicationStatus.SUCCEEDED)) {
            int failures = failedLaunches.incrementAndGet();
            LOG.info("Unassigned AM failed."
                + ", totalFailedCount=" + failures);
            if (failures > maxLaunchFailures) {
              throw new RuntimeException("Getting too many failed launches"
                  + ", failure count exceeded " + maxLaunchFailures
                  + ", exiting");
            }
          }
        }
        removeAppId(appId);
        int currentAMCount = pendingLaunches.get() +
            appContexts.size() +
            unassignedAppContexts.size();
        if (launchAMOnCompletion
            || !isManaged
            || currentAMCount < numAMs) {
          LOG.info("Launching new AM on receiving finish event"
              + ", finishedAppId=" + appId
              + ", launchOnCompletion=" + launchAMOnCompletion
              + ", finishedAMWasManaged=" + isManaged
              + ", currentAMCount=" + currentAMCount
              + ", minAMCount=" + numAMs);
          AMPoolEvent launchAMEvent =
              new AMPoolEvent(AMPoolEventType.LAUNCH_AM);
          this.dispatcher.getEventHandler().handle(launchAMEvent);
        }
        break;
      case AM_LAUNCHED:
        AMLaunchedEvent e = (AMLaunchedEvent) event;
        AMContext amContext = new AMContext(e.getApplicationId());
        unassignedAppContexts.add(amContext);
        int currentCount = pendingLaunches.decrementAndGet();
        LOG.info("AM launched for applicationId="
            + e.getApplicationId()
            + ", currentPending=" + currentCount);
        AMMonitorEvent mEvent = new AMMonitorEvent(
            AMMonitorEventType.AM_MONITOR_START,
            e.getApplicationId());
        this.dispatcher.getEventHandler().handle(mEvent);
    }

  }

  public synchronized boolean isManagedApp(ApplicationId applicationId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Managed app contexts size=" + appContexts.size());
      for (ApplicationId appId : appContexts.keySet()) {
        LOG.debug("Dumping appContexts, appId=" + appId.toString());
      }
    }
    return appContexts.containsKey(applicationId);
  }

  public synchronized AMContext getAMContext(
      ApplicationAttemptId applicationAttemptId) {
    LOG.info("Received a getSubmissionContext request from"
        + " applicationAttemptId=" + applicationAttemptId.toString());
    if (appContexts.containsKey(applicationAttemptId.getApplicationId())) {
      AMContext amContext = appContexts.get(
          applicationAttemptId.getApplicationId());
      LOG.info("Received a getSubmissionContext request from"
          + " applicationAttemptId=" + applicationAttemptId.toString()
          + ", assigning job");
      amContext.setCurrentApplicationAttemptId(applicationAttemptId);
      if (amContext.getSubmissionContext() != null) {
        amContext.setJobPickUpTime(System.currentTimeMillis());
      }
      return amContext;
    }
    boolean found = false;
    for (AMContext amContext : unassignedAppContexts) {
      if (amContext.getApplicationId().equals(
          applicationAttemptId.getApplicationId())) {
        found = true;
      }
    }
    if (!found) {
      throw new RuntimeException("Could not find application id");
    }
    LOG.info("Received a getSubmissionContext request from"
        + " applicationAttemptId=" + applicationAttemptId.toString()
        + ", no job to assign");
    return null;
  }

  public int getPendingLaunches() {
    return pendingLaunches.get();
  }

  public int getFailedLaunches() {
    return failedLaunches.get();
  }

  public synchronized List<AMContext> getUnassignedApplications() {
    return Collections.unmodifiableList(unassignedAppContexts);
  }

  public synchronized Map<ApplicationId, AMContext> getApplicationsDump() {
    return Collections.unmodifiableMap(appContexts);
  }

  private synchronized void removeAppId(ApplicationId appId) {
    appContexts.remove(appId);
    for (int i = 0; i < unassignedAppContexts.size(); ++i) {
      AMContext amContext = unassignedAppContexts.get(i);
      if (amContext.getApplicationId().equals(appId)) {
        unassignedAppContexts.remove(i);
        break;
      }
    }
  }

  public synchronized AMContext getAMContext(ApplicationId applicationId) {
    return appContexts.get(applicationId);
  }

}
