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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.tez.ampool.manager.AMFinishedEvent;

public class AMMonitorService extends AbstractService
  implements EventHandler<AMMonitorEvent> {

  private static final Log LOG = LogFactory.getLog(AMMonitorService.class);

  private final AMPoolContext context;
  private ScheduledExecutorService scheduler = null;
  Map<ApplicationId, ScheduledFuture<?>> scheduledFutures;
  private long heartbeatInterval = 1000l;

  public AMMonitorService(AMPoolContext context) {
    super(AMMonitorService.class.getName());
    // TODO Auto-generated constructor stub
    this.context = context;
    this.scheduledFutures = new HashMap<ApplicationId, ScheduledFuture<?>>();
  }

  @Override
  public void init(Configuration conf) {
    scheduler = Executors.newScheduledThreadPool(
        conf.getInt(AMPoolConfiguration.MAX_AM_POOL_SIZE,
            AMPoolConfiguration.DEFAULT_MAX_AM_POOL_SIZE));

    super.init(conf);
  }

  @Override
  public void start() {
    LOG.info("Starting AMMonitorService");
    super.start();
  }

  @Override
  public synchronized void stop() {
    LOG.info("Stopping AMMonitorService");
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }

  private synchronized void addAppToMonitor(ApplicationId applicationId) {
    LOG.info("Starting to monitor application"
        + ", applicationId=" + applicationId);
    AMMonitorTask amMonitorTask =
        new AMMonitorTask(applicationId);
    ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(
        amMonitorTask, heartbeatInterval, heartbeatInterval,
        TimeUnit.MILLISECONDS);
    scheduledFutures.put(applicationId, future);
  }

  private synchronized void removeAppFromMonitor(ApplicationId applicationId) {
    LOG.info("Stopping to monitor application"
        + ", applicationId=" + applicationId);
    if (!scheduledFutures.containsKey(applicationId)) {
      return;
    }
    scheduledFutures.get(applicationId).cancel(true);
    scheduledFutures.remove(applicationId);
  }

  private class AMMonitorTask implements Runnable {
    private final ApplicationId applicationId;

    public AMMonitorTask(ApplicationId applicationId) {
      this.applicationId = applicationId;
    }

    @SuppressWarnings("unchecked")
    public void run() {
      try {
        LOG.info("Trying to monitor status for application"
            + ", applicationId=" + applicationId);
        ApplicationReport report =
            context.getRMYarnClient().getApplicationReport(applicationId);
        if (report == null) {
          LOG.warn("Received null report from RM for"
              + " applicationId=" + applicationId);
          return;
        }
        LOG.info("Monitoring status for application"
            + ", applicationId=" + applicationId
            + ", yarnApplicationState=" + report.getYarnApplicationState()
            + ", finalApplicationStatus="
            + report.getFinalApplicationStatus());

        // TODO needs a more cleaner fix
        if (report.getYarnApplicationState() != null) {
          if (report.getYarnApplicationState()
                == YarnApplicationState.FAILED
              || report.getYarnApplicationState()
                == YarnApplicationState.KILLED
              || report.getYarnApplicationState()
                == YarnApplicationState.FINISHED) {
            if (report.getFinalApplicationStatus() ==
                FinalApplicationStatus.UNDEFINED) {
              report.setFinalApplicationStatus(FinalApplicationStatus.FAILED);
            }
          }
        }
        if (report.getFinalApplicationStatus() ==
            FinalApplicationStatus.FAILED
            || report.getFinalApplicationStatus() ==
            FinalApplicationStatus.SUCCEEDED
            || report.getFinalApplicationStatus() ==
            FinalApplicationStatus.KILLED) {
          AMFinishedEvent event = new AMFinishedEvent(applicationId,
              report.getFinalApplicationStatus());
          context.getDispatcher().getEventHandler().handle(event);
          AMMonitorEvent mEvent = new AMMonitorEvent(
              AMMonitorEventType.AM_MONITOR_STOP, applicationId);
          context.getDispatcher().getEventHandler().handle(mEvent);
        }
      } catch (YarnRemoteException e) {
        // TODO Auto-generated catch block
        LOG.error("Failed to acquire monitoring status for application"
            + ", applicationId=" + applicationId);
        e.printStackTrace();
      }
    }

  }

  @Override
  public synchronized void handle(AMMonitorEvent event) {
    if (event.getType() == AMMonitorEventType.AM_MONITOR_START) {
      addAppToMonitor(event.getApplicationId());
    } else if (event.getType() == AMMonitorEventType.AM_MONITOR_STOP) {
      removeAppFromMonitor(event.getApplicationId());
    }
  }

}
