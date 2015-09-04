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

package org.apache.tez.dag.history.logging.ats;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.tez.dag.history.events.DAGRecoveredEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.security.HistoryACLPolicyManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezReflectionException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.logging.HistoryLoggingService;
import org.apache.tez.dag.records.TezDAGID;

import com.google.common.annotations.VisibleForTesting;

public class ATSHistoryLoggingService extends HistoryLoggingService {

  private static final Logger LOG = LoggerFactory.getLogger(ATSHistoryLoggingService.class);

  private LinkedBlockingQueue<DAGHistoryEvent> eventQueue =
      new LinkedBlockingQueue<DAGHistoryEvent>();

  private Thread eventHandlingThread;
  private AtomicBoolean stopped = new AtomicBoolean(false);
  private int eventCounter = 0;
  private int eventsProcessed = 0;
  private final Object lock = new Object();
  private boolean historyLoggingEnabled = true;

  @VisibleForTesting
  TimelineClient timelineClient;

  private HashSet<TezDAGID> skippedDAGs = new HashSet<TezDAGID>();
  private Map<TezDAGID, String> dagDomainIdMap = new HashMap<TezDAGID, String>();
  private long maxTimeToWaitOnShutdown;
  private boolean waitForeverOnShutdown = false;

  private int maxEventsPerBatch;
  private long maxPollingTimeMillis;

  private String sessionDomainId;
  private static final String atsHistoryLoggingServiceClassName =
      "org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService";
  private static final String atsHistoryACLManagerClassName =
      "org.apache.tez.dag.history.ats.acls.ATSHistoryACLPolicyManager";
  private HistoryACLPolicyManager historyACLPolicyManager;

  public ATSHistoryLoggingService() {
    super(ATSHistoryLoggingService.class.getName());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    historyLoggingEnabled = conf.getBoolean(TezConfiguration.TEZ_AM_HISTORY_LOGGING_ENABLED,
        TezConfiguration.TEZ_AM_HISTORY_LOGGING_ENABLED_DEFAULT);
    if (!historyLoggingEnabled) {
      LOG.info("ATSService: History Logging disabled. "
          + TezConfiguration.TEZ_AM_HISTORY_LOGGING_ENABLED + " set to false");
      return;
    }
    LOG.info("Initializing ATSService");

    if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
      YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
      timelineClient = TimelineClient.createTimelineClient();
      timelineClient.init(conf);
    } else {
      this.timelineClient = null;
      if (conf.get(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS, "")
        .equals(atsHistoryLoggingServiceClassName)) {
        LOG.warn(atsHistoryLoggingServiceClassName
            + " is disabled due to Timeline Service being disabled, "
            + YarnConfiguration.TIMELINE_SERVICE_ENABLED + " set to false");
      }
    }
    maxTimeToWaitOnShutdown = conf.getLong(
        TezConfiguration.YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS,
        TezConfiguration.YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS_DEFAULT);
    maxEventsPerBatch = conf.getInt(
        TezConfiguration.YARN_ATS_MAX_EVENTS_PER_BATCH,
        TezConfiguration.YARN_ATS_MAX_EVENTS_PER_BATCH_DEFAULT);
    maxPollingTimeMillis = conf.getInt(
        TezConfiguration.YARN_ATS_MAX_POLLING_TIME_PER_EVENT,
        TezConfiguration.YARN_ATS_MAX_POLLING_TIME_PER_EVENT_DEFAULT);
    if (maxTimeToWaitOnShutdown < 0) {
      waitForeverOnShutdown = true;
    }
    sessionDomainId = conf.get(TezConfiguration.YARN_ATS_ACL_SESSION_DOMAIN_ID);

    LOG.info("Using " + atsHistoryACLManagerClassName + " to manage Timeline ACLs");
    try {
      historyACLPolicyManager = ReflectionUtils.createClazzInstance(
          atsHistoryACLManagerClassName);
      historyACLPolicyManager.setConf(conf);
    } catch (TezReflectionException e) {
      LOG.warn("Could not instantiate object for " + atsHistoryACLManagerClassName
          + ". ACLs cannot be enforced correctly for history data in Timeline", e);
      if (!conf.getBoolean(TezConfiguration.TEZ_AM_ALLOW_DISABLED_TIMELINE_DOMAINS,
          TezConfiguration.TEZ_AM_ALLOW_DISABLED_TIMELINE_DOMAINS_DEFAULT)) {
        throw e;
      }
      historyACLPolicyManager = null;
    }

  }

  @Override
  public void serviceStart() {
    if (!historyLoggingEnabled || timelineClient == null) {
      return;
    }
    LOG.info("Starting ATSService");
    timelineClient.start();

    eventHandlingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        List<DAGHistoryEvent> events = new LinkedList<DAGHistoryEvent>();
        boolean interrupted = false;
        while (!stopped.get() && !Thread.currentThread().isInterrupted()
              && !interrupted) {

          // Log the size of the event-queue every so often.
          if (eventCounter != 0 && eventCounter % 1000 == 0) {
            if (eventsProcessed != 0 && !events.isEmpty()) {
              LOG.info("Event queue stats"
                  + ", eventsProcessedSinceLastUpdate=" + eventsProcessed
                  + ", eventQueueSize=" + eventQueue.size());
            }
            eventCounter = 0;
            eventsProcessed = 0;
          } else {
            ++eventCounter;
          }

          synchronized (lock) {
            try {
              getEventBatch(events);
            } catch (InterruptedException e) {
              // Finish processing events and then return
              interrupted = true;
            }

            if (events.isEmpty()) {
              continue;
            }

            eventsProcessed += events.size();
            try {
              handleEvents(events);
            } catch (Exception e) {
              LOG.warn("Error handling events", e);
            }
          }
        }
      }
    }, "HistoryEventHandlingThread");
    eventHandlingThread.start();
  }

  @Override
  public void serviceStop() {
    if (!historyLoggingEnabled || timelineClient == null) {
      return;
    }
    LOG.info("Stopping ATSService"
        + ", eventQueueBacklog=" + eventQueue.size());
    stopped.set(true);
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }
    synchronized (lock) {
      if (!eventQueue.isEmpty()) {
        LOG.warn("ATSService being stopped"
            + ", eventQueueBacklog=" + eventQueue.size()
            + ", maxTimeLeftToFlush=" + maxTimeToWaitOnShutdown
            + ", waitForever=" + waitForeverOnShutdown);
        long startTime = appContext.getClock().getTime();
        long endTime = startTime + maxTimeToWaitOnShutdown;
        List<DAGHistoryEvent> events = new LinkedList<DAGHistoryEvent>();
        while (waitForeverOnShutdown || (endTime >= appContext.getClock().getTime())) {
          try {
            getEventBatch(events);
          } catch (InterruptedException e) {
            LOG.info("ATSService interrupted while shutting down. Exiting."
                  + " EventQueueBacklog=" + eventQueue.size());
          }
          if (events.isEmpty()) {
            LOG.info("Event queue empty, stopping ATS Service");
            break;
          }
          try {
            handleEvents(events);
          } catch (Exception e) {
            LOG.warn("Error handling event", e);
            break;
          }
        }
      }
    }
    if (!eventQueue.isEmpty()) {
      LOG.warn("Did not finish flushing eventQueue before stopping ATSService"
          + ", eventQueueBacklog=" + eventQueue.size());
    }
    timelineClient.stop();
  }

  private void getEventBatch(List<DAGHistoryEvent> events) throws InterruptedException {
    events.clear();
    int counter = 0;
    while (counter < maxEventsPerBatch) {
      DAGHistoryEvent event = eventQueue.poll(maxPollingTimeMillis, TimeUnit.MILLISECONDS);
      if (event == null) {
        break;
      }
      if (!isValidEvent(event)) {
        continue;
      }
      ++counter;
      events.add(event);
      if (event.getHistoryEvent().getEventType().equals(HistoryEventType.DAG_SUBMITTED)) {
        // Special case this as it might be a large payload
        break;
      }
    }
  }


  public void handle(DAGHistoryEvent event) {
    if (historyLoggingEnabled && timelineClient != null) {
      eventQueue.add(event);
    }
  }

  private boolean isValidEvent(DAGHistoryEvent event) {
    HistoryEventType eventType = event.getHistoryEvent().getEventType();
    TezDAGID dagId = event.getDagID();

    if (eventType.equals(HistoryEventType.DAG_SUBMITTED)) {
      DAGSubmittedEvent dagSubmittedEvent =
          (DAGSubmittedEvent) event.getHistoryEvent();
      String dagName = dagSubmittedEvent.getDAGName();
      if ((dagName != null
          && dagName.startsWith(TezConstants.TEZ_PREWARM_DAG_NAME_PREFIX))
          || (!dagSubmittedEvent.isHistoryLoggingEnabled())) {
        // Skip recording pre-warm DAG events
        skippedDAGs.add(dagId);
        return false;
      }
      if (historyACLPolicyManager != null) {
        String dagDomainId = dagSubmittedEvent.getConf().get(
            TezConfiguration.YARN_ATS_ACL_DAG_DOMAIN_ID);
        if (dagDomainId != null) {
          dagDomainIdMap.put(dagId, dagDomainId);
        }
      }
    }
    if (eventType.equals(HistoryEventType.DAG_RECOVERED)) {
      DAGRecoveredEvent dagRecoveredEvent = (DAGRecoveredEvent) event.getHistoryEvent();
      if (!dagRecoveredEvent.isHistoryLoggingEnabled()) {
        skippedDAGs.add(dagRecoveredEvent.getDagID());
        return false;
      }
    }
    if (eventType.equals(HistoryEventType.DAG_FINISHED)) {
      // Remove from set to keep size small
      // No more events should be seen after this point.
      if (skippedDAGs.remove(dagId)) {
        return false;
      }
    }

    if (dagId != null && skippedDAGs.contains(dagId)) {
      // Skip pre-warm DAGs
      return false;
    }

    return true;
  }

  private void handleEvents(List<DAGHistoryEvent> events) {
    TimelineEntity[] entities = new TimelineEntity[events.size()];
    for (int i = 0; i < events.size(); ++i) {
      DAGHistoryEvent event = events.get(i);
      String domainId = sessionDomainId;
      TezDAGID dagId = event.getDagID();

      if (historyACLPolicyManager != null && dagId != null) {
        if (dagDomainIdMap.containsKey(dagId)) {
          domainId = dagDomainIdMap.get(dagId);
        }
      }

      entities[i] = HistoryEventTimelineConversion.convertToTimelineEntity(event.getHistoryEvent());
      if (historyACLPolicyManager != null) {
        if (domainId != null && !domainId.isEmpty()) {
          historyACLPolicyManager.updateTimelineEntityDomain(entities[i], domainId);
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending event batch to Timeline, batchSize=" + events.size());
    }
    try {
      TimelinePutResponse response =
          timelineClient.putEntities(entities);
      if (response != null
        && !response.getErrors().isEmpty()) {
        int count = response.getErrors().size();
        for (int i = 0; i < count; ++i) {
          TimelinePutError err = response.getErrors().get(i);
          if (err.getErrorCode() != 0) {
            LOG.warn("Could not post history event to ATS"
                + ", atsPutError=" + err.getErrorCode()
                + ", entityId=" + err.getEntityId());
          }
        }
      }
      // Do nothing additional, ATS client library should handle throttling
      // or auto-disable as needed
    } catch (Exception e) {
      LOG.warn("Could not handle history events", e);
    }
  }

}
