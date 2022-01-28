/*
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

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.tez.common.security.DAGAccessControls;
import org.apache.tez.common.security.HistoryACLPolicyException;
import org.apache.tez.common.security.HistoryACLPolicyManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezReflectionException;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.logging.HistoryLoggingService;
import org.apache.tez.dag.records.TezDAGID;

import com.google.common.annotations.VisibleForTesting;

public class ATSHistoryLoggingService extends HistoryLoggingService {

  private static final Logger LOG = LoggerFactory.getLogger(ATSHistoryLoggingService.class);

  @VisibleForTesting
  LinkedBlockingQueue<DAGHistoryEvent> eventQueue = new LinkedBlockingQueue<DAGHistoryEvent>();

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

  @VisibleForTesting
  HistoryACLPolicyManager historyACLPolicyManager;

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

    LOG.info("Initializing " + ATSHistoryLoggingService.class.getSimpleName() + " with "
      + "maxEventsPerBatch=" + maxEventsPerBatch
      + ", maxPollingTime(ms)=" + maxPollingTimeMillis
      + ", waitTimeForShutdown(ms)=" + maxTimeToWaitOnShutdown
      + ", TimelineACLManagerClass=" + atsHistoryACLManagerClassName);

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

    timelineClient.start();

    try {
      sessionDomainId = createSessionDomain();
    } catch (HistoryACLPolicyException | IOException e) {
      LOG.warn("Could not setup history acls, disabling history logging.", e);
      historyLoggingEnabled = false;
      return;
    }

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
    if (timelineClient != null) {
      timelineClient.stop();
    }
    if (historyACLPolicyManager != null) {
      historyACLPolicyManager.close();
    }
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
    TezDAGID dagId = event.getDAGID();

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
    List<TimelineEntity> entities = new ArrayList<>(events.size());
    for (DAGHistoryEvent event : events) {
      String domainId = getDomainForEvent(event);
      // skippedDags is updated in the above call so check again.
      if (event.getDAGID() != null && skippedDAGs.contains(event.getDAGID())) {
        continue;
      }
      List<TimelineEntity> eventEntities = HistoryEventTimelineConversion.convertToTimelineEntities(
          event.getHistoryEvent());
      entities.addAll(eventEntities);
      if (historyACLPolicyManager != null && domainId != null && !domainId.isEmpty()) {
        for (TimelineEntity entity: eventEntities) {
          historyACLPolicyManager.updateTimelineEntityDomain(entity, domainId);
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending event batch to Timeline, batchSize=" + events.size());
    }
    try {
      TimelinePutResponse response =
          timelineClient.putEntities(entities.toArray(new TimelineEntity[entities.size()]));
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

  private String getDomainForEvent(DAGHistoryEvent event) {
    String domainId = sessionDomainId;
    if (historyACLPolicyManager == null) {
      return domainId;
    }

    TezDAGID dagId = event.getDAGID();
    HistoryEvent historyEvent = event.getHistoryEvent();
    if (dagId == null || !HistoryEventType.isDAGSpecificEvent(historyEvent.getEventType())) {
      return domainId;
    }

    if (dagDomainIdMap.containsKey(dagId)) {
      // If we already have the domain for the dag id return it
      domainId = dagDomainIdMap.get(dagId);
      // Cleanup if this is the last event.
      if (historyEvent.getEventType() == HistoryEventType.DAG_FINISHED) {
        dagDomainIdMap.remove(dagId);
      }
    } else if (HistoryEventType.DAG_SUBMITTED == historyEvent.getEventType()
        || HistoryEventType.DAG_RECOVERED == historyEvent.getEventType()) {
      // In case this is the first event for the dag, create and populate dag domain.
      Configuration conf;
      DAGPlan dagPlan;
      if (HistoryEventType.DAG_SUBMITTED == historyEvent.getEventType()) {
          conf = ((DAGSubmittedEvent)historyEvent).getConf();
          dagPlan = ((DAGSubmittedEvent)historyEvent).getDAGPlan();
      } else {
         conf = appContext.getCurrentDAG().getConf();
         dagPlan = appContext.getCurrentDAG().getJobPlan();
      }
      domainId = createDagDomain(conf, dagPlan, dagId);

      // createDagDomain updates skippedDAGs so another check here.
      if (skippedDAGs.contains(dagId)) {
        return null;
      }

      dagDomainIdMap.put(dagId, domainId);
    }
    return domainId;
  }

  /**
   * Creates a domain for the session.
   * @return domainId to be used. null if acls are disabled.
   * @throws HistoryACLPolicyException, IOException Forward if historyACLPolicyManger exception.
   */
  private String createSessionDomain() throws HistoryACLPolicyException, IOException {
    if (historyACLPolicyManager == null) {
      return null;
    }
    Map<String, String> domainInfo = historyACLPolicyManager.setupSessionACLs(getConfig(),
        appContext.getApplicationID());
    if (domainInfo != null) {
      return domainInfo.get(TezConfiguration.YARN_ATS_ACL_SESSION_DOMAIN_ID);
    }
    return null;
  }

  /**
   * When running in session mode, create a domain for the dag and return it.
   * @param dagConf The configuration the dag for which domain has to be created.
   * @param dagPlan The dag plan which contains the ACLs.
   * @param dagId The dagId for which domain has to be created.
   * @return The created domain id on success.
   *     sessionDomainId: If there is a failure also disable history logging for this dag.
   *     sessionDomainId: If historyACLPolicyManager returns null.
   */
  private String createDagDomain(Configuration dagConf, DAGPlan dagPlan, TezDAGID dagId) {
    // In non session mode dag domain is same as session domain id.
    if (!appContext.isSession()) {
      return sessionDomainId;
    }
    DAGAccessControls dagAccessControls = dagPlan.hasAclInfo()
        ? DagTypeConverters.convertDAGAccessControlsFromProto(dagPlan.getAclInfo())
        : null;
    try {
      Map<String, String> domainInfo = historyACLPolicyManager.setupSessionDAGACLs(
          dagConf, appContext.getApplicationID(), Integer.toString(dagId.getId()),
          dagAccessControls);
      if (domainInfo != null) {
        return domainInfo.get(TezConfiguration.YARN_ATS_ACL_DAG_DOMAIN_ID);
      }
      // Fallback to session domain, if domainInfo was null
      return sessionDomainId;
    } catch (IOException | HistoryACLPolicyException e) {
      LOG.warn("Could not setup ACLs for DAG, disabling history logging for dag.", e);
      skippedDAGs.add(dagId);
      // Return value is not used, check for skippedDAG is important.
      return null;
    }
  }
}
