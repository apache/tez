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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.security.DAGAccessControls;
import org.apache.tez.common.security.HistoryACLPolicyException;
import org.apache.tez.common.security.HistoryACLPolicyManager;
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
import org.apache.tez.dag.history.events.DAGRecoveredEvent;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.records.TezDAGID;

import com.google.common.annotations.VisibleForTesting;

public class ATSV15HistoryLoggingService extends HistoryLoggingService {

  private static final Logger LOG = LoggerFactory.getLogger(ATSV15HistoryLoggingService.class);

  @VisibleForTesting
  LinkedBlockingQueue<DAGHistoryEvent> eventQueue =
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

  private long maxPollingTimeMillis;

  private String sessionDomainId;
  private static final String atsHistoryLoggingServiceClassName =
      ATSV15HistoryLoggingService.class.getName();
  private static final String atsHistoryACLManagerClassName =
      "org.apache.tez.dag.history.ats.acls.ATSV15HistoryACLPolicyManager";

  @VisibleForTesting
  HistoryACLPolicyManager historyACLPolicyManager;

  private int numDagsPerGroup;

  public ATSV15HistoryLoggingService() {
    super(ATSV15HistoryLoggingService.class.getName());
  }

  @Override
  public void serviceInit(Configuration serviceConf) throws Exception {
    Configuration conf = new Configuration(serviceConf);

    String summaryEntityTypesStr = EntityTypes.TEZ_APPLICATION
        + "," + EntityTypes.TEZ_APPLICATION_ATTEMPT
        + "," + EntityTypes.TEZ_DAG_ID;

    // Ensure that summary entity types are defined properly for Tez.
    if (conf.getBoolean(TezConfiguration.TEZ_AM_ATS_V15_OVERRIDE_SUMMARY_TYPES,
        TezConfiguration.TEZ_AM_ATS_V15_OVERRIDE_SUMMARY_TYPES_DEFAULT)) {
      conf.set(YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SUMMARY_ENTITY_TYPES,
          summaryEntityTypesStr);
    }

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
    maxPollingTimeMillis = conf.getInt(
        TezConfiguration.YARN_ATS_MAX_POLLING_TIME_PER_EVENT,
        TezConfiguration.YARN_ATS_MAX_POLLING_TIME_PER_EVENT_DEFAULT);
    if (maxTimeToWaitOnShutdown < 0) {
      waitForeverOnShutdown = true;
    }

    LOG.info("Initializing " + ATSV15HistoryLoggingService.class.getSimpleName() + " with "
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

    numDagsPerGroup = conf.getInt(TezConfiguration.TEZ_HISTORY_LOGGING_TIMELINE_NUM_DAGS_PER_GROUP,
        TezConfiguration.TEZ_HISTORY_LOGGING_TIMELINE_NUM_DAGS_PER_GROUP_DEFAULT);
  }

  @Override
  public void serviceStart() {
    if (!historyLoggingEnabled || timelineClient == null) {
      return;
    }
    timelineClient.start();

    // create a session domain id, if it fails then disable history logging.
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
        boolean interrupted = false;
        TezUtilsInternal.setHadoopCallerContext(appContext.getHadoopShim(),
            appContext.getApplicationID());
        while (!stopped.get() && !Thread.currentThread().isInterrupted()
              && !interrupted) {

          // Log the size of the event-queue every so often.
          if (eventCounter != 0 && eventCounter % 1000 == 0) {
            if (eventsProcessed != 0 && !eventQueue.isEmpty()) {
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
              DAGHistoryEvent event = eventQueue.poll(maxPollingTimeMillis, TimeUnit.MILLISECONDS);
              if (event == null) {
                continue;
              }
              if (!isValidEvent(event)) {
                continue;
              }

              try {
                handleEvents(event);
                eventsProcessed += 1;
              } catch (Exception e) {
                LOG.warn("Error handling events", e);
              }
            } catch (InterruptedException e) {
              // Finish processing events and then return
              interrupted = true;
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
    try {
      TezUtilsInternal.setHadoopCallerContext(appContext.getHadoopShim(),
          appContext.getApplicationID());
      synchronized (lock) {
        if (!eventQueue.isEmpty()) {
          LOG.warn("ATSService being stopped"
              + ", eventQueueBacklog=" + eventQueue.size()
              + ", maxTimeLeftToFlush=" + maxTimeToWaitOnShutdown
              + ", waitForever=" + waitForeverOnShutdown);
          long startTime = appContext.getClock().getTime();
          long endTime = startTime + maxTimeToWaitOnShutdown;
          while (waitForeverOnShutdown || (endTime >= appContext.getClock().getTime())) {
            try {
              DAGHistoryEvent event = eventQueue.poll(maxPollingTimeMillis, TimeUnit.MILLISECONDS);
              if (event == null) {
                LOG.info("Event queue empty, stopping ATS Service");
                break;
              }
              if (!isValidEvent(event)) {
                continue;
              }
              try {
                handleEvents(event);
              } catch (Exception e) {
                LOG.warn("Error handling event", e);
              }
            } catch (InterruptedException e) {
              LOG.info("ATSService interrupted while shutting down. Exiting."
                  + " EventQueueBacklog=" + eventQueue.size());
            }
          }
        }
      }
    } finally {
      appContext.getHadoopShim().clearHadoopCallerContext();
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

  @VisibleForTesting
  public TimelineEntityGroupId getGroupId(DAGHistoryEvent event) {
    // Changing this function will impact TimelineCachePluginImpl and should be done very
    // carefully to account for handling different versions of Tez
    switch (event.getHistoryEvent().getEventType()) {
      case DAG_SUBMITTED:
      case DAG_INITIALIZED:
      case DAG_STARTED:
      case DAG_FINISHED:
      case DAG_KILL_REQUEST:
      case VERTEX_INITIALIZED:
      case VERTEX_STARTED:
      case VERTEX_CONFIGURE_DONE:
      case VERTEX_FINISHED:
      case TASK_STARTED:
      case TASK_FINISHED:
      case TASK_ATTEMPT_STARTED:
      case TASK_ATTEMPT_FINISHED:
      case DAG_COMMIT_STARTED:
      case VERTEX_COMMIT_STARTED:
      case VERTEX_GROUP_COMMIT_STARTED:
      case VERTEX_GROUP_COMMIT_FINISHED:
      case DAG_RECOVERED:
        String entityGroupId = numDagsPerGroup > 1
            ? event.getDagID().getGroupId(numDagsPerGroup)
            : event.getDagID().toString();
        return TimelineEntityGroupId.newInstance(event.getDagID().getApplicationId(), entityGroupId);
      case APP_LAUNCHED:
      case AM_LAUNCHED:
      case AM_STARTED:
      case CONTAINER_LAUNCHED:
      case CONTAINER_STOPPED:
        return TimelineEntityGroupId.newInstance(appContext.getApplicationID(),
            appContext.getApplicationID().toString());
    }
    return null;
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

  private void handleEvents(DAGHistoryEvent event) {
    String domainId = getDomainForEvent(event);
    // skippedDags is updated in the above call so check again.
    if (event.getDagID() != null && skippedDAGs.contains(event.getDagID())) {
      return;
    }
    TimelineEntityGroupId groupId = getGroupId(event);
    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(
        event.getHistoryEvent());
    for (TimelineEntity entity : entities) {
      logEntity(groupId, entity, domainId);
    }
  }

  private void logEntity(TimelineEntityGroupId groupId, TimelineEntity entity, String domainId) {
    if (historyACLPolicyManager != null && domainId != null && !domainId.isEmpty()) {
      historyACLPolicyManager.updateTimelineEntityDomain(entity, domainId);
    }

    try {
      TimelinePutResponse response = timelineClient.putEntities(
          appContext.getApplicationAttemptId(), groupId, entity);
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

    TezDAGID dagId = event.getDagID();
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
  private String createSessionDomain() throws IOException, HistoryACLPolicyException {
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
