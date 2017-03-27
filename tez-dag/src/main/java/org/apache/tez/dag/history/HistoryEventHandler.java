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

package org.apache.tez.dag.history;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.HistoryLogLevel;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.logging.HistoryLoggingService;
import org.apache.tez.dag.history.recovery.RecoveryService;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;

public class HistoryEventHandler extends CompositeService {

  private static Logger LOG = LoggerFactory.getLogger(HistoryEventHandler.class);
  private static Logger LOG_CRITICAL_EVENTS =
      LoggerFactory.getLogger(LOG.getName() + ".criticalEvents");

  private final AppContext context;
  private RecoveryService recoveryService;
  private boolean recoveryEnabled;
  private HistoryLoggingService historyLoggingService;

  private HistoryLogLevel amHistoryLogLevel;
  private final Map<TezDAGID, HistoryLogLevel> dagIdToLogLevel = new ConcurrentHashMap<>();
  private Set<TaskAttemptTerminationCause> amTaskAttemptFilters;
  private final Map<TezDAGID, Set<TaskAttemptTerminationCause>> dagIdToTaskAttemptFilters =
      new ConcurrentHashMap<>();

  private final ConcurrentHashMap<TezTaskAttemptID, DAGHistoryEvent> suppressedEvents =
      new ConcurrentHashMap<>();

  private final AtomicLong criticalEventCount = new AtomicLong();

  public HistoryEventHandler(AppContext context) {
    super(HistoryEventHandler.class.getName());
    this.context = context;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    this.recoveryEnabled = context.getAMConf().getBoolean(TezConfiguration.DAG_RECOVERY_ENABLED,
        TezConfiguration.DAG_RECOVERY_ENABLED_DEFAULT);

    String historyServiceClassName = context.getAMConf().get(
        TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS,
        TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS_DEFAULT);

    LOG.info("Initializing HistoryEventHandler with"
        + "recoveryEnabled=" + recoveryEnabled
        + ", historyServiceClassName=" + historyServiceClassName);

    historyLoggingService =
        ReflectionUtils.createClazzInstance(historyServiceClassName);
    historyLoggingService.setAppContext(context);
    addService(historyLoggingService);

    if (recoveryEnabled) {
      String recoveryServiceClass = conf.get(TezConfiguration.TEZ_AM_RECOVERY_SERVICE_CLASS,
          TezConfiguration.TEZ_AM_RECOVERY_SERVICE_CLASS_DEFAULT);
      recoveryService = ReflectionUtils.createClazzInstance(recoveryServiceClass,
          new Class[]{AppContext.class}, new Object[] {context});
      addService(recoveryService);
    }

    amHistoryLogLevel = HistoryLogLevel.getLogLevel(context.getAMConf(), HistoryLogLevel.DEFAULT);
    amTaskAttemptFilters = TezUtilsInternal.getEnums(
        context.getAMConf(),
        TezConfiguration.TEZ_HISTORY_LOGGING_TASKATTEMPT_FILTERS,
        TaskAttemptTerminationCause.class,
        null);

    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    LOG.info("Stopping HistoryEventHandler");
    super.serviceStop();
  }

  /**
   * Used by events that are critical for recovery
   * DAG Submission/finished and any commit related activites are critical events
   * In short, any events that are instances of SummaryEvent
   * @param event History event
   * @throws IOException
   */
  public void handleCriticalEvent(DAGHistoryEvent event) throws IOException {
    TezDAGID dagId = event.getDagID();
    String dagIdStr = "N/A";
    if(dagId != null) {
      dagIdStr = dagId.toString();
    }
    HistoryEvent historyEvent = event.getHistoryEvent();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Handling history event"
          + ", eventType=" + historyEvent.getEventType());
    }
    if (recoveryEnabled && historyEvent.isRecoveryEvent()) {
      recoveryService.handle(event);
    }
    if (historyEvent.isHistoryEvent() && shouldLogEvent(event)) {
      DAGHistoryEvent suppressedEvent = getSupressedEvent(historyEvent);
      if (suppressedEvent != null) {
        historyLoggingService.handle(suppressedEvent);
      }
      historyLoggingService.handle(event);
    }

    if (LOG_CRITICAL_EVENTS.isInfoEnabled()) {
      // TODO at some point we should look at removing this once
      // there is a UI in place
      LOG_CRITICAL_EVENTS.info("[HISTORY]"
          + "[DAG:" + dagIdStr + "]"
          + "[Event:" + event.getHistoryEvent().getEventType().name() + "]"
          + ": " + event.getHistoryEvent().toString());
    } else {
      if (criticalEventCount.incrementAndGet() % 1000 == 0) {
        LOG.info("Got {} critical events", criticalEventCount);
      }
    }
  }

  private boolean shouldLogEvent(DAGHistoryEvent event) {
    TezDAGID dagId = event.getDagID();

    HistoryLogLevel dagLogLevel = null;
    if (dagId != null) {
      dagLogLevel = dagIdToLogLevel.get(dagId);
    }
    if (dagLogLevel == null) {
      dagLogLevel = amHistoryLogLevel;
    }

    HistoryEvent historyEvent = event.getHistoryEvent();
    HistoryEventType eventType = historyEvent.getEventType();
    if (eventType == HistoryEventType.DAG_SUBMITTED) {
      Configuration dagConf = ((DAGSubmittedEvent)historyEvent).getConf();
      dagLogLevel = HistoryLogLevel.getLogLevel(dagConf, amHistoryLogLevel);
      dagIdToLogLevel.put(dagId, dagLogLevel);
      maybeUpdateDagTaskAttemptFilters(dagId, dagLogLevel, dagConf);
    } else if (eventType == HistoryEventType.DAG_RECOVERED) {
      if (context.getCurrentDAG() != null) {
        Configuration dagConf = context.getCurrentDAG().getConf();
        dagLogLevel = HistoryLogLevel.getLogLevel(dagConf, amHistoryLogLevel);
        dagIdToLogLevel.put(dagId, dagLogLevel);
        maybeUpdateDagTaskAttemptFilters(dagId, dagLogLevel, dagConf);
      }
    } else if (eventType == HistoryEventType.DAG_FINISHED) {
      dagIdToLogLevel.remove(dagId);
      dagIdToTaskAttemptFilters.remove(dagId);
      suppressedEvents.clear();
    }

    if (dagLogLevel.shouldLog(historyEvent.getEventType().getHistoryLogLevel())) {
      return shouldLogTaskAttemptEvents(event, dagLogLevel);
    }
    return false;
  }

  // If the log level is set to TASK_ATTEMPT and filters are configured, then we should suppress
  // the start event and publish it only when TaskAttemptFinishedEvent is received after
  // matching against the filter.
  // Note: if the AM is killed before we get the TaskAttemptFinishedEvent, we'll lose this event.
  private boolean shouldLogTaskAttemptEvents(DAGHistoryEvent event, HistoryLogLevel dagLogLevel) {
    HistoryEvent historyEvent = event.getHistoryEvent();
    HistoryEventType eventType = historyEvent.getEventType();
    if (dagLogLevel == HistoryLogLevel.TASK_ATTEMPT &&
        (eventType == HistoryEventType.TASK_ATTEMPT_STARTED ||
         eventType == HistoryEventType.TASK_ATTEMPT_FINISHED)) {
      TezDAGID dagId = event.getDagID();
      Set<TaskAttemptTerminationCause> filters = null;
      if (dagId != null) {
        filters = dagIdToTaskAttemptFilters.get(dagId);
      }
      if (filters == null) {
        filters = amTaskAttemptFilters;
      }
      if (filters == null) {
        return true;
      }
      if (eventType == HistoryEventType.TASK_ATTEMPT_STARTED) {
        suppressedEvents.put(((TaskAttemptStartedEvent)historyEvent).getTaskAttemptID(), event);
        return false;
      } else { // TaskAttemptFinishedEvent
        TaskAttemptFinishedEvent finishedEvent = (TaskAttemptFinishedEvent)historyEvent;
        if (filters.contains(finishedEvent.getTaskAttemptError())) {
          suppressedEvents.remove(finishedEvent.getTaskAttemptID());
          return false;
        }
      }
    }
    return true;
  }

  private void maybeUpdateDagTaskAttemptFilters(TezDAGID dagId, HistoryLogLevel dagLogLevel,
      Configuration dagConf) {
    if (dagLogLevel == HistoryLogLevel.TASK_ATTEMPT) {
      Set<TaskAttemptTerminationCause> filters = TezUtilsInternal.getEnums(
          dagConf,
          TezConfiguration.TEZ_HISTORY_LOGGING_TASKATTEMPT_FILTERS,
          TaskAttemptTerminationCause.class,
          null);
      if (filters != null) {
        dagIdToTaskAttemptFilters.put(dagId, filters);
      }
    }
  }

  private DAGHistoryEvent getSupressedEvent(HistoryEvent historyEvent) {
    if (historyEvent.getEventType() == HistoryEventType.TASK_ATTEMPT_FINISHED) {
      TaskAttemptFinishedEvent finishedEvent = (TaskAttemptFinishedEvent)historyEvent;
      return suppressedEvents.remove(finishedEvent.getTaskAttemptID());
    }
    return null;
  }

  public void handle(DAGHistoryEvent event) {
    try {
      handleCriticalEvent(event);
    } catch (IOException e) {
      LOG.warn("Failed to handle recovery event"
          + ", eventType=" + event.getHistoryEvent().getEventType(), e);
    }
  }

  public boolean hasRecoveryFailed() {
    if (recoveryEnabled) {
      return recoveryService.hasRecoveryFailed();
    } else {
      return false;
    }
  }

}
