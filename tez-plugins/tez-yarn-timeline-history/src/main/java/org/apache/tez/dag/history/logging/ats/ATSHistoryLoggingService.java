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

import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.logging.HistoryLoggingService;
import org.apache.tez.dag.records.TezDAGID;

import com.google.common.annotations.VisibleForTesting;

public class ATSHistoryLoggingService extends HistoryLoggingService {

  private static final Log LOG = LogFactory.getLog(ATSHistoryLoggingService.class);

  private LinkedBlockingQueue<DAGHistoryEvent> eventQueue =
      new LinkedBlockingQueue<DAGHistoryEvent>();

  private Thread eventHandlingThread;
  private AtomicBoolean stopped = new AtomicBoolean(false);
  private int eventCounter = 0;
  private int eventsProcessed = 0;
  private final Object lock = new Object();

  @VisibleForTesting
  TimelineClient timelineClient;

  private HashSet<TezDAGID> skippedDAGs = new HashSet<TezDAGID>();
  private long maxTimeToWaitOnShutdown;

  public ATSHistoryLoggingService() {
    super(ATSHistoryLoggingService.class.getName());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    LOG.info("Initializing ATSService");
    timelineClient = TimelineClient.createTimelineClient();
    timelineClient.init(conf);
    maxTimeToWaitOnShutdown = conf.getLong(
        TezConfiguration.YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS,
        TezConfiguration.YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS_DEFAULT);
  }

  @Override
  public void serviceStart() {
    LOG.info("Starting ATSService");
    timelineClient.start();
    eventHandlingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        DAGHistoryEvent event;
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {

          // Log the size of the event-queue every so often.
          if (eventCounter != 0 && eventCounter % 1000 == 0) {
            LOG.info("Event queue stats"
                + ", eventsProcessedSinceLastUpdate=" + eventsProcessed
                + ", eventQueueSize=" + eventQueue.size());
            eventCounter = 0;
            eventsProcessed = 0;
          } else {
            ++eventCounter;
          }

          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            LOG.info("EventQueue take interrupted. Returning");
            return;
          }

          synchronized (lock) {
            ++eventsProcessed;
            try {
              handleEvent(event);
            } catch (Exception e) {
              // TODO handle failures - treat as fatal or ignore?
              LOG.warn("Error handling event", e);
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
            + ", maxTimeLeftToFlush=" + maxTimeToWaitOnShutdown);
        long startTime = appContext.getClock().getTime();
        if (maxTimeToWaitOnShutdown > 0) {
          long endTime = startTime + maxTimeToWaitOnShutdown;
          while (endTime >= appContext.getClock().getTime()) {
            DAGHistoryEvent event = eventQueue.poll();
            if (event == null) {
              break;
            }
            try {
              handleEvent(event);
            } catch (Exception e) {
              LOG.warn("Error handling event", e);
              break;
            }
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

  public void handle(DAGHistoryEvent event) {
    eventQueue.add(event);
  }

  private void handleEvent(DAGHistoryEvent event) {
    HistoryEventType eventType = event.getHistoryEvent().getEventType();

    TezDAGID dagId = event.getDagID();

    if (eventType.equals(HistoryEventType.DAG_SUBMITTED)) {
      DAGSubmittedEvent dagSubmittedEvent =
          (DAGSubmittedEvent) event.getHistoryEvent();
      String dagName = dagSubmittedEvent.getDAGName();
      if (dagName != null
          && dagName.startsWith(
          TezConfiguration.TEZ_PREWARM_DAG_NAME_PREFIX)) {
        // Skip recording pre-warm DAG events
        skippedDAGs.add(dagId);
        return;
      }
    }
    if (eventType.equals(HistoryEventType.DAG_FINISHED)) {
      // Remove from set to keep size small
      // No more events should be seen after this point.
      if (skippedDAGs.remove(dagId)) {
        return;
      }
    }

    if (dagId != null && skippedDAGs.contains(dagId)) {
      // Skip pre-warm DAGs
      return;
    }

    try {
      TimelinePutResponse response =
          timelineClient.putEntities(
              HistoryEventTimelineConversion.convertToTimelineEntity(event.getHistoryEvent()));
      if (response != null
        && !response.getErrors().isEmpty()) {
        TimelinePutError err = response.getErrors().get(0);
        if (err.getErrorCode() != 0) {
          LOG.warn("Could not post history event to ATS, eventType="
              + eventType
              + ", atsPutError=" + err.getErrorCode());
        }
      }
      // Do nothing additional, ATS client library should handle throttling
      // or auto-disable as needed
    } catch (Exception e) {
      LOG.warn("Could not handle history event, eventType="
          + eventType, e);
    }
  }

}
