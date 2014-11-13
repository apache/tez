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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
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
  private boolean waitForeverOnShutdown = false;

  private int maxEventsPerBatch;
  private long maxPollingTimeMillis;

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
    maxEventsPerBatch = conf.getInt(
        TezConfiguration.YARN_ATS_MAX_EVENTS_PER_BATCH,
        TezConfiguration.YARN_ATS_MAX_EVENTS_PER_BATCH_DEFAULT);
    maxPollingTimeMillis = conf.getInt(
        TezConfiguration.YARN_ATS_MAX_POLLING_TIME_PER_EVENT,
        TezConfiguration.YARN_ATS_MAX_POLLING_TIME_PER_EVENT_DEFAULT);
    if (maxTimeToWaitOnShutdown < 0) {
      waitForeverOnShutdown = true;
    }
  }

  @Override
  public void serviceStart() {
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
            LOG.info("Event queue stats"
                + ", eventsProcessedSinceLastUpdate=" + eventsProcessed
                + ", eventQueueSize=" + eventQueue.size());
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
    eventQueue.add(event);
  }

  private boolean isValidEvent(DAGHistoryEvent event) {
    HistoryEventType eventType = event.getHistoryEvent().getEventType();
    TezDAGID dagId = event.getDagID();

    if (eventType.equals(HistoryEventType.DAG_SUBMITTED)) {
      DAGSubmittedEvent dagSubmittedEvent =
          (DAGSubmittedEvent) event.getHistoryEvent();
      String dagName = dagSubmittedEvent.getDAGName();
      if (dagName != null
          && dagName.startsWith(
          TezConstants.TEZ_PREWARM_DAG_NAME_PREFIX)) {
        // Skip recording pre-warm DAG events
        skippedDAGs.add(dagId);
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
      entities[i] = HistoryEventTimelineConversion.convertToTimelineEntity(
          events.get(i).getHistoryEvent());
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
                + ", entityId=" + entities[i].getEntityId()
                + ", eventType=" + events.get(i).getHistoryEvent().getEventType());
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
