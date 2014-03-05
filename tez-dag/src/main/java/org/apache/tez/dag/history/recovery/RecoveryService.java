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

package org.apache.tez.dag.history.recovery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.SummaryEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.records.TezDAGID;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecoveryService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(RecoveryService.class);
  private final AppContext appContext;

  private LinkedBlockingQueue<DAGHistoryEvent> eventQueue =
      new LinkedBlockingQueue<DAGHistoryEvent>();
  private Set<TezDAGID> completedDAGs = new HashSet<TezDAGID>();
  private Set<TezDAGID> skippedDAGs = new HashSet<TezDAGID>();

  private Thread eventHandlingThread;
  private AtomicBoolean stopped = new AtomicBoolean(false);
  private AtomicBoolean started = new AtomicBoolean(false);
  private int eventCounter = 0;
  private int eventsProcessed = 0;
  private final Object lock = new Object();
  private FileSystem recoveryDirFS; // FS where staging dir exists
  Path recoveryPath;
  Map<TezDAGID, FSDataOutputStream> outputStreamMap = new
      HashMap<TezDAGID, FSDataOutputStream>();
  private int bufferSize;
  private FSDataOutputStream summaryStream;
  private int unflushedEventsCount = 0;
  private long lastFlushTime = -1;
  private int maxUnflushedEvents;
  private int flushInterval;

  public RecoveryService(AppContext appContext) {
    super(RecoveryService.class.getName());
    this.appContext = appContext;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    LOG.info("Initializing RecoveryService");
    recoveryPath = appContext.getCurrentRecoveryDir();
    recoveryDirFS = FileSystem.get(recoveryPath.toUri(), conf);
    bufferSize = conf.getInt(TezConfiguration.DAG_RECOVERY_FILE_IO_BUFFER_SIZE,
        TezConfiguration.DAG_RECOVERY_FILE_IO_BUFFER_SIZE_DEFAULT);

    flushInterval = conf.getInt(TezConfiguration.DAG_RECOVERY_FLUSH_INTERVAL_SECS,
        TezConfiguration.DAG_RECOVERY_FLUSH_INTERVAL_SECS_DEFAULT);
    maxUnflushedEvents = conf.getInt(TezConfiguration.DAG_RECOVERY_MAX_UNFLUSHED_EVENTS,
        TezConfiguration.DAG_RECOVERY_MAX_UNFLUSHED_EVENTS_DEFAULT);
  }

  @Override
  public void serviceStart() {
    LOG.info("Starting RecoveryService");
    lastFlushTime = appContext.getClock().getTime();
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
            try {
              ++eventsProcessed;
              handleEvent(event);
            } catch (Exception e) {
              // TODO handle failures - treat as fatal or ignore?
              LOG.warn("Error handling recovery event", e);
            }
          }
        }
      }
    }, "RecoveryEventHandlingThread");
    eventHandlingThread.start();
    started.set(true);
  }

  @Override
  public void serviceStop() {
    LOG.info("Stopping RecoveryService");
    stopped.set(true);
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }

    if (summaryStream != null) {
      try {
        LOG.info("Closing Summary Stream");
        summaryStream.hsync();
        summaryStream.close();
      } catch (IOException ioe) {
        LOG.warn("Error when closing summary stream", ioe);
      }
    }
    for (Entry<TezDAGID, FSDataOutputStream> entry : outputStreamMap.entrySet()) {
      try {
        LOG.info("Closing Output Stream for DAG " + entry.getKey());
        entry.getValue().hsync();
        entry.getValue().close();
      } catch (IOException ioe) {
        LOG.warn("Error when closing output stream", ioe);
      }
    }
  }

  public void handle(DAGHistoryEvent event) {
    if (stopped.get()) {
      LOG.warn("Igoring event as service stopped, eventType"
          + event.getHistoryEvent().getEventType());
      return;
    }
    HistoryEventType eventType = event.getHistoryEvent().getEventType();
    if (!started.get()) {
      LOG.warn("Adding event of type " + eventType
          + " to queue as service not started");
      eventQueue.add(event);
      return;
    }

    if (eventType.equals(HistoryEventType.DAG_SUBMITTED)
      || eventType.equals(HistoryEventType.DAG_FINISHED)) {
      // handle submissions and completion immediately
      synchronized (lock) {
        try {
          handleEvent(event);
          summaryStream.hsync();
          if (eventType.equals(HistoryEventType.DAG_SUBMITTED)) {
            if (outputStreamMap.containsKey(event.getDagID())) {
              doFlush(outputStreamMap.get(event.getDagID()),
                  appContext.getClock().getTime(), true);
            }
          } else if (eventType.equals(HistoryEventType.DAG_FINISHED)) {
            completedDAGs.add(event.getDagID());
            if (outputStreamMap.containsKey(event.getDagID())) {
              try {
                doFlush(outputStreamMap.get(event.getDagID()),
                    appContext.getClock().getTime(), true);
                outputStreamMap.get(event.getDagID()).close();
                outputStreamMap.remove(event.getDagID());
              } catch (IOException ioe) {
                LOG.warn("Error when trying to flush/close recovery file for"
                    + " dag, dagId=" + event.getDagID());
                // FIXME handle error ?
              }
            }
          }
        } catch (Exception e) {
          // FIXME handle failures
          LOG.warn("Error handling recovery event", e);
        }
      }
      LOG.info("DAG completed"
          + ", dagId=" + event.getDagID()
          + ", queueSize=" + eventQueue.size());
    } else {
      // All other events just get queued
      if (LOG.isDebugEnabled()) {
        LOG.debug("Queueing Recovery event of type " + eventType.name());
      }
      eventQueue.add(event);
    }
  }


  private void handleEvent(DAGHistoryEvent event) {
    HistoryEventType eventType = event.getHistoryEvent().getEventType();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Handling recovery event of type "
          + event.getHistoryEvent().getEventType());
    }
    if (event.getDagID() == null) {
      // AM event
      // anything to be done?
      // TODO
      LOG.info("Skipping Recovery Event as DAG is null"
          + ", eventType=" + event.getHistoryEvent().getEventType());
      return;
    }

    TezDAGID dagID = event.getDagID();
    if (completedDAGs.contains(dagID)
        || skippedDAGs.contains(dagID)) {
      // Skip events for completed and skipped DAGs
      // no need to recover completed DAGs
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping Recovery Event as either completed or skipped"
            + ", dagId=" + dagID
            + ", completed=" + completedDAGs.contains(dagID)
            + ", skipped=" + skippedDAGs.contains(dagID)
            + ", eventType=" + event.getHistoryEvent().getEventType());
      }
      return;
    }

    try {

      if (summaryStream == null) {
        Path summaryPath = new Path(recoveryPath,
            appContext.getApplicationID()
                + TezConfiguration.DAG_RECOVERY_SUMMARY_FILE_SUFFIX);
        if (!recoveryDirFS.exists(summaryPath)) {
          summaryStream = recoveryDirFS.create(summaryPath, false,
              bufferSize);
        } else {
          summaryStream = recoveryDirFS.append(summaryPath, bufferSize);
        }
      }

      if (eventType.equals(HistoryEventType.DAG_SUBMITTED)
          || eventType.equals(HistoryEventType.DAG_FINISHED)) {
        if (eventType.equals(HistoryEventType.DAG_SUBMITTED)) {
          DAGSubmittedEvent dagSubmittedEvent =
              (DAGSubmittedEvent) event.getHistoryEvent();
          String dagName = dagSubmittedEvent.getDAGName();
          if (dagName != null
              && dagName.startsWith(
              TezConfiguration.TEZ_PREWARM_DAG_NAME_PREFIX)) {
            // Skip recording pre-warm DAG events
            skippedDAGs.add(dagID);
            return;
          }
        }
        SummaryEvent summaryEvent = (SummaryEvent) event.getHistoryEvent();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Writing recovery event to summary stream"
              + ", dagId=" + dagID
              + ", type="
              + event.getHistoryEvent().getEventType());
        }
        summaryEvent.toSummaryProtoStream(summaryStream);
      }

      if (!outputStreamMap.containsKey(dagID)) {
        Path dagFilePath = new Path(recoveryPath,
            dagID.toString() + TezConfiguration.DAG_RECOVERY_RECOVER_FILE_SUFFIX);
        FSDataOutputStream outputStream;
        if (recoveryDirFS.exists(dagFilePath)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Opening DAG recovery file in append mode"
                + ", filePath=" + dagFilePath);
          }
          outputStream = recoveryDirFS.append(dagFilePath, bufferSize);
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Opening DAG recovery file in create mode"
                + ", filePath=" + dagFilePath);
          }
          outputStream = recoveryDirFS.create(dagFilePath, false, bufferSize);
        }
        outputStreamMap.put(dagID, outputStream);
      }

      FSDataOutputStream outputStream = outputStreamMap.get(dagID);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Writing recovery event to output stream"
            + ", dagId=" + dagID
            + ", type="
            + event.getHistoryEvent().getEventType());
      }
      ++unflushedEventsCount;
      outputStream.writeInt(event.getHistoryEvent().getEventType().ordinal());
      event.getHistoryEvent().toProtoStream(outputStream);
      if (!EnumSet.of(HistoryEventType.DAG_SUBMITTED,
          HistoryEventType.DAG_FINISHED).contains(eventType)) {
        maybeFlush(outputStream);
      }
    } catch (IOException ioe) {
      // FIXME handle failures
      LOG.warn("Failed to write to stream", ioe);
    }

  }

  private void maybeFlush(FSDataOutputStream outputStream) throws IOException {
    long currentTime = appContext.getClock().getTime();
    boolean doFlush = false;
    if (unflushedEventsCount >= maxUnflushedEvents) {
      doFlush = true;
    } else if (flushInterval >= 0
        && ((currentTime - lastFlushTime) >= (flushInterval*1000))) {
      doFlush = true;
    }

    if (!doFlush) {
      return;
    }

    doFlush(outputStream, currentTime, false);
  }

  private void doFlush(FSDataOutputStream outputStream,
      long currentTime, boolean sync) throws IOException {
    if (sync) {
      outputStream.hsync();
    } else {
      outputStream.hflush();
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Flushing output stream"
          + ", lastTimeSinceFLush=" + lastFlushTime
          + ", unflushedEventsCount=" + unflushedEventsCount
          + ", maxUnflushedEvents=" + maxUnflushedEvents
          + ", currentTime=" + currentTime);
    }

    unflushedEventsCount = 0;
    lastFlushTime = currentTime;
  }

}
