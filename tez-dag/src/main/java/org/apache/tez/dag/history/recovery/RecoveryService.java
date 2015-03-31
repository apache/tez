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

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.SummaryEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.records.TezDAGID;

import com.google.common.annotations.VisibleForTesting;

public class RecoveryService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(RecoveryService.class);
  private final AppContext appContext;

  public static final String RECOVERY_FATAL_OCCURRED_DIR =
      "RecoveryFatalErrorOccurred";
  /**
   * whether to handle remaining event in the eventqueue when AM is stopped
   */
  @VisibleForTesting
  public static final String TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED =
      TezConfiguration.TEZ_PREFIX + "test.recovery.drain_event";

  /**
   * by default handle remaining event when AM is stopped.
   * This should be helpful for recovery
   */
  @VisibleForTesting
  public static final boolean TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED_DEFAULT = true;

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
  private AtomicBoolean recoveryFatalErrorOccurred = new AtomicBoolean(false);
  private boolean drainEventsFlag;

  // Indicates all the remaining events on stop have been drained
  // and processed.
  private volatile boolean drained = true;
  private Object waitForDrained = new Object();

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

    drainEventsFlag = conf.getBoolean(
        TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED,
        TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED_DEFAULT);
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
          drained = eventQueue.isEmpty();
          // adding this service state check is to avoid the overhead of acquiring the lock
          // and calling notify every time in the normal run of the loop.
          if (getServiceState() == STATE.STOPPED) {
            synchronized (waitForDrained) {
              if (drained) {
                waitForDrained.notify();
              }
            }
          }

          if (recoveryFatalErrorOccurred.get()) {
            LOG.error("Recovery failure occurred. Stopping recovery thread."
                + " Current eventQueueSize=" + eventQueue.size());
            eventQueue.clear();
            return;
          }

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
              handleRecoveryEvent(event);
            } catch (Exception e) {
              // For now, ignore any such errors as these are non-critical
              // All summary event related errors are handled as critical
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
  public void serviceStop() throws Exception {
    LOG.info("Stopping RecoveryService");

    if (drainEventsFlag) {
      LOG.info("Handle the remaining events in queue, queue size=" + eventQueue.size());
      synchronized (waitForDrained) {
        while (!drained && eventHandlingThread.isAlive()) {
          waitForDrained.wait(1000);
          LOG.info("Waiting for RecoveryEventHandlingThread to drain.");
        }
      }
    }

    stopped.set(true);
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
      try {
        eventHandlingThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted Exception while stopping", ie);
      }
    }

    if (summaryStream != null) {
      try {
        LOG.info("Closing Summary Stream");
        summaryStream.hflush();
        summaryStream.close();
      } catch (IOException ioe) {
        LOG.warn("Error when closing summary stream", ioe);
      }
    }
    for (Entry<TezDAGID, FSDataOutputStream> entry : outputStreamMap.entrySet()) {
      try {
        LOG.info("Closing Output Stream for DAG " + entry.getKey());
        entry.getValue().hflush();
        entry.getValue().close();
      } catch (IOException ioe) {
        LOG.warn("Error when closing output stream", ioe);
      }
    }
  }

  // ---------- IMPORTANT ----------------------
  // ALWAYS USE THIS METHOD TO ADD EVENT TO QUEUE
  private void addToEventQueue(DAGHistoryEvent event) {
    drained = false;
    eventQueue.add(event);
  }

  public void handle(DAGHistoryEvent event) throws IOException {
    if (stopped.get()) {
      LOG.warn("Igoring event as service stopped, eventType"
          + event.getHistoryEvent().getEventType());
      return;
    }
    HistoryEventType eventType = event.getHistoryEvent().getEventType();

    if (recoveryFatalErrorOccurred.get()) {
      return;
    }

    if (!started.get()) {
      LOG.warn("Adding event of type " + eventType
          + " to queue as service not started");
      addToEventQueue(event);
      return;
    }

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
        return;
      }
    }
    if (dagId == null || skippedDAGs.contains(dagId)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping event for DAG"
            + ", eventType=" + eventType
            + ", dagId=" + (dagId == null ? "null" : dagId.toString())
            + ", isSkippedDAG=" + (dagId == null ? "null"
            : skippedDAGs.contains(dagId)));
      }
      return;
    }

    if (event.getHistoryEvent() instanceof SummaryEvent) {
      synchronized (lock) {
        try {
          SummaryEvent summaryEvent = (SummaryEvent) event.getHistoryEvent();
          handleSummaryEvent(dagId, eventType, summaryEvent);
          summaryStream.hflush();
          if (summaryEvent.writeToRecoveryImmediately()) {
            handleRecoveryEvent(event);
            doFlush(outputStreamMap.get(event.getDagID()),
                appContext.getClock().getTime());
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Queueing Non-immediate Summary/Recovery event of type"
                  + eventType.name());
            }
            addToEventQueue(event);
          }
          if (eventType.equals(HistoryEventType.DAG_FINISHED)) {
            LOG.info("DAG completed"
                + ", dagId=" + event.getDagID()
                + ", queueSize=" + eventQueue.size());
            completedDAGs.add(dagId);
            if (outputStreamMap.containsKey(dagId)) {
              try {
                outputStreamMap.get(dagId).close();
                outputStreamMap.remove(dagId);
              } catch (IOException ioe) {
                LOG.warn("Error when trying to flush/close recovery file for"
                    + " dag, dagId=" + event.getDagID());
              }
            }
          }
        } catch (IOException ioe) {
          LOG.error("Error handling summary event"
              + ", eventType=" + event.getHistoryEvent().getEventType(), ioe);
          Path fatalErrorDir = new Path(recoveryPath, RECOVERY_FATAL_OCCURRED_DIR);
          try {
            LOG.error("Adding a flag to ensure next AM attempt does not start up"
                + ", flagFile=" + fatalErrorDir.toString());
            recoveryFatalErrorOccurred.set(true);
            recoveryDirFS.mkdirs(fatalErrorDir);
            if (recoveryDirFS.exists(fatalErrorDir)) {
              LOG.error("Recovery failure occurred. Skipping all events");
            } else {
              // throw error if fatal error flag could not be set
              throw ioe;
            }
          } catch (IOException e) {
            LOG.fatal("Failed to create fatal error flag dir "
                + fatalErrorDir.toString(), e);
            throw ioe;
          }
          if (eventType.equals(HistoryEventType.DAG_SUBMITTED)) {
            // Throw error to tell client that dag submission failed
            throw ioe;
          }
        }
      }
    } else {
      // All other events just get queued
      if (LOG.isDebugEnabled()) {
        LOG.debug("Queueing Non-Summary Recovery event of type " + eventType.name());
      }
      addToEventQueue(event);
    }
  }

  private void handleSummaryEvent(TezDAGID dagID,
      HistoryEventType eventType,
      SummaryEvent summaryEvent) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Handling summary event"
          + ", dagID=" + dagID
          + ", eventType=" + eventType);
    }
    if (summaryStream == null) {
      Path summaryPath = TezCommonUtils.getSummaryRecoveryPath(recoveryPath);
      if (LOG.isDebugEnabled()) {
        LOG.debug("AppId :" + appContext.getApplicationID() + " summaryPath " + summaryPath);
      }
      if (!recoveryDirFS.exists(summaryPath)) {
        summaryStream = recoveryDirFS.create(summaryPath, false,
            bufferSize);
      } else {
        summaryStream = recoveryDirFS.append(summaryPath, bufferSize);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing recovery event to summary stream"
          + ", dagId=" + dagID
          + ", eventType=" + eventType);
    }
    summaryEvent.toSummaryProtoStream(summaryStream);
  }

  @VisibleForTesting
  protected void handleRecoveryEvent(DAGHistoryEvent event) throws IOException {
    HistoryEventType eventType = event.getHistoryEvent().getEventType();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Handling recovery event of type "
          + event.getHistoryEvent().getEventType());
    }
    TezDAGID dagID = event.getDagID();

    if (completedDAGs.contains(dagID)) {
      // no need to recover completed DAGs
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping Recovery Event as DAG completed"
            + ", dagId=" + dagID
            + ", completed=" + completedDAGs.contains(dagID)
            + ", skipped=" + skippedDAGs.contains(dagID)
            + ", eventType=" + eventType);
      }
      return;
    }

    if (!outputStreamMap.containsKey(dagID)) {
      Path dagFilePath = TezCommonUtils.getDAGRecoveryPath(recoveryPath, dagID.toString());
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
          + ", eventType=" + eventType);
    }
    ++unflushedEventsCount;
    outputStream.writeInt(event.getHistoryEvent().getEventType().ordinal());
    event.getHistoryEvent().toProtoStream(outputStream);
    if (!EnumSet.of(HistoryEventType.DAG_SUBMITTED,
        HistoryEventType.DAG_FINISHED).contains(eventType)) {
      maybeFlush(outputStream);
    }
  }

  private void maybeFlush(FSDataOutputStream outputStream) throws IOException {
    long currentTime = appContext.getClock().getTime();
    boolean doFlush = false;
    if (maxUnflushedEvents >=0
        && unflushedEventsCount >= maxUnflushedEvents) {
      if  (LOG.isDebugEnabled()) {
        LOG.debug("Max unflushed events count reached. Flushing recovery data"
            + ", unflushedEventsCount=" + unflushedEventsCount
            + ", maxUnflushedEvents=" + maxUnflushedEvents);
      }
      doFlush = true;
    } else if (flushInterval >= 0
        && ((currentTime - lastFlushTime) >= (flushInterval*1000))) {
      LOG.debug("Flush interval time period elapsed. Flushing recovery data"
          + ", lastTimeSinceFLush=" + lastFlushTime
          + ", timeSinceLastFlush=" + (currentTime - lastFlushTime));
      doFlush = true;
    }
    if (!doFlush) {
      return;
    }
    doFlush(outputStream, currentTime);
  }

  private void doFlush(FSDataOutputStream outputStream,
      long currentTime) throws IOException {
    outputStream.hflush();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Flushing output stream"
          + ", lastTimeSinceFLush=" + lastFlushTime
          + ", timeSinceLastFlush=" + (currentTime - lastFlushTime)
          + ", unflushedEventsCount=" + unflushedEventsCount
          + ", maxUnflushedEvents=" + maxUnflushedEvents);
    }

    unflushedEventsCount = 0;
    lastFlushTime = currentTime;
  }

  public boolean hasRecoveryFailed() {
    return recoveryFatalErrorOccurred.get();
  }

  public void await() {
    while (!this.drained) {
      Thread.yield();
    }
  }
}
