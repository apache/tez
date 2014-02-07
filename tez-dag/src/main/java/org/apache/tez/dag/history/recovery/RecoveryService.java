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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecoveryService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(RecoveryService.class);
  private final AppContext appContext;

  private LinkedBlockingQueue<DAGHistoryEvent> eventQueue =
      new LinkedBlockingQueue<DAGHistoryEvent>();
  private Set<TezDAGID> completedDAGs = new HashSet<TezDAGID>();

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
  // FSDataOutputStream metaInfoStream;
  private int bufferSize;
  private FSDataOutputStream summaryStream;

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
  }

  @Override
  public void serviceStart() {
    LOG.info("Starting RecoveryService");
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
        summaryStream.flush();
        summaryStream.close();
      } catch (IOException ioe) {
        LOG.warn("Error when closing summary stream", ioe);
      }
    }
    for (FSDataOutputStream outputStream : outputStreamMap.values()) {
      try {
        outputStream.flush();
        outputStream.close();
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
    if (!started.get()) {
      eventQueue.add(event);
      return;
    }
    HistoryEventType eventType = event.getHistoryEvent().getEventType();
    if (eventType.equals(HistoryEventType.DAG_SUBMITTED)
      || eventType.equals(HistoryEventType.DAG_FINISHED)) {
      // handle submissions and completion immediately
      synchronized (lock) {
        try {
          handleEvent(event);
          summaryStream.flush();
          if (eventType.equals(HistoryEventType.DAG_SUBMITTED)) {
            outputStreamMap.get(event.getDagID()).flush();
          } else if (eventType.equals(HistoryEventType.DAG_FINISHED)) {
            completedDAGs.add(event.getDagID());
            if (outputStreamMap.containsKey(event.getDagID())) {
              try {
                outputStreamMap.get(event.getDagID()).flush();
                outputStreamMap.get(event.getDagID()).close();
                outputStreamMap.remove(event.getDagID());
              } catch (IOException ioe) {
                LOG.warn("Error when trying to flush/close recovery file for"
                    + " dag, dagId=" + event.getDagID());
              }
            } else {
              // TODO this is an error
            }
          }
        } catch (Exception e) {
            // TODO handle failures - treat as fatal or ignore?
            LOG.warn("Error handling recovery event", e);
        }
      }
      LOG.info("DAG completed"
          + ", dagId=" + event.getDagID()
          + ", queueSize=" + eventQueue.size());
    } else {
      // All other events just get queued
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
      return;
    }

    TezDAGID dagID = event.getDagID();
    if (completedDAGs.contains(dagID)) {
      // Skip events for completed DAGs
      // no need to recover completed DAGs
      return;
    }

    try {

      if (eventType.equals(HistoryEventType.DAG_SUBMITTED)
          || eventType.equals(HistoryEventType.DAG_FINISHED)) {
        if (summaryStream == null) {
          Path summaryPath = new Path(recoveryPath,
              appContext.getApplicationID()
              + TezConfiguration.DAG_RECOVERY_SUMMARY_FILE_SUFFIX);
          summaryStream = recoveryDirFS.create(summaryPath, false,
              bufferSize);
        }
        if (eventType.equals(HistoryEventType.DAG_SUBMITTED)) {
          DAGSubmittedEvent dagSubmittedEvent =
              (DAGSubmittedEvent) event.getHistoryEvent();
          String dagName = dagSubmittedEvent.getDAGName();
          if (dagName != null
              && dagName.startsWith(
              TezConfiguration.TEZ_PREWARM_DAG_NAME_PREFIX)) {
            // Skip recording pre-warm DAG events
            return;
          }
          Path dagFilePath = new Path(recoveryPath,
              dagID.toString() + TezConfiguration.DAG_RECOVERY_RECOVER_FILE_SUFFIX);
          FSDataOutputStream outputStream =
              recoveryDirFS.create(dagFilePath, false, bufferSize);
          outputStreamMap.put(dagID, outputStream);
        }

        if (outputStreamMap.containsKey(dagID)) {
          SummaryEvent summaryEvent = (SummaryEvent) event.getHistoryEvent();
          summaryEvent.toSummaryProtoStream(summaryStream);
        }
      }

      FSDataOutputStream outputStream = outputStreamMap.get(dagID);
      if (outputStream == null) {
        return;
      }

      outputStream.write(event.getHistoryEvent().getEventType().ordinal());
      event.getHistoryEvent().toProtoStream(outputStream);
    } catch (IOException ioe) {
      // TODO handle failures - treat as fatal or ignore?
      LOG.warn("Failed to write to stream", ioe);
    }

  }

}
