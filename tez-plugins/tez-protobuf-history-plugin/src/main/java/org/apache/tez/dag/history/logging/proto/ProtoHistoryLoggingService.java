/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.history.logging.proto;

import java.io.IOException;
import java.time.LocalDate;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.logging.HistoryLoggingService;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.ManifestEntryProto;
import org.apache.tez.dag.records.TezDAGID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logging service to write history events serialized using protobuf into sequence files.
 * This can be used as external tables in hive. Or the reader can be used independently to
 * read the data from these files.
 */
public class ProtoHistoryLoggingService extends HistoryLoggingService {
  private static final Logger LOG = LoggerFactory.getLogger(ProtoHistoryLoggingService.class);
  private final HistoryEventProtoConverter converter =
      new HistoryEventProtoConverter();
  private boolean loggingDisabled = false;

  private final LinkedBlockingQueue<DAGHistoryEvent> eventQueue =
      new LinkedBlockingQueue<>(10000);
  private Thread eventHandlingThread;
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  private TezProtoLoggers loggers;
  private ProtoMessageWriter<HistoryEventProto> appEventsWriter;
  private ProtoMessageWriter<HistoryEventProto> dagEventsWriter;
  private ProtoMessageWriter<ManifestEntryProto> manifestEventsWriter;
  private LocalDate manifestDate;
  private TezDAGID currentDagId;
  private long dagSubmittedEventOffset = -1;

  private String appEventsFile;
  private long appLaunchedEventOffset;

  public ProtoHistoryLoggingService() {
    super(ProtoHistoryLoggingService.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    LOG.info("Initing ProtoHistoryLoggingService");
    setConfig(conf);
    loggingDisabled = !conf.getBoolean(TezConfiguration.TEZ_AM_HISTORY_LOGGING_ENABLED,
        TezConfiguration.TEZ_AM_HISTORY_LOGGING_ENABLED_DEFAULT);
    LOG.info("Inited ProtoHistoryLoggingService");
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting ProtoHistoryLoggingService");
    if (!loggingDisabled) {
      loggers = new TezProtoLoggers();
      if (!loggers.setup(getConfig(), appContext.getClock())) {
        LOG.warn("Log file location for ProtoHistoryLoggingService not specified, " +
            "logging disabled");
        loggingDisabled = true;
        return;
      }
      appEventsWriter = loggers.getAppEventsLogger().getWriter(
          appContext.getApplicationAttemptId().toString());
      eventHandlingThread = new Thread(this::loop, "HistoryEventHandlingThread");
      eventHandlingThread.start();
    }
    LOG.info("Started ProtoHistoryLoggingService");
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping ProtoHistoryLoggingService, eventQueueBacklog=" + eventQueue.size());
    stopped.set(true);
    eventHandlingThread.join();
    IOUtils.closeQuietly(appEventsWriter);
    IOUtils.closeQuietly(dagEventsWriter);
    IOUtils.closeQuietly(manifestEventsWriter);
    LOG.info("Stopped ProtoHistoryLoggingService");
  }

  @Override
  public void handle(DAGHistoryEvent event) {
    if (loggingDisabled || stopped.get()) {
      return;
    }
    try {
      eventQueue.add(event);
    } catch (IllegalStateException e) {
      LOG.error("Queue capacity filled up, ignoring event: " +
          event.getHistoryEvent().getEventType());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Queue capacity filled up, ignoring event: {}", event.getHistoryEvent());
      }
    }
  }

  private void loop() {
    // Keep looping while the service is not stopped.
    // Drain any left over events after the service has been stopped.
    while (!stopped.get() || !eventQueue.isEmpty()) {
      DAGHistoryEvent evt = null;
      try {
        evt = eventQueue.poll(100, TimeUnit.MILLISECONDS);
        if (evt != null) {
          handleEvent(evt);
        }
      } catch (InterruptedException e) {
        LOG.info("EventQueue poll interrupted, ignoring it.", e);
      } catch (IOException e) {
        TezDAGID dagid = evt.getDagID();
        HistoryEventType type = evt.getHistoryEvent().getEventType();
        // Retry is hard, because there are several places where this exception can happen
        // the state will get messed up a lot.
        LOG.error("Got exception while handling event {} for dag {}.", type, dagid, e);
      }
    }
  }

  private void handleEvent(DAGHistoryEvent event) throws IOException {
    if (loggingDisabled) {
      return;
    }
    HistoryEvent historyEvent = event.getHistoryEvent();
    if (event.getDagID() == null) {
      if (historyEvent.getEventType() == HistoryEventType.APP_LAUNCHED) {
        appEventsFile = appEventsWriter.getPath().toString();
        appLaunchedEventOffset = appEventsWriter.getOffset();
      }
      appEventsWriter.writeProto(converter.convert(historyEvent));
    } else {
      HistoryEventType type = historyEvent.getEventType();
      TezDAGID dagId = event.getDagID();
      if (type == HistoryEventType.DAG_FINISHED) {
        finishCurrentDag((DAGFinishedEvent)historyEvent);
      } else if (type == HistoryEventType.DAG_SUBMITTED) {
        finishCurrentDag(null);
        currentDagId = dagId;
        dagEventsWriter = loggers.getDagEventsLogger().getWriter(dagId.toString()
            + "_" + appContext.getApplicationAttemptId().getAttemptId());
        dagSubmittedEventOffset = dagEventsWriter.getOffset();
        dagEventsWriter.writeProto(converter.convert(historyEvent));
      } else if (dagEventsWriter != null) {
        dagEventsWriter.writeProto(converter.convert(historyEvent));
      }
    }
  }

  private void finishCurrentDag(DAGFinishedEvent event) throws IOException {
    if (dagEventsWriter == null) {
      return;
    }
    try {
      long finishEventOffset = -1;
      if (event != null) {
        finishEventOffset = dagEventsWriter.getOffset();
        dagEventsWriter.writeProto(converter.convert(event));
      }
      DatePartitionedLogger<ManifestEntryProto> manifestLogger = loggers.getManifestEventsLogger();
      if (manifestDate == null || !manifestDate.equals(manifestLogger.getNow().toLocalDate())) {
        // The day has changed write to a new file.
        IOUtils.closeQuietly(manifestEventsWriter);
        manifestEventsWriter = manifestLogger.getWriter(
            appContext.getApplicationAttemptId().toString());
        manifestDate = manifestLogger.getDateFromDir(
            manifestEventsWriter.getPath().getParent().getName());
      }
      ManifestEntryProto.Builder entry = ManifestEntryProto.newBuilder()
          .setDagId(currentDagId.toString())
          .setAppId(currentDagId.getApplicationId().toString())
          .setDagSubmittedEventOffset(dagSubmittedEventOffset)
          .setDagFinishedEventOffset(finishEventOffset)
          .setDagFilePath(dagEventsWriter.getPath().toString())
          .setAppFilePath(appEventsFile)
          .setAppLaunchedEventOffset(appLaunchedEventOffset)
          .setWriteTime(System.currentTimeMillis());
      if (event != null) {
        entry.setDagId(event.getDagID().toString());
      }
      manifestEventsWriter.writeProto(entry.build());
      manifestEventsWriter.hflush();
      appEventsWriter.hflush();
    } finally {
      // On an error, cleanup everything this will ensure, we do not use one dag's writer
      // into another dag.
      IOUtils.closeQuietly(dagEventsWriter);
      dagEventsWriter = null;
      currentDagId = null;
      dagSubmittedEventOffset = -1;
    }
  }
}
