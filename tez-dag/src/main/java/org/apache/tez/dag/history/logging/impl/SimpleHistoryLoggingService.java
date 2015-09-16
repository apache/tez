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

package org.apache.tez.dag.history.logging.impl;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.logging.HistoryLoggingService;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class SimpleHistoryLoggingService extends HistoryLoggingService {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleHistoryLoggingService.class);
  private Path logFileLocation;
  private FileSystem logFileFS;
  private FSDataOutputStream outputStream;
  private LinkedBlockingQueue<DAGHistoryEvent> eventQueue =
      new LinkedBlockingQueue<DAGHistoryEvent>();
  public static final String RECORD_SEPARATOR = "\u0001" + System.getProperty("line.separator");
  public static final String LOG_FILE_NAME_PREFIX = "history.txt";

  private Thread eventHandlingThread;
  private AtomicBoolean stopped = new AtomicBoolean(false);

  private int consecutiveErrors = 0;
  private int maxErrors;
  private boolean loggingDisabled = false;

  public SimpleHistoryLoggingService() {
    super(SimpleHistoryLoggingService.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    String logDirPath = conf.get(TezConfiguration.TEZ_SIMPLE_HISTORY_LOGGING_DIR);
    final String logFileName = LOG_FILE_NAME_PREFIX + "." + appContext.getApplicationAttemptId();
    if (logDirPath == null || logDirPath.isEmpty()) {
      String logDir = appContext.getLogDirs()[new Random().nextInt(appContext.getLogDirs().length)];
      LOG.info("Log file location for SimpleHistoryLoggingService not specified, defaulting to"
          + " containerLogDir=" + logDir);
      Path p;
      logFileFS = FileSystem.getLocal(conf).getRawFileSystem();
      if (logDir != null) {
        p = new Path(logDir, logFileName);
      } else {
        p = new Path(logFileName);
      }
      logFileLocation = p;
    } else {
      LOG.info("Using configured log file location for SimpleHistoryLoggingService"
          + " logDirPath=" + logDirPath);
      Path p = new Path(logDirPath);
      logFileFS = p.getFileSystem(conf);
      if (!logFileFS.exists(p)) {
        logFileFS.mkdirs(p);
      }
      logFileLocation = new Path(logFileFS.resolvePath(p), logFileName);
    }
    maxErrors = conf.getInt(TezConfiguration.TEZ_SIMPLE_HISTORY_LOGGING_MAX_ERRORS,
        TezConfiguration.TEZ_SIMPLE_HISTORY_LOGGING_MAX_ERRORS_DEFAULT);
    LOG.info("Initializing SimpleHistoryLoggingService, logFileLocation=" + logFileLocation
        + ", maxErrors=" + maxErrors);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting SimpleHistoryLoggingService");
    outputStream = logFileFS.create(logFileLocation, true);
    eventHandlingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        DAGHistoryEvent event;
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            LOG.info("EventQueue take interrupted. Returning");
            return;
          }
          handleEvent(event);
        }
      }
    }, "HistoryEventHandlingThread");
    eventHandlingThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping SimpleHistoryLoggingService"
        + ", eventQueueBacklog=" + eventQueue.size());
    stopped.set(true);
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }
    while (!eventQueue.isEmpty()) {
      DAGHistoryEvent event = eventQueue.poll();
      if (event == null) {
        break;
      }
      handleEvent(event);
    }
    try {
      if (outputStream != null) {
        outputStream.hflush();
        outputStream.close();
      }
    } catch (IOException ioe) {
      LOG.warn("Failed to close output stream", ioe);
    }
    super.serviceStop();
  }

  @Override
  public void handle(DAGHistoryEvent event) {
    eventQueue.add(event);
  }

  private synchronized void handleEvent(DAGHistoryEvent event) {
    if (loggingDisabled) {
      return;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Writing event " + event.getHistoryEvent().getEventType() + " to history file");
    }
    try {
      try {
        JSONObject eventJson = HistoryEventJsonConversion.convertToJson(event.getHistoryEvent());
        outputStream.writeBytes(eventJson.toString());
        outputStream.writeBytes(RECORD_SEPARATOR);
      } catch (JSONException e) {
        LOG.warn("Failed to convert event to json", e);
      }
      consecutiveErrors = 0;
    } catch (IOException ioe) {
      ++consecutiveErrors;
      if (consecutiveErrors < maxErrors) {
        LOG.error("Failed to write to output stream, consecutiveErrorCount=" + consecutiveErrors, ioe);
      } else {
        loggingDisabled = true;
        LOG.error("Disabling SimpleHistoryLoggingService due to multiple errors," +
            "consecutive max errors reached, maxErrors=" + maxErrors);
      }
    }

  }
}
