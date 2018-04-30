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

package org.apache.tez.dag.app;

import java.io.EOFException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.protobuf.CodedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.AMLaunchedEvent;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;
import org.apache.tez.dag.history.events.ContainerStoppedEvent;
import org.apache.tez.dag.history.events.DAGCommitStartedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGKillRequestEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexCommitStartedEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitStartedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexConfigurationDoneEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.recovery.RecoveryService;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.recovery.records.RecoveryProtos;
import org.apache.tez.dag.recovery.records.RecoveryProtos.SummaryEventProto;
import org.apache.tez.runtime.api.impl.TezEvent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;


/**
 * RecoverParser is mainly for Tez AM Recovery. It would read the recovery events. (summary & non-summary)
 * 
 */
public class RecoveryParser {

  private static final Logger LOG = LoggerFactory.getLogger(RecoveryParser.class);

  private final DAGAppMaster dagAppMaster;
  private final FileSystem recoveryFS;
  private final Path recoveryDataDir;
  private final Path currentAttemptRecoveryDataDir;
  private final int recoveryBufferSize;
  private final int currentAttemptId;

  public RecoveryParser(DAGAppMaster dagAppMaster,
      FileSystem recoveryFS,
      Path recoveryDataDir,
      int currentAttemptId) throws IOException {
    this.dagAppMaster = dagAppMaster;
    this.recoveryFS = recoveryFS;
    this.recoveryDataDir = recoveryDataDir;
    this.currentAttemptId = currentAttemptId;
    this.currentAttemptRecoveryDataDir = TezCommonUtils.getAttemptRecoveryPath(recoveryDataDir,
        currentAttemptId);
    recoveryBufferSize = dagAppMaster.getConfig().getInt(
        TezConfiguration.DAG_RECOVERY_FILE_IO_BUFFER_SIZE,
        TezConfiguration.DAG_RECOVERY_FILE_IO_BUFFER_SIZE_DEFAULT);
    this.recoveryFS.mkdirs(currentAttemptRecoveryDataDir);
  }

  public static class DAGRecoveryData {

    public TezDAGID recoveredDagID = null;
    public DAGImpl recoveredDAG = null;
    public DAGState dagState = null;
    public boolean isCompleted = false;
    public boolean nonRecoverable = false;
    public boolean isSessionStopped = false;
    public String reason = null;
    public Map<String, LocalResource> cumulativeAdditionalResources = null;
    public List<URL> additionalUrlsForClasspath = null;

    public Map<TezVertexID, VertexRecoveryData> vertexRecoveryDataMap =
        new HashMap<TezVertexID, RecoveryParser.VertexRecoveryData>();
    private DAGInitializedEvent dagInitedEvent;
    private DAGStartedEvent dagStartedEvent;
    private DAGFinishedEvent dagFinishedEvent;

    private Map<TezVertexID, Boolean> vertexCommitStatus =
        new HashMap<TezVertexID, Boolean>();
    private Map<String, Boolean> vertexGroupCommitStatus =
        new HashMap<String, Boolean>();
    private Map<TezVertexID, Boolean> vertexGroupMemberCommitStatus =
        new HashMap<TezVertexID, Boolean>();

    public DAGRecoveryData(DAGSummaryData dagSummaryData) {
      if (dagSummaryData.completed) {
        this.isCompleted = true;
        this.dagState = dagSummaryData.dagState;
      }
      dagSummaryData.checkRecoverableSummary();
      this.nonRecoverable = dagSummaryData.nonRecoverable;
      this.reason = dagSummaryData.reason;
      this.vertexCommitStatus = dagSummaryData.vertexCommitStatus;
      this.vertexGroupCommitStatus = dagSummaryData.vertexGroupCommitStatus;
      this.vertexGroupMemberCommitStatus = dagSummaryData.vertexGroupMemberCommitStatus;
    }

    // DAG is not recoverable if vertex has committer and has completed the commit (based on summary recovery events)
    // but its full recovery events are not seen. (based on non-summary recovery events)
    // Unrecoverable reason: vertex is committed we cannot rerun it and if vertex recovery events are not completed 
    // we cannot run other vertices that may depend on this one. So we have to abort.
    public void checkRecoverableNonSummary() {
      // It is OK without full recovering events if the dag is completed based on summary event.
      if (isCompleted) {
        return;
      }
      for (Map.Entry<TezVertexID, Boolean> entry : vertexCommitStatus.entrySet()) {
        // vertex has finished committing
        TezVertexID vertexId = entry.getKey();
        boolean commitFinished = entry.getValue();
        if(commitFinished
            && (!vertexRecoveryDataMap.containsKey(vertexId)
            || vertexRecoveryDataMap.get(vertexId).getVertexFinishedEvent() == null)) {
          this.nonRecoverable = true;
          this.reason = "Vertex has been committed, but its full recovery events are not seen, vertexId="
              + vertexId;
          return;
        }
      }
      for (Map.Entry<TezVertexID, Boolean> entry : vertexGroupMemberCommitStatus.entrySet()) {
        // vertex has finished committing
        TezVertexID vertexId = entry.getKey();
        boolean commitFinished = entry.getValue();
        if(commitFinished
            && (!vertexRecoveryDataMap.containsKey(vertexId)
            || vertexRecoveryDataMap.get(vertexId).getVertexFinishedEvent() == null)) {
          this.nonRecoverable = true;
          this.reason = "Vertex has been committed as member of vertex group"
              + ", but its full recovery events are not seen, vertexId=" + vertexId;
          return;
        }
      }
    }

    public DAGInitializedEvent getDAGInitializedEvent() {
      return dagInitedEvent;
    }

    public DAGStartedEvent getDAGStartedEvent() {
      return dagStartedEvent;
    }

    public DAGFinishedEvent getDAGFinishedEvent() {
      return dagFinishedEvent;
    }

    public boolean isVertexGroupCommitted(String groupName) {
      return vertexGroupCommitStatus.containsKey(groupName)
          && vertexGroupCommitStatus.get(groupName);
    }

    public VertexRecoveryData getVertexRecoveryData(TezVertexID vertexId) {
      return vertexRecoveryDataMap.get(vertexId);
    }

    public TaskRecoveryData getTaskRecoveryData(TezTaskID taskId) {
      VertexRecoveryData vertexRecoveryData = getVertexRecoveryData(taskId.getVertexID());
      if (vertexRecoveryData != null) {
        return vertexRecoveryData.taskRecoveryDataMap.get(taskId);
      } else {
        return null;
      }
    }

    public TaskAttemptRecoveryData getTaskAttemptRecoveryData(TezTaskAttemptID taId) {
      TaskRecoveryData taskRecoveryData = getTaskRecoveryData(taId.getTaskID());
      if (taskRecoveryData != null) {
        return taskRecoveryData.taRecoveryDataMap.get(taId);
      } else {
        return null;
      }
    }

    public VertexRecoveryData maybeCreateVertexRecoveryData(TezVertexID vertexId) {
      VertexRecoveryData vRecoveryData = vertexRecoveryDataMap.get(vertexId);
      if (vRecoveryData == null) {
        vRecoveryData = new VertexRecoveryData(vertexCommitStatus.containsKey(vertexId)
            ? vertexCommitStatus.get(vertexId) : false);
        vertexRecoveryDataMap.put(vertexId, vRecoveryData);
      }
      return vRecoveryData;
    }
  }

  private static void parseSummaryFile(FSDataInputStream inputStream)
      throws IOException {
    while (true) {
      RecoveryProtos.SummaryEventProto proto =
          RecoveryProtos.SummaryEventProto.parseDelimitedFrom(inputStream);
      if (proto == null) {
        LOG.info("Reached end of summary stream");
        break;
      }
      LOG.info("[SUMMARY]"
          + " dagId=" + proto.getDagId()
          + ", timestamp=" + proto.getTimestamp()
          + ", event=" + HistoryEventType.values()[proto.getEventType()]);
    }
  }

  private static HistoryEvent getNextEvent(CodedInputStream inputStream)
      throws IOException {
    boolean isAtEnd = inputStream.isAtEnd();
    if (isAtEnd) {
      return null;
    }
    int eventTypeOrdinal = -1;
    try {
      eventTypeOrdinal = inputStream.readFixed32();
    } catch (EOFException eof) {
      return null;
    }
    if (eventTypeOrdinal < 0 || eventTypeOrdinal >=
        HistoryEventType.values().length) {
      // Corrupt data
      // reached end
      throw new IOException("Corrupt data found when trying to read next event type"
          + ", eventTypeOrdinal=" + eventTypeOrdinal);
    }
    HistoryEventType eventType = HistoryEventType.values()[eventTypeOrdinal];
    HistoryEvent event;
    switch (eventType) {
      case AM_LAUNCHED:
        event = new AMLaunchedEvent();
        break;
      case AM_STARTED:
        event = new AMStartedEvent();
        break;
      case DAG_SUBMITTED:
        event = new DAGSubmittedEvent();
        break;
      case DAG_INITIALIZED:
        event = new DAGInitializedEvent();
        break;
      case DAG_STARTED:
        event = new DAGStartedEvent();
        break;
      case DAG_COMMIT_STARTED:
        event = new DAGCommitStartedEvent();
        break;
      case DAG_FINISHED:
        event = new DAGFinishedEvent();
        break;
      case DAG_KILL_REQUEST:
        event = new DAGKillRequestEvent();
        break;
      case CONTAINER_LAUNCHED:
        event = new ContainerLaunchedEvent();
        break;
      case CONTAINER_STOPPED:
        event = new ContainerStoppedEvent();
        break;
      case VERTEX_INITIALIZED:
        event = new VertexInitializedEvent();
        break;
      case VERTEX_CONFIGURE_DONE:
        event = new VertexConfigurationDoneEvent();
        break;
      case VERTEX_STARTED:
        event = new VertexStartedEvent();
        break;
      case VERTEX_COMMIT_STARTED:
        event = new VertexCommitStartedEvent();
        break;
      case VERTEX_GROUP_COMMIT_STARTED:
        event = new VertexGroupCommitStartedEvent();
        break;
      case VERTEX_GROUP_COMMIT_FINISHED:
        event = new VertexGroupCommitFinishedEvent();
        break;
      case VERTEX_FINISHED:
        event = new VertexFinishedEvent();
        break;
      case TASK_STARTED:
        event = new TaskStartedEvent();
        break;
      case TASK_FINISHED:
        event = new TaskFinishedEvent();
        break;
      case TASK_ATTEMPT_STARTED:
        event = new TaskAttemptStartedEvent();
        break;
      case TASK_ATTEMPT_FINISHED:
        event = new TaskAttemptFinishedEvent();
        break;
      default:
        throw new IOException("Invalid data found, unknown event type "
            + eventType);

    }
    try {
      event.fromProtoStream(inputStream);
    } catch (EOFException eof) {
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Parsed event from input stream"
          + ", eventType=" + eventType
          + ", event=" + event.toString());
    }
    return event;
  }

  public static List<HistoryEvent> parseDAGRecoveryFile(FSDataInputStream inputStream)
      throws IOException {
    List<HistoryEvent> historyEvents = new ArrayList<HistoryEvent>();
    CodedInputStream codedInputStream = CodedInputStream.newInstance(inputStream);
    codedInputStream.setSizeLimit(Integer.MAX_VALUE);
    while (true) {
      HistoryEvent historyEvent = getNextEvent(codedInputStream);
      if (historyEvent == null) {
        LOG.info("Reached end of stream");
        break;
      }
      LOG.debug("Read HistoryEvent, eventType={}, event={}", historyEvent.getEventType(), historyEvent);
      historyEvents.add(historyEvent);
    }
    return historyEvents;
  }

  public static List<HistoryEvent> readRecoveryEvents(TezConfiguration tezConf, ApplicationId appId,
      int attempt) throws IOException {
    Path tezSystemStagingDir =
        TezCommonUtils.getTezSystemStagingPath(tezConf, appId.toString());
    Path recoveryDataDir =
        TezCommonUtils.getRecoveryPath(tezSystemStagingDir, tezConf);
    FileSystem fs = tezSystemStagingDir.getFileSystem(tezConf);
    List<HistoryEvent> historyEvents = new ArrayList<HistoryEvent>();
    for (int i=1; i <= attempt; ++i) {
      Path currentAttemptRecoveryDataDir =
          TezCommonUtils.getAttemptRecoveryPath(recoveryDataDir, i);
      Path recoveryFilePath =
          new Path(currentAttemptRecoveryDataDir, appId.toString().replace(
              "application", "dag")
              + "_1" + TezConstants.DAG_RECOVERY_RECOVER_FILE_SUFFIX);
      if (fs.exists(recoveryFilePath)) {
        LOG.info("Read recovery file:" + recoveryFilePath);
        FSDataInputStream in = null;
        try {
          in = fs.open(recoveryFilePath);
          historyEvents.addAll(RecoveryParser.parseDAGRecoveryFile(in));
        } catch (IOException e) {
          throw e;
        } finally {
          if (in != null) {
            in.close();
          }
        }
      }
    }
    return historyEvents;
  }

  public static void main(String argv[]) throws IOException {
    // TODO clean up with better usage and error handling
    Configuration conf = new Configuration();
    String summaryPath = argv[0];
    List<String> dagPaths = new ArrayList<String>();
    if (argv.length > 1) {
      for (int i = 1; i < argv.length; ++i) {
        dagPaths.add(argv[i]);
      }
    }
    FileSystem fs = FileSystem.get(conf);
    LOG.info("Parsing Summary file " + summaryPath);
    parseSummaryFile(fs.open(new Path(summaryPath)));
    for (String dagPath : dagPaths) {
      LOG.info("Parsing DAG recovery file " + dagPath);
      List<HistoryEvent> historyEvents = parseDAGRecoveryFile(fs.open(new Path(dagPath)));
      for (HistoryEvent historyEvent : historyEvents) {
        LOG.info("Parsed event from recovery stream"
            + ", eventType=" + historyEvent.getEventType()
            + ", event=" + historyEvent);
      }
    }
  }

  private Path getSummaryPath(Path attemptRrecoveryDataDir) {
    return TezCommonUtils.getSummaryRecoveryPath(attemptRrecoveryDataDir);
  }

  private FSDataInputStream getSummaryStream(Path summaryPath)
      throws IOException {
    if (!recoveryFS.exists(summaryPath)) {
      return null;
    }
    return recoveryFS.open(summaryPath, recoveryBufferSize);
  }

  private Path getDAGRecoveryFilePath(Path recoveryDataDir,
      TezDAGID dagID) {
    return new Path(recoveryDataDir,
        dagID.toString() + TezConstants.DAG_RECOVERY_RECOVER_FILE_SUFFIX);
  }

  @VisibleForTesting
  DAGSummaryData getLastCompletedOrInProgressDAG(
      Map<TezDAGID, DAGSummaryData> dagSummaryDataMap) {
    DAGSummaryData inProgressDAG = null;
    DAGSummaryData lastCompletedDAG = null;
    for (Map.Entry<TezDAGID, DAGSummaryData> entry : dagSummaryDataMap.entrySet()) {
      if (!entry.getValue().completed) {
        if (inProgressDAG != null) {
          throw new RuntimeException("Multiple in progress DAGs seen"
              + ", dagId=" + inProgressDAG.dagId
              + ", dagId=" + entry.getKey());
        }
        inProgressDAG = entry.getValue();
      } else {
        if (lastCompletedDAG == null ||
            lastCompletedDAG.dagId.getId() < entry.getValue().dagId.getId()) {
          lastCompletedDAG = entry.getValue();
        }
      }
    }
    if (inProgressDAG == null) {
      return lastCompletedDAG;
    }
    return inProgressDAG;
  }

  @VisibleForTesting
  static class DAGSummaryData {

    final TezDAGID dagId;
    boolean completed = false;
    boolean dagCommitCompleted = true;
    boolean nonRecoverable = false;
    String reason;
    DAGState dagState;
    public Map<TezVertexID, Boolean> vertexCommitStatus =
        new HashMap<TezVertexID, Boolean>();
    public Map<String, Boolean> vertexGroupCommitStatus =
        new HashMap<String, Boolean>();
    public Map<TezVertexID, Boolean> vertexGroupMemberCommitStatus =
        new HashMap<TezVertexID, Boolean>();

    DAGSummaryData(TezDAGID dagId) {
      this.dagId = dagId;
    }

    void handleSummaryEvent(SummaryEventProto proto) throws IOException {
      HistoryEventType eventType =
          HistoryEventType.values()[proto.getEventType()];
      switch (eventType) {
        case DAG_SUBMITTED:
          completed = false;
          DAGSubmittedEvent dagSubmittedEvent = new DAGSubmittedEvent();
          dagSubmittedEvent.fromSummaryProtoStream(proto);
          break;
        case DAG_FINISHED:
          completed = true;
          dagCommitCompleted = true;
          DAGFinishedEvent dagFinishedEvent = new DAGFinishedEvent();
          dagFinishedEvent.fromSummaryProtoStream(proto);
          dagState = dagFinishedEvent.getState();
          break;
        case DAG_KILL_REQUEST:
          DAGKillRequestEvent killRequestEvent = new DAGKillRequestEvent();
          killRequestEvent.fromSummaryProtoStream(proto);
          break;
        case DAG_COMMIT_STARTED:
          dagCommitCompleted = false;
          break;
        case VERTEX_COMMIT_STARTED:
          VertexCommitStartedEvent vertexCommitStartedEvent =
              new VertexCommitStartedEvent();
          vertexCommitStartedEvent.fromSummaryProtoStream(proto);
          vertexCommitStatus.put(
              vertexCommitStartedEvent.getVertexID(), false);
          break;
        case VERTEX_FINISHED:
          VertexFinishedEvent vertexFinishedEvent =
              new VertexFinishedEvent();
          vertexFinishedEvent.fromSummaryProtoStream(proto);
          if (vertexCommitStatus.containsKey(vertexFinishedEvent.getVertexID())) {
            vertexCommitStatus.put(
                vertexFinishedEvent.getVertexID(), true);
          }
          break;
        case VERTEX_GROUP_COMMIT_STARTED:
          VertexGroupCommitStartedEvent vertexGroupCommitStartedEvent =
              new VertexGroupCommitStartedEvent();
          vertexGroupCommitStartedEvent.fromSummaryProtoStream(proto);
          vertexGroupCommitStatus.put(
              vertexGroupCommitStartedEvent.getVertexGroupName(), false);
          for (TezVertexID member : vertexGroupCommitStartedEvent.getVertexIds()) {
            vertexGroupMemberCommitStatus.put(member, false);
          }
          break;
        case VERTEX_GROUP_COMMIT_FINISHED:
          VertexGroupCommitFinishedEvent vertexGroupCommitFinishedEvent =
              new VertexGroupCommitFinishedEvent();
          vertexGroupCommitFinishedEvent.fromSummaryProtoStream(proto);
          vertexGroupCommitStatus.put(
              vertexGroupCommitFinishedEvent.getVertexGroupName(), true);
          for (TezVertexID member : vertexGroupCommitFinishedEvent.getVertexIds()) {
            vertexGroupMemberCommitStatus.put(member, true);
          }
          break;
        default:
          String message = "Found invalid summary event that was not handled"
              + ", eventType=" + eventType.name();
          throw new IOException(message);
      }
    }

    // Check whether DAG is recoverable based on DAGSummaryData
    //  1. Whether vertex is in the middle of committing
    //  2. Whether vertex group is in the middle of committing
    private void checkRecoverableSummary() {
      if (!dagCommitCompleted) {
        this.nonRecoverable = true;
        this.reason = "DAG Commit was in progress, not recoverable"
            + ", dagId=" + dagId;
      }
      if (!vertexCommitStatus.isEmpty()) {
        for (Entry<TezVertexID, Boolean> entry : vertexCommitStatus.entrySet()) {
          if (!(entry.getValue().booleanValue())) {
            this.nonRecoverable = true;
            this.reason = "Vertex Commit was in progress, not recoverable"
                + ", dagId=" + dagId
                + ", vertexId=" + entry.getKey();
          }
        }
      }
      if (!vertexGroupCommitStatus.isEmpty()) {
        for (Entry<String, Boolean> entry : vertexGroupCommitStatus.entrySet()) {
          if (!(entry.getValue().booleanValue())) {
            this.nonRecoverable = true;
            this.reason = "Vertex Group Commit was in progress, not recoverable"
                + ", dagId=" + dagId
                + ", vertexGroup=" + entry.getKey();
          }
        }
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("dagId=").append(dagId);
      sb.append(", dagCompleted=").append(completed);
      if (!vertexCommitStatus.isEmpty()) {
        sb.append(", vertexCommitStatuses=[");
        for (Entry<TezVertexID, Boolean> entry : vertexCommitStatus.entrySet()) {
          sb.append("{ vertexId=").append(entry.getKey())
              .append(", committed=").append(entry.getValue()).append("}, ");
        }
        sb.append("]");
      }
      if (!vertexGroupCommitStatus.isEmpty()) {
        sb.append(", vertexGroupCommitStatuses=[");
        for (Entry<String, Boolean> entry : vertexGroupCommitStatus.entrySet()) {
          sb.append("{ vertexGroup=").append(entry.getKey())
              .append(", committed=").append(entry.getValue()).append("}, ");
        }
        sb.append("]");
      }
      return sb.toString();
    }
  }

  private List<Path> getSummaryFiles() throws IOException {
    List<Path> summaryFiles = new ArrayList<Path>();
    for (int i = 1; i < currentAttemptId; ++i) {
      Path attemptPath = TezCommonUtils.getAttemptRecoveryPath(recoveryDataDir, i);
      Path fatalErrorOccurred = new Path(attemptPath,
          RecoveryService.RECOVERY_FATAL_OCCURRED_DIR);
      if (recoveryFS.exists(fatalErrorOccurred)) {
        throw new IOException("Found that a fatal error occurred in"
            + " recovery during previous attempt, foundFile="
            + fatalErrorOccurred.toString());
      }
      Path summaryFile = getSummaryPath(attemptPath);
      if (recoveryFS.exists(summaryFile)) {
        summaryFiles.add(summaryFile);
      }
    }
    return summaryFiles;
  }

  private List<Path> getDAGRecoveryFiles(TezDAGID dagId) throws IOException {
    List<Path> recoveryFiles = new ArrayList<Path>();
    for (int i = 1; i < currentAttemptId; ++i) {
      Path attemptPath = TezCommonUtils.getAttemptRecoveryPath(recoveryDataDir, i);
      Path recoveryFile = getDAGRecoveryFilePath(attemptPath, dagId);
      if (recoveryFS.exists(recoveryFile)) {
        recoveryFiles.add(recoveryFile);
      }
    }
    return recoveryFiles;
  }

  /**
   * 1. Read Summary Recovery file and build DAGSummaryData
   *    Check whether it is recoverable based on the summary file (whether dag is 
   *    in the middle of committing)
   * 2. Read the non-Summary Recovery file and build DAGRecoveryData
   *    Check whether it is recoverable based on both the summary file and non-summary file
   *    (whether vertex has completed its committing, but its full non-summary recovery events are not seen)
   * @return DAGRecoveryData
   * @throws IOException
   */
  public DAGRecoveryData parseRecoveryData() throws IOException {
    int dagCounter = 0;
    Map<TezDAGID, DAGSummaryData> dagSummaryDataMap =
        new HashMap<TezDAGID, DAGSummaryData>();
    List<Path> summaryFiles = getSummaryFiles();
    LOG.debug("SummaryFile size:" + summaryFiles.size());
    for (Path summaryFile : summaryFiles) {
      FileStatus summaryFileStatus = recoveryFS.getFileStatus(summaryFile);
      LOG.info("Parsing summary file"
          + ", path=" + summaryFile.toString()
          + ", len=" + summaryFileStatus.getLen()
          + ", lastModTime=" + summaryFileStatus.getModificationTime());
      FSDataInputStream summaryStream = getSummaryStream(
          summaryFile);
      while (true) {
        RecoveryProtos.SummaryEventProto proto;
        try {
          proto = RecoveryProtos.SummaryEventProto.parseDelimitedFrom(summaryStream);
          if (proto == null) {
            LOG.info("Reached end of summary stream");
            break;
          }
        } catch (EOFException eof) {
          LOG.info("Reached end of summary stream");
          break;
        }
        HistoryEventType eventType =
            HistoryEventType.values()[proto.getEventType()];
        if (LOG.isDebugEnabled()) {
          LOG.debug("[RECOVERY SUMMARY]"
              + " dagId=" + proto.getDagId()
              + ", timestamp=" + proto.getTimestamp()
              + ", event=" + eventType);
        }
        TezDAGID dagId;
        try {
          dagId = TezDAGID.fromString(proto.getDagId());
        } catch (IllegalArgumentException e) {
          throw new IOException("Invalid dagId, summary records may be corrupted", e);
        }
        if (dagCounter < dagId.getId()) {
          dagCounter = dagId.getId();
        }
        if (!dagSummaryDataMap.containsKey(dagId)) {
          dagSummaryDataMap.put(dagId, new DAGSummaryData(dagId));
        }
        try {
          dagSummaryDataMap.get(dagId).handleSummaryEvent(proto);
        } catch (Exception e) {
          // any exception when parsing protobuf
          throw new IOException("Error when parsing summary event proto", e);
        }
      }
      summaryStream.close();
    }

    // Set counter for next set of DAGs & update dagNames Set in DAGAppMaster
    dagAppMaster.setDAGCounter(dagCounter);
    for (DAGSummaryData dagSummaryData: dagSummaryDataMap.values()){
      dagAppMaster.dagIDs.add(dagSummaryData.dagId.toString());
    }

    DAGSummaryData lastInProgressDAGData =
        getLastCompletedOrInProgressDAG(dagSummaryDataMap);
    if (lastInProgressDAGData == null) {
      LOG.info("Nothing to recover as no uncompleted/completed DAGs found");
      return null;
    }
    TezDAGID lastInProgressDAG = lastInProgressDAGData.dagId;
    if (lastInProgressDAG == null) {
      LOG.info("Nothing to recover as no uncompleted/completed DAGs found");
      return null;
    }

    LOG.info("Checking if DAG is in recoverable state"
        + ", dagId=" + lastInProgressDAGData.dagId);

    final DAGRecoveryData recoveredDAGData = new DAGRecoveryData(lastInProgressDAGData);
    List<Path> dagRecoveryFiles = getDAGRecoveryFiles(lastInProgressDAG);
    boolean skipAllOtherEvents = false;
    Path lastRecoveryFile = null;
    // read the non summary events even when it is nonrecoverable. (Just read the DAGSubmittedEvent
    // to create the DAGImpl)
    for (Path dagRecoveryFile : dagRecoveryFiles) {
      if (skipAllOtherEvents) {
        LOG.warn("Other recovery files will be skipped due to error in the previous recovery file"
            + lastRecoveryFile);
        break;
      }
      FileStatus fileStatus = recoveryFS.getFileStatus(dagRecoveryFile);
      lastRecoveryFile = dagRecoveryFile;
      LOG.info("Trying to recover dag from recovery file"
          + ", dagId=" + lastInProgressDAG.toString()
          + ", dagRecoveryFile=" + dagRecoveryFile
          + ", len=" + fileStatus.getLen());
      FSDataInputStream dagRecoveryStream = recoveryFS.open(dagRecoveryFile, recoveryBufferSize);
      CodedInputStream codedInputStream = CodedInputStream.newInstance(dagRecoveryStream);
      codedInputStream.setSizeLimit(Integer.MAX_VALUE);
      while (true) {
        HistoryEvent event;
        try {
          event = getNextEvent(codedInputStream);
          if (event == null) {
            LOG.info("Reached end of dag recovery stream");
            break;
          }
        } catch (EOFException eof) {
          LOG.info("Reached end of dag recovery stream");
          break;
        } catch (IOException ioe) {
          LOG.warn("Corrupt data found when trying to read next event", ioe);
          break;
        }
        if (skipAllOtherEvents) {
          // hit an error - skip reading other events
          break;
        }

        HistoryEventType eventType = event.getEventType();
        LOG.info("Recovering from event"
            + ", eventType=" + eventType
            + ", event=" + event.toString());
        switch (eventType) {
          case DAG_SUBMITTED:
            DAGSubmittedEvent submittedEvent = (DAGSubmittedEvent) event;
            recoveredDAGData.recoveredDAG = dagAppMaster.createDAG(submittedEvent.getDAGPlan(),
                lastInProgressDAG);
            recoveredDAGData.cumulativeAdditionalResources = submittedEvent
              .getCumulativeAdditionalLocalResources();
            recoveredDAGData.recoveredDagID = recoveredDAGData.recoveredDAG.getID();
            dagAppMaster.setCurrentDAG(recoveredDAGData.recoveredDAG);
            if (recoveredDAGData.nonRecoverable) {
              skipAllOtherEvents = true;
            }
            break;
          case DAG_INITIALIZED:
            recoveredDAGData.dagInitedEvent = (DAGInitializedEvent)event;
            break;
          case DAG_STARTED:
            recoveredDAGData.dagStartedEvent= (DAGStartedEvent)event;
            break;
          case DAG_FINISHED:
            recoveredDAGData.dagFinishedEvent = (DAGFinishedEvent)event;
            skipAllOtherEvents = true;
            break; 
          case DAG_COMMIT_STARTED:
          case VERTEX_GROUP_COMMIT_STARTED:
          case VERTEX_GROUP_COMMIT_FINISHED: 
          case CONTAINER_LAUNCHED:
          {
            // Nothing to do for now
            break;
          }
          case DAG_KILL_REQUEST:
          {
            break;
          }
          case VERTEX_INITIALIZED:

          {
            VertexInitializedEvent vertexInitEvent = (VertexInitializedEvent)event;
            VertexRecoveryData vertexRecoveryData = recoveredDAGData.maybeCreateVertexRecoveryData(vertexInitEvent.getVertexID());
            vertexRecoveryData.vertexInitedEvent = vertexInitEvent;
            break;
          }
          case VERTEX_CONFIGURE_DONE:
          {
            VertexConfigurationDoneEvent reconfigureDoneEvent = (VertexConfigurationDoneEvent)event;
            VertexRecoveryData vertexRecoveryData = recoveredDAGData.maybeCreateVertexRecoveryData(reconfigureDoneEvent.getVertexID());
            vertexRecoveryData.vertexConfigurationDoneEvent = reconfigureDoneEvent;
            break;
          }
          case VERTEX_STARTED:
          {
            VertexStartedEvent vertexStartedEvent = (VertexStartedEvent)event;
            VertexRecoveryData vertexRecoveryData = recoveredDAGData.vertexRecoveryDataMap.get(vertexStartedEvent.getVertexID());
            Preconditions.checkArgument(vertexRecoveryData != null, "No VertexInitializedEvent before VertexStartedEvent");
            vertexRecoveryData.vertexStartedEvent = vertexStartedEvent;
            break;
          }
          case VERTEX_COMMIT_STARTED:
          {
            break;
          }
          case VERTEX_FINISHED:
          {
            VertexFinishedEvent vertexFinishedEvent = (VertexFinishedEvent)event;
            VertexRecoveryData vertexRecoveryData = recoveredDAGData.maybeCreateVertexRecoveryData(vertexFinishedEvent.getVertexID());
            vertexRecoveryData.vertexFinishedEvent = vertexFinishedEvent;
            break;
          }
          case TASK_STARTED:
          {
            TaskStartedEvent taskStartedEvent = (TaskStartedEvent) event;
            VertexRecoveryData vertexRecoveryData = recoveredDAGData.vertexRecoveryDataMap.get(taskStartedEvent.getTaskID().getVertexID());
            Preconditions.checkArgument(vertexRecoveryData != null,
                "Invalid TaskStartedEvent, its vertex does not exist:" + taskStartedEvent.getTaskID().getVertexID());
            TaskRecoveryData taskRecoveryData = vertexRecoveryData.maybeCreateTaskRecoveryData(taskStartedEvent.getTaskID());
            taskRecoveryData.taskStartedEvent = taskStartedEvent;
            break;
          }
          case TASK_FINISHED:
          {
            TaskFinishedEvent taskFinishedEvent = (TaskFinishedEvent) event;
            VertexRecoveryData vertexRecoveryData = recoveredDAGData.vertexRecoveryDataMap.get(taskFinishedEvent.getTaskID().getVertexID());
            Preconditions.checkArgument(vertexRecoveryData != null,
                "Invalid TaskFinishedEvent, its vertex does not exist:" + taskFinishedEvent.getTaskID().getVertexID());
            TaskRecoveryData taskRecoveryData = vertexRecoveryData.maybeCreateTaskRecoveryData(taskFinishedEvent.getTaskID());
            taskRecoveryData.taskFinishedEvent = taskFinishedEvent;
            break;
          }
          case TASK_ATTEMPT_STARTED:
          {
            TaskAttemptStartedEvent taStartedEvent = (TaskAttemptStartedEvent)event;
            VertexRecoveryData vertexRecoveryData = recoveredDAGData.vertexRecoveryDataMap.get(
                taStartedEvent.getTaskAttemptID().getTaskID().getVertexID());
            Preconditions.checkArgument(vertexRecoveryData != null,
                "Invalid TaskAttemptStartedEvent, its vertexId does not exist, taId=" + taStartedEvent.getTaskAttemptID());
            TaskRecoveryData taskRecoveryData = vertexRecoveryData.taskRecoveryDataMap
                .get(taStartedEvent.getTaskAttemptID().getTaskID());
            Preconditions.checkArgument(taskRecoveryData != null,
                "Invalid TaskAttemptStartedEvent, its taskId does not exist, taId=" + taStartedEvent.getTaskAttemptID());
            TaskAttemptRecoveryData taRecoveryData = taskRecoveryData.maybeCreateTaskAttemptRecoveryData(taStartedEvent.getTaskAttemptID());
            taRecoveryData.taStartedEvent = taStartedEvent;
            break;
          }
          case TASK_ATTEMPT_FINISHED:
          {
            TaskAttemptFinishedEvent taFinishedEvent = (TaskAttemptFinishedEvent)event;
            VertexRecoveryData vertexRecoveryData = recoveredDAGData.vertexRecoveryDataMap.get(
                taFinishedEvent.getTaskAttemptID().getTaskID().getVertexID());
            Preconditions.checkArgument(vertexRecoveryData != null,
                "Invalid TaskAttemtFinishedEvent, its vertexId does not exist, taId=" + taFinishedEvent.getTaskAttemptID());
            TaskRecoveryData taskRecoveryData = vertexRecoveryData.taskRecoveryDataMap
                .get(taFinishedEvent.getTaskAttemptID().getTaskID());
            Preconditions.checkArgument(taskRecoveryData != null,
                "Invalid TaskAttemptFinishedEvent, its taskId does not exist, taId=" + taFinishedEvent.getTaskAttemptID());
            TaskAttemptRecoveryData taRecoveryData = taskRecoveryData.maybeCreateTaskAttemptRecoveryData(taFinishedEvent.getTaskAttemptID());
            taRecoveryData.taFinishedEvent = taFinishedEvent;
            break;
          }
          default:
            throw new RuntimeException("Invalid data found, unknown event type "
                + eventType);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("[DAG RECOVERY]"
              + " dagId=" + lastInProgressDAG
              + ", eventType=" + eventType
              + ", event=" + event.toString());
        }
      }
      dagRecoveryStream.close();
    }
    recoveredDAGData.checkRecoverableNonSummary();
    return recoveredDAGData;
  }

  public static class VertexRecoveryData {

    private VertexInitializedEvent vertexInitedEvent;
    private VertexConfigurationDoneEvent vertexConfigurationDoneEvent;
    private VertexStartedEvent vertexStartedEvent;
    private VertexFinishedEvent vertexFinishedEvent;
    private Map<TezTaskID, TaskRecoveryData> taskRecoveryDataMap =
        new HashMap<TezTaskID, RecoveryParser.TaskRecoveryData>();
    private boolean commited;

    @VisibleForTesting
    public VertexRecoveryData(VertexInitializedEvent vertexInitedEvent,
        VertexConfigurationDoneEvent vertexReconfigureDoneEvent,
        VertexStartedEvent vertexStartedEvent,
        VertexFinishedEvent vertexFinishedEvent,
        Map<TezTaskID, TaskRecoveryData> taskRecoveryDataMap, boolean commited) {
      super();
      this.vertexInitedEvent = vertexInitedEvent;
      this.vertexConfigurationDoneEvent = vertexReconfigureDoneEvent;
      this.vertexStartedEvent = vertexStartedEvent;
      this.vertexFinishedEvent = vertexFinishedEvent;
      this.taskRecoveryDataMap = taskRecoveryDataMap;
      this.commited = commited;
    }

    public VertexRecoveryData(boolean committed) {
      this.commited = committed;
    }
 
    public VertexInitializedEvent getVertexInitedEvent() {
      return vertexInitedEvent;
    }

    public VertexStartedEvent getVertexStartedEvent() {
      return vertexStartedEvent;
    }

    public VertexFinishedEvent getVertexFinishedEvent() {
      return vertexFinishedEvent;
    }

    public VertexConfigurationDoneEvent getVertexConfigurationDoneEvent() {
      return vertexConfigurationDoneEvent;
    }

    public boolean isReconfigureDone() {
      return vertexConfigurationDoneEvent != null;
    }

    public boolean isVertexInited() {
      return vertexInitedEvent != null;
    }

    public boolean shouldSkipInit() {
      return vertexInitedEvent != null && vertexConfigurationDoneEvent != null;
    }

    public boolean isVertexStarted() {
      return vertexStartedEvent != null;
    }

    public boolean isVertexSucceeded() {
      if (vertexFinishedEvent == null) {
        return false;
      }
      return vertexFinishedEvent.getState().equals(VertexState.SUCCEEDED);
    }

    public boolean isVertexFinished() {
      return vertexFinishedEvent != null;
    }

    public boolean isVertexCommitted() {
      return this.commited;
    }

    public TaskRecoveryData getTaskRecoveryData(TezTaskID taskId) {
      return taskRecoveryDataMap.get(taskId);
    }

    public TaskRecoveryData maybeCreateTaskRecoveryData(TezTaskID taskId) {
      TaskRecoveryData taskRecoveryData = taskRecoveryDataMap.get(taskId);
      if (taskRecoveryData == null) {
        taskRecoveryData = new TaskRecoveryData();
        taskRecoveryDataMap.put(taskId, taskRecoveryData);
      }
      return taskRecoveryData;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("VertexInitedEvent=" + vertexInitedEvent);
      builder.append("");
      return builder.toString();
    }
  }

  public static class TaskRecoveryData {

    private TaskStartedEvent taskStartedEvent;
    private TaskFinishedEvent taskFinishedEvent;
    private Map<TezTaskAttemptID, TaskAttemptRecoveryData> taRecoveryDataMap =
        new HashMap<TezTaskAttemptID, RecoveryParser.TaskAttemptRecoveryData>();

    public TaskRecoveryData() {

    }

    @VisibleForTesting
    public TaskRecoveryData(TaskStartedEvent taskStartedEvent,
        TaskFinishedEvent taskFinishedEvent,
        Map<TezTaskAttemptID, TaskAttemptRecoveryData> taRecoveryDataMap) {
      super();
      this.taskStartedEvent = taskStartedEvent;
      this.taskFinishedEvent = taskFinishedEvent;
      this.taRecoveryDataMap = taRecoveryDataMap;
    }

    public TaskStartedEvent getTaskStartedEvent() {
      return taskStartedEvent;
    }

    public TaskFinishedEvent getTaskFinishedEvent() {
      return taskFinishedEvent;
    }

    public boolean isTaskStarted() {
      return getTaskStartedEvent() != null;
    }

    public boolean isTaskAttemptSucceeded(TezTaskAttemptID taId) {
      TaskAttemptRecoveryData taRecoveryData = taRecoveryDataMap.get(taId);
      return taRecoveryData == null ? false : taRecoveryData.isTaskAttemptSucceeded();
    }

    public TaskAttemptRecoveryData maybeCreateTaskAttemptRecoveryData(TezTaskAttemptID taId) {
      TaskAttemptRecoveryData taRecoveryData = taRecoveryDataMap.get(taId);
      if (taRecoveryData == null) {
        taRecoveryData = new TaskAttemptRecoveryData();
        taRecoveryDataMap.put(taId, taRecoveryData);
      }
      return taRecoveryData;
    }
  }

  public static class TaskAttemptRecoveryData {

    private TaskAttemptStartedEvent taStartedEvent;
    private TaskAttemptFinishedEvent taFinishedEvent;

    public TaskAttemptRecoveryData() {

    }

    @VisibleForTesting
    public TaskAttemptRecoveryData(TaskAttemptStartedEvent taStartedEvent,
        TaskAttemptFinishedEvent taFinishedEvent) {
      super();
      this.taStartedEvent = taStartedEvent;
      this.taFinishedEvent = taFinishedEvent;
    }

    public TaskAttemptStartedEvent getTaskAttemptStartedEvent() {
      return taStartedEvent;
    }

    public TaskAttemptFinishedEvent getTaskAttemptFinishedEvent() {
      return taFinishedEvent;
    }

    public boolean isTaskAttemptSucceeded() {
      TaskAttemptFinishedEvent taFinishedEvent = getTaskAttemptFinishedEvent();
      return taFinishedEvent == null ?
          false : taFinishedEvent.getState() == TaskAttemptState.SUCCEEDED;
    }
  }
}
