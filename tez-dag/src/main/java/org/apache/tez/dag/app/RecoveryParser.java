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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.AMLaunchedEvent;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;
import org.apache.tez.dag.history.events.DAGCommitStartedEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexCommitStartedEvent;
import org.apache.tez.dag.history.events.VertexDataMovementEventsGeneratedEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexParallelismUpdatedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.recovery.records.RecoveryProtos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class RecoveryParser {

  private static final Log LOG = LogFactory.getLog(RecoveryParser.class);

  private final DAGAppMaster dagAppMaster;
  private final FileSystem recoveryFS;
  private final Path recoveryDataDir;
  private final Path currentAttemptRecoveryDataDir;
  private final int recoveryBufferSize;
  private final int currentAttemptId;

  private static final String dataRecoveredFileFlag = "dataRecovered";

  public RecoveryParser(DAGAppMaster dagAppMaster,
      FileSystem recoveryFS,
      Path recoveryDataDir,
      int currentAttemptId) {
    this.dagAppMaster = dagAppMaster;
    this.recoveryFS = recoveryFS;
    this.recoveryDataDir = recoveryDataDir;
    this.currentAttemptId = currentAttemptId;
    this.currentAttemptRecoveryDataDir =
        getAttemptRecoveryDataDir(recoveryDataDir, currentAttemptId);
    recoveryBufferSize = dagAppMaster.getConfig().getInt(
        TezConfiguration.DAG_RECOVERY_FILE_IO_BUFFER_SIZE,
        TezConfiguration.DAG_RECOVERY_FILE_IO_BUFFER_SIZE_DEFAULT);
  }

  private static void parseSummaryFile(FSDataInputStream inputStream)
      throws IOException {
    while (inputStream.available() > 0) {
      RecoveryProtos.SummaryEventProto proto =
          RecoveryProtos.SummaryEventProto.parseDelimitedFrom(inputStream);
      LOG.info("[SUMMARY]"
          + " dagId=" + proto.getDagId()
          + ", timestamp=" + proto.getTimestamp()
          + ", event=" + HistoryEventType.values()[proto.getEventType()]);
    }
  }

  private static HistoryEvent getNextEvent(FSDataInputStream inputStream)
      throws IOException {
    int eventTypeOrdinal = inputStream.readInt();
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
      case CONTAINER_LAUNCHED:
        event = new ContainerLaunchedEvent();
        break;
      case VERTEX_INITIALIZED:
        event = new VertexInitializedEvent();
        break;
      case VERTEX_STARTED:
        event = new VertexStartedEvent();
        break;
      case VERTEX_PARALLELISM_UPDATED:
        event = new VertexParallelismUpdatedEvent();
        break;
      case VERTEX_COMMIT_STARTED:
        event = new VertexCommitStartedEvent();
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
      case VERTEX_DATA_MOVEMENT_EVENTS_GENERATED:
        event = new VertexDataMovementEventsGeneratedEvent();
        break;
      default:
        throw new IOException("Invalid data found, unknown event type "
            + eventType);

    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Parsing event from input stream"
          + ", eventType=" + eventType);
    }
    event.fromProtoStream(inputStream);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Parsed event from input stream"
          + ", eventType=" + eventType
          + ", event=" + event.toString());
    }
    return event;
  }





  private static void parseDAGRecoveryFile(FSDataInputStream inputStream)
      throws IOException {
    while (inputStream.available() > 0) {
      HistoryEvent historyEvent = getNextEvent(inputStream);
      LOG.info("Parsed event from recovery stream"
          + ", eventType=" + historyEvent.getEventType()
          + ", event=" + historyEvent);
    }
  }

  private Path getAttemptRecoveryDataDir(Path recoveryDataDir,
      int attemptId) {
    return new Path(recoveryDataDir, Integer.toString(attemptId));
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
      parseDAGRecoveryFile(fs.open(new Path(dagPath)));
    }
  }

  private Path getSummaryPath(Path recoveryDataDir) {
    return new Path(recoveryDataDir,
        dagAppMaster.getAttemptID().getApplicationId().toString()
        + TezConfiguration.DAG_RECOVERY_SUMMARY_FILE_SUFFIX);
  }

  private FSDataOutputStream getSummaryOutputStream(Path summaryPath)
      throws IOException {
    return recoveryFS.create(summaryPath, true, recoveryBufferSize);
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
        dagID.toString() + TezConfiguration.DAG_RECOVERY_RECOVER_FILE_SUFFIX);
  }

  private FSDataInputStream getDAGRecoveryStream(Path recoveryDataDir,
      TezDAGID dagID)
      throws IOException {
    Path dagRecoveryPath = getDAGRecoveryFilePath(recoveryDataDir, dagID);
    if (!recoveryFS.exists(dagRecoveryPath)) {
      return null;
    }
    return recoveryFS.open(dagRecoveryPath, recoveryBufferSize);
  }

  private FSDataOutputStream getDAGRecoveryOutputStream(Path recoveryDataDir,
      TezDAGID dagID)
      throws IOException {
    Path dagRecoveryPath = new Path(recoveryDataDir,
        dagID.toString() + TezConfiguration.DAG_RECOVERY_RECOVER_FILE_SUFFIX);
    return recoveryFS.create(dagRecoveryPath, true, recoveryBufferSize);
  }

  private TezDAGID getLastInProgressDAG(Map<TezDAGID, Boolean> seenDAGs) {
    TezDAGID inProgressDAG = null;
    for (Map.Entry<TezDAGID, Boolean> entry : seenDAGs.entrySet()) {
      if (!entry.getValue().booleanValue()) {
        if (inProgressDAG != null) {
          throw new RuntimeException("Multiple in progress DAGs seen"
              + ", dagId=" + inProgressDAG
              + ", dagId=" + entry.getKey());
        }
        inProgressDAG = entry.getKey();
      }
    }
    return inProgressDAG;
  }

  private Path getPreviousAttemptRecoveryDataDir() {
    int foundPreviousAttempt = -1;
    for (int i = currentAttemptId - 1; i > 0; --i) {
      Path attemptPath = getAttemptRecoveryDataDir(recoveryDataDir, i);
      Path dataRecoveredFile = new Path(attemptPath, dataRecoveredFileFlag);
      try {
        if (recoveryFS.exists(dataRecoveredFile)) {
          foundPreviousAttempt = i;
          break;
        }
      } catch (IOException e) {
        LOG.warn("Exception when checking previous attempt dir for "
            + dataRecoveredFile.toString(), e);
      }
    }
    if (foundPreviousAttempt == -1) {
      LOG.info("Falling back to first attempt as no other recovered attempts"
          + " found");
      foundPreviousAttempt = 1;
    }

    return getAttemptRecoveryDataDir(recoveryDataDir, foundPreviousAttempt);
  }


  public DAG parseRecoveryData() throws IOException {
    Path previousAttemptRecoveryDataDir = getPreviousAttemptRecoveryDataDir();
    LOG.info("Using " + previousAttemptRecoveryDataDir.toString()
        + " for recovering data from previous attempt");
    if (!recoveryFS.exists(previousAttemptRecoveryDataDir)) {
      LOG.info("Nothing to recover as previous attempt data does not exist"
          + ", previousAttemptDir=" + previousAttemptRecoveryDataDir.toString());
      return null;
    }

    Path summaryPath = getSummaryPath(previousAttemptRecoveryDataDir);
    FSDataInputStream summaryStream = getSummaryStream(
        summaryPath);
    if (summaryStream == null) {
      LOG.info("Nothing to recover as summary file does not exist"
          + ", previousAttemptDir=" + previousAttemptRecoveryDataDir.toString()
          + ", summaryPath=" + summaryPath.toString());
      return null;
    }

    Path newSummaryPath = getSummaryPath(currentAttemptRecoveryDataDir);
    FSDataOutputStream newSummaryStream =
        getSummaryOutputStream(newSummaryPath);

    Map<TezDAGID, Boolean> seenDAGs = new TreeMap<TezDAGID, Boolean>();

    FileStatus summaryFileStatus = recoveryFS.getFileStatus(summaryPath);
    LOG.info("Parsing summary file"
        + ", path=" + summaryPath.toString()
        + ", len=" + summaryFileStatus.getLen()
        + ", lastModTime=" + summaryFileStatus.getModificationTime());

    int dagCounter = 0;
    while (summaryStream.available() > 0) {
      RecoveryProtos.SummaryEventProto proto =
          RecoveryProtos.SummaryEventProto.parseDelimitedFrom(summaryStream);
      HistoryEventType eventType =
          HistoryEventType.values()[proto.getEventType()];
      if (LOG.isDebugEnabled()) {
        LOG.debug("[RECOVERY SUMMARY]"
            + " dagId=" + proto.getDagId()
            + ", timestamp=" + proto.getTimestamp()
            + ", event=" + eventType);
      }
      TezDAGID dagId = TezDAGID.fromString(proto.getDagId());
      if (dagCounter < dagId.getId()) {
        dagCounter = dagId.getId();
      }
      if (eventType.equals(HistoryEventType.DAG_SUBMITTED)) {
        seenDAGs.put(dagId, false);
      } else if (eventType.equals(HistoryEventType.DAG_FINISHED)) {
        seenDAGs.put(dagId, true);
      }
      proto.writeDelimitedTo(newSummaryStream);
    }
    newSummaryStream.hsync();
    newSummaryStream.close();

    // Set counter for next set of DAGs
    dagAppMaster.setDAGCounter(dagCounter);

    TezDAGID lastInProgressDAG = getLastInProgressDAG(seenDAGs);
    if (lastInProgressDAG == null) {
      LOG.info("Nothing to recover as no uncompleted DAGs found");
      return null;
    }

    LOG.info("Trying to recover dag from recovery file"
        + ", dagId=" + lastInProgressDAG.toString()
        + ", dataDir=" + previousAttemptRecoveryDataDir
        + ", intoCurrentDir=" + currentAttemptRecoveryDataDir);

    FSDataInputStream dagRecoveryStream = getDAGRecoveryStream(
        previousAttemptRecoveryDataDir, lastInProgressDAG);
    if (dagRecoveryStream == null) {
      // Could not find data to recover
      // Error out
      throw new IOException("Could not find recovery data for last in progress DAG"
          + ", dagId=" + lastInProgressDAG);
    }

    DAGImpl recoveredDAG = null;

    LOG.info("Copying DAG data into Current Attempt directory"
        + ", filePath=" + getDAGRecoveryFilePath(currentAttemptRecoveryDataDir,
        lastInProgressDAG));
    FSDataOutputStream newDAGRecoveryStream =
        getDAGRecoveryOutputStream(currentAttemptRecoveryDataDir, lastInProgressDAG);

    while (dagRecoveryStream.available() > 0) {
      HistoryEvent event;
      try {
        event = getNextEvent(dagRecoveryStream);
      } catch (IOException ioe) {
        LOG.warn("Corrupt data found when trying to read next event", ioe);
        break;
      }
      if (event == null) {
        // reached end of data
        break;
      }
      HistoryEventType eventType = event.getEventType();
      switch (eventType) {
        case DAG_SUBMITTED:
        {
          LOG.info("Recovering from event"
              + ", eventType=" + eventType
              + ", event=" + event.toString());
          recoveredDAG = dagAppMaster.createDAG(((DAGSubmittedEvent) event).getDAGPlan(),
              lastInProgressDAG);
          dagAppMaster.setCurrentDAG(recoveredDAG);
          break;
        }
        case DAG_INITIALIZED:
        {
          LOG.info("Recovering from event"
              + ", eventType=" + eventType
              + ", event=" + event.toString());
          assert recoveredDAG != null;
          recoveredDAG.restoreFromEvent(event);
          break;
        }
        case DAG_STARTED:
        {
          LOG.info("Recovering from event"
              + ", eventType=" + eventType
              + ", event=" + event.toString());
          assert recoveredDAG != null;
          recoveredDAG.restoreFromEvent(event);
          break;
        }
        case DAG_COMMIT_STARTED:
        {
          LOG.info("Recovering from event"
              + ", eventType=" + eventType
              + ", event=" + event.toString());
          assert recoveredDAG != null;
          recoveredDAG.restoreFromEvent(event);
          break;
        }
        case DAG_FINISHED:
        {
          LOG.info("Recovering from event"
              + ", eventType=" + eventType
              + ", event=" + event.toString());
          // If this is seen, nothing to recover
          assert recoveredDAG != null;
          recoveredDAG.restoreFromEvent(event);
          return recoveredDAG;
        }
        case CONTAINER_LAUNCHED:
        {
          // Nothing to do?
          break;
        }
        case VERTEX_INITIALIZED:
        {
          LOG.info("Recovering from event"
              + ", eventType=" + eventType
              + ", event=" + event.toString());
          assert recoveredDAG != null;
          VertexInitializedEvent vEvent = (VertexInitializedEvent) event;
          Vertex v = recoveredDAG.getVertex(vEvent.getVertexID());
          v.restoreFromEvent(vEvent);
          break;
        }
        case VERTEX_STARTED:
        {
          LOG.info("Recovering from event"
              + ", eventType=" + eventType
              + ", event=" + event.toString());
          assert recoveredDAG != null;
          VertexStartedEvent vEvent = (VertexStartedEvent) event;
          Vertex v = recoveredDAG.getVertex(vEvent.getVertexID());
          v.restoreFromEvent(vEvent);
          break;
        }
        case VERTEX_PARALLELISM_UPDATED:
        {
          LOG.info("Recovering from event"
              + ", eventType=" + eventType
              + ", event=" + event.toString());
          assert recoveredDAG != null;
          VertexParallelismUpdatedEvent vEvent = (VertexParallelismUpdatedEvent) event;
          Vertex v = recoveredDAG.getVertex(vEvent.getVertexID());
          v.restoreFromEvent(vEvent);
          break;
        }
        case VERTEX_COMMIT_STARTED:
        {
          LOG.info("Recovering from event"
              + ", eventType=" + eventType
              + ", event=" + event.toString());
          assert recoveredDAG != null;
          VertexCommitStartedEvent vEvent = (VertexCommitStartedEvent) event;
          Vertex v = recoveredDAG.getVertex(vEvent.getVertexID());
          v.restoreFromEvent(vEvent);
          break;
        }
        case VERTEX_FINISHED:
        {
          LOG.info("Recovering from event"
              + ", eventType=" + eventType
              + ", event=" + event.toString());
          assert recoveredDAG != null;
          VertexFinishedEvent vEvent = (VertexFinishedEvent) event;
          Vertex v = recoveredDAG.getVertex(vEvent.getVertexID());
          v.restoreFromEvent(vEvent);
          break;
        }
        case TASK_STARTED:
        {
          LOG.info("Recovering from event"
              + ", eventType=" + eventType
              + ", event=" + event.toString());
          assert recoveredDAG != null;
          TaskStartedEvent tEvent = (TaskStartedEvent) event;
          Task task = recoveredDAG.getVertex(
              tEvent.getTaskID().getVertexID()).getTask(tEvent.getTaskID());
          task.restoreFromEvent(tEvent);
          break;
        }
        case TASK_FINISHED:
        {
          LOG.info("Recovering from event"
              + ", eventType=" + eventType
              + ", event=" + event.toString());
          assert recoveredDAG != null;
          TaskFinishedEvent tEvent = (TaskFinishedEvent) event;
          Task task = recoveredDAG.getVertex(
              tEvent.getTaskID().getVertexID()).getTask(tEvent.getTaskID());
          task.restoreFromEvent(tEvent);
          break;
        }
        case TASK_ATTEMPT_STARTED:
        {
          LOG.info("Recovering from event"
              + ", eventType=" + eventType
              + ", event=" + event.toString());
          assert recoveredDAG != null;
          TaskAttemptStartedEvent tEvent = (TaskAttemptStartedEvent) event;
          Task task =
              recoveredDAG.getVertex(
                  tEvent.getTaskAttemptID().getTaskID().getVertexID())
                      .getTask(tEvent.getTaskAttemptID().getTaskID());
          task.restoreFromEvent(tEvent);
          break;
        }
        case TASK_ATTEMPT_FINISHED:
        {
          LOG.info("Recovering from event"
              + ", eventType=" + eventType
              + ", event=" + event.toString());
          assert recoveredDAG != null;
          TaskAttemptFinishedEvent tEvent = (TaskAttemptFinishedEvent) event;
          Task task =
              recoveredDAG.getVertex(
                  tEvent.getTaskAttemptID().getTaskID().getVertexID())
                  .getTask(tEvent.getTaskAttemptID().getTaskID());
          task.restoreFromEvent(tEvent);
          break;
        }
        case VERTEX_DATA_MOVEMENT_EVENTS_GENERATED:
        {
          LOG.info("Recovering from event"
              + ", eventType=" + eventType
              + ", event=" + event.toString());
          assert recoveredDAG != null;
          VertexDataMovementEventsGeneratedEvent vEvent =
              (VertexDataMovementEventsGeneratedEvent) event;
          Vertex v = recoveredDAG.getVertex(vEvent.getVertexID());
          v.restoreFromEvent(vEvent);
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
      newDAGRecoveryStream.writeInt(eventType.ordinal());
      event.toProtoStream(newDAGRecoveryStream);
    }
    newDAGRecoveryStream.hsync();
    newDAGRecoveryStream.close();

    Path dataCopiedFlagPath = new Path(currentAttemptRecoveryDataDir,
        dataRecoveredFileFlag);
    LOG.info("Finished copying data from previous attempt into current attempt"
        + " - setting flag by creating file"
        + ", path=" + dataCopiedFlagPath.toString());
    FSDataOutputStream flagFile =
        recoveryFS.create(dataCopiedFlagPath, true, recoveryBufferSize);
    flagFile.writeInt(1);
    flagFile.hsync();
    flagFile.close();

    return recoveredDAG;
  }

}
