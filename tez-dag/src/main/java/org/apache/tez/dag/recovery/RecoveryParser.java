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

package org.apache.tez.dag.recovery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.AMLaunchedEvent;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexDataMovementEventsGeneratedEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.recovery.records.RecoveryProtos;

import java.io.IOException;

public class RecoveryParser {

  private static final Log LOG = LogFactory.getLog(RecoveryParser.class);

  Path recoveryDirectory;
  FileSystem recoveryDirFS;

  public RecoveryParser(Path recoveryDirectory, Configuration conf)
      throws IOException {
    this.recoveryDirectory = recoveryDirectory;
    recoveryDirFS = FileSystem.get(recoveryDirectory.toUri(), conf);

  }

  public void parse() throws IOException {
    RemoteIterator<LocatedFileStatus> locatedFilesStatus =
        recoveryDirFS.listFiles(recoveryDirectory, false);
    while (locatedFilesStatus.hasNext()) {
      LocatedFileStatus fileStatus = locatedFilesStatus.next();
      String fileName = fileStatus.getPath().getName();
      if (fileName.endsWith(TezConfiguration.DAG_RECOVERY_RECOVER_FILE_SUFFIX)) {
        FSDataInputStream inputStream =
            recoveryDirFS.open(fileStatus.getPath());
        LOG.info("Parsing DAG file " + fileName);
        parseDAGRecoveryFile(inputStream);
      } else if (fileName.endsWith(TezConfiguration.DAG_RECOVERY_SUMMARY_FILE_SUFFIX)) {
        FSDataInputStream inputStream =
            recoveryDirFS.open(fileStatus.getPath());
        LOG.info("Parsing Summary file " + fileName);
        parseSummaryFile(inputStream);
      } else {
        LOG.warn("Encountered unknown file in recovery dir, fileName="
            + fileName);
        continue;
      }
    }
  }

  private void parseSummaryFile(FSDataInputStream inputStream)
      throws IOException {
    int counter = 0;
    while (inputStream.available() > 0) {
      RecoveryProtos.SummaryEventProto proto =
          RecoveryProtos.SummaryEventProto.parseDelimitedFrom(inputStream);
      LOG.info("[SUMMARY]"
          + " dagId=" + proto.getDagId()
          + ", timestamp=" + proto.getTimestamp()
          + ", event=" + HistoryEventType.values()[proto.getEventType()]);
    }
  }

  private void parseDAGRecoveryFile(FSDataInputStream inputStream)
      throws IOException {
    int counter = 0;
    while (inputStream.available() > 0) {
      int eventTypeOrdinal = inputStream.read();
      if (eventTypeOrdinal < 0 || eventTypeOrdinal >=
          HistoryEventType.values().length) {
        // Corrupt data
        // reached end
        LOG.warn("Corrupt data found when trying to read next event type");
        break;
      }
      HistoryEventType eventType = HistoryEventType.values()[eventTypeOrdinal];
      HistoryEvent event = null;
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
      ++counter;
      LOG.info("Parsing event from input stream"
          + ", eventType=" + eventType
          + ", eventIndex=" + counter);
      event.fromProtoStream(inputStream);
      LOG.info("Parsed event from input stream"
          + ", eventType=" + eventType
          + ", eventIndex=" + counter
          + ", event=" + event.toString());
    }
  }

  public static void main(String argv[]) throws IOException {
    // TODO clean up with better usage and error handling
    Configuration conf = new Configuration();
    String dir = argv[0];
    RecoveryParser parser = new RecoveryParser(new Path(dir), conf);
    parser.parse();
  }
}
