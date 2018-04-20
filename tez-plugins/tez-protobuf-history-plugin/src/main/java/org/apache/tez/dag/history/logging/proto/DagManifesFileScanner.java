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

import java.io.Closeable;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.ManifestEntryProto;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Helper class to scan all the dag manifest files to get manifest entries.
 */
public class DagManifesFileScanner implements Closeable {
  private static final int OFFSET_VERSION = 1;

  private final ObjectMapper mapper = new ObjectMapper();
  private final DatePartitionedLogger<ManifestEntryProto> manifestLogger;
  private final long syncTime;

  private String scanDir;
  private Map<String, Long> offsets;
  private List<Path> newFiles;

  private ProtoMessageReader<ManifestEntryProto> reader;

  public DagManifesFileScanner(DatePartitionedLogger<ManifestEntryProto> manifestLogger) {
    this.manifestLogger = manifestLogger;
    this.syncTime = manifestLogger.getConfig().getLong(
        TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_SYNC_WINDOWN_SECS,
        TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_SYNC_WINDOWN_SECS_DEFAULT);
    this.setOffset(LocalDate.ofEpochDay(0));
  }

  // All public to simplify json conversion.
  public static class DagManifestOffset {
    public int version;
    public String scanDir;
    public Map<String, Long> offsets;
  }

  public void setOffset(String offset) {
    try {
      DagManifestOffset dagOffset = mapper.readValue(offset, DagManifestOffset.class);
      if (dagOffset.version != OFFSET_VERSION) {
        throw new IllegalArgumentException("Version mismatch: " + dagOffset.version);
      }
      this.scanDir = dagOffset.scanDir;
      this.offsets = dagOffset.offsets;
      this.newFiles = new ArrayList<>();
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid offset", e);
    }
  }

  public void setOffset(LocalDate date) {
    this.scanDir = manifestLogger.getDirForDate(date);
    this.offsets = new HashMap<>();
    this.newFiles = new ArrayList<>();
  }

  public String getOffset() {
    try {
      DagManifestOffset offset = new DagManifestOffset();
      offset.version = OFFSET_VERSION;
      offset.scanDir = scanDir;
      offset.offsets = offsets;
      return mapper.writeValueAsString(offset);
    } catch (IOException e) {
      throw new RuntimeException("Unexpected exception while converting to json.", e);
    }
  }

  public ManifestEntryProto getNext() throws IOException {
    while (true) {
      if (reader != null) {
        ManifestEntryProto evt = reader.readEvent();
        if (evt != null) {
          offsets.put(reader.getFilePath().getName(), reader.getOffset());
          return evt;
        } else {
          IOUtils.closeQuietly(reader);
          reader = null;
        }
      }
      if (!newFiles.isEmpty()) {
        this.reader = manifestLogger.getReader(newFiles.remove(0));
      } else {
        if (!loadMore()) {
          return null;
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }

  private boolean loadMore() throws IOException {
    newFiles = manifestLogger.scanForChangedFiles(scanDir, offsets);
    while (newFiles.isEmpty()) {
      LocalDateTime utcNow = manifestLogger.getNow();
      if (utcNow.getHour() * 3600 + utcNow.getMinute() * 60 + utcNow.getSecond() < syncTime) {
        // We are in the delay window for today, do not advance date if we are moving from
        // yesterday.
        String yesterDir = manifestLogger.getDirForDate(utcNow.toLocalDate().minusDays(1));
        if (yesterDir.equals(scanDir)) {
          return false;
        }
      }
      String nextDir = manifestLogger.getNextDirectory(scanDir);
      if (nextDir == null) {
        return false;
      }
      scanDir = nextDir;
      offsets = new HashMap<>();
      newFiles = manifestLogger.scanForChangedFiles(scanDir, offsets);
    }
    return true;
  }
}
