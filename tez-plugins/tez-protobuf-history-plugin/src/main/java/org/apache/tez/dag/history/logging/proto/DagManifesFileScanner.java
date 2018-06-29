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
import java.security.PrivilegedAction;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.ManifestEntryProto;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to scan all the dag manifest files to get manifest entries. This class is
 * not thread safe.
 */
public class DagManifesFileScanner implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(DagManifesFileScanner.class);
  private static final int SCANNER_OFFSET_VERSION = 2;
  private static final int MAX_RETRY = 3;

  private final ObjectMapper mapper = new ObjectMapper();
  private final DatePartitionedLogger<ManifestEntryProto> manifestLogger;
  private final long syncTime;
  private final boolean withDoas;

  private String scanDir;
  private Map<String, Long> offsets;
  private Map<String, Integer> retryCount;
  private List<FileStatus> newFiles;

  private ProtoMessageReader<ManifestEntryProto> reader;
  private String currentFilePath;

  public DagManifesFileScanner(DatePartitionedLogger<ManifestEntryProto> manifestLogger) {
    this.manifestLogger = manifestLogger;
    this.syncTime = manifestLogger.getConfig().getLong(
        TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_SYNC_WINDOWN_SECS,
        TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_SYNC_WINDOWN_SECS_DEFAULT);
    this.withDoas = manifestLogger.getConfig().getBoolean(
        TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_DOAS,
        TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_DOAS_DEFAULT);
    this.setOffset(LocalDate.ofEpochDay(0));
  }

  // Update the offset version and checks below to ensure correct versions are supported.
  // All public to simplify json conversion.
  public static class DagManifestOffset {
    public int version;
    public String scanDir;
    public Map<String, Long> offsets;
    public Map<String, Integer> retryCount;
  }

  public void setOffset(String offset) {
    try {
      DagManifestOffset dagOffset = mapper.readValue(offset, DagManifestOffset.class);
      if (dagOffset.version > SCANNER_OFFSET_VERSION) {
        throw new IllegalArgumentException("Version mismatch: " + dagOffset.version);
      }
      this.scanDir = dagOffset.scanDir;
      this.offsets = dagOffset.offsets == null ? new HashMap<>() : dagOffset.offsets;
      this.retryCount = dagOffset.retryCount == null ? new HashMap<>() : dagOffset.retryCount;
      this.newFiles = new ArrayList<>();
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid offset", e);
    }
  }

  public void setOffset(LocalDate date) {
    this.scanDir = manifestLogger.getDirForDate(date);
    this.offsets = new HashMap<>();
    this.retryCount = new HashMap<>();
    this.newFiles = new ArrayList<>();
  }

  public String getOffset() {
    try {
      DagManifestOffset offset = new DagManifestOffset();
      offset.version = SCANNER_OFFSET_VERSION;
      offset.scanDir = scanDir;
      offset.offsets = offsets;
      offset.retryCount = retryCount;
      return mapper.writeValueAsString(offset);
    } catch (IOException e) {
      throw new RuntimeException("Unexpected exception while converting to json.", e);
    }
  }

  public ManifestEntryProto getNext() throws IOException {
    while (true) {
      if (reader != null) {
        ManifestEntryProto evt = null;
        try {
          evt = reader.readEvent();
          retryCount.remove(currentFilePath);
        } catch (IOException e) {
          LOG.error("Error trying to read event from file: {}", currentFilePath, e);
          incrementError(currentFilePath);
        }
        if (evt != null) {
          offsets.put(reader.getFilePath().getName(), reader.getOffset());
          return evt;
        } else {
          IOUtils.closeQuietly(reader);
          reader = null;
          currentFilePath = null;
        }
      }
      if (!newFiles.isEmpty()) {
        this.reader = getNextReader();
        this.currentFilePath = reader != null ? reader.getFilePath().toString() : null;
      } else {
        if (!loadMore()) {
          return null;
        }
      }
    }
  }

  private void incrementError(String path) {
    int count = retryCount.getOrDefault(path, 0);
    retryCount.put(path, count + 1);
  }

  private ProtoMessageReader<ManifestEntryProto> getNextReader() throws IOException {
    FileStatus status = newFiles.remove(0);
    PrivilegedAction<ProtoMessageReader<ManifestEntryProto>> action = () -> {
      try {
        return manifestLogger.getReader(status.getPath());
      } catch (IOException e) {
        String path = status.getPath().toString();
        LOG.error("Error trying to open file: {}", path, e);
        incrementError(path);
        return null;
      }
    };
    if (withDoas) {
      UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(
          status.getOwner(), UserGroupInformation.getCurrentUser());
      return proxyUser.doAs(action);
    } else {
      return action.run();
    }
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }

  private void filterErrors(List<FileStatus> files) {
    Iterator<FileStatus> iter = files.iterator();
    while (iter.hasNext()) {
      FileStatus status = iter.next();
      String path = status.getPath().toString();
      if (retryCount.getOrDefault(path, 0) > MAX_RETRY) {
        LOG.warn("Removing file {}, too many errors", path);
        iter.remove();
      }
    }
  }

  private void loadNewFiles(String todayDir) throws IOException {
    newFiles = manifestLogger.scanForChangedFiles(scanDir, offsets);
    if (!scanDir.equals(todayDir)) {
      filterErrors(newFiles);
    }
  }

  private boolean loadMore() throws IOException {
    LocalDateTime now = manifestLogger.getNow();
    LocalDate today = now.toLocalDate();
    String todayDir = manifestLogger.getDirForDate(today);
    loadNewFiles(todayDir);
    while (newFiles.isEmpty()) {
      if (now.getHour() * 3600 + now.getMinute() * 60 + now.getSecond() < syncTime) {
        // We are in the delay window for today, do not advance date if we are moving from
        // yesterday.
        if (scanDir.equals(manifestLogger.getDirForDate(today.minusDays(1)))) {
          return false;
        }
      }
      String nextDir = manifestLogger.getNextDirectory(scanDir);
      if (nextDir == null) {
        return false;
      }
      scanDir = nextDir;
      offsets = new HashMap<>();
      retryCount = new HashMap<>();
      loadNewFiles(todayDir);
    }
    return true;
  }
}
