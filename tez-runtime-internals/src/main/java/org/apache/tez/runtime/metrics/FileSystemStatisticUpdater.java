/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.runtime.metrics;

import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.tez.common.counters.FileSystemCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;

/**
 * An updater that tracks the last number reported for a given file system and
 * only creates the counters when they are needed.
 */
public class FileSystemStatisticUpdater {

  private List<FileSystem.Statistics> stats;
  private TezCounter readBytesCounter, writeBytesCounter, readOpsCounter, largeReadOpsCounter,
      writeOpsCounter;
  private String scheme;
  private TezCounters counters;

  FileSystemStatisticUpdater(TezCounters counters, List<FileSystem.Statistics> stats, String scheme) {
    this.stats = stats;
    this.scheme = scheme;
    this.counters = counters;
  }

  void updateCounters() {
    if (readBytesCounter == null) {
      readBytesCounter = counters.findCounter(scheme, FileSystemCounter.BYTES_READ);
    }
    if (writeBytesCounter == null) {
      writeBytesCounter = counters.findCounter(scheme, FileSystemCounter.BYTES_WRITTEN);
    }
    if (readOpsCounter == null) {
      readOpsCounter = counters.findCounter(scheme, FileSystemCounter.READ_OPS);
    }
    if (largeReadOpsCounter == null) {
      largeReadOpsCounter = counters.findCounter(scheme, FileSystemCounter.LARGE_READ_OPS);
    }
    if (writeOpsCounter == null) {
      writeOpsCounter = counters.findCounter(scheme, FileSystemCounter.WRITE_OPS);
    }
    long readBytes = 0;
    long writeBytes = 0;
    long readOps = 0;
    long largeReadOps = 0;
    long writeOps = 0;
    for (FileSystem.Statistics stat : stats) {
      readBytes = readBytes + stat.getBytesRead();
      writeBytes = writeBytes + stat.getBytesWritten();
      readOps = readOps + stat.getReadOps();
      largeReadOps = largeReadOps + stat.getLargeReadOps();
      writeOps = writeOps + stat.getWriteOps();
    }
    readBytesCounter.setValue(readBytes);
    writeBytesCounter.setValue(writeBytes);
    readOpsCounter.setValue(readOps);
    largeReadOpsCounter.setValue(largeReadOps);
    writeOpsCounter.setValue(writeOps);
  }
}
