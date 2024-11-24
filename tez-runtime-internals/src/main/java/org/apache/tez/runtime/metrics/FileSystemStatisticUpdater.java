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

import org.apache.hadoop.fs.StorageStatistics;
import org.apache.tez.common.counters.FileSystemCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;

/**
 * An updater that tracks the last number reported for a given file system and
 * only creates the counters when they are needed.
 */
public class FileSystemStatisticUpdater {

  private final StorageStatistics stats;
  private final TezCounters counters;

  FileSystemStatisticUpdater(TezCounters counters, StorageStatistics storageStatistics) {
    this.stats = storageStatistics;
    this.counters = counters;
  }

  void updateCounters() {
    // loop through FileSystemCounter enums as it is a smaller set
    for (FileSystemCounter fsCounter : FileSystemCounter.values()) {
      Long val = stats.getLong(fsCounter.getOpName());
      if (val != null && val != 0) {
        TezCounter counter = counters.findCounter(stats.getScheme(), fsCounter);
        counter.setValue(val);
      }
    }
  }
}
