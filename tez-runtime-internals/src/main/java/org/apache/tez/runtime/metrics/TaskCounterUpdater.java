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

package org.apache.tez.runtime.metrics;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.tez.util.TezMxBeanResourceCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.apache.tez.common.GcTimeUpdater;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;


/**
 * Updates counters with various task specific statistics. Currently, this
 * should be invoked only once per task. TODO Eventually - change this so that
 * counters can be updated incrementally during task execution.
 */
public class TaskCounterUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(TaskCounterUpdater.class);

  private final TezCounters tezCounters;
  private final Configuration conf;

//  /**
//   * A Map where Key-> URIScheme and value->FileSystemStatisticUpdater
//   */
//  private Map<String, FileSystemStatisticUpdater> statisticUpdaters =
//     new HashMap<>();
  /**
   * A Map where Key-> URIScheme and value->Map<Name, FileSystemStatisticUpdater>
   */
  private Map<String, Map<String, FileSystemStatisticUpdater>> statisticUpdaters =
      new HashMap<>();
  protected final GcTimeUpdater gcUpdater;
  private ResourceCalculatorProcessTree pTree;
  private long initCpuCumulativeTime = 0;
  private final String pid;
  
  public TaskCounterUpdater(TezCounters counters, Configuration conf, String pid) {
    this.tezCounters = counters;
    this.conf = conf;   
    this.gcUpdater = new GcTimeUpdater(tezCounters);
    this.pid = pid;
    initResourceCalculatorPlugin();
    recordInitialCpuStats();
  }

  
  public void updateCounters() {
    GlobalStorageStatistics globalStorageStatistics = FileSystem.getGlobalStorageStatistics();
    Iterator<StorageStatistics> iter = globalStorageStatistics.iterator();
    while (iter.hasNext()) {
      StorageStatistics stats = iter.next();
      // Fetch or initialize the updater set for the scheme
      Map<String, FileSystemStatisticUpdater> updaterSet = statisticUpdaters
          .computeIfAbsent(stats.getScheme(), k -> new TreeMap<>());
      // Fetch or create the updater for the specific statistic
      FileSystemStatisticUpdater updater = updaterSet
          .computeIfAbsent(stats.getName(), k -> new FileSystemStatisticUpdater(tezCounters, stats));
      updater.updateCounters();
    }

    gcUpdater.incrementGcCounter();
    updateResourceCounters();
  }
  
  private void recordInitialCpuStats() {
    if (pTree != null) {
      pTree.updateProcessTree();
      initCpuCumulativeTime = pTree.getCumulativeCpuTime();
    }
  }
  
  /**
   * Update resource information counters
   */
  void updateResourceCounters() {
    // Update generic resource counters
    updateHeapUsageCounter();

    // Updating resources specified in ResourceCalculatorPlugin
    if (pTree == null) {
      return;
    }
    pTree.updateProcessTree();
    long cpuTime = pTree.getCumulativeCpuTime();
    long pMem = pTree.getRssMemorySize();
    long vMem = pTree.getVirtualMemorySize();
    // Remove the CPU time consumed previously by JVM reuse
    cpuTime -= initCpuCumulativeTime;
    tezCounters.findCounter(TaskCounter.CPU_MILLISECONDS).setValue(cpuTime);
    tezCounters.findCounter(TaskCounter.PHYSICAL_MEMORY_BYTES).setValue(pMem);
    tezCounters.findCounter(TaskCounter.VIRTUAL_MEMORY_BYTES).setValue(vMem);
  }
  
  /**
   * Updates the {@link TaskCounter#COMMITTED_HEAP_BYTES} counter to reflect the
   * current total committed heap space usage of this JVM.
   */
  private void updateHeapUsageCounter() {
    long currentHeapUsage = Runtime.getRuntime().totalMemory();
    tezCounters.findCounter(TaskCounter.COMMITTED_HEAP_BYTES)
            .setValue(currentHeapUsage);
  }
  
  private void initResourceCalculatorPlugin() {
    Class<? extends ResourceCalculatorProcessTree> clazz = this.conf.getClass(
        TezConfiguration.TEZ_TASK_RESOURCE_CALCULATOR_PROCESS_TREE_CLASS,
        TezMxBeanResourceCalculator.class,
        ResourceCalculatorProcessTree.class); 

    pTree = ResourceCalculatorProcessTree.getResourceCalculatorProcessTree(pid, clazz, conf);

    LOG.info("Using ResourceCalculatorProcessTree : " + clazz.getName());
  }
}
