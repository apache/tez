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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tez.util.TezMxBeanResourceCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
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

  /**
   * A Map where Key-> URIScheme and value->FileSystemStatisticUpdater
   */
  private Map<String, FileSystemStatisticUpdater> statisticUpdaters =
     new HashMap<String, FileSystemStatisticUpdater>();
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
    // FileSystemStatistics are reset each time a new task is seen by the
    // container.
    // This doesn't remove the fileSystem, and does not clear all statistics -
    // so there is a potential of an unused FileSystem showing up for a
    // Container, and strange values for READ_OPS etc.
    Map<String, List<FileSystem.Statistics>> map = new
        HashMap<String, List<FileSystem.Statistics>>();
    for(Statistics stat: FileSystem.getAllStatistics()) {
      String uriScheme = stat.getScheme();
      if (map.containsKey(uriScheme)) {
        List<FileSystem.Statistics> list = map.get(uriScheme);
        list.add(stat);
      } else {
        List<FileSystem.Statistics> list = new ArrayList<FileSystem.Statistics>();
        list.add(stat);
        map.put(uriScheme, list);
      }
    }
    for (Map.Entry<String, List<FileSystem.Statistics>> entry: map.entrySet()) {
      FileSystemStatisticUpdater updater = statisticUpdaters.get(entry.getKey());
      if(updater==null) {//new FileSystem has been found in the cache
        updater =
            new FileSystemStatisticUpdater(tezCounters, entry.getValue(),
                entry.getKey());
        statisticUpdaters.put(entry.getKey(), updater);
      }
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
    long pMem = pTree.getCumulativeRssmem();
    long vMem = pTree.getCumulativeVmem();
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
