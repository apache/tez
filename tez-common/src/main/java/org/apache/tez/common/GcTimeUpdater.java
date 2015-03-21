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

package org.apache.tez.common;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.common.counters.TaskCounter;

/**
 * An updater that tracks the amount of time this task has spent in GC.
 */
@Private
public class GcTimeUpdater {
  private long lastGcMillis = 0;
  private List<GarbageCollectorMXBean> gcBeans = null;
  TezCounters counters;

  public GcTimeUpdater(TezCounters counters) {
    this.gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    getElapsedGc(); // Initialize 'lastGcMillis' with the current time spent.
    this.counters = counters;
  }
  
  public long getCumulativaGcTime() {
    long thisGcMillis = 0;
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      thisGcMillis += gcBean.getCollectionTime();
    }
    return thisGcMillis;
  }

  /**
   * Get the elapsed time since the last call. This is not thread safe.
   * @return the number of milliseconds that the gc has used for CPU since the
   *         last time this method was called.
   */
  protected long getElapsedGc() {
    long thisGcMillis = getCumulativaGcTime();

    long delta = thisGcMillis - lastGcMillis;
    this.lastGcMillis = thisGcMillis;
    return delta;
  }

  /**
   * Increment the gc-elapsed-time counter.
   */
  public void incrementGcCounter() {
    if (null == counters) {
      return; // nothing to do.
    }

    TezCounter gcCounter = counters.findCounter(TaskCounter.GC_TIME_MILLIS);
    if (null != gcCounter) {
      gcCounter.increment(getElapsedGc());
    }
  }
}
