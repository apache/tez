/*
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
package org.apache.tez.serviceplugins.api;

import org.apache.hadoop.util.Time;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TezCounters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Statistics aggregator for task scheduler requests.
 */
public class TaskSchedulerStatistics {
  private static final Logger LOG = LoggerFactory.getLogger(TaskSchedulerStatistics.class);

  /*
   * Tracking task allocation pending times.
   */
  private int maxPendingTime = 0;
  private int sumPendingTime = 0;
  private int averagePendingTime = 0;
  private int pendingTaskRequestSamples = 0;

  /**
   * Called by task schedulers when a request is removed.
   */
  public void trackRequestPendingTime(TaskRequestData request) {
    if (request == null) {
      LOG.info("Got null TaskRequestData, ignoring (it's fine in case of early shutdowns)");
      return;
    }
    long requestPendingTime = Time.now() - request.getCreatedTime();
    LOG.debug("Tracking request pending time: {}", requestPendingTime);

    sumPendingTime += (int) requestPendingTime;
    pendingTaskRequestSamples += 1;
    maxPendingTime = Math.max(maxPendingTime, (int) requestPendingTime);
  }

  /**
   * Calculates some derived statistics.
   * @return this instance
   */
  protected TaskSchedulerStatistics aggregate() {
    // it's fine to lose float precision here, we're interested in average of milliseconds
    this.averagePendingTime = pendingTaskRequestSamples > 0 ? sumPendingTime / pendingTaskRequestSamples : 0;
    return this;
  }

  /**
   * Adds the stats from another aggregator to this one and calculates the derived fields by calling aggregate().
   * @return this instance with ready-to-use data
   */
  public TaskSchedulerStatistics add(TaskSchedulerStatistics other) {
    if (other == null || other.pendingTaskRequestSamples == 0) {
      return aggregate();
    }
    this.maxPendingTime = Math.max(maxPendingTime, other.maxPendingTime);
    this.sumPendingTime += other.sumPendingTime;
    this.pendingTaskRequestSamples += other.pendingTaskRequestSamples;
    return aggregate();
  }

  public TezCounters getCounters() {
    TezCounters counters = new TezCounters();
    LOG.info("Getting counters from statistics: {}", this.statsToString());
    // prevent filling the counters with useless 0 values if any (e.g. in case of unused TaskScheduler)
    if (pendingTaskRequestSamples != 0) {
      counters.findCounter(DAGCounter.TASK_SCHEDULER_MAX_PENDING_TIME_MS).setValue(maxPendingTime);
      counters.findCounter(DAGCounter.TASK_SCHEDULER_SUM_PENDING_TIME_MS).setValue(sumPendingTime);
      counters.findCounter(DAGCounter.TASK_SCHEDULER_AVG_PENDING_TIME_MS).setValue(averagePendingTime);
    }
    return counters;
  }

  public TaskSchedulerStatistics clear() {
    averagePendingTime = 0;
    sumPendingTime = 0;
    pendingTaskRequestSamples = 0;
    maxPendingTime = 0;
    return this;
  }

  /**
   * Classes implementing this interface tells basic characteristics about task requests they encapsulate.
   */
  public interface TaskRequestData {
    // The time when the request was created
    long getCreatedTime();
  }

  public String statsToString(){
    return String.format("[pending: {max: %d, sum: %d, average: %d, samples: %d}]", maxPendingTime, sumPendingTime,
        averagePendingTime, pendingTaskRequestSamples);
  }
}
