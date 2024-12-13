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

package org.apache.tez.runtime.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTaskCounterUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(TestTaskCounterUpdater.class);
  private static final Configuration CONF = new Configuration();

  @Test
  public void basicTest() {
    TezCounters counters = new TezCounters();
    TaskCounterUpdater updater = new TaskCounterUpdater(counters, CONF, "pid");

    updater.updateCounters();
    LOG.info("Counters (after first update): {}", counters);
    assertCounter(counters, TaskCounter.GC_TIME_MILLIS);
    TezCounter cpuCounter = assertCounter(counters, TaskCounter.CPU_MILLISECONDS);

    long oldVal = cpuCounter.getValue();
    Assert.assertTrue(cpuCounter.getValue() > 0);

    updater.updateCounters();
    LOG.info("Counters (after second update): {}", counters);
    Assert.assertTrue("Counter not updated, old=" + oldVal
        + ", new=" + cpuCounter.getValue(), cpuCounter.getValue() > oldVal);
  }

  private TezCounter assertCounter(TezCounters counters, TaskCounter taskCounter) {
    TezCounter counter = counters.findCounter(taskCounter);
    Assert.assertNotNull(counter);
    return counter;
  }
}
