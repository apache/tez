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

package org.apache.tez.util;

import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Uses sun's MBeans to return process information.
 */
public class TezMxBeanResourceCalculator extends ResourceCalculatorProcessTree {

  private final com.sun.management.OperatingSystemMXBean osBean;
  private final Runtime runtime;
  private final AtomicLong cumulativeCPU;

  /**
   * Create process-tree instance with specified root process.
   * <p/>
   * Subclass must override this.
   *
   * @param root process-tree root-process
   */
  public TezMxBeanResourceCalculator(String root) {
    super(root);
    runtime = Runtime.getRuntime();
    osBean =
        (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    cumulativeCPU = new AtomicLong();
  }

  @Override public void updateProcessTree() {
    //nothing needs to be done as the data is read from OS mbeans.
  }

  @Override public String getProcessTreeDump() {
    return "";
  }

  @Override public long getCumulativeVmem(int olderThanAge) {
    return osBean.getCommittedVirtualMemorySize();
  }

  @Override public long getCumulativeRssmem(int olderThanAge) {
    //Not supported directly (RSS ~= memory consumed by JVM from Xmx)
    return runtime.totalMemory();
  }

  @Override public long getCumulativeCpuTime() {
    //convert to milliseconds
    return TimeUnit.MILLISECONDS.convert(cumulativeCPU.addAndGet(osBean.getProcessCpuTime()),
        TimeUnit.MILLISECONDS);
  }

  @Override public boolean checkPidPgrpidForMatch() {
    return true;
  }
}
