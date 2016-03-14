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
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Uses Sun/Oracle or IBM MBeans to return process information.
 */
public class TezMxBeanResourceCalculator extends ResourceCalculatorProcessTree {

  private final OperatingSystemMXBean osBean;
  private final Runtime runtime;
  private static final Method getCommittedVirtualMemorySize = getMxBeanMethod("getCommittedVirtualMemorySize");
  private static final Method getProcessCpuTime = getMxBeanMethod("getProcessCpuTime");

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
    osBean = ManagementFactory.getOperatingSystemMXBean();

  }

  @Override public void updateProcessTree() {
    //nothing needs to be done as the data is read from OS mbeans.
  }

  @Override public String getProcessTreeDump() {
    return "";
  }

  @Override public long getCumulativeVmem(int olderThanAge) {
    try {
      return (Long) getCommittedVirtualMemorySize.invoke(osBean);
    } catch (IllegalArgumentException e) {
      return -1;
    } catch (IllegalAccessException e) {
      return -1;
    } catch (InvocationTargetException e) {
      return -1;
    }
  }

  @Override public long getCumulativeRssmem(int olderThanAge) {
    //Not supported directly (RSS ~= memory consumed by JVM from Xmx)
    return runtime.totalMemory();
  }

  @Override public long getCumulativeCpuTime() {
    //convert to milliseconds
    try {
      return TimeUnit.MILLISECONDS.convert(
        (Long) getProcessCpuTime.invoke(osBean), TimeUnit.NANOSECONDS);
    } catch (InvocationTargetException e) {
      return -1;
    } catch (IllegalArgumentException e) {
      return -1;
    } catch (IllegalAccessException e) {
      return -1;
    }
  }

  @Override public boolean checkPidPgrpidForMatch() {
    return true;
  }

  public float getCpuUsagePercent() {
    //osBean.getProcessCpuLoad() can be closer and returns [0 - 1.0], but might not be accurate.
    //Returning -1 to indicate, this feature is not yet supported.
    return -1;
  }
  
  private static Method getMxBeanMethod(String methodName) {
	// New Method to support IBM and Oracle/OpenJDK JDK with OperatingSystemMXBean
    final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");
    final boolean IBM_JAVA = JAVA_VENDOR_NAME.contains("IBM");
    try {
      final Class<?> mbeanClazz;
      if (IBM_JAVA) {
        mbeanClazz = Class.forName("com.ibm.lang.management.OperatingSystemMXBean");
      } else {
        mbeanClazz = Class.forName("com.sun.management.OperatingSystemMXBean");
      }
      if (IBM_JAVA){
        if (methodName.equals("getCommittedVirtualMemorySize")) {
          methodName = "getProcessVirtualMemorySize";
        }
        if (methodName.equals("getProcessCpuTime")) {
          methodName = "getProcessCpuTimeByNS";
        }
      }
      final Method method = mbeanClazz
         .getMethod(methodName);
      return method;
    } catch (ClassNotFoundException e) {
      return null;
    } catch (SecurityException e) {
      return null;
    } catch (NoSuchMethodException e) {
      return null;
    }
  }
}
