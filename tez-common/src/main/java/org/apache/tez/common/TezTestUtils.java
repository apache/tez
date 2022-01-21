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

package org.apache.tez.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class TezTestUtils {
  /**
   * Ensures a reasonably high limit for yarn disk utilization. This is very important for tests,
   * as devs keep bumping into silent test hangs where yarn simply considers their machines as unhealthy,
   * as the default limit is 90%, even if a machine with 90% full disk is still able to function.
   * @param conf
   */
  public static void ensureHighDiskUtilizationLimit(Configuration conf) {
    if (conf.getFloat(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE,
        YarnConfiguration.DEFAULT_NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE) ==
          YarnConfiguration.DEFAULT_NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE) {
      conf.setFloat(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE, 99.0f);
    }
  }
}
