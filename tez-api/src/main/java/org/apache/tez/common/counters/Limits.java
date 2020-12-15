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

package org.apache.tez.common.counters;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;

@InterfaceAudience.Private
public class Limits {
  
  private static final Logger LOG = LoggerFactory.getLogger(Limits.class);

  private static final Configuration DEFAULT_CONFIGURATION = new TezConfiguration();
  private static Configuration conf = null;
  private static int GROUP_NAME_MAX;
  private static int COUNTER_NAME_MAX;
  private static int GROUPS_MAX;
  private static int COUNTERS_MAX;

  static {
    init(DEFAULT_CONFIGURATION);
  }

  public synchronized static void setConfiguration(Configuration conf) {
    // see change to reset()
    if (Limits.conf == DEFAULT_CONFIGURATION && conf != null) {
      init(conf);
    }
  }

  private static void init(Configuration conf) {
    Limits.conf = conf;
    GROUP_NAME_MAX = conf.getInt(TezConfiguration.TEZ_COUNTERS_GROUP_NAME_MAX_LENGTH,
        TezConfiguration.TEZ_COUNTERS_GROUP_NAME_MAX_LENGTH_DEFAULT);
    COUNTER_NAME_MAX = conf.getInt(TezConfiguration.TEZ_COUNTERS_COUNTER_NAME_MAX_LENGTH,
        TezConfiguration.TEZ_COUNTERS_COUNTER_NAME_MAX_LENGTH_DEFAULT);
    GROUPS_MAX = conf.getInt(TezConfiguration.TEZ_COUNTERS_MAX_GROUPS,
        TezConfiguration.TEZ_COUNTERS_MAX_GROUPS_DEFAULT);
    COUNTERS_MAX =
        conf.getInt(TezConfiguration.TEZ_COUNTERS_MAX, TezConfiguration.TEZ_COUNTERS_MAX_DEFAULT);
    LOG.info("Counter limits initialized with parameters: " + " GROUP_NAME_MAX=" + GROUP_NAME_MAX
        + ", MAX_GROUPS=" + GROUPS_MAX + ", COUNTER_NAME_MAX=" + COUNTER_NAME_MAX
        + ", MAX_COUNTERS=" + COUNTERS_MAX);
  }

  private int totalCounters;
  private LimitExceededException firstViolation;

  public static String filterName(String name, int maxLen) {
    return name.length() > maxLen ? name.substring(0, maxLen - 1) : name;
  }

  public static String filterCounterName(String name) {
    return filterName(name, COUNTER_NAME_MAX);
  }

  public static String filterGroupName(String name) {
    return filterName(name, GROUP_NAME_MAX);
  }

  public synchronized void checkCounters(int size) {
    if (firstViolation != null) {
      throw new LimitExceededException(firstViolation);
    }
    if (size > COUNTERS_MAX) {
      firstViolation = new LimitExceededException("Too many counters: "+ size +
                                                  " max="+ COUNTERS_MAX);
      throw firstViolation;
    }
  }

  public synchronized void incrCounters() {
    checkCounters(totalCounters + 1);
    ++totalCounters;
  }

  public synchronized void checkGroups(int size) {
    if (firstViolation != null) {
      throw new LimitExceededException(firstViolation);
    }
    if (size > GROUPS_MAX) {
      firstViolation = new LimitExceededException("Too many counter groups: "+
                                                  size +" max="+ GROUPS_MAX);
    }
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  public synchronized static void reset() {
    conf = DEFAULT_CONFIGURATION;
  }

}
