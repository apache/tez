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

package org.apache.tez.dag.api;

import java.util.Locale;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;

/**
 * The log level for the history, this is used to determine which events are sent to the history
 * logger. The default level is ALL.
 */
@Public
public enum HistoryLogLevel {
  NONE,
  AM,
  DAG,
  VERTEX,
  TASK,
  TASK_ATTEMPT,
  ALL;

  public static final HistoryLogLevel DEFAULT = ALL;

  public boolean shouldLog(HistoryLogLevel eventLevel) {
    return eventLevel.ordinal() <= ordinal();
  }

  public static HistoryLogLevel getLogLevel(Configuration conf, HistoryLogLevel defaultValue) {
    String logLevel = conf.getTrimmed(TezConfiguration.TEZ_HISTORY_LOGGING_LOGLEVEL);
    if (logLevel == null) {
      return defaultValue;
    }
    return valueOf(logLevel.toUpperCase(Locale.ENGLISH));
  }

  public static boolean validateLogLevel(String logLevel) {
    if (logLevel != null) {
      try {
        valueOf(logLevel.toUpperCase(Locale.ENGLISH));
      } catch (IllegalArgumentException e) {
        return false;
      }
    }
    return true;
  }
}
