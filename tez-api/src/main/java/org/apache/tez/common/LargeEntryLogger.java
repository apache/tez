/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.common;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Consumer;

import static org.apache.tez.dag.api.TezConfiguration.TEZ_LOGGING_PROPERTY_MASK;
import static org.apache.tez.dag.api.TezConfiguration.TEZ_LOGGING_PROPERTY_MASK_DEFAULT;
import static org.apache.tez.dag.api.TezConfiguration.TEZ_LOGGING_PROPERTY_SIZE_THRESHOLD;
import static org.apache.tez.dag.api.TezConfiguration.TEZ_LOGGING_PROPERTY_SIZE_THRESHOLD_DEFAULT;

/**
 * A configurable logger for large configuration/payload entries.
 */
public class LargeEntryLogger implements Consumer<Map.Entry<String, String>> {
  private static final Logger LOG = LoggerFactory.getLogger(LargeEntryLogger.class);
  private final int threshold;
  private final boolean mask;

  public static LargeEntryLogger from(Configuration c) {
    return new LargeEntryLogger(
        c.getInt(TEZ_LOGGING_PROPERTY_SIZE_THRESHOLD, TEZ_LOGGING_PROPERTY_SIZE_THRESHOLD_DEFAULT),
        c.getBoolean(TEZ_LOGGING_PROPERTY_MASK, TEZ_LOGGING_PROPERTY_MASK_DEFAULT));
  }

  public static LargeEntryLogger from(Map<String, String> c) {
    String threshold = c.getOrDefault(TEZ_LOGGING_PROPERTY_SIZE_THRESHOLD,
        String.valueOf(TEZ_LOGGING_PROPERTY_SIZE_THRESHOLD_DEFAULT));
    String mask = c.getOrDefault(TEZ_LOGGING_PROPERTY_MASK, String.valueOf(TEZ_LOGGING_PROPERTY_MASK_DEFAULT));
    return new LargeEntryLogger(Integer.parseInt(threshold), Boolean.parseBoolean(mask));
  }

  private LargeEntryLogger(int threshold, boolean mask) {
    this.threshold = threshold;
    this.mask = mask;
  }

  public void accept(Map.Entry<String, String> e) {
    String key = e.getKey();
    String value = e.getValue();
    if (value == null) {
      LOG.debug("Skipping entry '{}' cause value is null.", key);
      return;
    }
    if (value.length() > threshold) {
      LOG.warn("Entry '{}' is unusually big ({} bytes); large entries may lead to OOM.", key, value.length());
      if (!mask) {
        LOG.warn("Large entry '{}': {}", key, value);
      }
    }
  }
}
