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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestHistoryLogLevel {

  @Test
  public void testGetLogLevel() {
    assertNull(HistoryLogLevel.getLogLevel(getConfiguration(null), null));
    assertEquals(HistoryLogLevel.DEFAULT,
        HistoryLogLevel.getLogLevel(getConfiguration(null), HistoryLogLevel.DEFAULT));
    assertEquals(HistoryLogLevel.NONE,
        HistoryLogLevel.getLogLevel(getConfiguration("NONE"), HistoryLogLevel.DEFAULT));
    assertEquals(HistoryLogLevel.NONE,
        HistoryLogLevel.getLogLevel(getConfiguration("none"), HistoryLogLevel.DEFAULT));
    try {
      HistoryLogLevel.getLogLevel(getConfiguration("invalid"), HistoryLogLevel.DEFAULT);
      fail("Expected IllegalArugment Exception");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testValidateLogLevel() {
    assertTrue(HistoryLogLevel.validateLogLevel(null));
    assertTrue(HistoryLogLevel.validateLogLevel("NONE"));
    assertTrue(HistoryLogLevel.validateLogLevel("none"));
    assertFalse(HistoryLogLevel.validateLogLevel("invalid"));
  }

  private Configuration getConfiguration(String confValue) {
    Configuration conf = new Configuration(false);
    if (confValue != null) {
      conf.set(TezConfiguration.TEZ_HISTORY_LOGGING_LOGLEVEL, confValue);
    }
    return conf;
  }
}
