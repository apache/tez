/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.dag.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestSimple2LevelVersionComparator {

  private final Simple2LevelVersionComparator comparator = new Simple2LevelVersionComparator();

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testBasicEqualChecks() {
    assertEquals(0, comparator.compare("1-SNAPSHOT", "1-SNAPSHOT"));
    assertEquals(0, comparator.compare("1.1-SNAPSHOT", "1.1-SNAPSHOT"));
    assertEquals(0, comparator.compare("1.1.0-SNAPSHOT", "1.1.0-SNAPSHOT"));
    assertEquals(0, comparator.compare("0.1.0", "0.1.0"));
    assertEquals(0, comparator.compare("0.1", "0.1"));
    assertEquals(0, comparator.compare("0.1.2", "0.1.3"));
    assertEquals(0, comparator.compare("1.1.2", "1.1.5-RC"));
    assertEquals(0, comparator.compare("1", "1"));
    assertEquals(0, comparator.compare("1.1.x", "1.1.x"));
    assertEquals(0, comparator.compare("1-SNAP.x", "1-SNAP.x"));
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testInvalidVersions() {
    assertEquals(-1, comparator.compare("x.1", "x.1"));
    assertEquals(-1, comparator.compare("x.1", "0.1"));
    assertEquals(-1, comparator.compare("1.x", "1.1"));
    assertEquals(-1, comparator.compare("1.1", "x.1"));
    assertEquals(-1, comparator.compare("1.1", "1.x"));
    assertEquals(-1, comparator.compare("1.1", "Unknown"));
    assertEquals(-1, comparator.compare("Unknown", "1.1"));
    assertEquals(-1, comparator.compare("Unknown", "Unknown"));
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testInequalityChecks() {
    assertEquals(-1, comparator.compare("1.1", "2.1"));
    assertEquals(1, comparator.compare("1.1", "0.1"));
    assertEquals(1, comparator.compare("1.1.2", "0.1.2"));
    assertEquals(-1, comparator.compare("1.1.9", "4.1.9"));
    assertEquals(-1, comparator.compare("1.1-RC", "2.1.x"));
    assertEquals(-1, comparator.compare("1.1", ""));
    assertEquals(-1, comparator.compare("", "1.1"));
  }


}
