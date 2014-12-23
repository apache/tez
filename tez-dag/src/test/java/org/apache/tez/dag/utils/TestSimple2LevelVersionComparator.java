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

package org.apache.tez.dag.utils;

import org.junit.Assert;
import org.junit.Test;

public class TestSimple2LevelVersionComparator {

  private final Simple2LevelVersionComparator comparator = new Simple2LevelVersionComparator();

  @Test(timeout = 5000)
  public void testBasicEqualChecks() {
    Assert.assertEquals(0, comparator.compare("1-SNAPSHOT", "1-SNAPSHOT"));
    Assert.assertEquals(0, comparator.compare("1.1-SNAPSHOT", "1.1-SNAPSHOT"));
    Assert.assertEquals(0, comparator.compare("1.1.0-SNAPSHOT", "1.1.0-SNAPSHOT"));
    Assert.assertEquals(0, comparator.compare("0.1.0", "0.1.0"));
    Assert.assertEquals(0, comparator.compare("0.1", "0.1"));
    Assert.assertEquals(0, comparator.compare("0.1.2", "0.1.3"));
    Assert.assertEquals(0, comparator.compare("1.1.2", "1.1.5-RC"));
    Assert.assertEquals(0, comparator.compare("1", "1"));
    Assert.assertEquals(0, comparator.compare("1.1.x", "1.1.x"));
    Assert.assertEquals(0, comparator.compare("1-SNAP.x", "1-SNAP.x"));
  }

  @Test(timeout = 5000)
  public void testInvalidVersions() {
    Assert.assertEquals(-1, comparator.compare("x.1", "x.1"));
    Assert.assertEquals(-1, comparator.compare("x.1", "0.1"));
    Assert.assertEquals(-1, comparator.compare("1.x", "1.1"));
    Assert.assertEquals(-1, comparator.compare("1.1", "x.1"));
    Assert.assertEquals(-1, comparator.compare("1.1", "1.x"));
    Assert.assertEquals(-1, comparator.compare("1.1", "Unknown"));
    Assert.assertEquals(-1, comparator.compare("Unknown", "1.1"));
    Assert.assertEquals(-1, comparator.compare("Unknown", "Unknown"));
  }

  @Test(timeout = 5000)
  public void testInequalityChecks() {
    Assert.assertEquals(-1, comparator.compare("1.1", "2.1"));
    Assert.assertEquals(1, comparator.compare("1.1", "0.1"));
    Assert.assertEquals(1, comparator.compare("1.1.2", "0.1.2"));
    Assert.assertEquals(-1, comparator.compare("1.1.9", "4.1.9"));
    Assert.assertEquals(-1, comparator.compare("1.1-RC", "2.1.x"));
    Assert.assertEquals(-1, comparator.compare("1.1", ""));
    Assert.assertEquals(-1, comparator.compare("", "1.1"));
  }


}
