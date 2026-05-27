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
package org.apache.tez.dag.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestTaskLocationHint {

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testEquality() {
    TaskLocationHint t1 = TaskLocationHint.createTaskLocationHint("v1", 0);
    TaskLocationHint t2 = TaskLocationHint.createTaskLocationHint("v1", 0);
    TaskLocationHint t3 = TaskLocationHint.createTaskLocationHint("v2", 0);
    TaskLocationHint t4 = TaskLocationHint.createTaskLocationHint("v1", 1);
    TaskLocationHint t5 = TaskLocationHint.createTaskLocationHint(null, null);
    TaskLocationHint t6 = TaskLocationHint.createTaskLocationHint(null, null);
    TaskLocationHint t7 = TaskLocationHint.createTaskLocationHint(
        Sets.newHashSet(new String[] {"n1", "n2"}), Sets.newHashSet(new String[] {"r1", "r2"}));
    TaskLocationHint t8 = TaskLocationHint.createTaskLocationHint(
        Sets.newHashSet(new String[] {"n1", "n2"}), Sets.newHashSet(new String[] {"r1", "r2"}));
    TaskLocationHint t9 = TaskLocationHint.createTaskLocationHint(
        Sets.newHashSet(new String[] {"n1", "n2"}), Sets.newHashSet(new String[] {"r1"}));
    TaskLocationHint t10 = TaskLocationHint.createTaskLocationHint(
        Sets.newHashSet(new String[] {"n1"}), Sets.newHashSet(new String[] {"r1", "r2"}));

    assertEquals(t1, t2);
    assertEquals(t5, t6);
    assertEquals(t7, t8);
    assertEquals(t2, t1);
    assertEquals(t6, t5);
    assertEquals(t8, t7);
    assertNotEquals(t1, t3);
    assertNotEquals(t3, t1);
    assertNotEquals(t1, t4);
    assertNotEquals(t4, t1);
    assertNotEquals(t1, t5);
    assertNotEquals(t5, t1);
    assertNotEquals(t8, t9);
    assertNotEquals(t9, t8);
    assertNotEquals(t9, t10);
    assertNotEquals(t10, t9);
  }
}
