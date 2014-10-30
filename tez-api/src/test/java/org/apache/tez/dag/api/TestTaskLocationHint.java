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

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestTaskLocationHint {
  
  @Test (timeout = 5000)
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

    Assert.assertEquals(t1, t2);
    Assert.assertEquals(t5, t6);
    Assert.assertEquals(t7, t8);
    Assert.assertEquals(t2, t1);
    Assert.assertEquals(t6, t5);
    Assert.assertEquals(t8, t7);
    Assert.assertNotEquals(t1, t3);
    Assert.assertNotEquals(t3, t1);
    Assert.assertNotEquals(t1, t4);
    Assert.assertNotEquals(t4, t1);
    Assert.assertNotEquals(t1, t5);
    Assert.assertNotEquals(t5, t1);
    Assert.assertNotEquals(t8, t9);
    Assert.assertNotEquals(t9, t8);
    Assert.assertNotEquals(t9, t10);
    Assert.assertNotEquals(t10, t9);
  }
}
