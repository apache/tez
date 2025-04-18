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

package org.apache.tez.runtime.library.common;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class TestInputIdentifiers {
  
  @Test(timeout = 5000)
  public void testInputAttemptIdentifier() {
    Set<InputAttemptIdentifier> set = new HashSet<InputAttemptIdentifier>();
    InputAttemptIdentifier i1 = new InputAttemptIdentifier(1, 1, InputAttemptIdentifier.PATH_PREFIX);
    InputAttemptIdentifier i2 = new InputAttemptIdentifier(1, 1, null);
    InputAttemptIdentifier i3 = new InputAttemptIdentifier(1, 0, null);
    InputAttemptIdentifier i4 = new InputAttemptIdentifier(0, 1, null);
    
    Assert.assertTrue(set.add(i1));
    Assert.assertFalse(set.add(i1));
    Assert.assertFalse(set.add(i2));
    Assert.assertTrue(set.add(i3));
    Assert.assertTrue(set.add(i4));
  }

  @Test(timeout = 5000)
  public void testInputAttemptIdentifierIncludes() {
    InputAttemptIdentifier inputData0Attempt0 = new InputAttemptIdentifier(0, 0);
    InputAttemptIdentifier inputData1Attempt0 = new InputAttemptIdentifier(1, 0);
    InputAttemptIdentifier inputData2Attempt0 = new InputAttemptIdentifier(2, 0);
    InputAttemptIdentifier inputData3Attempt0 = new InputAttemptIdentifier(3, 0);
    InputAttemptIdentifier inputData1Attempt1 = new InputAttemptIdentifier(1, 1);
    CompositeInputAttemptIdentifier inputData12Attempt0 = new CompositeInputAttemptIdentifier(1, 0, null, 2);

    Assert.assertTrue(inputData1Attempt0.includes(inputData1Attempt0));
    Assert.assertFalse(inputData1Attempt0.includes(inputData2Attempt0));
    Assert.assertFalse(inputData1Attempt0.includes(inputData1Attempt1));

    Assert.assertFalse(inputData12Attempt0.includes(inputData0Attempt0));
    Assert.assertTrue(inputData12Attempt0.includes(inputData1Attempt0));
    Assert.assertTrue(inputData12Attempt0.includes(inputData2Attempt0));
    Assert.assertFalse(inputData12Attempt0.includes(inputData3Attempt0));
    Assert.assertFalse(inputData12Attempt0.includes(inputData1Attempt1));
  }
}
