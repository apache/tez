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
package org.apache.tez.runtime.library.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestInputIdentifiers {

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testInputAttemptIdentifier() {
    Set<InputAttemptIdentifier> set = new HashSet<InputAttemptIdentifier>();
    InputAttemptIdentifier i1 = new InputAttemptIdentifier(1, 1, InputAttemptIdentifier.PATH_PREFIX);
    InputAttemptIdentifier i2 = new InputAttemptIdentifier(1, 1, null);
    InputAttemptIdentifier i3 = new InputAttemptIdentifier(1, 0, null);
    InputAttemptIdentifier i4 = new InputAttemptIdentifier(0, 1, null);

    assertTrue(set.add(i1));
    assertFalse(set.add(i1));
    assertFalse(set.add(i2));
    assertTrue(set.add(i3));
    assertTrue(set.add(i4));
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testInputAttemptIdentifierIncludes() {
    InputAttemptIdentifier inputData0Attempt0 = new InputAttemptIdentifier(0, 0);
    InputAttemptIdentifier inputData1Attempt0 = new InputAttemptIdentifier(1, 0);
    InputAttemptIdentifier inputData2Attempt0 = new InputAttemptIdentifier(2, 0);
    InputAttemptIdentifier inputData3Attempt0 = new InputAttemptIdentifier(3, 0);
    InputAttemptIdentifier inputData1Attempt1 = new InputAttemptIdentifier(1, 1);
    CompositeInputAttemptIdentifier inputData12Attempt0 = new CompositeInputAttemptIdentifier(1, 0, null, 2);

    assertTrue(inputData1Attempt0.includes(inputData1Attempt0));
    assertFalse(inputData1Attempt0.includes(inputData2Attempt0));
    assertFalse(inputData1Attempt0.includes(inputData1Attempt1));

    assertFalse(inputData12Attempt0.includes(inputData0Attempt0));
    assertTrue(inputData12Attempt0.includes(inputData1Attempt0));
    assertTrue(inputData12Attempt0.includes(inputData2Attempt0));
    assertFalse(inputData12Attempt0.includes(inputData3Attempt0));
    assertFalse(inputData12Attempt0.includes(inputData1Attempt1));
  }
}
