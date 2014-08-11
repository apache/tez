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

package org.apache.tez.runtime;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.MergedLogicalInput;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.MergedInputContext;
import org.apache.tez.runtime.api.impl.TezMergedInputContextImpl;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


public class TestInputReadyTracker {

  private static final long SLEEP_TIME = 500l;
  
  @Test(timeout = 5000)
  public void testWithoutGrouping1() throws InterruptedException {
    InputReadyTracker inputReadyTracker = new InputReadyTracker();

    ImmediatelyReadyInputForTest input1 = new ImmediatelyReadyInputForTest(inputReadyTracker);
    ControlledReadyInputForTest input2 = new ControlledReadyInputForTest(inputReadyTracker);

    // Test for simple inputs
    List<Input> requestList;
    long startTime = 0l;
    long readyTime = 0l;
    requestList = new ArrayList<Input>();
    requestList.add(input1);
    requestList.add(input2);
    Input readyInput = inputReadyTracker.waitForAnyInputReady(requestList);
    assertTrue(input1.isReady);
    assertFalse(input2.isReady);
    assertEquals(input1, readyInput);
    
    startTime = System.currentTimeMillis();
    setDelayedInputReady(input2);
    inputReadyTracker.waitForAllInputsReady(requestList);
    readyTime = System.currentTimeMillis();
    // Should have moved into ready state - only happens when the setReady function is invoked.
    // Ensure the method returned only after the specific Input was told it is ready
    assertTrue(input2.isReady);
    assertTrue(readyTime >= startTime + SLEEP_TIME);
    assertTrue(input1.isReady);
  }

  @Test(timeout = 5000)
  public void testWithoutGrouping2() throws InterruptedException {
    InputReadyTracker inputReadyTracker = new InputReadyTracker();

    ControlledReadyInputForTest input1 = new ControlledReadyInputForTest(inputReadyTracker);
    ControlledReadyInputForTest input2 = new ControlledReadyInputForTest(inputReadyTracker);
    ControlledReadyInputForTest input3 = new ControlledReadyInputForTest(inputReadyTracker);

    // Test for simple inputs
    List<Input> requestList;
    long startTime = 0l;
    long readyTime = 0l;
    
    requestList = new ArrayList<Input>();
    requestList.add(input1);
    requestList.add(input2);
    requestList.add(input3);
    
    startTime = System.currentTimeMillis();
    setDelayedInputReady(input2);
    Input readyInput = inputReadyTracker.waitForAnyInputReady(requestList);
    assertEquals(input2, readyInput);
    readyTime = System.currentTimeMillis();
    // Should have moved into ready state - only happens when the setReady function is invoked.
    // Ensure the method returned only after the specific Input was told it is ready
    assertTrue(input2.isReady);
    assertTrue(readyTime >= startTime + SLEEP_TIME);
    assertFalse(input1.isReady);
    assertFalse(input3.isReady);
    
    requestList = new ArrayList<Input>();
    requestList.add(input1);
    requestList.add(input3);
    startTime = System.currentTimeMillis();
    setDelayedInputReady(input1);
    readyInput = inputReadyTracker.waitForAnyInputReady(requestList);
    assertEquals(input1, readyInput);
    readyTime = System.currentTimeMillis();
    // Should have moved into ready state - only happens when the setReady function is invoked.
    // Ensure the method returned only after the specific Input was told it is ready
    assertTrue(input1.isReady);
    assertTrue(readyTime >= startTime + SLEEP_TIME);
    assertTrue(input2.isReady);
    assertFalse(input3.isReady);
    
    requestList = new ArrayList<Input>();
    requestList.add(input3);
    startTime = System.currentTimeMillis();
    setDelayedInputReady(input3);
    readyInput = inputReadyTracker.waitForAnyInputReady(requestList);
    assertEquals(input3, readyInput);
    readyTime = System.currentTimeMillis();
    // Should have moved into ready state - only happens when the setReady function is invoked.
    // Ensure the method returned only after the specific Input was told it is ready
    assertTrue(input3.isReady);
    assertTrue(readyTime >= startTime + SLEEP_TIME);
    assertTrue(input1.isReady);
    assertTrue(input2.isReady);
  }

  @Test(timeout = 5000)
  public void testGrouped() throws InterruptedException {
    InputReadyTracker inputReadyTracker = new InputReadyTracker();

    ImmediatelyReadyInputForTest input1 = new ImmediatelyReadyInputForTest(inputReadyTracker);
    ControlledReadyInputForTest input2 = new ControlledReadyInputForTest(inputReadyTracker);
    
    ImmediatelyReadyInputForTest input3 = new ImmediatelyReadyInputForTest(inputReadyTracker);
    ControlledReadyInputForTest input4 = new ControlledReadyInputForTest(inputReadyTracker);
    

    
    List<Input> group1Inputs = new ArrayList<Input>();
    group1Inputs.add(input1);
    group1Inputs.add(input2);
    
    List<Input> group2Inputs = new ArrayList<Input>();
    group2Inputs.add(input3);
    group2Inputs.add(input4);

    Map<String, MergedLogicalInput> mergedInputMap = new HashMap<String, MergedLogicalInput>();
    MergedInputContext mergedInputContext1 = new TezMergedInputContextImpl(null, "group1", mergedInputMap, inputReadyTracker, null);
    MergedInputContext mergedInputContext2 = new TezMergedInputContextImpl(null, "group2", mergedInputMap, inputReadyTracker, null);

    AnyOneMergedInputForTest group1 = new AnyOneMergedInputForTest(mergedInputContext1, group1Inputs);
    AllMergedInputForTest group2 = new AllMergedInputForTest(mergedInputContext2, group2Inputs);
    mergedInputMap.put("group1", group1);
    mergedInputMap.put("group2", group2);

    // Register groups with tracker
    List<MergedLogicalInput> groups = Lists.newArrayList(group1, group2);
    inputReadyTracker.setGroupedInputs(groups);

    // Test for simple inputs
    List<Input> requestList;
    long startTime = 0l;
    long readyTime = 0l;
    requestList = new ArrayList<Input>();
    requestList.add(group1);
    Input readyInput = inputReadyTracker.waitForAnyInputReady(requestList);
    assertTrue(group1.isReady);
    assertTrue(input1.isReady);
    assertFalse(input2.isReady);
    assertEquals(group1, readyInput);
    
    
    requestList = new ArrayList<Input>();
    requestList.add(group2);
    
    
    startTime = System.currentTimeMillis();
    setDelayedInputReady(input4);
    inputReadyTracker.waitForAllInputsReady(requestList);
    readyTime = System.currentTimeMillis();
    // Should have moved into ready state - only happens when the setReady function is invoked.
    // Ensure the method returned only after the specific Input was told it is ready
    assertTrue(group2.isReady);
    assertTrue(input3.isReady);
    assertTrue(input4.isReady);
    assertTrue(readyTime >= startTime + SLEEP_TIME);
    
  }
  
  private long setDelayedInputReady(final ControlledReadyInputForTest input) {
    long startTime = System.currentTimeMillis();
    new Thread() {
      public void run() {
        try {
          Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        input.setInputIsReady();
      }
    }.start();
    return startTime;
  }

  private static class ImmediatelyReadyInputForTest extends AbstractLogicalInput {

    private volatile boolean isReady = false;
    
    ImmediatelyReadyInputForTest(InputReadyTracker inputReadyTracker) {
      super(null, 0);
      isReady = true;
      inputReadyTracker.setInputIsReady(this);
    }

    @Override
    public List<Event> initialize() throws Exception {
      return null;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public Reader getReader() throws Exception {
      return null;
    }

    @Override
    public void handleEvents(List<Event> inputEvents) throws Exception {
    }

    @Override
    public List<Event> close() throws Exception {
      return null;
    }
  }
  
  private static class ControlledReadyInputForTest extends AbstractLogicalInput {

    private volatile boolean isReady = false;
    private InputReadyTracker inputReadyTracker;
    
    ControlledReadyInputForTest(InputReadyTracker inputReadyTracker) {
      super(null, 0);
      this.inputReadyTracker = inputReadyTracker;
    }

    @Override
    public List<Event> initialize() throws Exception {
      return null;
    }

    @Override
    public void start() throws Exception {      
    }

    @Override
    public Reader getReader() throws Exception {
      return null;
    }

    @Override
    public void handleEvents(List<Event> inputEvents) throws Exception {
    }

    @Override
    public List<Event> close() throws Exception {
      return null;
    }

   // Used by the test to control when this input will be ready
    public void setInputIsReady() {
      isReady = true;
      inputReadyTracker.setInputIsReady(this);
    }
  }

  private static class AnyOneMergedInputForTest extends MergedLogicalInput {

    private volatile boolean isReady = false;

    public AnyOneMergedInputForTest(MergedInputContext context, List<Input> inputs) {
      super(context, inputs);
    }

    @Override
    public Reader getReader() throws Exception {
      return null;
    }

    @Override
    public void setConstituentInputIsReady(Input input) {
      isReady = true;
      informInputReady();
    }
  }

  private static class AllMergedInputForTest extends MergedLogicalInput {

    private volatile boolean isReady = false;
    private Set<Input> readyInputs = Sets.newHashSet();

    public AllMergedInputForTest(MergedInputContext context, List<Input> inputs) {
      super(context, inputs);
    }

    @Override
    public Reader getReader() throws Exception {
      return null;
    }

    @Override
    public void setConstituentInputIsReady(Input input) {
      synchronized (this) {
        readyInputs.add(input);
      }
      if (readyInputs.size() == getInputs().size()) {
        isReady = true;
        informInputReady();
      }
    }
  }
}
