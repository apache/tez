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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.MergedLogicalInput;

import org.apache.tez.common.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class InputReadyTracker {

  private final ConcurrentMap<Input, Boolean> readyInputs;
  
  private ConcurrentMap<Input, List<MergedLogicalInput>> inputToGroupMap;
  
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();

  public InputReadyTracker() {
    readyInputs = Maps.newConcurrentMap();
  }

  // Called by the InputContext once it's ready.
  public void setInputIsReady(Input input) {
    lock.lock();
    try {
      Boolean old = readyInputs.putIfAbsent(input, true);
      if (old == null) {
        informGroupedInputs(input);
        condition.signalAll();
      } else {
        // Ignore duplicate inputReady from the same Input
      }
    } finally {
      lock.unlock();
    }
  }


  private void informGroupedInputs(Input input) {
    if (inputToGroupMap != null) {
      List<MergedLogicalInput> mergedInputList = inputToGroupMap.get(input);
      if (mergedInputList != null) {
        for (MergedLogicalInput mergedInput : mergedInputList) {
          mergedInput.setConstituentInputIsReady(input);
        }
      }
    }
  }

  public Input waitForAnyInputReady(Collection<Input> inputs) throws InterruptedException {
    return waitForAnyInputReady(inputs, -1);
  }

  public Input waitForAnyInputReady(Collection<Input> inputs, long timeoutMillis) throws InterruptedException {
    Preconditions.checkArgument(inputs != null && inputs.size() > 0,
        "At least one input should be specified");
    InputReadyMonitor inputReadyMonitor = new InputReadyMonitor(inputs, true);
    try {
      return inputReadyMonitor.awaitCondition(timeoutMillis);
    } catch (TimeoutException e) {
      return null;
    }
  }

  public void waitForAllInputsReady(Collection<Input> inputs) throws InterruptedException {
    waitForAllInputsReady(inputs, -1);
  }

  public boolean waitForAllInputsReady(Collection<Input> inputs, long timeoutMillis) throws InterruptedException {
    Preconditions.checkArgument(inputs != null && inputs.size() > 0,
        "At least one input should be specified");
    boolean succeeded = true;
    InputReadyMonitor inputReadyMonitor = new InputReadyMonitor(inputs, false);

    try {
      inputReadyMonitor.awaitCondition(timeoutMillis);
    } catch (TimeoutException e) {
      succeeded = false;
    }
    return succeeded;
  }

  private class InputReadyMonitor {

    private final Set<Input> pendingInputs;
    private final boolean selectOne;

    public InputReadyMonitor(Collection<Input> inputs, boolean anyOne) {
      pendingInputs = Collections.newSetFromMap(new ConcurrentHashMap<Input, Boolean>());
      pendingInputs.addAll(inputs);
      this.selectOne = anyOne;
    }

    public Input awaitCondition(long timeoutMillis) throws InterruptedException, TimeoutException {
      lock.lock();
      try {
        while (pendingInputs.size() > 0) {
          Iterator<Input> inputIter = pendingInputs.iterator();
          while (inputIter.hasNext()) {
            Input input = inputIter.next();
            if (readyInputs.containsKey(input)) {
              inputIter.remove();
              // Return early in case of an ANY request
              if (selectOne) {
                return input;
              }
            }
          }
          if (pendingInputs.size() > 0) {
            if (timeoutMillis >= 0) {
              boolean succeeded = condition.await(timeoutMillis, TimeUnit.MILLISECONDS);
              if (!succeeded) {
                throw new TimeoutException("pending Inputs timeout");
              }
            } else { // timeout < 0
              condition.await();
            }
          }
        }
      } finally {
        lock.unlock();
      }
      return null;
    }
  }

  public void setGroupedInputs(Collection<MergedLogicalInput> inputGroups) {
    lock.lock();
    try {
      if (inputGroups != null) {
        inputToGroupMap = Maps.newConcurrentMap();
        for (MergedLogicalInput mergedInput : inputGroups) {
          for (Input dest : mergedInput.getInputs()) {
            // Check already ready Inputs - may have become ready during initialize
            if (readyInputs.containsKey(dest)) {
              mergedInput.setConstituentInputIsReady(dest);
            }
            List<MergedLogicalInput> mergedList = inputToGroupMap.get(dest);
            if (mergedList == null) {
              mergedList = Lists.newArrayList();
              inputToGroupMap.put(dest, mergedList);
            }
            mergedList.add(mergedInput);
          }
        }
      }
    } finally {
      lock.unlock();
    }
  }
}
