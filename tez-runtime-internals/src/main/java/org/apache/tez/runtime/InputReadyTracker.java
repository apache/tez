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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;

import org.apache.tez.common.Preconditions;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.MergedLogicalInput;

/**
 * A class for tracking a global list of ready {@code Inputs} and waiting for a
 * certain subset of {@code Inputs} to appear in the global list.
 */
public class InputReadyTracker {

  @GuardedBy("lock")
  private final Set<Input> readyInputs;

  @GuardedBy("lock")
  private Map<Input, List<MergedLogicalInput>> inputToGroupMap;
  
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();

  /**
   * Constructor.
   */
  public InputReadyTracker() {
    readyInputs = new HashSet<>();
    inputToGroupMap = Collections.emptyMap();
  }

  /**
   * Mark an input as being ready. If the same Input is marked as ready multiple
   * times, all subsequent attempts will be ignored.
   *
   * @param input The input to consider as ready
   */
  public void setInputIsReady(Input input) {
    lock.lock();
    try {
      boolean added = readyInputs.add(input);
      if (added) {
        informGroupedInputs(input);
        condition.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }

  private void informGroupedInputs(Input input) {
    List<MergedLogicalInput> mergedInputList =
        inputToGroupMap.getOrDefault(input, Collections.emptyList());
    for (MergedLogicalInput mergedInput : mergedInputList) {
      mergedInput.setConstituentInputIsReady(input);
    }
  }

  /**
   * Wait for any one of the specified inputs to be ready.
   *
   * @param inputs A collection of inputs to wait for
   * @param timeoutMillis The amount of time to wait for any one {@code Input}
   *          to be considered ready. A value less than zero indicates that it
   *          should wait forever (or until interrupted).
   * @return The first Input that was marked as ready
   * @throws InterruptedException if the current thread is interrupted (and
   *           interruption of thread suspension is supported)
   */
  public Input waitForAnyInputReady(Collection<Input> inputs, long timeoutMillis) throws InterruptedException {
    Preconditions.checkArgument(inputs != null && inputs.size() > 0,
        "At least one input should be specified");
    InputReadyMonitor inputReadyMonitor = new InputReadyMonitor(inputs, true);

    boolean inputReady = timeoutMillis < 0L ? inputReadyMonitor.awaitCondition()
        : inputReadyMonitor.awaitCondition(timeoutMillis,
            TimeUnit.MILLISECONDS);
    return inputReady
        ? inputReadyMonitor.getReadyMonitorInputs().iterator().next()
        : null;
  }

  /**
   * Wait for all of the specified inputs to be ready.
   *
   * @param inputs A collection of inputs to wait for
   * @param timeoutMillis The amount of time to wait for all {@code Input} to be
   *          considered ready. A value less than zero indicates that it should
   *          wait forever (or until interrupted).
   * @return True if all Inputs are considered ready before the timeout expired; false otherwise
   * @throws InterruptedException if the current thread is interrupted (and
   *           interruption of thread suspension is supported)
   */
  public boolean waitForAllInputsReady(Collection<Input> inputs, long timeoutMillis) throws InterruptedException {
    Preconditions.checkArgument(inputs != null && inputs.size() > 0,
        "At least one input should be specified");
    InputReadyMonitor inputReadyMonitor = new InputReadyMonitor(inputs, false);
    return timeoutMillis < 0L ? inputReadyMonitor.awaitCondition()
        : inputReadyMonitor.awaitCondition(timeoutMillis,
            TimeUnit.MILLISECONDS);
  }

  /**
   * Add grouped inputs.
   *
   * @param inputGroups The input groups to add
   */
  public void setGroupedInputs(Collection<MergedLogicalInput> inputGroups) {
    lock.lock();
    try {
      inputToGroupMap = new HashMap<>();
      for (MergedLogicalInput mergedInput : inputGroups) {
        for (Input dest : mergedInput.getInputs()) {
          // Check already ready Inputs - may have become ready during
          // initialize
          if (readyInputs.contains(dest)) {
            mergedInput.setConstituentInputIsReady(dest);
          }
          inputToGroupMap.computeIfAbsent(dest, in -> new ArrayList<>())
              .add(mergedInput);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private class InputReadyMonitor {

    @GuardedBy("lock")
    private final Set<Input> pendingMonitorInputs;
    private final Set<Input> readyMonitorInputs;
    private final boolean selectAny;

    public InputReadyMonitor(Collection<Input> inputs, boolean selectAny) {
      this.pendingMonitorInputs = new HashSet<>(inputs);
      this.readyMonitorInputs = new HashSet<>(selectAny ? 1 : inputs.size());
      this.selectAny = selectAny;
    }

    public boolean awaitCondition(long timeout, TimeUnit unit) throws InterruptedException {
      long nanos = unit.toNanos(timeout);
      lock.lock();
      try {
        while (!pendingMonitorInputs.isEmpty()) {
          Iterator<Input> inputIter = pendingMonitorInputs.iterator();
          while (inputIter.hasNext()) {
            Input input = inputIter.next();
            if (readyInputs.contains(input)) {
              readyMonitorInputs.add(input);
              inputIter.remove();
              // Return early in case of an ANY request
              if (selectAny) {
                return true;
              }
            }
          }
          if (!pendingMonitorInputs.isEmpty()) {
            nanos = condition.awaitNanos(nanos);
            if (nanos <= 0L) {
              return false;
            }
          }
        }
      } finally {
        lock.unlock();
      }
      return true;
    }

    public boolean awaitCondition() throws InterruptedException {
      lock.lock();
      try {
        while (!pendingMonitorInputs.isEmpty()) {
          Iterator<Input> pendingInputIter = pendingMonitorInputs.iterator();
          while (pendingInputIter.hasNext()) {
            Input input = pendingInputIter.next();
            if (readyInputs.contains(input)) {
              readyMonitorInputs.add(input);
              pendingInputIter.remove();
              // Return early in case of an ANY request
              if (selectAny) {
                return true;
              }
            }
          }
          if (!pendingMonitorInputs.isEmpty()) {
            condition.await();
          }
        }
      } finally {
        lock.unlock();
      }
      return true;
    }

    public Set<Input> getReadyMonitorInputs() {
      return readyMonitorInputs;
    }
  }
}
