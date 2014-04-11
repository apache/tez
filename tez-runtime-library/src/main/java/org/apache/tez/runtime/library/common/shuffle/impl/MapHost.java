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
package org.apache.tez.runtime.library.common.shuffle.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;

@Private
class MapHost {
  
  public static enum State {
    IDLE,               // No map outputs available
    BUSY,               // Map outputs are being fetched
    PENDING,            // Known map outputs which need to be fetched
    PENALIZED           // Host penalized due to shuffle failures
  }
  
  private State state = State.IDLE;
  private final String hostIdentifier;
  private final int partitionId;
  private final String baseUrl;
  private final String identifier;
  // Tracks attempt IDs
  private List<InputAttemptIdentifier> maps = new ArrayList<InputAttemptIdentifier>();
  
  public MapHost(int partitionId, String hostPort, String baseUrl) {
    this.partitionId = partitionId;
    this.hostIdentifier = hostPort;
    this.baseUrl = baseUrl;
    this.identifier = createIdentifier(hostPort, partitionId);
  }
  
  public static String createIdentifier(String hostName, int partitionId) {
    return hostName + ":" + Integer.toString(partitionId);
  }
  
  public String getIdentifier() {
    return identifier;
  }
  
  public int getPartitionId() {
    return partitionId;
  }

  public State getState() {
    return state;
  }

  public String getHostIdentifier() {
    return hostIdentifier;
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  public synchronized void addKnownMap(InputAttemptIdentifier srcAttempt) {
    maps.add(srcAttempt);
    if (state == State.IDLE) {
      state = State.PENDING;
    }
  }

  public synchronized List<InputAttemptIdentifier> getAndClearKnownMaps() {
    List<InputAttemptIdentifier> currentKnownMaps = maps;
    maps = new ArrayList<InputAttemptIdentifier>();
    return currentKnownMaps;
  }
  
  public synchronized void markBusy() {
    state = State.BUSY;
  }
  
  public synchronized void markPenalized() {
    state = State.PENALIZED;
  }
  
  public synchronized int getNumKnownMapOutputs() {
    return maps.size();
  }

  /**
   * Called when the node is done with its penalty or done copying.
   * @return the host's new state
   */
  public synchronized State markAvailable() {
    if (maps.isEmpty()) {
      state = State.IDLE;
    } else {
      state = State.PENDING;
    }
    return state;
  }
  
  @Override
  public String toString() {
    return hostIdentifier;
  }
  
  /**
   * Mark the host as penalized
   */
  public synchronized void penalize() {
    state = State.PENALIZED;
  }
}
