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
package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

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

  public static class HostPort {

    final String host;
    final int port;

    HostPort(String host, int port) {
      this.host = host;
      this.port = port;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((host == null) ? 0 : host.hashCode());
      result = prime * result + port;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      HostPort other = (HostPort) obj;
      if (host == null) {
        if (other.host != null)
          return false;
      } else if (!host.equals(other.host))
        return false;
      if (port != other.port)
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "HostPort [host=" + host + ", port=" + port + "]";
    }
  }

  public static class HostPortPartition {

    final String host;
    final int port;
    final int partition;

    HostPortPartition(String host, int port, int partition) {
      this.host = host;
      this.port = port;
      this.partition = partition;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((host == null) ? 0 : host.hashCode());
      result = prime * result + partition;
      result = prime * result + port;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      HostPortPartition other = (HostPortPartition) obj;
      if (partition != other.partition)
        return false;
      if (host == null) {
        if (other.host != null)
          return false;
      } else if (!host.equals(other.host))
        return false;
      if (port != other.port)
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "HostPortPartition [host=" + host + ", port=" + port + ", partition=" + partition + "]";
    }
  }

  private State state = State.IDLE;
  private final String host;
  private final int port;
  private final int partition;
  // Tracks attempt IDs
  private List<InputAttemptIdentifier> maps = new ArrayList<InputAttemptIdentifier>();
  
  public MapHost(String host, int port, int partition) {
    this.host = host;
    this.port = port;
    this.partition = partition;
  }

  public int getPartitionId() {
    return partition;
  }

  public State getState() {
    return state;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getHostIdentifier() {
    return host + ":" + port;
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
    return getHostIdentifier();
  }
  
  /**
   * Mark the host as penalized
   */
  public synchronized void penalize() {
    state = State.PENALIZED;
  }
}
