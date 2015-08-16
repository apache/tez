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

package org.apache.tez.runtime.library.common.shuffle;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;

/**
 * Represents a Host with respect to the MapReduce ShuffleHandler.
 * 
 * srcPhysicalIndex / partition is part of this since that only knows how to
 * serve ine partition at a time.
 */
public class InputHost {

  private final String host;
  private final int port;
  private final int srcPhysicalIndex;
  private final String identifier;
  private String additionalInfo;

  private final BlockingQueue<InputAttemptIdentifier> inputs = new LinkedBlockingQueue<InputAttemptIdentifier>();

  public static String createIdentifier(String host, int port) {
    return (host + ":" + String.valueOf(port));
  }
  
  public InputHost(String hostName, int port, ApplicationId appId, int srcPhysicalIndex) {
    this.host = hostName;
    this.port = port;
    this.srcPhysicalIndex = srcPhysicalIndex;
    this.identifier = createIdentifier(hostName, port);
  }

  public String getHost() {
    return this.host;
  }

  public int getPort() {
    return this.port;
  }
  
  public String getIdentifier() {
    return this.identifier;
  }

  public void setAdditionalInfo(String additionalInfo) {
    this.additionalInfo = additionalInfo;
  }

  public String getAdditionalInfo() {
    return (additionalInfo == null) ? "" : additionalInfo;
  }

  public int getSrcPhysicalIndex() {
    return this.srcPhysicalIndex;
  }

  public int getNumPendingInputs() {
    return inputs.size();
  }
  
  public void addKnownInput(InputAttemptIdentifier srcAttempt) {
    inputs.add(srcAttempt);
  }

  public List<InputAttemptIdentifier> clearAndGetPendingInputs() {
    List<InputAttemptIdentifier> inputsCopy = new ArrayList<InputAttemptIdentifier>(
        inputs.size());
    inputs.drainTo(inputsCopy);
    return inputsCopy;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((host == null) ? 0 : host.hashCode());
    result = prime * result + port;
    result = prime * result + srcPhysicalIndex;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    InputHost other = (InputHost) obj;
    if (host == null) {
      if (other.host != null) {
        return false;
      }
    } else if (!host.equals(other.host))
      return false;
    if (port != other.port) {
      return false;
    }
    if (srcPhysicalIndex != other.srcPhysicalIndex) {
      return false;
    }
    return true;
  }

  public String toDetailedString() {
    return "InputHost [host=" + host + ", port=" + port + ",srcPhysicalIndex=" + srcPhysicalIndex
        + ", inputs=" + inputs + "]";
  }
  
  @Override
  public String toString() {
    return "InputHost [host=" + host + ", port=" + port + ", srcPhysicalIndex=" + srcPhysicalIndex
        + "]";
  }
}
