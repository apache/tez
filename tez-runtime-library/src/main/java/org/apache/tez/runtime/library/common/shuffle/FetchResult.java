/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.library.common.shuffle;

import org.apache.tez.runtime.library.common.InputAttemptIdentifier;

public class FetchResult {

  private final String host;
  private final int port;
  private final int partition;
  private final int partitionCount;
  private final Iterable<InputAttemptIdentifier> pendingInputs;
  private final String additionalInfo;

  public FetchResult(String host, int port, int partition, int partitionCount,
                     Iterable<InputAttemptIdentifier> pendingInputs) {
    this(host, port, partition, partitionCount, pendingInputs, null);
  }

  public FetchResult(String host, int port, int partition, int partitionCount,
                     Iterable<InputAttemptIdentifier> pendingInputs, String additionalInfo) {
    this.host = host;
    this.port = port;
    this.partition = partition;
    this.partitionCount = partitionCount;
    this.pendingInputs = pendingInputs;
    this.additionalInfo = additionalInfo;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public int getPartition() {
    return partition;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public Iterable<InputAttemptIdentifier> getPendingInputs() {
    return pendingInputs;
  }

  public String getAdditionalInfo() {
    return additionalInfo;
  }
}
