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

package org.apache.tez.dag.library.vertexmanager;

// Each destination task fetches data from numOfSourceTasks of consecutive
// source tasks with the first source task index being firstSourceTaskIndex.
// For any source task in that range, each destination task fetches
// numOfPartitions consecutive physical outputs with the first physical output
// index being firstPartitionId.
class DestinationTaskInputsProperty {
  private final int firstPartitionId;
  private final int numOfPartitions;
  private final int firstSourceTaskIndex;
  private final int numOfSourceTasks;
  public DestinationTaskInputsProperty(int firstPartitionId,
      int numOfPartitions, int firstSourceTaskIndex, int numOfSourceTasks) {
    this.firstPartitionId = firstPartitionId;
    this.numOfPartitions = numOfPartitions;
    this.firstSourceTaskIndex = firstSourceTaskIndex;
    this.numOfSourceTasks = numOfSourceTasks;
  }
  public int getFirstPartitionId() {
    return firstPartitionId;
  }
  public int getNumOfPartitions() {
    return numOfPartitions;
  }
  public int getFirstSourceTaskIndex() {
    return firstSourceTaskIndex;
  }
  public int getNumOfSourceTasks() {
    return numOfSourceTasks;
  }
  public boolean isSourceTaskInRange(int sourceTaskIndex) {
    return firstSourceTaskIndex <= sourceTaskIndex &&
        sourceTaskIndex < firstSourceTaskIndex +
            numOfSourceTasks;
  }
  public boolean isPartitionInRange(int partitionId) {
    return firstPartitionId <= partitionId &&
        partitionId < firstPartitionId + numOfPartitions;
  }

  // The first physical input index for the source task
  public int getFirstPhysicalInputIndex(int sourceTaskIndex) {
    return getPhysicalInputIndex(sourceTaskIndex, firstPartitionId);
  }

  // The physical input index for the physical output index of the source task
  public int getPhysicalInputIndex(int sourceTaskIndex, int partitionId) {
    if (isSourceTaskInRange(sourceTaskIndex) &&
        isPartitionInRange(partitionId)) {
      return (sourceTaskIndex - firstSourceTaskIndex) * numOfPartitions +
          (partitionId - firstPartitionId);
    } else {
      return -1;
    }
  }

  public int getNumOfPhysicalInputs() {
    return numOfPartitions * numOfSourceTasks;
  }

  public int getSourceTaskIndex(int physicalInputIndex) {
    return firstSourceTaskIndex + physicalInputIndex / numOfPartitions;
  }

  @Override
  public String toString() {
    return "firstPartitionId = " + firstPartitionId +
        " ,numOfPartitions = " + numOfPartitions +
        " ,firstSourceTaskIndex = " + firstSourceTaskIndex +
        " ,numOfSourceTasks = " + numOfSourceTasks;
  }
}

