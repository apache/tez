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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.library.vertexmanager.FairShuffleUserPayloads.FairShuffleEdgeManagerConfigPayloadProto;
import org.apache.tez.dag.library.vertexmanager.FairShuffleUserPayloads.FairShuffleEdgeManagerDestinationTaskPropProto;
import org.apache.tez.dag.library.vertexmanager.FairShuffleUserPayloads.RangeProto;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map.Entry;


/**
 * Handles edge configuration serialization and de-serialization between
 * {@link FairShuffleVertexManager} and {@link FairShuffleEdgeManager}.
 */
class FairEdgeConfiguration {
  private final int numBuckets;
  private final HashMap<Integer, DestinationTaskInputsProperty>
      destinationInputsProperties;

  public FairEdgeConfiguration(int numBuckets,
      HashMap<Integer, DestinationTaskInputsProperty> routingTable) {
    this.destinationInputsProperties = routingTable;
    this.numBuckets = numBuckets;
  }

  private FairShuffleEdgeManagerConfigPayloadProto getConfigPayload() {
    FairShuffleEdgeManagerConfigPayloadProto.Builder builder =
        FairShuffleEdgeManagerConfigPayloadProto.newBuilder();
    builder.setNumBuckets(numBuckets);
    if (destinationInputsProperties != null) {
      for (Entry<Integer, DestinationTaskInputsProperty> entry :
          destinationInputsProperties.entrySet()) {
        FairShuffleEdgeManagerDestinationTaskPropProto.Builder taskBuilder =
            FairShuffleEdgeManagerDestinationTaskPropProto.newBuilder();
        taskBuilder.
            setDestinationTaskIndex(entry.getKey()).
            setPartitions(newRange(entry.getValue().getFirstPartitionId(),
            entry.getValue().getNumOfPartitions())).
            setSourceTasks(newRange(entry.getValue().
            getFirstSourceTaskIndex(), entry.getValue().getNumOfSourceTasks()));
        builder.addDestinationTaskProps(taskBuilder.build());
      }
    }
    return builder.build();
  }

  private RangeProto newRange(int firstIndex, int numOfIndexes) {
    return RangeProto.newBuilder().
        setFirstIndex(firstIndex).setNumOfIndexes(numOfIndexes).build();
  }

  static FairEdgeConfiguration fromUserPayload(UserPayload payload)
      throws InvalidProtocolBufferException {
    HashMap<Integer, DestinationTaskInputsProperty> routingTable = new HashMap<>();
    FairShuffleEdgeManagerConfigPayloadProto proto =
        FairShuffleEdgeManagerConfigPayloadProto.parseFrom(
            ByteString.copyFrom(payload.getPayload()));
    int numBuckets = proto.getNumBuckets();
    if (proto.getDestinationTaskPropsList() != null) {
      for (int i = 0; i < proto.getDestinationTaskPropsList().size(); i++) {
        FairShuffleEdgeManagerDestinationTaskPropProto propProto =
            proto.getDestinationTaskPropsList().get(i);
        routingTable.put(
            propProto.getDestinationTaskIndex(),
            new DestinationTaskInputsProperty(
                propProto.getPartitions().getFirstIndex(),
                propProto.getPartitions().getNumOfIndexes(),
                propProto.getSourceTasks().getFirstIndex(),
                propProto.getSourceTasks().getNumOfIndexes()));
      }
    }
    return new FairEdgeConfiguration(numBuckets, routingTable);
  }

  public HashMap<Integer, DestinationTaskInputsProperty> getRoutingTable() {
    return destinationInputsProperties;
  }

  // The number of partitions used by source vertex.
  int getNumBuckets() {
    return numBuckets;
  }

  UserPayload getBytePayload() {
    return UserPayload.create(ByteBuffer.wrap(
        getConfigPayload().toByteArray()));
  }
}
