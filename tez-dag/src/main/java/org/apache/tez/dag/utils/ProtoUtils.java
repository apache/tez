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

package org.apache.tez.dag.utils;

import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.recovery.records.RecoveryProtos;

import com.google.protobuf.ByteString;

public class ProtoUtils {

  public static RecoveryProtos.SummaryEventProto toSummaryEventProto(
      TezDAGID dagID, long timestamp, HistoryEventType historyEventType, byte[] payload) {
    RecoveryProtos.SummaryEventProto.Builder builder =
        RecoveryProtos.SummaryEventProto.newBuilder()
        .setDagId(dagID.toString())
        .setTimestamp(timestamp)
        .setEventType(historyEventType.ordinal());
    if (payload != null){
      builder.setEventPayload(ByteString.copyFrom(payload));
    }
    return builder.build();
  }

}
