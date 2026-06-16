/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.runtime.internals.protocolPB;

import static org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolUtils.translate;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.TezPBConverters;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.CanCommitRequestProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.CanCommitResponseProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.GetTaskRequestProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.GetTaskResponseProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.HeartbeatRequestProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.HeartbeatResponseProto;

/**
 * Protobuf-based server implementation for the TezTaskUmbilicalProtocol. This class translates
 * incoming RPC calls into internal Java protocol method calls.
 */
@Private
public class TezTaskUmbilicalProtocolPBServerImpl implements TezTaskUmbilicalProtocolBlockingPB {

  private final TezTaskUmbilicalProtocol real;

  public TezTaskUmbilicalProtocolPBServerImpl(TezTaskUmbilicalProtocol real) {
    this.real = real;
  }

  /** Receives a request for work from a Task process. */
  @Override
  public GetTaskResponseProto getTask(RpcController controller, GetTaskRequestProto request)
      throws ServiceException {
    return translate(
        () -> {
          ContainerContext containerContext = null;
          if (request.hasContainerContext()) {
            containerContext =
                new ContainerContext(request.getContainerContext().getContainerIdentifier());
          }
          ContainerTask response = real.getTask(containerContext);
          GetTaskResponseProto.Builder builder = GetTaskResponseProto.newBuilder();
          if (response != null) {
            builder.setContainerTask(TezPBConverters.convertToProto(response));
          }
          return builder.build();
        });
  }

  /** Receives a request to check if a task is allowed to commit its outputs. */
  @Override
  public CanCommitResponseProto canCommit(RpcController controller, CanCommitRequestProto request)
      throws ServiceException {
    return translate(
        () -> {
          TezTaskAttemptID taskAttemptID = null;
          if (request.hasTaskAttemptId()) {
            taskAttemptID = TezPBConverters.convertFromProto(request.getTaskAttemptId());
          }
          boolean response = real.canCommit(taskAttemptID);
          return CanCommitResponseProto.newBuilder().setResponse(response).build();
        });
  }

  /** Receives heartbeats from Task processes. */
  @Override
  public HeartbeatResponseProto heartbeat(RpcController controller, HeartbeatRequestProto request)
      throws ServiceException {
    return translate(
        () -> {
          TezHeartbeatRequest heartbeatRequest = TezPBConverters.convertFromProto(request);
          TezHeartbeatResponse response = real.heartbeat(heartbeatRequest);
          return TezPBConverters.convertToProto(response);
        });
  }
}
