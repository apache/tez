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

import static org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolUtils.service;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.ipc.RPC;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.TezPBConverters;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.CanCommitRequestProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.ContainerContextProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.GetTaskRequestProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.GetTaskResponseProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.HeartbeatRequestProto;

/**
 * Protobuf-based client implementation for the TezTaskUmbilicalProtocol. This class handles the
 * translation between Java API calls and remote RPC calls.
 */
@Private
public class TezTaskUmbilicalProtocolPBClientImpl implements TezTaskUmbilicalProtocol, Closeable {

  private final TezTaskUmbilicalProtocolBlockingPB proxy;

  public TezTaskUmbilicalProtocolPBClientImpl(TezTaskUmbilicalProtocolBlockingPB proxy) {
    this.proxy = proxy;
  }

  @Override
  public void close() throws IOException {
    if (proxy != null) {
      RPC.stopProxy(proxy);
    }
  }

  /** Called by the Task process to retrieve the work from the AM. */
  @Override
  public ContainerTask getTask(ContainerContext containerContext) throws IOException {
    return service(
        () -> {
          GetTaskRequestProto.Builder builder = GetTaskRequestProto.newBuilder();
          if (containerContext != null) {
            builder.setContainerContext(
                ContainerContextProto.newBuilder()
                    .setContainerIdentifier(containerContext.getContainerIdentifier())
                    .build());
          }
          GetTaskResponseProto response = proxy.getTask(null, builder.build());
          return response.hasContainerTask()
              ? TezPBConverters.convertFromProto(response.getContainerTask())
              : null;
        });
  }

  /** Called by the Task process to check if it's allowed to commit its outputs. */
  @Override
  public boolean canCommit(TezTaskAttemptID taskid) throws IOException {
    return service(
        () -> {
          CanCommitRequestProto.Builder builder = CanCommitRequestProto.newBuilder();
          if (taskid != null) {
            builder.setTaskAttemptId(TezPBConverters.convertToProto(taskid));
          }
          return proxy.canCommit(null, builder.build()).getResponse();
        });
  }

  /** Heartbeat sent by the Task to report status and retrieve events from the AM. */
  @Override
  public TezHeartbeatResponse heartbeat(TezHeartbeatRequest request)
      throws IOException, TezException {
    return service(
        () -> {
          HeartbeatRequestProto requestProto = TezPBConverters.convertToProto(request);
          return TezPBConverters.convertFromProto(proxy.heartbeat(null, requestProto));
        });
  }
}
