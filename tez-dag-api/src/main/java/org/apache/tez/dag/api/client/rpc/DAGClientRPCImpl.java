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

package org.apache.tez.dag.api.client.rpc;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAllDAGsRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetVertexStatusRequestProto;

import com.google.protobuf.ServiceException;

public class DAGClientRPCImpl implements DAGClient, Closeable {

  private DAGClientAMProtocolBlockingPB proxy = null;
  
  public DAGClientRPCImpl(long clientVersion, 
                          InetSocketAddress addr,
                          TezConfiguration conf) throws IOException {
    RPC.setProtocolEngine(conf, 
                          DAGClientAMProtocolBlockingPB.class, 
                          ProtobufRpcEngine.class);
    proxy =
        (DAGClientAMProtocolBlockingPB) 
        RPC.getProxy(DAGClientAMProtocolBlockingPB.class, 
                     clientVersion,
                     addr, 
                     conf);
  }
  
  @Override
  public List<String> getAllDAGs() throws TezException {
    GetAllDAGsRequestProto requestProto = 
        GetAllDAGsRequestProto.newBuilder().build();
    try {
      return proxy.getAllDAGs(null, requestProto).getDagIdList();
    } catch (ServiceException e) {
      throw new TezException(e);
    }
  }

  @Override
  public DAGStatus getDAGStatus(String dagId) throws TezException {
    GetDAGStatusRequestProto requestProto = 
        GetDAGStatusRequestProto.newBuilder().setDagId(dagId).build();
    
    try {
      return new DAGStatus(
                 proxy.getDAGStatus(null, requestProto).getDagStatus());
    } catch (ServiceException e) {
      throw new TezException(e);
    }
  }

  @Override
  public VertexStatus getVertexStatus(String dagId, String vertexName)
      throws TezException {
    GetVertexStatusRequestProto requestProto = 
        GetVertexStatusRequestProto.newBuilder().
                        setDagId(dagId).setVertexName(vertexName).build();
    
    try {
      return new VertexStatus(
                 proxy.getVertexStatus(null, requestProto).getVertexStatus());
    } catch (ServiceException e) {
      throw new TezException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

}
