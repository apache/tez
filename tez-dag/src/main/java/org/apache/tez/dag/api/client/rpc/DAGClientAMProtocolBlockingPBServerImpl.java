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

import java.io.IOException;
import java.util.List;

import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.DAGStatusBuilder;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.VertexStatusBuilder;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolBlockingPB;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAllDAGsRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAllDAGsResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGStatusResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetVertexStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetVertexStatusResponseProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class DAGClientAMProtocolBlockingPBServerImpl implements
    DAGClientAMProtocolBlockingPB {
  
  DAGClient real;
  
  public DAGClientAMProtocolBlockingPBServerImpl(DAGClient real) {
    this.real = real;
  }

  @Override
  public GetAllDAGsResponseProto getAllDAGs(RpcController controller,
      GetAllDAGsRequestProto request) throws ServiceException {
    try{
      List<String> dagIds = real.getAllDAGs();
      return GetAllDAGsResponseProto.newBuilder().addAllDagId(dagIds).build();
    } catch(TezException e) {
      throw wrapException(e);
    } catch(IOException e) {
      throw wrapException(e);
    }
  }

  @Override
  public GetDAGStatusResponseProto getDAGStatus(RpcController controller,
      GetDAGStatusRequestProto request) throws ServiceException {
    try {
      String dagId = request.getDagId();
      DAGStatus status;
      status = real.getDAGStatus(dagId);
      assert status instanceof DAGStatusBuilder;
      DAGStatusBuilder builder = (DAGStatusBuilder) status;
      return GetDAGStatusResponseProto.newBuilder().
                                setDagStatus(builder.getProto()).build();
    } catch (TezException e) {
      throw wrapException(e);
    } catch(IOException e) {
      throw wrapException(e);
    }
  }

  @Override
  public GetVertexStatusResponseProto getVertexStatus(RpcController controller,
      GetVertexStatusRequestProto request) throws ServiceException {
    try {
      String dagId = request.getDagId();
      String vertexName = request.getVertexName();
      VertexStatus status = real.getVertexStatus(dagId, vertexName);
      assert status instanceof VertexStatusBuilder;
      VertexStatusBuilder builder = (VertexStatusBuilder) status;
      return GetVertexStatusResponseProto.newBuilder().
                            setVertexStatus(builder.getProto()).build();
    } catch (TezException e) {
      throw wrapException(e);
    } catch(IOException e) {
      throw wrapException(e);
    }
  }
  
  ServiceException wrapException(Exception e){
    return new ServiceException(e);
  }

}
