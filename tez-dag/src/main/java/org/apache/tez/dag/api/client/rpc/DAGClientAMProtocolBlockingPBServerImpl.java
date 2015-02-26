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
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezAppMasterStatus;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClientHandler;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.DAGStatusBuilder;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.VertexStatusBuilder;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAMStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAMStatusResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAllDAGsRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAllDAGsResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGStatusResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetVertexStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetVertexStatusResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.ShutdownSessionRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.ShutdownSessionResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.SubmitDAGRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.SubmitDAGResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.TryKillDAGRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.TryKillDAGResponseProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class DAGClientAMProtocolBlockingPBServerImpl implements DAGClientAMProtocolBlockingPB {

  DAGClientHandler real;

  public DAGClientAMProtocolBlockingPBServerImpl(DAGClientHandler real) {
    this.real = real;
  }

  private UserGroupInformation getRPCUser() throws ServiceException {
    try {
      return UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw wrapException(e);
    }
  }

  @Override
  public GetAllDAGsResponseProto getAllDAGs(RpcController controller,
      GetAllDAGsRequestProto request) throws ServiceException {
    UserGroupInformation user = getRPCUser();
    if (!real.getACLManager().checkAMViewAccess(user)) {
      throw new AccessControlException("User " + user + " cannot perform AM view operation");
    }
    try{
      List<String> dagIds = real.getAllDAGs();
      return GetAllDAGsResponseProto.newBuilder().addAllDagId(dagIds).build();
    } catch(TezException e) {
      throw wrapException(e);
    }
  }

  @Override
  public GetDAGStatusResponseProto getDAGStatus(RpcController controller,
      GetDAGStatusRequestProto request) throws ServiceException {
    UserGroupInformation user = getRPCUser();
    try {
      String dagId = request.getDagId();
      long timeout = request.getTimeout();
      if (!real.getACLManager(dagId).checkDAGViewAccess(user)) {
        throw new AccessControlException("User " + user + " cannot perform DAG view operation");
      }
      DAGStatus status;
      status = real.getDAGStatus(dagId,
        DagTypeConverters.convertStatusGetOptsFromProto(
            request.getStatusOptionsList()), timeout);
      assert status instanceof DAGStatusBuilder;
      DAGStatusBuilder builder = (DAGStatusBuilder) status;
      return GetDAGStatusResponseProto.newBuilder().
                                setDagStatus(builder.getProto()).build();
    } catch (TezException e) {
      throw wrapException(e);
    }
  }

  @Override
  public GetVertexStatusResponseProto getVertexStatus(RpcController controller,
      GetVertexStatusRequestProto request) throws ServiceException {
    UserGroupInformation user = getRPCUser();
    try {
      String dagId = request.getDagId();
      if (!real.getACLManager(dagId).checkDAGViewAccess(user)) {
        throw new AccessControlException("User " + user + " cannot perform DAG view operation");
      }
      String vertexName = request.getVertexName();
      VertexStatus status = real.getVertexStatus(dagId, vertexName,
        DagTypeConverters.convertStatusGetOptsFromProto(
          request.getStatusOptionsList()));
      assert status instanceof VertexStatusBuilder;
      VertexStatusBuilder builder = (VertexStatusBuilder) status;
      return GetVertexStatusResponseProto.newBuilder().
                            setVertexStatus(builder.getProto()).build();
    } catch (TezException e) {
      throw wrapException(e);
    }
  }

  @Override
  public TryKillDAGResponseProto tryKillDAG(RpcController controller,
      TryKillDAGRequestProto request) throws ServiceException {
    UserGroupInformation user = getRPCUser();
    try {
      String dagId = request.getDagId();
      if (!real.getACLManager(dagId).checkDAGModifyAccess(user)) {
        throw new AccessControlException("User " + user + " cannot perform DAG modify operation");
      }
      real.tryKillDAG(dagId);
      return TryKillDAGResponseProto.newBuilder().build();
    } catch (TezException e) {
      throw wrapException(e);
    }
  }

  @Override
  public SubmitDAGResponseProto submitDAG(RpcController controller,
      SubmitDAGRequestProto request) throws ServiceException {
    UserGroupInformation user = getRPCUser();
    if (!real.getACLManager().checkAMModifyAccess(user)) {
      throw new AccessControlException("User " + user + " cannot perform AM modify operation");
    }
    try{
      DAGPlan dagPlan = request.getDAGPlan();
      Map<String, LocalResource> additionalResources = null;
      if (request.hasAdditionalAmResources()) {
        additionalResources = DagTypeConverters.convertFromPlanLocalResources(request
            .getAdditionalAmResources());
      }
      String dagId = real.submitDAG(dagPlan, additionalResources);
      return SubmitDAGResponseProto.newBuilder().setDagId(dagId).build();
    } catch(TezException e) {
      throw wrapException(e);
    }
  }

  ServiceException wrapException(Exception e){
    return new ServiceException(e);
  }

  @Override
  public ShutdownSessionResponseProto shutdownSession(RpcController arg0,
      ShutdownSessionRequestProto arg1) throws ServiceException {
    UserGroupInformation user = getRPCUser();
    if (!real.getACLManager().checkAMModifyAccess(user)) {
      throw new AccessControlException("User " + user + " cannot perform AM modify operation");
    }
    real.shutdownAM();
    return ShutdownSessionResponseProto.newBuilder().build();
  }

  @Override
  public GetAMStatusResponseProto getAMStatus(RpcController controller,
      GetAMStatusRequestProto request) throws ServiceException {
    UserGroupInformation user = getRPCUser();
    if (!real.getACLManager().checkAMViewAccess(user)) {
      throw new AccessControlException("User " + user + " cannot perform AM view operation");
    }
    try {
      TezAppMasterStatus sessionStatus = real.getSessionStatus();
      return GetAMStatusResponseProto.newBuilder().setStatus(
          DagTypeConverters.convertTezSessionStatusToProto(sessionStatus))
          .build();
    } catch(TezException e) {
      throw wrapException(e);
    }
  }

}
