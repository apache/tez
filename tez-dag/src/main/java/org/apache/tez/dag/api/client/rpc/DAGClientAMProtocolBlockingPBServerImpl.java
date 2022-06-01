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
import java.util.List;
import java.util.Map;

import com.google.protobuf.CodedInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetWebUIAddressRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetWebUIAddressResponseProto;
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
  final FileSystem stagingFs;

  public DAGClientAMProtocolBlockingPBServerImpl(DAGClientHandler real, FileSystem stagingFs) {
    this.real = real;
    this.stagingFs = stagingFs;
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
    real.updateLastHeartbeatTime();
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
      real.updateLastHeartbeatTime();
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
      real.updateLastHeartbeatTime();
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
      real.updateLastHeartbeatTime();
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
    real.updateLastHeartbeatTime();
    try{
      if (request.hasSerializedRequestPath()) {
        // need to deserialize large request from hdfs
        Path requestPath = new Path(request.getSerializedRequestPath());
        FileSystem fs = requestPath.getFileSystem(stagingFs.getConf());
        try (FSDataInputStream fsDataInputStream = fs.open(requestPath)) {
          CodedInputStream in =
              CodedInputStream.newInstance(fsDataInputStream);
          in.setSizeLimit(Integer.MAX_VALUE);
          request = SubmitDAGRequestProto.parseFrom(in);
        } catch (IOException e) {
          throw wrapException(e);
        }
      }
      DAGPlan dagPlan = request.getDAGPlan();
      Map<String, LocalResource> additionalResources = null;
      if (request.hasAdditionalAmResources()) {
        additionalResources = DagTypeConverters.convertFromPlanLocalResources(request
            .getAdditionalAmResources());
      }
      String dagId = real.submitDAG(dagPlan, additionalResources);
      return SubmitDAGResponseProto.newBuilder().setDagId(dagId).build();
    } catch(IOException | TezException e) {
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
    real.updateLastHeartbeatTime();
    try {
      real.shutdownAM();
      return ShutdownSessionResponseProto.newBuilder().build();
    } catch(TezException e) {
      throw wrapException(e);
    }
  }

  @Override
  public GetAMStatusResponseProto getAMStatus(RpcController controller,
      GetAMStatusRequestProto request) throws ServiceException {
    UserGroupInformation user = getRPCUser();
    if (!real.getACLManager().checkAMViewAccess(user)) {
      throw new AccessControlException("User " + user + " cannot perform AM view operation");
    }
    real.updateLastHeartbeatTime();
    try {
      TezAppMasterStatus sessionStatus = real.getTezAppMasterStatus();
      return GetAMStatusResponseProto.newBuilder().setStatus(
          DagTypeConverters.convertTezAppMasterStatusToProto(sessionStatus))
          .build();
    } catch(TezException e) {
      throw wrapException(e);
    }
  }

  @Override
  public GetWebUIAddressResponseProto getWebUIAddress(RpcController controller, GetWebUIAddressRequestProto request)
      throws ServiceException {
    String address = real.getWebUIAddress();
    return GetWebUIAddressResponseProto.newBuilder().setWebUiAddress(address).build();
  }
}
