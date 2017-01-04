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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezAppMasterStatus;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClientHandler;
import org.apache.tez.dag.api.client.DAGInformation;
import org.apache.tez.dag.api.client.DAGInformationBuilder;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.DAGStatusBuilder;
import org.apache.tez.dag.api.client.TaskInformation;
import org.apache.tez.dag.api.client.TaskInformationBuilder;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.VertexStatusBuilder;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAMStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAMStatusResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAllDAGsRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAllDAGsResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGInformationRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGInformationResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetTaskInformationRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetTaskInformationResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetTaskInformationListRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetTaskInformationListResponseProto;
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
  public GetDAGInformationResponseProto getDAGInformation(RpcController controller,
      GetDAGInformationRequestProto request) throws ServiceException {
    UserGroupInformation user = getRPCUser();
    try {
      String dagId = request.getDagId();
      if (!real.getACLManager(dagId).checkDAGViewAccess(user)) {
        throw new AccessControlException("User " + user + " cannot perform get DAG information operation");
      }
      DAGInformation information = real.getDAGInformation(dagId);
      assert information instanceof DAGInformationBuilder;
      DAGInformationBuilder builder = (DAGInformationBuilder) information;
      return
        GetDAGInformationResponseProto.newBuilder()
        .setDagInformation(builder.getProto())
        .build();
    } catch (TezException e) {
      throw wrapException(e);
    }
  }

  @Override
  public GetTaskInformationResponseProto getTaskInformation(RpcController controller,
      GetTaskInformationRequestProto request) throws ServiceException {
    UserGroupInformation user = getRPCUser();
    try {
      String dagId = request.getDagId();
      if (!real.getACLManager(dagId).checkDAGViewAccess(user)) {
        throw new AccessControlException("User " + user + " cannot perform get Task information operation");
      }
      TaskInformation taskInformation = real.getTaskInformation(dagId, request.getVertexId(), request.getTaskId());
      assert taskInformation instanceof TaskInformationBuilder;
      TaskInformationBuilder builder = (TaskInformationBuilder) taskInformation;
      return GetTaskInformationResponseProto.newBuilder()
        .setTaskInformation(builder.getProto())
        .build();
    } catch (TezException e) {
      throw wrapException(e);
    }
  }

  @Override
  public GetTaskInformationListResponseProto getTaskInformationList(RpcController controller,
      GetTaskInformationListRequestProto request) throws ServiceException {
    UserGroupInformation user = getRPCUser();
    try {
      String dagId = request.getDagId();
      if (!real.getACLManager(dagId).checkDAGViewAccess(user)) {
        throw new AccessControlException("User " + user + " cannot perform get Task information operation");
      }
      String startTaskId = request.hasStartTaskId() ? request.getStartTaskId() : null;
      List<TaskInformation> taskInformationList =
        real.getTaskInformationList(dagId, request.getVertexId(), startTaskId, request.getLimit());
      GetTaskInformationListResponseProto.Builder builder = GetTaskInformationListResponseProto.newBuilder();
      for(TaskInformation taskInformation : taskInformationList) {
        assert taskInformation instanceof TaskInformationBuilder;
        TaskInformationBuilder taskInfoBuilder = (TaskInformationBuilder) taskInformation;
        builder.addTaskInformation(taskInfoBuilder.getProto());
      }

      return builder.build();
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
        try (FSDataInputStream fsDataInputStream = stagingFs.open(requestPath)) {
          request = SubmitDAGRequestProto.parseFrom(fsDataInputStream);
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

}
