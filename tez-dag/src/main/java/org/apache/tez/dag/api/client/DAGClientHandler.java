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

package org.apache.tez.dag.api.client;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezAppMasterStatus;
import org.apache.tez.dag.api.DAGNotRunningException;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.DAGAppMaster;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.dag.records.TezDAGID;

public class DAGClientHandler {

  private Logger LOG = LoggerFactory.getLogger(DAGClientHandler.class);

  private DAGAppMaster dagAppMaster;
  private final AtomicLong lastHeartbeatTime;
  
  public DAGClientHandler(DAGAppMaster dagAppMaster) {
    this.dagAppMaster = dagAppMaster;
    this.lastHeartbeatTime = new AtomicLong(dagAppMaster.getContext().getClock().getTime());
  }

  private DAG getCurrentDAG() {
    return dagAppMaster.getContext().getCurrentDAG();
  }

  private Set<String> getAllDagIDs() {
    return dagAppMaster.getContext().getAllDAGIDs();
  }

  public List<String> getAllDAGs() throws TezException {
    return Collections.singletonList(getCurrentDAG().getID().toString());
  }

  public DAGStatus getDAGStatus(String dagIdStr,
      Set<StatusGetOpts> statusOptions) throws TezException {
    return getDAG(dagIdStr).getDAGStatus(statusOptions);
  }

  public DAGStatus getDAGStatus(String dagIdStr,
      Set<StatusGetOpts> statusOptions, long timeout) throws TezException {
    return getDAG(dagIdStr).getDAGStatus(statusOptions, timeout);
  }

  public VertexStatus getVertexStatus(String dagIdStr, String vertexName,
      Set<StatusGetOpts> statusOptions) throws TezException {
    VertexStatus status =
        getDAG(dagIdStr).getVertexStatus(vertexName, statusOptions);
    if (status == null) {
      throw new TezException("Unknown vertexName: " + vertexName);
    }

    return status;
  }

  DAG getDAG(String dagIdStr) throws TezException {
    TezDAGID dagId;
    try {
      dagId = TezDAGID.fromString(dagIdStr);
    } catch (IllegalArgumentException e) {
      throw new TezException("Bad dagId: " + dagIdStr, e);
    }

    DAG currentDAG = getCurrentDAG();
    if (currentDAG == null) {
      throw new TezException("No running dag at present");
    }

    final String currentDAGIdStr = currentDAG.getID().toString();
    if (!currentDAGIdStr.equals(dagIdStr)) {
      if (getAllDagIDs().contains(dagIdStr)) {
        LOG.debug("Looking for finished dagId {} current dag is {}", dagIdStr, currentDAGIdStr);
        throw new DAGNotRunningException("DAG " + dagIdStr + " Not running, current dag is " +
            currentDAGIdStr);
      } else {
        LOG.warn("Current DAGID : " + currentDAGIdStr + ", Looking for string (not found): " +
            dagIdStr + ", dagIdObj: " + dagId);
        throw new TezException("Unknown dagId: " + dagIdStr);
      }
    }

    return currentDAG;
  }

  private String getClientInfo() throws TezException {
    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw new TezException(ie);
    }
    String message = callerUGI.toString();
    if(null != Server.getRemoteAddress()) {
      message += " at " + Server.getRemoteAddress();
    }
    return message;
  }

  public void tryKillDAG(String dagIdStr) throws TezException {
    DAG dag = getDAG(dagIdStr);
    String message = "Sending client kill from " + getClientInfo() +
        " to dag " + dagIdStr;
    LOG.info(message);
    dagAppMaster.tryKillDAG(dag, message);
  }

  public synchronized String submitDAG(DAGPlan dagPlan,
      Map<String, LocalResource> additionalAmResources) throws TezException {
    return dagAppMaster.submitDAGToAppMaster(dagPlan, additionalAmResources);
  }

  // Only to be invoked by the DAGClient.
  public synchronized void shutdownAM() throws TezException {
    String message = "Received message to shutdown AM from " + getClientInfo();
    LOG.info(message);
    if (dagAppMaster != null) {
      dagAppMaster.shutdownTezAM(message);
    }
  }

  public synchronized TezAppMasterStatus getTezAppMasterStatus() throws TezException {
    switch (dagAppMaster.getState()) {
    case NEW:
    case INITED:
      return TezAppMasterStatus.INITIALIZING;
    case IDLE:
      return TezAppMasterStatus.READY;
    case RECOVERING:
    case RUNNING:
      return TezAppMasterStatus.RUNNING;
    case ERROR:
    case FAILED:
    case SUCCEEDED:
    case KILLED:
      return TezAppMasterStatus.SHUTDOWN;
    }
    return TezAppMasterStatus.INITIALIZING;
  }

  public ACLManager getACLManager() {
    return dagAppMaster.getACLManager();
  }

  public ACLManager getACLManager(String dagIdStr) throws TezException {
    DAG dag = getDAG(dagIdStr);
    return dag.getACLManager();
  }

  public void updateLastHeartbeatTime() {
    lastHeartbeatTime.set(dagAppMaster.getContext().getClock().getTime());
  }

  public long getLastHeartbeatTime() {
    return lastHeartbeatTime.get();
  }

  public String getWebUIAddress() {
    return dagAppMaster.getWebUIAddress();
  }
}
