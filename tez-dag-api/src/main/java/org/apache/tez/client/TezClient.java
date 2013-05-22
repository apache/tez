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

package org.apache.tez.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezRemoteException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.rpc.DAGClientRPCImpl;

public class TezClient {
  private final TezConfiguration conf;
  
  public TezClient(TezConfiguration conf) {
    this.conf = conf;
  }

  public DAGClient getDAGClient(String appIdStr) throws IOException, TezException {
    try {
      System.out.println("Fetching app: " + appIdStr);
      ApplicationId appId = ConverterUtils.toApplicationId(appIdStr);
      YarnClient yarnClient = new YarnClientImpl();
      yarnClient.init(conf);
      yarnClient.start();
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      String host = appReport.getHost();
      int port = appReport.getRpcPort();
      return getDAGClient(host, port);
    } catch (YarnRemoteException e) {
      throw new TezException(e);
    }
  }
  
  public DAGClient getDAGClient(String host, int port) throws IOException {
    System.out.println("App host port: " + host + ":" + port);
    InetSocketAddress addr = new InetSocketAddress(host, port);
    DAGClient dagClient;
    dagClient = new DAGClientRPCImpl(1, addr, conf);
    return dagClient;    
  }
  
  public static void main(String[] args) {
    try {
      TezClient tezClient = new TezClient(
          new TezConfiguration(new YarnConfiguration()));
      DAGClient dagClient = tezClient.getDAGClient(args[1]);
      String dagId = dagClient.getAllDAGs().get(0);
      DAGStatus dagStatus = dagClient.getDAGStatus(dagId);
      System.out.println("DAG: " + dagId + 
                         " State: " + dagStatus.getState() +
                         " Progress: " + dagStatus.getDAGProgress());
      for(String vertexName : dagStatus.getVertexProgress().keySet()) {
        System.out.println("VertexStatus from DagStatus:" +
                           " Vertex: " + vertexName +
                           " Progress: " + dagStatus.getVertexProgress().get(vertexName));
        VertexStatus vertexStatus = dagClient.getVertexStatus(dagId, vertexName);
        System.out.println("VertexStatus:" + 
                           " Vertex: " + vertexName + 
                           " State: " + vertexStatus.getState() + 
                           " Progress: " + vertexStatus.getProgress());
      }
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
