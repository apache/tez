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
import java.util.List;

import org.apache.tez.dag.api.TezRemoteException;

/*
 * Inteface class for monitoring the <code>DAG</code> running in a Tez DAG
 * Application Master.
 */
public interface DAGClient {
  
  /**
   * Return the identifiers for all DAG's
   */
  List<String> getAllDAGs() throws IOException, TezRemoteException;
  
  /**
   * Get the status of a DAG
   */
  DAGStatus getDAGStatus(String dagId) throws IOException, TezRemoteException;
  
  /**
   * Get the status of a Vertex of a DAG 
   */
  VertexStatus getVertexStatus(String dagId, 
                               String vertexName) 
                                   throws IOException, TezRemoteException;
}
