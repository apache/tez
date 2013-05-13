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

package org.apache.tez.dag.app.dag;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.engine.records.TezDAGID;
import org.apache.tez.engine.records.TezVertexID;

/**
 * Main interface to interact with the job. Provides only getters. 
 */
public interface DAG {

  TezDAGID getID();
  String getName();
  DAGState getState();
  DAGReport getReport();

  /**
   * Get all the counters of this DAG. This includes job-counters aggregated
   * together with the counters of each task. This creates a clone of the
   * Counters, so use this judiciously.  
   * @return job-counters and aggregate task-counters
   */
  TezCounters getAllCounters();

  Map<TezVertexID,Vertex> getVertices();
  Vertex getVertex(TezVertexID vertexId);
  List<String> getDiagnostics();
  int getTotalVertices();
  int getSuccessfulVertices();
  float getProgress();
  boolean isUber();
  String getUserName();
  String getQueueName();
  
  TezConfiguration getConf();
  
  DAGPlan getJobPlan();
  
  /**
   * @return the ACLs for this job for each type of JobACL given. 
   */
  Map<ApplicationAccessType, String> getJobACLs();

  /**
   * @return information for MR AppMasters (previously failed and current)
   */
  // TODO Recovery
  //List<AMInfo> getAMInfos();
  
  boolean checkAccess(UserGroupInformation callerUGI, ApplicationAccessType jobOperation);

}
