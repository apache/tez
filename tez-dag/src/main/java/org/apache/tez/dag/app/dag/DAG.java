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
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGStatusBuilder;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatusBuilder;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.serviceplugins.api.DagInfo;

/**
 * Main interface to interact with the job.
 */
public interface DAG extends DagInfo {

  TezDAGID getID();
  Map<String, LocalResource> getLocalResources();
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
  TezCounters getCachedCounters();

  @SuppressWarnings("rawtypes")
  EventHandler getEventHandler();

  /**
   * Get Vertex by vertex name
   */
  Vertex getVertex(String vertexName);
  Map<TezVertexID,Vertex> getVertices();
  Vertex getVertex(TezVertexID vertexId);
  List<String> getDiagnostics();
  int getSuccessfulVertices();
  float getProgress();
  float getCompletedTaskProgress();
  boolean isUber();
  String getUserName();

  Configuration getConf();

  DAGPlan getJobPlan();
  DAGStatusBuilder getDAGStatus(Set<StatusGetOpts> statusOptions);
  DAGStatusBuilder getDAGStatus(Set<StatusGetOpts> statusOptions, long timeout)
      throws TezException;
  VertexStatusBuilder getVertexStatus(String vertexName,
                                      Set<StatusGetOpts> statusOptions);

  boolean isComplete();

  UserGroupInformation getDagUGI();

  ACLManager getACLManager();

  Map<String, TezVertexID> getVertexNameIDMapping();

  long getStartTime();

  StateChangeNotifier getStateChangeNotifier();

  org.apache.tez.dag.api.Vertex.VertexExecutionContext getDefaultExecutionContext();

}
