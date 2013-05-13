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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.InputSpec;
import org.apache.tez.common.OutputSpec;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.DAGPlan.VertexPlan;
import org.apache.tez.engine.records.TezDependentTaskCompletionEvent;
import org.apache.tez.engine.records.TezTaskID;
import org.apache.tez.engine.records.TezVertexID;


/**
 * Main interface to interact with the job. Provides only getters. 
 */
public interface Vertex extends Comparable<Vertex> {

  TezVertexID getVertexId();
  public VertexPlan getVertexPlan();
  
  int getDistanceFromRoot();
  String getName();
  VertexState getState();
  Configuration getConf();

  /**
   * Get all the counters of this vertex. 
   * @return aggregate task-counters
   */
  TezCounters getAllCounters();

  Map<TezTaskID, Task> getTasks();
  Task getTask(TezTaskID taskID);
  List<String> getDiagnostics();
  int getTotalTasks();
  int getCompletedTasks();
  float getProgress();
  
  TezDependentTaskCompletionEvent[]
      getTaskAttemptCompletionEvents(int fromEventId, int maxEvents);
  
  void setInputVertices(Map<Vertex, EdgeProperty> inVertices);
  void setOutputVertices(Map<Vertex, EdgeProperty> outVertices);

  Map<Vertex, EdgeProperty> getInputVertices();
  Map<Vertex, EdgeProperty> getOutputVertices();
  
  List<InputSpec> getInputSpecList();
  List<OutputSpec> getOutputSpecList();

  int getInputVerticesCount();
  int getOutputVerticesCount();
  void scheduleTasks(Collection<TezTaskID> taskIDs);
  Resource getTaskResource();

  public DAG getDAG();
}
