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

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.OutputCommitterDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.RootInputLeafOutput;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.records.DAGProtos.RootInputLeafOutputProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.api.client.ProgressBuilder;
import org.apache.tez.dag.api.client.VertexStatusBuilder;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.event.SpeculatorEvent;
import org.apache.tez.dag.app.dag.impl.AMUserCodeException;
import org.apache.tez.dag.app.dag.impl.Edge;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.impl.GroupInputSpec;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;


/**
 * Main interface to interact with the job. Provides only getters.
 */
public interface Vertex extends Comparable<Vertex> {

  TezVertexID getVertexId();
  public VertexPlan getVertexPlan();

  int getDistanceFromRoot();
  String getName();
  VertexState getState();

  /**
   * Get all the counters of this vertex.
   * @return aggregate task-counters
   */
  TezCounters getAllCounters();

  Map<TezTaskID, Task> getTasks();
  Task getTask(TezTaskID taskID);
  Task getTask(int taskIndex);
  List<String> getDiagnostics();
  int getTotalTasks();
  int getCompletedTasks();
  int getSucceededTasks();
  int getRunningTasks();
  float getProgress();
  ProgressBuilder getVertexProgress();
  VertexStatusBuilder getVertexStatus(Set<StatusGetOpts> statusOptions);

  @Nullable
  TaskLocationHint getTaskLocationHint(TezTaskID taskID);

  void setParallelism(int parallelism, VertexLocationHint vertexLocationHint,
      Map<String, EdgeManagerPluginDescriptor> sourceEdgeManagers,
      Map<String, InputSpecUpdate> rootInputSpecUpdate, boolean fromVertexManager)
      throws AMUserCodeException;
  
  public void reconfigureVertex(int parallelism,
      @Nullable VertexLocationHint locationHint,
      @Nullable Map<String, EdgeProperty> sourceEdgeProperties) throws AMUserCodeException;

  void setVertexLocationHint(VertexLocationHint vertexLocationHint);
  void vertexReconfigurationPlanned();
  void doneReconfiguringVertex();

  // CHANGE THESE TO LISTS AND MAINTAIN ORDER?
  void setInputVertices(Map<Vertex, Edge> inVertices);
  void setOutputVertices(Map<Vertex, Edge> outVertices);

  Map<Vertex, Edge> getInputVertices();
  Map<Vertex, Edge> getOutputVertices();
  
  Map<String, OutputCommitter> getOutputCommitters();

  void setAdditionalInputs(List<RootInputLeafOutputProto> inputs);
  void setAdditionalOutputs(List<RootInputLeafOutputProto> outputs);

  @Nullable
  public Map<String, RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> 
    getAdditionalInputs();
  @Nullable
  public Map<String, RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor>> 
    getAdditionalOutputs();

  List<InputSpec> getInputSpecList(int taskIndex) throws AMUserCodeException;
  List<OutputSpec> getOutputSpecList(int taskIndex) throws AMUserCodeException;
  
  List<GroupInputSpec> getGroupInputSpecList(int taskIndex);
  void addSharedOutputs(Set<String> outputs);
  Set<String> getSharedOutputs();

  int getInputVerticesCount();
  int getOutputVerticesCount();
  void scheduleTasks(List<TaskWithLocationHint> tasks);
  void scheduleSpeculativeTask(TezTaskID taskId);
  Resource getTaskResource();
  
  void handleSpeculatorEvent(SpeculatorEvent event);

  ProcessorDescriptor getProcessorDescriptor();
  public DAG getDAG();
  @Nullable
  VertexTerminationCause getTerminationCause();

  // TODO remove this once RootInputVertexManager is fixed to not use
  // internal apis
  AppContext getAppContext();

  VertexState restoreFromEvent(HistoryEvent event);

  String getLogIdentifier();

  public void incrementFailedTaskAttemptCount();

  public void incrementKilledTaskAttemptCount();

  public int getFailedTaskAttemptCount();

  public int getKilledTaskAttemptCount();

  public Configuration getConf();
}
