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
package org.apache.tez.dag.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.DAGPlan.PlanTaskLocationHint;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.dag.api.DAGPlan.*;


public class DAG { // FIXME rename to Topology
  List<Vertex> vertices;
  List<Edge> edges;
  String name;
  
  HashMap<String, String> config = new HashMap<String, String>();
  
  public DAG() {
    this.vertices = new ArrayList<Vertex>();
    this.edges = new ArrayList<Edge>();
  }

  public synchronized void addVertex(Vertex vertex) {
    if (vertices.contains(vertex)) {
      throw new IllegalArgumentException(
          "Vertex " + vertex + " already defined!");
    }
    vertices.add(vertex);
  }
  
  public synchronized void addEdge(Edge edge) {
    // Sanity checks
    if (!vertices.contains(edge.getInputVertex())) {
      throw new IllegalArgumentException(
          "Input vertex " + edge.getInputVertex() + " doesn't exist!");
    }
    if (!vertices.contains(edge.getOutputVertex())) {
      throw new IllegalArgumentException(
          "Output vertex " + edge.getOutputVertex() + " doesn't exist!");    
    }
    if (edges.contains(edge)) {
      throw new IllegalArgumentException(
          "Edge " + edge + " already defined!");
    }
    
    // Inform the vertices
    edge.getInputVertex().addOutputVertex(edge.getOutputVertex(), edge.getId());
    edge.getOutputVertex().addInputVertex(edge.getInputVertex(), edge.getId());
    
    edges.add(edge);
  }
  
  public void addConfiguration(String key, String value) {
    config.put(key, value);
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public void verify() throws TezException { // FIXME better exception
    //FIXME are task resources compulsory or will the DAG AM put in a default
    //for each vertex if not specified?
  }
    
  // create protobuf message describing DAG
  public JobPlan createDag(){
    JobPlan.Builder jobBuilder = JobPlan.newBuilder();

    jobBuilder.setName(this.name);

    for(Vertex vertex : vertices){
      VertexPlan.Builder vertexBuilder = VertexPlan.newBuilder();
      vertexBuilder.setName(vertex.getVertexName());
      vertexBuilder.setType(PlanVertexType.NORMAL); // vertex type is implicitly NORMAL until  TEZ-46.
      vertexBuilder.setProcessorName(vertex.getProcessorName());

      //task config
      PlanTaskConfiguration.Builder taskConfigBuilder = PlanTaskConfiguration.newBuilder();
      Resource resource = vertex.getTaskResource();
      taskConfigBuilder.setNumTasks(vertex.getParallelism());
      taskConfigBuilder.setMemoryMb(resource.getMemory());
      taskConfigBuilder.setVirtualCores(resource.getVirtualCores());
      taskConfigBuilder.setJavaOpts(vertex.getJavaOpts());

      taskConfigBuilder.setTaskModule(vertex.getVertexName());
      PlanLocalResource.Builder localResourcesBuilder = PlanLocalResource.newBuilder();
      Map<String,LocalResource> lrs = vertex.getTaskLocalResources();
      for(String key : lrs.keySet()){
        LocalResource lr = lrs.get(key);
        localResourcesBuilder.setName(key);
        localResourcesBuilder.setUri(DagTypeConverters.convertToDAGPlan(lr.getResource()));
        localResourcesBuilder.setSize(lr.getSize());
        localResourcesBuilder.setTimeStamp(lr.getTimestamp());
        localResourcesBuilder.setType(DagTypeConverters.convertToDAGPlan(lr.getType()));
        localResourcesBuilder.setVisibility(DagTypeConverters.convertToDAGPlan(lr.getVisibility()));
        if(lr.getType() == LocalResourceType.PATTERN){
          assert lr.getPattern() != null : "resourceType=PATTERN but pattern is null";
          localResourcesBuilder.setPattern(lr.getPattern());
        }
        taskConfigBuilder.addLocalResource(localResourcesBuilder);
      }

      if(vertex.getTaskEnvironment() != null){
        for(String key : vertex.getTaskEnvironment().keySet()){
          PlanKeyValuePair.Builder envSettingBuilder = PlanKeyValuePair.newBuilder();
          envSettingBuilder.setKey(key);
          envSettingBuilder.setValue(vertex.getTaskEnvironment().get(key));
          taskConfigBuilder.addEnvironmentSetting(envSettingBuilder);
        }
      }

      if(vertex.getTaskLocationsHint() != null ){
        if(vertex.getTaskLocationsHint().getTaskLocationHints() != null){
          for(TaskLocationHint hint : vertex.getTaskLocationsHint().getTaskLocationHints()){
            PlanTaskLocationHint.Builder taskLocationHintBuilder = PlanTaskLocationHint.newBuilder();

            if(hint.getDataLocalHosts() != null){
              taskLocationHintBuilder.addAllHost(Arrays.asList(hint.getDataLocalHosts()));
            }
            if(hint.getRacks() != null){
              taskLocationHintBuilder.addAllRack(Arrays.asList(hint.getRacks()));
            }

            vertexBuilder.addTaskLocationHint(taskLocationHintBuilder);
          }
        }
      }

      for(String inEdgeId : vertex.getInputEdgeIds()){
        vertexBuilder.addInEdgeId(inEdgeId);
      }
      
      for(String outEdgeId : vertex.getOutputEdgeIds()){
        vertexBuilder.addOutEdgeId(outEdgeId);
      }
      
      vertexBuilder.setTaskConfig(taskConfigBuilder);
      jobBuilder.addVertex(vertexBuilder);
    }

    for(Edge edge : edges){
      EdgePlan.Builder edgeBuilder = EdgePlan.newBuilder();
      edgeBuilder.setId(edge.getId());
      edgeBuilder.setInputVertexName(edge.getInputVertex().getVertexName());
      edgeBuilder.setOutputVertexName(edge.getOutputVertex().getVertexName());
      edgeBuilder.setConnectionPattern(DagTypeConverters.convertToDAGPlan(edge.getEdgeProperty().connectionPattern));
      edgeBuilder.setSourceType(DagTypeConverters.convertToDAGPlan(edge.getEdgeProperty().getSourceType()));
      edgeBuilder.setInputClass(edge.getEdgeProperty().inputClass);
      edgeBuilder.setOutputClass(edge.getEdgeProperty().outputClass);

      jobBuilder.addEdge(edgeBuilder);
    }
    
    for(Entry<String, String> entry : this.config.entrySet()){
      PlanKeyValuePair.Builder kvp = PlanKeyValuePair.newBuilder();
      kvp.setKey(entry.getKey());
      kvp.setValue(entry.getValue());
      jobBuilder.addJobSetting(kvp);
    }

    return jobBuilder.build();
  }
}
