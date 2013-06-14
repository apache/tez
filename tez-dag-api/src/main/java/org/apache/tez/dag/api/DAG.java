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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;
import org.apache.tez.dag.api.records.DAGProtos.PlanLocalResource;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.PlanVertexType;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;


public class DAG { // FIXME rename to Topology
  final List<Vertex> vertices;
  final List<Edge> edges;
  final String name;
  
  HashMap<String, String> config = new HashMap<String, String>();
  
  public DAG(String name) {
    this.vertices = new ArrayList<Vertex>();
    this.edges = new ArrayList<Edge>();
    this.name = name;
  }

  public synchronized DAG addVertex(Vertex vertex) {
    if (vertices.contains(vertex)) {
      throw new IllegalArgumentException(
          "Vertex " + vertex + " already defined!");
    }
    vertices.add(vertex);
    return this;
  }

  @Private
  public synchronized List<Vertex> getVertices() {
    return Collections.unmodifiableList(this.vertices);
  }
  
  public synchronized DAG addEdge(Edge edge) {
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
    return this;
  }
  
  public DAG addConfiguration(String key, String value) {
    config.put(key, value);
    return this;
  }

  public String getName() {
    return this.name;
  }
  
  // AnnotatedVertex is used by verify() 
  private class AnnotatedVertex {
    Vertex v;
  
    int index; //for Tarjan's algorithm    
    int lowlink; //for Tarjan's algorithm
    boolean onstack; //for Tarjan's algorithm 

    int inDegree;
    int outDegree;
    
    private AnnotatedVertex(Vertex v){
       this.v = v;
       index = -1;
       lowlink = -1;
       inDegree = 0;
       outDegree = 0;
    }
  }
  
  // verify()
  // 
  // Default rules
  //   Illegal:
  //     - duplicate vertex id
  //     - cycles
  //
  //   Ok:
  //     - orphaned vertex.  Occurs in map-only
  //     - islands.  Occurs if job has unrelated workflows.
  //
  //   Not yet categorized:
  //     - orphaned vertex in DAG of >1 vertex.  Could be unrelated map-only job.
  //     - v1->v2 via two edges.  perhaps some self-join job would use this?
  //
  // "restricted" mode: 
  //   In short term, the supported DAGs are limited. Call with restricted=true for these verifications.  
  //   Illegal: 
  //     - any vertex with more than one input or output edge. (n-ary input, n-ary merge) 
  public void verify() throws IllegalStateException {
    verify(true);
  }
  
  public void verify(boolean restricted) throws IllegalStateException  { 
    Map<Vertex, List<Edge>> edgeMap = new HashMap<Vertex, List<Edge>>();
    for(Edge e : edges){
      Vertex inputVertex = e.getInputVertex();
      List<Edge> edgeList = edgeMap.get(inputVertex);
      if(edgeList == null){
        edgeList = new ArrayList<Edge>();
        edgeMap.put(inputVertex, edgeList);
      }
      edgeList.add(e);
    }
    
    // check for duplicate vertex names, and prepare for cycle detection
    Map<String, AnnotatedVertex> vertexMap = new HashMap<String, AnnotatedVertex>();
    for(Vertex v : vertices){
      if(vertexMap.containsKey(v.getVertexName())){
         throw new IllegalStateException("DAG contains multiple vertices with name: " + v.getVertexName());
      }
      vertexMap.put(v.getVertexName(), new AnnotatedVertex(v));
    }
    
    detectCycles(edgeMap, vertexMap);
    
    if(restricted){
      for(Edge e : edges){
        vertexMap.get(e.getInputVertex().getVertexName()).outDegree++;
        vertexMap.get(e.getOutputVertex().getVertexName()).inDegree++;
      }
      for(AnnotatedVertex av: vertexMap.values()){
        if(av.inDegree > 1){
          throw new IllegalStateException("Vertex has inDegree>1: " + av.v.getVertexName());
        }
        if(av.outDegree > 1){
          throw new IllegalStateException("Vertex has outDegree>1: " + av.v.getVertexName());
        }
      }
    }
  }
  
  // Adaptation of Tarjan's algorithm for connected components.
  // http://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
  private void detectCycles(Map<Vertex, List<Edge>> edgeMap, Map<String, AnnotatedVertex> vertexMap) 
      throws IllegalStateException{
    Integer nextIndex = 0; // boxed integer so it is passed by reference.
    Stack<AnnotatedVertex> stack = new Stack<DAG.AnnotatedVertex>();
    for(AnnotatedVertex av: vertexMap.values()){
      if(av.index == -1){
        assert stack.empty();
        strongConnect(av, vertexMap, edgeMap, stack, nextIndex);
      }
    }
  }

  // part of Tarjan's algorithm for connected components.
  // http://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
  private void strongConnect(
          AnnotatedVertex av, 
          Map<String, AnnotatedVertex> vertexMap, 
          Map<Vertex, List<Edge>> edgeMap, 
          Stack<AnnotatedVertex> stack, Integer nextIndex) throws IllegalStateException{
    av.index = nextIndex;
    av.lowlink = nextIndex;
    nextIndex++;
    stack.push(av);
    av.onstack = true;
    
    List<Edge> edges = edgeMap.get(av.v);
    if(edges != null){
      for(Edge e : edgeMap.get(av.v)){
        AnnotatedVertex outVertex = vertexMap.get(e.getOutputVertex().getVertexName());
        if(outVertex.index == -1){
          strongConnect(outVertex, vertexMap, edgeMap, stack, nextIndex);
          av.lowlink = Math.min(av.lowlink, outVertex.lowlink);
        }
        else if(outVertex.onstack){
          // strongly connected component detected, but we will wait till later so that the full cycle can be displayed.
          // update lowlink in case outputVertex should be considered the root of this component.
          av.lowlink = Math.min(av.lowlink, outVertex.index);
        }
      }
    }

    if(av.lowlink == av.index ){
       AnnotatedVertex pop = stack.pop();
       pop.onstack = false;
       if(pop != av){
         // there was something on the stack other than this "av".
         // this indicates there is a scc/cycle. It comprises all nodes from top of stack to "av"
         StringBuilder message = new StringBuilder();
         message.append(av.v.getVertexName() + " <- ");
         for( ; pop != av; pop = stack.pop()){ 
           message.append(pop.v.getVertexName() + " <- ");
           pop.onstack = false;
         }
         message.append(av.v.getVertexName());
         throw new IllegalStateException("DAG contains a cycle: " + message);
       }
    }
  }
 
  
  // create protobuf message describing DAG
  public DAGPlan createDag() {
    
    verify(true);
    
    DAGPlan.Builder jobBuilder = DAGPlan.newBuilder();  

    jobBuilder.setName(this.name);

    for(Vertex vertex : vertices){
      VertexPlan.Builder vertexBuilder = VertexPlan.newBuilder();
      vertexBuilder.setName(vertex.getVertexName());
      vertexBuilder.setType(PlanVertexType.NORMAL); // vertex type is implicitly NORMAL until  TEZ-46.
      vertexBuilder.setProcessorDescriptor(DagTypeConverters
          .convertToDAGPlan(vertex.getProcessorDescriptor()));      
      
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
      edgeBuilder.setEdgeSource(DagTypeConverters.convertToDAGPlan(edge.getEdgeProperty().getEdgeSource()));
      edgeBuilder.setEdgeDestination(DagTypeConverters.convertToDAGPlan(edge.getEdgeProperty().getEdgeDestination()));
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
