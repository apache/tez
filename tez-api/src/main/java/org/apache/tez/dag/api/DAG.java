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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualLinkedHashBidiMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.impl.LogUtils;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.VertexGroup.GroupInfo;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanGroupInputEdgeInfo;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;
import org.apache.tez.dag.api.records.DAGProtos.PlanLocalResource;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.PlanVertexGroupInfo;
import org.apache.tez.dag.api.records.DAGProtos.PlanVertexType;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class DAG {
  
  private static final Log LOG = LogFactory.getLog(DAG.class);
  
  final BidiMap<String, Vertex> vertices = 
      new DualLinkedHashBidiMap<String, Vertex>();
  final Set<Edge> edges = Sets.newHashSet();
  final String name;
  final Collection<URI> urisForCredentials = new HashSet<URI>();
  Credentials credentials;
  Set<VertexGroup> vertexGroups = Sets.newHashSet();
  Set<GroupInputEdge> groupInputEdges = Sets.newHashSet();

  public DAG(String name) {
    this.name = name;
  }

  public synchronized DAG addVertex(Vertex vertex) {
    if (vertices.containsKey(vertex.getName())) {
      throw new IllegalStateException(
        "Vertex " + vertex.getName() + " already defined!");
    }
    vertices.put(vertex.getName(), vertex);
    return this;
  }

  public synchronized Vertex getVertex(String vertexName) {
    return vertices.get(vertexName);
  }
  
  /**
   * One of the methods that can be used to provide information about required
   * Credentials when running on a secure cluster. A combination of this and
   * addURIsForCredentials should be used to specify information about all
   * credentials required by a DAG. AM specific credentials are not used when
   * executing a DAG.
   * 
   * Set credentials which will be required to run this dag. This method can be
   * used if the client has already obtained some or all of the required
   * credentials.
   * 
   * @param credentials Credentials for the DAG
   * @return this
   */
  public synchronized DAG setCredentials(Credentials credentials) {
    this.credentials = credentials;
    return this;
  }
  
  public synchronized VertexGroup createVertexGroup(String name, Vertex... members) {
    VertexGroup uv = new VertexGroup(name, members);
    vertexGroups.add(uv);
    return uv;
  }

  @Private
  public synchronized Credentials getCredentials() {
    return this.credentials;
  }

  /**
   * One of the methods that can be used to provide information about required
   * Credentials when running on a secure cluster. A combination of this and
   * setCredentials should be used to specify information about all credentials
   * required by a DAG. AM specific credentials are not used when executing a
   * DAG.
   * 
   * This method can be used to specify a list of URIs for which Credentials
   * need to be obtained so that the job can run. An incremental list of URIs
   * can be provided by making multiple calls to the method.
   * 
   * Currently, credentials can only be fetched for HDFS and other
   * {@link org.apache.hadoop.fs.FileSystem} implementations.
   * 
   * @param uris
   *          a list of {@link URI}s
   * @return the DAG instance being used
   */
  public synchronized DAG addURIsForCredentials(Collection<URI> uris) {
    Preconditions.checkNotNull(uris, "URIs cannot be null");
    urisForCredentials.addAll(uris);
    return this;
  }

  /**
   * 
   * @return an unmodifiable list representing the URIs for which credentials
   *         are required.
   */
  @Private
  public synchronized Collection<URI> getURIsForCredentials() {
    return Collections.unmodifiableCollection(urisForCredentials);
  }
  
  @Private
  public synchronized Set<Vertex> getVertices() {
    return Collections.unmodifiableSet(this.vertices.values());
  }

  public synchronized DAG addEdge(Edge edge) {
    // Sanity checks
    if (!vertices.containsValue(edge.getInputVertex())) {
      throw new IllegalArgumentException(
        "Input vertex " + edge.getInputVertex() + " doesn't exist!");
    }
    if (!vertices.containsValue(edge.getOutputVertex())) {
      throw new IllegalArgumentException(
        "Output vertex " + edge.getOutputVertex() + " doesn't exist!");
    }
    if (edges.contains(edge)) {
      throw new IllegalArgumentException(
        "Edge " + edge + " already defined!");
    }

    // inform the vertices
    edge.getInputVertex().addOutputVertex(edge.getOutputVertex(), edge);
    edge.getOutputVertex().addInputVertex(edge.getInputVertex(), edge);

    edges.add(edge);
    return this;
  }
  
  public synchronized DAG addEdge(GroupInputEdge edge) {
    // Sanity checks
    if (!vertexGroups.contains(edge.getInputVertexGroup())) {
      throw new IllegalArgumentException(
        "Input vertex " + edge.getInputVertexGroup() + " doesn't exist!");
    }
    if (!vertices.containsValue(edge.getOutputVertex())) {
      throw new IllegalArgumentException(
        "Output vertex " + edge.getOutputVertex() + " doesn't exist!");
    }
    if (groupInputEdges.contains(edge)) {
      throw new IllegalArgumentException(
        "Edge " + edge + " already defined!");
    }

    VertexGroup av = edge.getInputVertexGroup();
    av.addOutputVertex(edge.getOutputVertex(), edge);
    groupInputEdges.add(edge);
    return this;
  }
  
  public String getName() {
    return this.name;
  }
  
  private void processEdgesAndGroups() throws IllegalStateException {
    // process all VertexGroups by transferring outgoing connections to the members
    
    // add edges between VertexGroup members and destination vertices
    List<Edge> newEdges = Lists.newLinkedList();
    for (GroupInputEdge e : groupInputEdges) {
      Vertex  dstVertex = e.getOutputVertex();
      VertexGroup uv = e.getInputVertexGroup();
      for (Vertex member : uv.getMembers()) {
        newEdges.add(new Edge(member, dstVertex, e.getEdgeProperty()));
      }
      dstVertex.addGroupInput(uv.getGroupName(), uv.getGroupInfo());
    }
    
    for (Edge e : newEdges) {
      addEdge(e);
    }
    
    // add outputs to VertexGroup members
    for(VertexGroup av : vertexGroups) {
      for (RootInputLeafOutput<OutputDescriptor> output : av.getOutputs()) {
        for (Vertex member : av.getMembers()) {
          member.addAdditionalOutput(output);
        }
      }
    }
  }
  
  void checkAndInferOneToOneParallelism() {
    // infer all 1-1 via dependencies
    // collect all 1-1 edges where the source parallelism is set
    Set<Vertex> newKnownTasksVertices = Sets.newHashSet();
    for (Vertex vertex : vertices.values()) {
      if (vertex.getParallelism() > -1) {
        newKnownTasksVertices.add(vertex);
      }
    }
    
    // walk through all known source 1-1 edges and infer parallelism
    // add newly inferred vertices for consideration as known sources
    // the outer loop will run for every new level of inferring the parallelism
    // however, the entire logic will process each vertex only once
    while(!newKnownTasksVertices.isEmpty()) {
      Set<Vertex> knownTasksVertices = Sets.newHashSet(newKnownTasksVertices);
      newKnownTasksVertices.clear();
      for (Vertex v : knownTasksVertices) {
        for (Edge e : v.getOutputEdges()) {
          if (e.getEdgeProperty().getDataMovementType() == DataMovementType.ONE_TO_ONE) {
            Vertex outVertex = e.getOutputVertex();
            if (outVertex.getParallelism() == -1) {
              LOG.info("Inferring parallelism for vertex: "
                  + outVertex.getName() + " to be " + v.getParallelism()
                  + " from 1-1 connection with vertex " + v.getName());
              outVertex.setParallelism(v.getParallelism());
              newKnownTasksVertices.add(outVertex);
            }
          }
        }
      }
    }
    
    // check for inconsistency and errors
    for (Edge e : edges) {
      Vertex inputVertex = e.getInputVertex();
      Vertex outputVertex = e.getOutputVertex();
      
      if (e.getEdgeProperty().getDataMovementType() == DataMovementType.ONE_TO_ONE) {
        if (inputVertex.getParallelism() != outputVertex.getParallelism()) {
          // both should be equal or equal to -1.
          if (outputVertex.getParallelism() != -1) {
            throw new TezUncheckedException(
                "1-1 Edge. Destination vertex parallelism must match source vertex. "
                + "Vertex: " + inputVertex.getName() + " does not match vertex: " 
                + outputVertex.getName());
          }
        }
      }
    }
  }
  
  // AnnotatedVertex is used by verify()
  private static class AnnotatedVertex {
    Vertex v;

    int index; //for Tarjan's algorithm
    int lowlink; //for Tarjan's algorithm
    boolean onstack; //for Tarjan's algorithm


    private AnnotatedVertex(Vertex v) {
      this.v = v;
      index = -1;
      lowlink = -1;
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

  public void verify(boolean restricted) throws IllegalStateException {
    if (vertices.isEmpty()) {
      throw new IllegalStateException("Invalid dag containing 0 vertices");
    }

    processEdgesAndGroups();
    
    // check for valid vertices, duplicate vertex names,
    // and prepare for cycle detection
    Map<String, AnnotatedVertex> vertexMap = new HashMap<String, AnnotatedVertex>();
    Map<Vertex, Set<String>> inboundVertexMap = new HashMap<Vertex, Set<String>>();
    Map<Vertex, Set<String>> outboundVertexMap = new HashMap<Vertex, Set<String>>();
    for (Vertex v : vertices.values()) {
      if (vertexMap.containsKey(v.getName())) {
        throw new IllegalStateException("DAG contains multiple vertices"
          + " with name: " + v.getName());
      }
      vertexMap.put(v.getName(), new AnnotatedVertex(v));
    }

    Map<Vertex, List<Edge>> edgeMap = new HashMap<Vertex, List<Edge>>();
    for (Edge e : edges) {
      // Construct structure for cycle detection
      Vertex inputVertex = e.getInputVertex();
      Vertex outputVertex = e.getOutputVertex();      
      List<Edge> edgeList = edgeMap.get(inputVertex);
      if (edgeList == null) {
        edgeList = new ArrayList<Edge>();
        edgeMap.put(inputVertex, edgeList);
      }
      edgeList.add(e);
      
      // Construct map for Input name verification
      Set<String> inboundSet = inboundVertexMap.get(outputVertex);
      if (inboundSet == null) {
        inboundSet = new HashSet<String>();
        inboundVertexMap.put(outputVertex, inboundSet);
      }
      inboundSet.add(inputVertex.getName());
      
      // Construct map for Output name verification
      Set<String> outboundSet = outboundVertexMap.get(inputVertex);
      if (outboundSet == null) {
        outboundSet = new HashSet<String>();
        outboundVertexMap.put(inputVertex, outboundSet);
      }
      outboundSet.add(outputVertex.getName());
    }

    // check input and output names don't collide with vertex names
    for (Vertex vertex : vertices.values()) {
      for (RootInputLeafOutput<InputDescriptor> input : vertex.getInputs()) {
        if (vertexMap.containsKey(input.getName())) {
          throw new IllegalStateException("Vertex: "
              + vertex.getName()
              + " contains an Input with the same name as vertex: "
              + input.getName());
        }
      }
      for (RootInputLeafOutput<OutputDescriptor> output : vertex.getOutputs()) {
        if (vertexMap.containsKey(output.getName())) {
          throw new IllegalStateException("Vertex: "
              + vertex.getName()
              + " contains an Output with the same name as vertex: "
              + output.getName());
        }
      }
    }

    // Check for valid InputNames
    for (Entry<Vertex, Set<String>> entry : inboundVertexMap.entrySet()) {
      Vertex vertex = entry.getKey();
      for (RootInputLeafOutput<InputDescriptor> input : vertex.getInputs()) {
        if (entry.getValue().contains(input.getName())) {
          throw new IllegalStateException("Vertex: "
              + vertex.getName()
              + " contains an incoming vertex and Input with the same name: "
              + input.getName());
        }
      }
    }

    // Check for valid OutputNames
    for (Entry<Vertex, Set<String>> entry : outboundVertexMap.entrySet()) {
      Vertex vertex = entry.getKey();
      for (RootInputLeafOutput<OutputDescriptor> output : vertex.getOutputs()) {
        if (entry.getValue().contains(output.getName())) {
          throw new IllegalStateException("Vertex: "
              + vertex.getName()
              + " contains an outgoing vertex and Output with the same name: "
              + output.getName());
        }
      }
    }
    
    
    // Not checking for repeated input names / output names vertex names on the same vertex,
    // since we only allow 1 at the moment.
    // When additional inputs are supported, this can be chceked easily (and early)
    // within the addInput / addOutput call itself.

    detectCycles(edgeMap, vertexMap);
    
    checkAndInferOneToOneParallelism();

    if (restricted) {
      for (Edge e : edges) {
        if (e.getEdgeProperty().getDataSourceType() !=
          DataSourceType.PERSISTED) {
          throw new IllegalStateException(
            "Unsupported source type on edge. " + e);
        }
        if (e.getEdgeProperty().getSchedulingType() !=
          SchedulingType.SEQUENTIAL) {
          throw new IllegalStateException(
            "Unsupported scheduling type on edge. " + e);
        }
      }
    }
  }

  // Adaptation of Tarjan's algorithm for connected components.
  // http://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
  private void detectCycles(Map<Vertex, List<Edge>> edgeMap, Map<String, AnnotatedVertex> vertexMap)
    throws IllegalStateException {
    Integer nextIndex = 0; // boxed integer so it is passed by reference.
    Stack<AnnotatedVertex> stack = new Stack<DAG.AnnotatedVertex>();
    for (AnnotatedVertex av : vertexMap.values()) {
      if (av.index == -1) {
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
    Stack<AnnotatedVertex> stack, Integer nextIndex) throws IllegalStateException {
    av.index = nextIndex;
    av.lowlink = nextIndex;
    nextIndex++;
    stack.push(av);
    av.onstack = true;

    List<Edge> edges = edgeMap.get(av.v);
    if (edges != null) {
      for (Edge e : edgeMap.get(av.v)) {
        AnnotatedVertex outVertex = vertexMap.get(e.getOutputVertex().getName());
        if (outVertex.index == -1) {
          strongConnect(outVertex, vertexMap, edgeMap, stack, nextIndex);
          av.lowlink = Math.min(av.lowlink, outVertex.lowlink);
        } else if (outVertex.onstack) {
          // strongly connected component detected, but we will wait till later so that the full cycle can be displayed.
          // update lowlink in case outputVertex should be considered the root of this component.
          av.lowlink = Math.min(av.lowlink, outVertex.index);
        }
      }
    }

    if (av.lowlink == av.index) {
      AnnotatedVertex pop = stack.pop();
      pop.onstack = false;
      if (pop != av) {
        // there was something on the stack other than this "av".
        // this indicates there is a scc/cycle. It comprises all nodes from top of stack to "av"
        StringBuilder message = new StringBuilder();
        message.append(av.v.getName()).append(" <- ");
        for (; pop != av; pop = stack.pop()) {
          message.append(pop.v.getName()).append(" <- ");
          pop.onstack = false;
        }
        message.append(av.v.getName());
        throw new IllegalStateException("DAG contains a cycle: " + message);
      }
    }
  }


  // create protobuf message describing DAG
  @Private
  public DAGPlan createDag(Configuration dagConf) {
    verify(true);

    DAGPlan.Builder dagBuilder = DAGPlan.newBuilder();

    dagBuilder.setName(this.name);
    
    if (!vertexGroups.isEmpty()) {
      for (VertexGroup av : vertexGroups) {
        GroupInfo groupInfo = av.getGroupInfo();
        PlanVertexGroupInfo.Builder groupBuilder = PlanVertexGroupInfo.newBuilder();
        groupBuilder.setGroupName(groupInfo.getGroupName());
        for (Vertex v : groupInfo.getMembers()) {
          groupBuilder.addGroupMembers(v.getName());
        }
        groupBuilder.addAllOutputs(groupInfo.outputs);
        for (Map.Entry<String, InputDescriptor> entry : 
             groupInfo.edgeMergedInputs.entrySet()) {
          groupBuilder.addEdgeMergedInputs(
              PlanGroupInputEdgeInfo.newBuilder().setDestVertexName(entry.getKey()).
              setMergedInput(DagTypeConverters.convertToDAGPlan(entry.getValue())));
        }
        dagBuilder.addVertexGroups(groupBuilder); 
      }
    }

    for (Vertex vertex : vertices.values()) {
      VertexPlan.Builder vertexBuilder = VertexPlan.newBuilder();
      vertexBuilder.setName(vertex.getName());
      vertexBuilder.setType(PlanVertexType.NORMAL); // vertex type is implicitly NORMAL until  TEZ-46.
      vertexBuilder.setProcessorDescriptor(DagTypeConverters
        .convertToDAGPlan(vertex.getProcessorDescriptor()));
      if (vertex.getInputs().size() > 0) {
        for (RootInputLeafOutput<InputDescriptor> input : vertex.getInputs()) {
          vertexBuilder.addInputs(DagTypeConverters.convertToDAGPlan(input));
        }
      }
      if (vertex.getOutputs().size() > 0) {
        for (RootInputLeafOutput<OutputDescriptor> output : vertex.getOutputs()) {
          vertexBuilder.addOutputs(DagTypeConverters.convertToDAGPlan(output));
        }
      }

      //task config
      PlanTaskConfiguration.Builder taskConfigBuilder = PlanTaskConfiguration.newBuilder();
      Resource resource = vertex.getTaskResource();
      taskConfigBuilder.setNumTasks(vertex.getParallelism());
      taskConfigBuilder.setMemoryMb(resource.getMemory());
      taskConfigBuilder.setVirtualCores(resource.getVirtualCores());
      taskConfigBuilder.setJavaOpts(vertex.getTaskLaunchCmdOpts());

      taskConfigBuilder.setTaskModule(vertex.getName());
      PlanLocalResource.Builder localResourcesBuilder = PlanLocalResource.newBuilder();
      localResourcesBuilder.clear();
      for (Entry<String, LocalResource> entry :
             vertex.getTaskLocalFiles().entrySet()) {
        String key = entry.getKey();
        LocalResource lr = entry.getValue();
        localResourcesBuilder.setName(key);
        localResourcesBuilder.setUri(
          DagTypeConverters.convertToDAGPlan(lr.getResource()));
        localResourcesBuilder.setSize(lr.getSize());
        localResourcesBuilder.setTimeStamp(lr.getTimestamp());
        localResourcesBuilder.setType(
          DagTypeConverters.convertToDAGPlan(lr.getType()));
        localResourcesBuilder.setVisibility(
          DagTypeConverters.convertToDAGPlan(lr.getVisibility()));
        if (lr.getType() == LocalResourceType.PATTERN) {
          if (lr.getPattern() == null || lr.getPattern().isEmpty()) {
            throw new TezUncheckedException("LocalResource type set to pattern"
              + " but pattern is null or empty");
          }
          localResourcesBuilder.setPattern(lr.getPattern());
        }
        taskConfigBuilder.addLocalResource(localResourcesBuilder);
      }
      
      for (String key : vertex.getTaskEnvironment().keySet()) {
        PlanKeyValuePair.Builder envSettingBuilder = PlanKeyValuePair.newBuilder();
        envSettingBuilder.setKey(key);
        envSettingBuilder.setValue(vertex.getTaskEnvironment().get(key));
        taskConfigBuilder.addEnvironmentSetting(envSettingBuilder);
      }

      if (vertex.getTaskLocationsHint() != null) {
        if (vertex.getTaskLocationsHint().getTaskLocationHints() != null) {
          for (TaskLocationHint hint : vertex.getTaskLocationsHint().getTaskLocationHints()) {
            PlanTaskLocationHint.Builder taskLocationHintBuilder = PlanTaskLocationHint.newBuilder();

            if (hint.getAffinitizedContainer() != null) {
              throw new TezUncheckedException(
                  "Container affinity may not be specified via the DAG API");
            }
            if (hint.getDataLocalHosts() != null) {
              taskLocationHintBuilder.addAllHost(hint.getDataLocalHosts());
            }
            if (hint.getRacks() != null) {
              taskLocationHintBuilder.addAllRack(hint.getRacks());
            }

            vertexBuilder.addTaskLocationHint(taskLocationHintBuilder);
          }
        }
      }
      
      if (vertex.getVertexManagerPlugin() != null) {
        vertexBuilder.setVertexManagerPlugin(DagTypeConverters
            .convertToDAGPlan(vertex.getVertexManagerPlugin()));
      }

      for (Edge inEdge : vertex.getInputEdges()) {
        vertexBuilder.addInEdgeId(inEdge.getId());
      }

      for (Edge outEdge : vertex.getOutputEdges()) {
        vertexBuilder.addOutEdgeId(outEdge.getId());
      }

      vertexBuilder.setTaskConfig(taskConfigBuilder);
      dagBuilder.addVertex(vertexBuilder);
    }

    for (Edge edge : edges) {
      EdgePlan.Builder edgeBuilder = EdgePlan.newBuilder();
      edgeBuilder.setId(edge.getId());
      edgeBuilder.setInputVertexName(edge.getInputVertex().getName());
      edgeBuilder.setOutputVertexName(edge.getOutputVertex().getName());
      edgeBuilder.setDataMovementType(DagTypeConverters.convertToDAGPlan(edge.getEdgeProperty().getDataMovementType()));
      edgeBuilder.setDataSourceType(DagTypeConverters.convertToDAGPlan(edge.getEdgeProperty().getDataSourceType()));
      edgeBuilder.setSchedulingType(DagTypeConverters.convertToDAGPlan(edge.getEdgeProperty().getSchedulingType()));
      edgeBuilder.setEdgeSource(DagTypeConverters.convertToDAGPlan(edge.getEdgeProperty().getEdgeSource()));
      edgeBuilder.setEdgeDestination(DagTypeConverters.convertToDAGPlan(edge.getEdgeProperty().getEdgeDestination()));
      if (edge.getEdgeProperty().getDataMovementType() == DataMovementType.CUSTOM) {
        if (edge.getEdgeProperty().getEdgeManagerDescriptor() != null) {
          edgeBuilder.setEdgeManager(DagTypeConverters.convertToDAGPlan(edge.getEdgeProperty().getEdgeManagerDescriptor()));
        } // else the AM will deal with this.
      }
      dagBuilder.addEdge(edgeBuilder);
    }

    if (dagConf != null) {
      Iterator<Entry<String, String>> iter = dagConf.iterator();
      ConfigurationProto.Builder confProtoBuilder =
        ConfigurationProto.newBuilder();
      while (iter.hasNext()) {
        Entry<String, String> entry = iter.next();
        PlanKeyValuePair.Builder kvp = PlanKeyValuePair.newBuilder();
        kvp.setKey(entry.getKey());
        kvp.setValue(entry.getValue());
        confProtoBuilder.addConfKeyValues(kvp);
      }
      dagBuilder.setDagKeyValues(confProtoBuilder);
    }
    if (credentials != null) {
      dagBuilder.setCredentialsBinary(DagTypeConverters.convertCredentialsToProto(credentials));
      LogUtils.logCredentials(LOG, credentials, "dag");
    }
    return dagBuilder.build();
  }
}
