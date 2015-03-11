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
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.common.security.DAGAccessControls;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezYARNUtils;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.VertexGroup.GroupInfo;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanGroupInputEdgeInfo;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.PlanVertexGroupInfo;
import org.apache.tez.dag.api.records.DAGProtos.PlanVertexType;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Top level entity that defines the DAG (Directed Acyclic Graph) representing 
 * the data flow graph. Consists of a set of Vertices and Edges connecting the 
 * vertices. Vertices represent transformations of data and edges represent 
 * movement of data between vertices.
 */
@Public
public class DAG {
  
  private static final Log LOG = LogFactory.getLog(DAG.class);
  
  final BidiMap<String, Vertex> vertices =
      new DualLinkedHashBidiMap<String, Vertex>();
  final Set<Edge> edges = Sets.newHashSet();
  final String name;
  final Collection<URI> urisForCredentials = new HashSet<URI>();
  Credentials credentials = new Credentials();
  Set<VertexGroup> vertexGroups = Sets.newHashSet();
  Set<GroupInputEdge> groupInputEdges = Sets.newHashSet();

  private DAGAccessControls dagAccessControls;
  Map<String, LocalResource> commonTaskLocalFiles = Maps.newHashMap();
  String dagInfo;
  private Map<String,String> dagConf = new HashMap<String, String>();

  private Stack<String> topologicalVertexStack = new Stack<String>();

  private DAG(String name) {
    this.name = name;
  }

  /**
   * Create a DAG with the specified name.
   * @param name the name of the DAG
   * @return this {@link DAG}
   */
  public static DAG create(String name) {
    return new DAG(name);
  }

  /**
   * Set the files etc that must be provided to the tasks of this DAG
   * @param localFiles
   *          files that must be available locally for each task. These files
   *          may be regular files, archives etc. as specified by the value
   *          elements of the map.
   * @return {@link DAG}
   */
  public synchronized DAG addTaskLocalFiles(Map<String, LocalResource> localFiles) {
    Preconditions.checkNotNull(localFiles);
    TezCommonUtils.addAdditionalLocalResources(localFiles, commonTaskLocalFiles, "DAG " + getName());
    return this;
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
   * @return {@link DAG}
   */
  public synchronized DAG setCredentials(Credentials credentials) {
    this.credentials = credentials;
    return this;
  }

  /**
   * Set description info for this DAG that can be used for visualization purposes.
   * @param dagInfo JSON blob as a serialized string.
   *                Recognized keys by the UI are:
   *                    "context" - The application context in which this DAG is being used.
   *                                For example, this could be set to "Hive" or "Pig" if
   *                                this is being run as part of a Hive or Pig script.
   *                    "description" - General description on what this DAG is going to do.
   *                                In the case of Hive, this could be the SQL query text.
   * @return {@link DAG}
   */
  public synchronized DAG setDAGInfo(String dagInfo) {
    Preconditions.checkNotNull(dagInfo);
    this.dagInfo = dagInfo;
    return this;
  }

  /**
   * Create a group of vertices that share a common output. This can be used to implement 
   * unions efficiently.
   * @param name Name of the group.
   * @param members {@link Vertex} members of the group
   * @return {@link DAG}
   */
  public synchronized VertexGroup createVertexGroup(String name, Vertex... members) {
    VertexGroup uv = new VertexGroup(name, members);
    if (!vertexGroups.add(uv)){
      throw new IllegalStateException(
          "VertexGroup " + name + " already defined!");
    }
    return uv;
  }

  @Private
  public synchronized Credentials getCredentials() {
    return this.credentials;
  }


  /**
   * Set Access controls for the DAG. Which user/groups can view the DAG progess/history and
   * who can modify the DAG i.e. kill the DAG.
   * The owner of the Tez Session and the user submitting the DAG are super-users and have access
   * to all operations on the DAG.
   * @param accessControls Access Controls
   * @return {@link DAG}
   */
  public synchronized DAG setAccessControls(DAGAccessControls accessControls) {
    this.dagAccessControls = accessControls;
    return this;
  }

  @Private
  public synchronized DAGAccessControls getDagAccessControls() {
    return dagAccessControls;
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
   * Currently, @{link credentials} can only be fetched for HDFS and other
   * {@link org.apache.hadoop.fs.FileSystem} implementations that support
   * credentials.
   * 
   * @param uris
   *          a list of {@link URI}s
   * @return {@link DAG}
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

  /**
   * Add an {@link Edge} connecting vertices in the DAG
   * @param edge The edge to be added
   * @return {@link DAG}
   */
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
  
  /**
   * Add a {@link GroupInputEdge} to the DAG.
   * @param edge {@link GroupInputEdge}
   * @return {@link DAG}
   */
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
        "GroupInputEdge " + edge + " already defined!");
    }

    VertexGroup av = edge.getInputVertexGroup();
    av.addOutputVertex(edge.getOutputVertex(), edge);
    groupInputEdges.add(edge);
    
    // add new edge between members of VertexGroup and destVertex of the GroupInputEdge
    List<Edge> newEdges = Lists.newLinkedList();
    Vertex dstVertex = edge.getOutputVertex();
    VertexGroup uv = edge.getInputVertexGroup();
    for (Vertex member : uv.getMembers()) {
      newEdges.add(Edge.create(member, dstVertex, edge.getEdgeProperty()));
    }
    dstVertex.addGroupInput(uv.getGroupName(), uv.getGroupInfo());
    
    for (Edge e : newEdges) {
      addEdge(e);
    }
    
    return this;
  }
  
  /**
   * Get the DAG name
   * @return DAG name
   */
  public String getName() {
    return this.name;
  }

  public DAG setConf(String property, String value) {
    TezConfiguration.validateProperty(property, Scope.DAG);
    dagConf.put(property, value);
    return this;
  }

  @Private
  public Map<String, LocalResource> getTaskLocalFiles() {
    return commonTaskLocalFiles;
  }

  @Private
  @VisibleForTesting
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

    // check the vertices with -1 parallelism, currently only 3 cases are allowed to has -1 parallelism.
    // It is OK not using topological order to check vertices here.
    // 1. has input initializers
    // 2. 1-1 uninited sources
    // 3. has custom vertex manager
    for (Vertex vertex : vertices.values()) {
      if (vertex.getParallelism() == -1) {
        boolean hasInputInitializer = false;
        if (vertex.getDataSources() != null && !vertex.getDataSources().isEmpty()) {
          for (DataSourceDescriptor ds : vertex.getDataSources()) {
            if (ds.getInputInitializerDescriptor() != null) {
              hasInputInitializer = true;
              break;
            }
          }
        }
        if (hasInputInitializer) {
          continue;
        } else {
          // Account for the case where the vertex has a data source with a determined number of
          // shards e.g. splits calculated on the client and not in the AM
          // In this case, vertex parallelism is setup later using the data source's numShards
          // and as a result, an initializer is not needed.
          if (vertex.getDataSources() != null
              && vertex.getDataSources().size() == 1
              &&  vertex.getDataSources().get(0).getNumberOfShards() > -1) {
            continue;
          }
        }

        boolean has1to1UninitedSources = false;
        if (vertex.getInputVertices()!= null && !vertex.getInputVertices().isEmpty()) {
          for (Vertex srcVertex : vertex.getInputVertices()) {
            if (srcVertex.getParallelism() == -1) {
              has1to1UninitedSources = true;
              break;
            }
          }
        }
        if (has1to1UninitedSources) {
          continue;
        }

        if (vertex.getVertexManagerPlugin() != null) {
          continue;
        }
        throw new IllegalStateException(vertex.getName() +
            " has -1 tasks but does not have input initializers, " +
            "1-1 uninited sources or custom vertex manager to set it at runtime");
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
  @VisibleForTesting
  void verify() throws IllegalStateException {
    verify(true);
  }

  @VisibleForTesting
  void verify(boolean restricted) throws IllegalStateException {
    if (vertices.isEmpty()) {
      throw new IllegalStateException("Invalid dag containing 0 vertices");
    }
    
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
      for (RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> 
           input : vertex.getInputs()) {
        if (vertexMap.containsKey(input.getName())) {
          throw new IllegalStateException("Vertex: "
              + vertex.getName()
              + " contains an Input with the same name as vertex: "
              + input.getName());
        }
      }
      for (RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor> 
            output : vertex.getOutputs()) {
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
      for (RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> 
           input : vertex.getInputs()) {
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
      for (RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor> 
            output : vertex.getOutputs()) {
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
      } else {
        // detect self-cycle
        if (edgeMap.containsKey(pop.v)) {
          for (Edge edge : edgeMap.get(pop.v)) {
            if (edge.getOutputVertex().equals(pop.v)) {
              throw new IllegalStateException("DAG contains a self-cycle on vertex:" + pop.v.getName());
            }
          }
        }
      }
      topologicalVertexStack.push(av.v.getName());
    }
  }

  // create protobuf message describing DAG
  @Private
  public DAGPlan createDag(Configuration tezConf, Credentials extraCredentials,
                           Map<String, LocalResource> tezJarResources, LocalResource binaryConfig,
                           boolean tezLrsAsArchive) {
    return createDag(tezConf, extraCredentials, tezJarResources, binaryConfig, tezLrsAsArchive,
        null);
  }

  // create protobuf message describing DAG
  @Private
  public synchronized DAGPlan createDag(Configuration tezConf, Credentials extraCredentials,
      Map<String, LocalResource> tezJarResources, LocalResource binaryConfig,
      boolean tezLrsAsArchive, Map<String, String> additionalConfigs) {
    verify(true);

    DAGPlan.Builder dagBuilder = DAGPlan.newBuilder();
    dagBuilder.setName(this.name);
    if (this.dagInfo != null && !this.dagInfo.isEmpty()) {
      dagBuilder.setDagInfo(this.dagInfo);
    }
    
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

    Credentials dagCredentials = new Credentials();
    if (extraCredentials != null) {
      dagCredentials.mergeAll(extraCredentials);
    }
    dagCredentials.mergeAll(credentials);
    if (!commonTaskLocalFiles.isEmpty()) {
      dagBuilder.addAllLocalResource(DagTypeConverters.convertToDAGPlan(commonTaskLocalFiles));
    }

    Preconditions.checkArgument(topologicalVertexStack.size() == vertices.size(),
        "size of topologicalVertexStack is:" + topologicalVertexStack.size() +
        " while size of vertices is:" + vertices.size() +
        ", make sure they are the same in order to sort the vertices");
    while(!topologicalVertexStack.isEmpty()) {
      Vertex vertex = vertices.get(topologicalVertexStack.pop());
      // infer credentials, resources and parallelism from data source
      Resource vertexTaskResource = vertex.getTaskResource();
      if (vertexTaskResource == null) {
        vertexTaskResource = Resource.newInstance(tezConf.getInt(
            TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB,
            TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB_DEFAULT), tezConf.getInt(
            TezConfiguration.TEZ_TASK_RESOURCE_CPU_VCORES,
            TezConfiguration.TEZ_TASK_RESOURCE_CPU_VCORES_DEFAULT));
      }
      Map<String, LocalResource> vertexLRs = Maps.newHashMap();
      vertexLRs.putAll(vertex.getTaskLocalFiles());
      List<DataSourceDescriptor> dataSources = vertex.getDataSources();
      for (DataSourceDescriptor dataSource : dataSources) {
        if (dataSource.getCredentials() != null) {
          dagCredentials.addAll(dataSource.getCredentials());
        }
        if (dataSource.getAdditionalLocalFiles() != null) {
          TezCommonUtils
              .addAdditionalLocalResources(dataSource.getAdditionalLocalFiles(), vertexLRs,
                  "Vertex " + vertex.getName());
        }
      }
      if (tezJarResources != null) {
        TezCommonUtils
            .addAdditionalLocalResources(tezJarResources, vertexLRs, "Vertex " + vertex.getName());
      }
      if (binaryConfig != null) {
        vertexLRs.put(TezConstants.TEZ_PB_BINARY_CONF_NAME, binaryConfig);
      }
      int vertexParallelism = vertex.getParallelism();
      VertexLocationHint vertexLocationHint = vertex.getLocationHint();
      if (dataSources.size() == 1) {
        DataSourceDescriptor dataSource = dataSources.get(0);
        if (vertexParallelism == -1 && dataSource.getNumberOfShards() > -1) {
          vertexParallelism = dataSource.getNumberOfShards();
        }
        if (vertexLocationHint == null && dataSource.getLocationHint() != null) {
          vertexLocationHint = dataSource.getLocationHint();
        }
      }
      if (vertexParallelism == -1) {
        Preconditions.checkState(vertexLocationHint == null,
            "Cannot specify vertex location hint without specifying vertex parallelism. Vertex: "
                + vertex.getName());
      } else if (vertexLocationHint != null) {
        Preconditions.checkState(vertexParallelism == vertexLocationHint.getTaskLocationHints().size(),
            "vertex task location hint must equal vertex parallelism. Vertex: " + vertex.getName());
      }
      for (DataSinkDescriptor dataSink : vertex.getDataSinks()) {
        if (dataSink.getCredentials() != null) {
          dagCredentials.addAll(dataSink.getCredentials());
        }
      }
      
      VertexPlan.Builder vertexBuilder = VertexPlan.newBuilder();
      vertexBuilder.setName(vertex.getName());
      vertexBuilder.setType(PlanVertexType.NORMAL); // vertex type is implicitly NORMAL until  TEZ-46.
      vertexBuilder.setProcessorDescriptor(DagTypeConverters
        .convertToDAGPlan(vertex.getProcessorDescriptor()));
      if (vertex.getInputs().size() > 0) {
        for (RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> input : vertex.getInputs()) {
          vertexBuilder.addInputs(DagTypeConverters.convertToDAGPlan(input));
        }
      }
      if (vertex.getOutputs().size() > 0) {
        for (RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor> output : vertex.getOutputs()) {
          vertexBuilder.addOutputs(DagTypeConverters.convertToDAGPlan(output));
        }
      }

      if (vertex.getConf()!= null && vertex.getConf().size() > 0) {
        ConfigurationProto.Builder confBuilder = ConfigurationProto.newBuilder();
        for (Map.Entry<String, String> entry : vertex.getConf().entrySet()) {
          PlanKeyValuePair.Builder keyValueBuilder = PlanKeyValuePair.newBuilder();
          keyValueBuilder.setKey(entry.getKey());
          keyValueBuilder.setValue(entry.getValue());
          confBuilder.addConfKeyValues(keyValueBuilder);
        }
        vertexBuilder.setVertexConf(confBuilder);
      }

      //task config
      PlanTaskConfiguration.Builder taskConfigBuilder = PlanTaskConfiguration.newBuilder();
      taskConfigBuilder.setNumTasks(vertexParallelism);
      taskConfigBuilder.setMemoryMb(vertexTaskResource.getMemory());
      taskConfigBuilder.setVirtualCores(vertexTaskResource.getVirtualCores());
      taskConfigBuilder.setJavaOpts(
          TezClientUtils.addDefaultsToTaskLaunchCmdOpts(vertex.getTaskLaunchCmdOpts(), tezConf));

      taskConfigBuilder.setTaskModule(vertex.getName());
      if (!vertexLRs.isEmpty()) {
        taskConfigBuilder.addAllLocalResource(DagTypeConverters.convertToDAGPlan(vertexLRs));
      }

      Map<String, String> taskEnv = Maps.newHashMap(vertex.getTaskEnvironment());
      TezYARNUtils.setupDefaultEnv(taskEnv, tezConf,
          TezConfiguration.TEZ_TASK_LAUNCH_ENV,
          TezConfiguration.TEZ_TASK_LAUNCH_ENV_DEFAULT, tezLrsAsArchive);
      for (Map.Entry<String, String> entry : taskEnv.entrySet()) {
        PlanKeyValuePair.Builder envSettingBuilder = PlanKeyValuePair.newBuilder();
        envSettingBuilder.setKey(entry.getKey());
        envSettingBuilder.setValue(entry.getValue());
        taskConfigBuilder.addEnvironmentSetting(envSettingBuilder);
      }

      if (vertexLocationHint != null) {
        if (vertexLocationHint.getTaskLocationHints() != null) {
          for (TaskLocationHint hint : vertexLocationHint.getTaskLocationHints()) {
            PlanTaskLocationHint.Builder taskLocationHintBuilder = PlanTaskLocationHint.newBuilder();
            // we can allow this later on if needed
            if (hint.getAffinitizedTask() != null) {
              throw new TezUncheckedException(
                  "Task based affinity may not be specified via the DAG API");
            }

            if (hint.getHosts() != null) {
              taskLocationHintBuilder.addAllHost(hint.getHosts());
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


    ConfigurationProto.Builder confProtoBuilder =
        ConfigurationProto.newBuilder();
    if (dagAccessControls != null) {
      Configuration aclConf = new Configuration(false);
      dagAccessControls.serializeToConfiguration(aclConf);
      Iterator<Entry<String, String>> aclConfIter = aclConf.iterator();
      while (aclConfIter.hasNext()) {
        Entry<String, String> entry = aclConfIter.next();
        PlanKeyValuePair.Builder kvp = PlanKeyValuePair.newBuilder();
        kvp.setKey(entry.getKey());
        kvp.setValue(entry.getValue());
        TezConfiguration.validateProperty(entry.getKey(), Scope.DAG);
        confProtoBuilder.addConfKeyValues(kvp);
      }
    }
    if (additionalConfigs != null && !additionalConfigs.isEmpty()) {
      for (Entry<String, String> entry : additionalConfigs.entrySet()) {
        PlanKeyValuePair.Builder kvp = PlanKeyValuePair.newBuilder();
        kvp.setKey(entry.getKey());
        kvp.setValue(entry.getValue());
        TezConfiguration.validateProperty(entry.getKey(), Scope.DAG);
        confProtoBuilder.addConfKeyValues(kvp);
      }
    }
    if (this.dagConf != null && !this.dagConf.isEmpty()) {
      for (Entry<String, String> entry : this.dagConf.entrySet()) {
        PlanKeyValuePair.Builder kvp = PlanKeyValuePair.newBuilder();
        kvp.setKey(entry.getKey());
        kvp.setValue(entry.getValue());
        confProtoBuilder.addConfKeyValues(kvp);
      }
    }
    dagBuilder.setDagConf(confProtoBuilder);

    if (dagCredentials != null) {
      dagBuilder.setCredentialsBinary(DagTypeConverters.convertCredentialsToProto(dagCredentials));
      TezCommonUtils.logCredentials(LOG, dagCredentials, "dag");
    }
    
    return dagBuilder.build();
  }
}
