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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.VertexGroup.GroupInfo;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.runtime.api.LogicalIOProcessor;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.TezRootInputInitializer;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class Vertex {

  private final String vertexName;
  private final ProcessorDescriptor processorDescriptor;

  private int parallelism;
  private VertexLocationHint taskLocationsHint;
  private final Resource taskResource;
  private Map<String, LocalResource> taskLocalResources = new HashMap<String, LocalResource>();
  private Map<String, String> taskEnvironment = new HashMap<String, String>();
  private final List<RootInputLeafOutput<InputDescriptor>> additionalInputs 
                      = new ArrayList<RootInputLeafOutput<InputDescriptor>>();
  private final List<RootInputLeafOutput<OutputDescriptor>> additionalOutputs 
                      = new ArrayList<RootInputLeafOutput<OutputDescriptor>>();
  private VertexManagerPluginDescriptor vertexManagerPlugin;

  private final List<Vertex> inputVertices = new ArrayList<Vertex>();
  private final List<Vertex> outputVertices = new ArrayList<Vertex>();
  private final List<Edge> inputEdges = new ArrayList<Edge>();
  private final List<Edge> outputEdges = new ArrayList<Edge>();
  private final Map<String, GroupInfo> groupInputs = Maps.newHashMap();
  
  private String javaOpts = "";

  public Vertex(String vertexName,
      ProcessorDescriptor processorDescriptor,
      int parallelism,
      Resource taskResource) {
    this.vertexName = vertexName;
    this.processorDescriptor = processorDescriptor;
    this.parallelism = parallelism;
    this.taskResource = taskResource;
    if (parallelism < -1) {
      throw new IllegalArgumentException(
          "Parallelism should be -1 if determined by the AM"
          + ", otherwise should be >= 0");
    }
    if (taskResource == null) {
      throw new IllegalArgumentException("Resource cannot be null");
    }
  }

  public String getVertexName() { // FIXME rename to getName()
    return vertexName;
  }

  public ProcessorDescriptor getProcessorDescriptor() {
    return this.processorDescriptor;
  }

  public int getParallelism() {
    return parallelism;
  }
  
  void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  public Resource getTaskResource() {
    return taskResource;
  }

  public Vertex setTaskLocationsHint(List<TaskLocationHint> locations) {
    if (locations == null) {
      return this;
    }
    Preconditions.checkArgument((locations.size() == parallelism), 
        "Locations array length must match the parallelism set for the vertex");
    taskLocationsHint = new VertexLocationHint(locations);
    return this;
  }

  // used internally to create parallelism location resource file
  VertexLocationHint getTaskLocationsHint() {
    return taskLocationsHint;
  }

  public Vertex setTaskLocalResources(Map<String, LocalResource> localResources) {
    if (localResources == null) {
      this.taskLocalResources = new HashMap<String, LocalResource>();
    } else {
      this.taskLocalResources = localResources;
    }
    return this;
  }

  public Map<String, LocalResource> getTaskLocalResources() {
    return taskLocalResources;
  }

  public Vertex setTaskEnvironment(Map<String, String> environment) {
    Preconditions.checkArgument(environment != null);
    this.taskEnvironment.putAll(environment);
    return this;
  }

  public Map<String, String> getTaskEnvironment() {
    return taskEnvironment;
  }

  public Vertex setJavaOpts(String javaOpts){
     this. javaOpts = javaOpts;
     return this;
  }
  
  /**
   * Specifies an Input for a Vertex. This is meant to be used when a Vertex
   * reads Input directly from an external source </p>
   * 
   * For vertices which read data generated by another vertex - use the
   * {@link DAG addEdge} method.
   * 
   * If a vertex needs to use data generated by another vertex in the DAG and
   * also from an external source, a combination of this API and the DAG.addEdge
   * API can be used. </p>
   * 
   * Note: If more than one RootInput exists on a vertex, which generates events which need to be
   * routed, or generates information to set parallelism, a custom vertex manager should be setup
   * to handle this. Not using a custom vertex manager for such a scenario will lead to a
   * runtime failure. 
   * 
   * @param inputName
   *          the name of the input. This will be used when accessing the input
   *          in the {@link LogicalIOProcessor}
   * @param inputDescriptor
   *          the inputDescriptor for this input
   * @param inputInitializer
   *          An initializer for this Input which may run within the AM. This
   *          can be used to set the parallelism for this vertex and generate
   *          {@link RootInputDataInformationEvent}s for the actual Input.</p>
   *          If this is not specified, the parallelism must be set for the
   *          vertex. In addition, the Input should know how to access data for
   *          each of it's tasks. </p> If a {@link TezRootInputInitializer} is
   *          meant to determine the parallelism of the vertex, the initial
   *          vertex parallelism should be set to -1.
   * @return this Vertex
   */
  public Vertex addInput(String inputName, InputDescriptor inputDescriptor,
      Class<? extends TezRootInputInitializer> inputInitializer) {
    additionalInputs.add(new RootInputLeafOutput<InputDescriptor>(inputName,
        inputDescriptor, inputInitializer));
    return this;
  }

  /**
   * Specifies an Output for a Vertex. This is meant to be used when a Vertex
   * writes Output directly to an external destination. </p>
   * 
   * If an output of the vertex is meant to be consumed by another Vertex in the
   * DAG - use the {@link DAG addEdge} method.
   * 
   * If a vertex needs generate data to an external source as well as for
   * another Vertex in the DAG, a combination of this API and the DAG.addEdge
   * API can be used.
   * 
   * @param outputName
   *          the name of the output. This will be used when accessing the
   *          output in the {@link LogicalIOProcessor}
   * @param outputDescriptor
   * @param outputCommitterClazz Class to be used for the OutputCommitter.
   *                             Can be null.
   * @return this Vertex
   */
  public Vertex addOutput(String outputName, OutputDescriptor outputDescriptor,
      Class<? extends OutputCommitter> outputCommitterClazz) {
    additionalOutputs.add(new RootInputLeafOutput<OutputDescriptor>(outputName,
        outputDescriptor, outputCommitterClazz));
    return this;
  }
  
  Vertex addAdditionalOutput(RootInputLeafOutput<OutputDescriptor> output) {
    additionalOutputs.add(output);
    return this;
  }

  public Vertex addOutput(String outputName, OutputDescriptor outputDescriptor) {
    return addOutput(outputName, outputDescriptor, null);
  }
  
  /**
   * Specifies a {@link VertexManagerPlugin} for the vertex. This plugin can be
   * used to modify the parallelism or reconfigure the vertex at runtime using
   * user defined code embedded in the plugin
   * 
   * @param vertexManagerPluginDescriptor
   * @return this Vertex
   */
  public Vertex setVertexManagerPlugin(
      VertexManagerPluginDescriptor vertexManagerPluginDescriptor) {
    this.vertexManagerPlugin = vertexManagerPluginDescriptor;
    return this;
  }

  public String getJavaOpts(){
	  return javaOpts;
  }

  @Override
  public String toString() {
    return "[" + vertexName + " : " + processorDescriptor.getClassName() + "]";
  }
  
  VertexManagerPluginDescriptor getVertexManagerPlugin() {
    return vertexManagerPlugin;
  }

  Map<String, GroupInfo> getGroupInputs() {
    return groupInputs;
  }
  
  void addGroupInput(String groupName, GroupInfo groupInputInfo) {
    if (groupInputs.put(groupName, groupInputInfo) != null) {
      throw new IllegalStateException(
          "Vertex: " + getVertexName() + 
          " already has group input with name:" + groupName);
    }
  }

  void addInputVertex(Vertex inputVertex, Edge edge) {
    inputVertices.add(inputVertex);
    inputEdges.add(edge);
  }

  void addOutputVertex(Vertex outputVertex, Edge edge) {
    outputVertices.add(outputVertex);
    outputEdges.add(edge);
  }
  
  public List<Vertex> getInputVertices() {
    return Collections.unmodifiableList(inputVertices);
  }

  public List<Vertex> getOutputVertices() {
    return Collections.unmodifiableList(outputVertices);
  }

  List<Edge> getInputEdges() {
    return inputEdges;
  }

  List<Edge> getOutputEdges() {
    return outputEdges;
  }
  
  List<RootInputLeafOutput<InputDescriptor>> getInputs() {
    return additionalInputs;
  }

  List<RootInputLeafOutput<OutputDescriptor>> getOutputs() {
    return additionalOutputs;
  }
  // FIXME how do we support profiling? Can't profile all tasks.
}
