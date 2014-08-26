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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.VertexGroup.GroupInfo;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.runtime.api.LogicalIOProcessor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Defines a vertex in the DAG. It represents the application logic that 
 * processes and transforms the input data to create the output data. The 
 * vertex represents the template from which tasks are created to execute 
 * the application in parallel across a distributed execution environment.
 */
@Public
public class Vertex {

  private final String vertexName;
  private final ProcessorDescriptor processorDescriptor;

  private int parallelism;
  private VertexLocationHint locationHint;
  private Resource taskResource;
  private final Map<String, LocalResource> taskLocalResources = new HashMap<String, LocalResource>();
  private Map<String, String> taskEnvironment = new HashMap<String, String>();
  private final List<RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> additionalInputs 
                      = new ArrayList<RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>>();
  private final List<RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor>> additionalOutputs 
                      = new ArrayList<RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor>>();
  private VertexManagerPluginDescriptor vertexManagerPlugin;

  private final List<Vertex> inputVertices = new ArrayList<Vertex>();
  private final List<Vertex> outputVertices = new ArrayList<Vertex>();
  private final List<Edge> inputEdges = new ArrayList<Edge>();
  private final List<Edge> outputEdges = new ArrayList<Edge>();
  private final Map<String, GroupInfo> groupInputs = Maps.newHashMap();
  private final List<DataSourceDescriptor> dataSources = Lists.newLinkedList();
  private final List<DataSinkDescriptor> dataSinks = Lists.newLinkedList();
  
  private String taskLaunchCmdOpts = "";

  @InterfaceAudience.Private
  Vertex(String vertexName,
         ProcessorDescriptor processorDescriptor,
         int parallelism,
         Resource taskResource) {
    this(vertexName, processorDescriptor, parallelism, taskResource, false);
  }

  private Vertex(String vertexName, ProcessorDescriptor processorDescriptor, int parallelism) {
    this(vertexName, processorDescriptor, parallelism, null, true);
  }
  

  private Vertex(String vertexName, ProcessorDescriptor processorDescriptor) {
    this(vertexName, processorDescriptor, -1);
  }
  
  private Vertex(String vertexName,
      ProcessorDescriptor processorDescriptor,
      int parallelism,
      Resource taskResource,
      boolean allowIncomplete) {
    this.vertexName = vertexName;
    this.processorDescriptor = processorDescriptor;
    this.parallelism = parallelism;
    this.taskResource = taskResource;
    if (parallelism < -1) {
      throw new IllegalArgumentException(
          "Parallelism should be -1 if determined by the AM"
          + ", otherwise should be >= 0");
    }
    if (!allowIncomplete && taskResource == null) {
      throw new IllegalArgumentException("Resource cannot be null");
    }
  }

  /**
   * Create a new vertex with the given name.
   *
   * @param vertexName
   *          Name of the vertex
   * @param processorDescriptor
   *          Description of the processor that is executed in every task of
   *          this vertex
   * @param parallelism
   *          Number of tasks in this vertex. Set to -1 if this is going to be
   *          decided at runtime. Parallelism may change at runtime due to graph
   *          reconfigurations.
   * @param taskResource
   *          Physical resources like memory/cpu thats used by each task of this
   *          vertex.
   * @return a new Vertex with the given parameters
   */
  public static Vertex create(String vertexName,
                              ProcessorDescriptor processorDescriptor,
                              int parallelism,
                              Resource taskResource) {
    return new Vertex(vertexName, processorDescriptor, parallelism, taskResource);
  }

  /**
   * Create a new vertex with the given name. <br>
   * The vertex task resource will be picked from configuration <br>
   * The vertex parallelism will be inferred. If it cannot be inferred then an
   * error will be reported. This constructor may be used for vertices that have
   * data sources, or connected via 1-1 edges or have runtime parallelism
   * estimation via data source initializers or vertex managers. Calling this
   * constructor is equivalent to calling
   * {@link Vertex#Vertex(String, ProcessorDescriptor, int)} with the
   * parallelism set to -1.
   *
   * @param vertexName
   *          Name of the vertex
   * @param processorDescriptor
   *          Description of the processor that is executed in every task of
   *          this vertex
   * @return a new Vertex with the given parameters
   */
  public static Vertex create(String vertexName, ProcessorDescriptor processorDescriptor) {
    return new Vertex(vertexName, processorDescriptor);
  }

  /**
   * Create a new vertex with the given name and parallelism. <br>
   * The vertex task resource will be picked from configuration
   * {@link TezConfiguration#TEZ_TASK_RESOURCE_MEMORY_MB} &
   * {@link TezConfiguration#TEZ_TASK_RESOURCE_CPU_VCORES} Applications that
   * want more control over their task resource specification may create their
   * own logic to determine task resources and use
   * {@link Vertex#Vertex(String, ProcessorDescriptor, int, Resource)} to create
   * the Vertex.
   *
   * @param vertexName
   *          Name of the vertex
   * @param processorDescriptor
   *          Description of the processor that is executed in every task of
   *          this vertex
   * @param parallelism
   *          Number of tasks in this vertex. Set to -1 if this is going to be
   *          decided at runtime. Parallelism may change at runtime due to graph
   *          reconfigurations.
   * @return a new Vertex with the given parameters
   */
  public static Vertex create(String vertexName, ProcessorDescriptor processorDescriptor,
                              int parallelism) {
    return new Vertex(vertexName, processorDescriptor, parallelism);
  }


  /**
   * Get the vertex name
   * @return vertex name
   */
  public String getName() {
    return vertexName;
  }

  /**
   * Get the vertex task processor descriptor
   * @return process descriptor
   */
  public ProcessorDescriptor getProcessorDescriptor() {
    return this.processorDescriptor;
  }

  /**
   * Get the specified number of tasks specified to run in this vertex. It may 
   * be -1 if the parallelism is defined at runtime. Parallelism may change at 
   * runtime
   * @return vertex parallelism
   */
  public int getParallelism() {
    return parallelism;
  }
  
  /**
   * Set the number of tasks for this vertex
   * @param parallelism Parallelism for this vertex
   */
  void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  /**
   * Get the resources for the vertex
   * @return the physical resources like pcu/memory of each vertex task
   */
  public Resource getTaskResource() {
    return taskResource;
  }

  /**
   * Specify location hints for the tasks of this vertex. Hints must be specified 
   * for all tasks as defined by the parallelism
   * @param locationHint list of locations for each task in the vertex
   * @return this Vertex
   */
  public Vertex setLocationHint(VertexLocationHint locationHint) {
    List<TaskLocationHint> locations = locationHint.getTaskLocationHints();
    if (locations == null) {
      return this;
    }
    Preconditions.checkArgument((locations.size() == parallelism), 
        "Locations array length must match the parallelism set for the vertex");
    this.locationHint = locationHint;
    return this;
  }

  // used internally to create parallelism location resource file
  VertexLocationHint getLocationHint() {
    return locationHint;
  }

  /**
   * Set the files etc that must be provided to the tasks of this vertex
   * @param localFiles
   *          files that must be available locally for each task. These files
   *          may be regular files, archives etc. as specified by the value
   *          elements of the map.
   * @return this Vertex
   */
  public Vertex addTaskLocalFiles(Map<String, LocalResource> localFiles) {
    if (localFiles != null) {
      TezCommonUtils.addAdditionalLocalResources(localFiles, taskLocalResources);
    }
    return this;
  }

  /**
   * Get the files etc that must be provided by the tasks of this vertex
   * @return local files of the vertex. Key is the file name.
   */
  public Map<String, LocalResource> getTaskLocalFiles() {
    return taskLocalResources;
  }

  /**
   * Set the Key-Value pairs of environment variables for tasks of this vertex.
   * This method should be used if different vertices need different env. Else,
   * set environment for all vertices via Tezconfiguration#TEZ_TASK_LAUNCH_ENV
   * @param environment
   * @return this Vertex
   */
  public Vertex setTaskEnvironment(Map<String, String> environment) {
    Preconditions.checkArgument(environment != null);
    this.taskEnvironment.putAll(environment);
    return this;
  }

  /**
   * Get the environment variables of the tasks
   * @return environment variable map
   */
  public Map<String, String> getTaskEnvironment() {
    return taskEnvironment;
  }

  /**
   * Set the command opts for tasks of this vertex. This method should be used 
   * when different vertices have different opts. Else, set the launch opts for '
   * all vertices via Tezconfiguration#TEZ_TASK_LAUNCH_CMD_OPTS
   * @param cmdOpts
   * @return this Vertex
   */
  public Vertex setTaskLaunchCmdOpts(String cmdOpts){
     this.taskLaunchCmdOpts = cmdOpts;
     return this;
  }
  
  /**
   * Specifies an external data source for a Vertex. This is meant to be used
   * when a Vertex reads Input directly from an external source </p>
   * 
   * For vertices which read data generated by another vertex - use the
   * {@link DAG addEdge} method.
   * 
   * If a vertex needs to use data generated by another vertex in the DAG and
   * also from an external source, a combination of this API and the DAG.addEdge
   * API can be used. </p>
   * 
   * Note: If more than one RootInput exists on a vertex, which generates events
   * which need to be routed, or generates information to set parallelism, a
   * custom vertex manager should be setup to handle this. Not using a custom
   * vertex manager for such a scenario will lead to a runtime failure.
   * 
   * @param inputName
   *          the name of the input. This will be used when accessing the input
   *          in the {@link LogicalIOProcessor}
   * @param dataSourceDescriptor
   *          the @{link DataSourceDescriptor} for this input.
   * @return this Vertex
   */
  public Vertex addDataSource(String inputName, DataSourceDescriptor dataSourceDescriptor) {
    additionalInputs
        .add(new RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>(
            inputName, dataSourceDescriptor.getInputDescriptor(),
            dataSourceDescriptor.getInputInitializerDescriptor()));
    this.dataSources.add(dataSourceDescriptor);
    return this;
  }

  /**
   * Specifies an external data sink for a Vertex. This is meant to be used when
   * a Vertex writes Output directly to an external destination. </p>
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
   * @param dataSinkDescriptor
   *          the {@link DataSinkDescriptor} for this output
   * @return this Vertex
   */
  public Vertex addDataSink(String outputName, DataSinkDescriptor dataSinkDescriptor) {
    additionalOutputs
        .add(new RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor>(
            outputName, dataSinkDescriptor.getOutputDescriptor(),
            dataSinkDescriptor.getOutputCommitterDescriptor()));
    this.dataSinks.add(dataSinkDescriptor);
    return this;
  }
  
  Vertex addAdditionalDataSink(RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor> output) {
    additionalOutputs.add(output);
    return this;
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

  /**
   * Get the launch command opts for tasks in this vertex
   * @return launch command opts
   */
  public String getTaskLaunchCmdOpts(){
	  return taskLaunchCmdOpts;
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
          "Vertex: " + getName() + 
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
  
  /**
   * Get the input vertices for this vertex
   * @return List of input vertices
   */
  public List<Vertex> getInputVertices() {
    return Collections.unmodifiableList(inputVertices);
  }

  /**
   * Get the output vertices for this vertex
   * @return List of output vertices
   */
  public List<Vertex> getOutputVertices() {
    return Collections.unmodifiableList(outputVertices);
  }

  /**
   * Set the cpu/memory etc resources used by tasks of this vertex
   * @param resource {@link Resource} for the tasks of this vertex
   */
  void setTaskResource(Resource resource) {
    this.taskResource = resource;
  }

  List<DataSourceDescriptor> getDataSources() {
    return dataSources;
  }
  
  List<DataSinkDescriptor> getDataSinks() {
    return dataSinks;
  }

  List<Edge> getInputEdges() {
    return inputEdges;
  }

  List<Edge> getOutputEdges() {
    return outputEdges;
  }
  
  List<RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> getInputs() {
    return additionalInputs;
  }

  List<RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor>> getOutputs() {
    return additionalOutputs;
  }
}
