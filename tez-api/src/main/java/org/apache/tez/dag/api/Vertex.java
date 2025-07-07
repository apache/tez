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
import java.util.Objects;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.VertexGroup.GroupInfo;
import org.apache.tez.runtime.api.LogicalIOProcessor;

import org.apache.tez.common.Preconditions;
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
  private Map<String, String> vertexConf = new HashMap<String, String>();
  private VertexExecutionContext vertexExecutionContext;
  private final Map<String, RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> additionalInputs
                      = new HashMap<String, RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>>();
  private final Map<String, RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor>> additionalOutputs
                      = new HashMap<String, RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor>>();
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
      TezCommonUtils.addAdditionalLocalResources(localFiles, taskLocalResources, "Vertex " + getName());
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
   * NullPointerException if {@code environment} is {@code null}
   */
  public Vertex setTaskEnvironment(Map<String, String> environment) {
    this.taskEnvironment.putAll(Objects.requireNonNull(environment));
    return this;
  }

  /**
   * Get the environment variables of the tasks
   * @return environment variable map
   */
  public Map<String, String> getTaskEnvironment() {
    return taskEnvironment;
  }

  public Map<String, String> getConf() {
    return vertexConf;
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
    Preconditions.checkArgument(StringUtils.isNotBlank(inputName),
        "InputName should not be null, empty or white space only, inputName=" + inputName);
    Preconditions.checkArgument(!additionalInputs.containsKey(inputName),
        "Duplicated input:" + inputName + ", vertexName=" + vertexName);
    additionalInputs
        .put(inputName, new RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>(
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
    Preconditions.checkArgument(StringUtils.isNotBlank(outputName),
        "OutputName should not be null, empty or white space only, outputName=" + outputName);
    Preconditions.checkArgument(!additionalOutputs.containsKey(outputName),
        "Duplicated output:" + outputName + ", vertexName=" + vertexName);
    additionalOutputs
        .put(outputName, new RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor>(
            outputName, dataSinkDescriptor.getOutputDescriptor(),
            dataSinkDescriptor.getOutputCommitterDescriptor()));
    this.dataSinks.add(dataSinkDescriptor);
    return this;
  }

  Vertex addAdditionalDataSink(RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor> output) {
    Preconditions.checkArgument(StringUtils.isNotBlank(output.getName()),
        "OutputName should not be null, empty or white space only, outputName=" + output.getName());
    Preconditions.checkArgument(!additionalOutputs.containsKey(output.getName()),
        "Duplicated output:" + output.getName() + ", vertexName=" + vertexName);
    additionalOutputs.put(output.getName(), output);
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

  /**
   * This is currently used to setup additional configuration parameters which will be available
   * in the Vertex specific configuration used in the AppMaster. This API would be used for properties which
   * are used by the Tez framework while executing this vertex as part of a larger DAG.
   * As an example, the number of attempts for a task. </p>
   *
   * A vertex inherits it's Configuration from the DAG, and can override properties for this Vertex only
   * using this method </p>
   *
   * Currently, properties which are used by the task runtime, such as the task to AM
   * heartbeat interval, cannot be changed using this method. </p>
   *
   * Note: This API does not add any configuration to runtime components such as InputInitializers,
   * OutputCommitters, Inputs and Outputs.
   *
   * @param property the property name
   * @param value the value for the property
   * @return the current DAG being constructed
   */
  @InterfaceStability.Unstable
  public Vertex setConf(String property, String value) {
    TezConfiguration.validateProperty(property, Scope.VERTEX);
    this.vertexConf.put(property, value);
    return this;
  }

  /**
   * Sets the execution context for this Vertex - i.e. the Task Scheduler, ContainerLauncher and
   * TaskCommunicator to be used. Also whether the vertex will be executed within the AM.
   * If partially specified, the default components in Tez will be used - which may or may not work
   * with the custom context.
   *
   * @param vertexExecutionContext the execution context for the vertex.
   *
   * @return this Vertex
   */
  public Vertex setExecutionContext(VertexExecutionContext vertexExecutionContext) {
    this.vertexExecutionContext = vertexExecutionContext;
    return this;
  }

  /**
   * The execution context for a running vertex.
   */
  @Public
  @InterfaceStability.Unstable
  public static class VertexExecutionContext {
    final boolean executeInAm;
    final boolean executeInContainers;
    final String taskSchedulerName;
    final String containerLauncherName;
    final String taskCommName;

    /**
     * Create an execution context which specifies whether the vertex needs to be executed in the
     * AM
     *
     * @param executeInAm whether to execute the vertex in the AM
     * @return the relevant execution context
     */
    public static VertexExecutionContext createExecuteInAm(boolean executeInAm) {
      return new VertexExecutionContext(executeInAm, false);
    }

    /**
     * Create an execution context which specifies whether the vertex needs to be executed in
     * regular containers
     *
     * @param executeInContainers whether to execute the vertex in regular containers
     * @return the relevant execution context
     */
    public static VertexExecutionContext createExecuteInContainers(boolean executeInContainers) {
      return new VertexExecutionContext(false, executeInContainers);
    }

    /**
     * @param taskSchedulerName     the task scheduler name which was setup while creating the
     *                              {@link org.apache.tez.client.TezClient}
     * @param containerLauncherName the container launcher name which was setup while creating the
     *                              {@link org.apache.tez.client.TezClient}
     * @param taskCommName          the task communicator name which was setup while creating the
     *                              {@link org.apache.tez.client.TezClient}
     * @return the relevant execution context
     */
    public static VertexExecutionContext create(String taskSchedulerName,
                                                String containerLauncherName,
                                                String taskCommName) {
      return new VertexExecutionContext(taskSchedulerName, containerLauncherName, taskCommName);
    }

    private VertexExecutionContext(boolean executeInAm, boolean executeInContainers) {
      this(executeInAm, executeInContainers, null, null, null);
    }

    private VertexExecutionContext(String taskSchedulerName, String containerLauncherName,
                                   String taskCommName) {
      this(false, false, taskSchedulerName, containerLauncherName, taskCommName);
    }

    private VertexExecutionContext(boolean executeInAm, boolean executeInContainers,
                                   String taskSchedulerName, String containerLauncherName,
                                   String taskCommName) {
      if (executeInAm || executeInContainers) {
        Preconditions.checkState(!(executeInAm && executeInContainers),
            "executeInContainers and executeInAM are mutually exclusive");
        Preconditions.checkState(
            taskSchedulerName == null && containerLauncherName == null && taskCommName == null,
            "Uber (in-AM) or container execution cannot be enabled with a custom plugins. TaskScheduler=" +
                taskSchedulerName + ", ContainerLauncher=" + containerLauncherName +
                ", TaskCommunicator=" + taskCommName);
      }
      if (taskSchedulerName != null || containerLauncherName != null || taskCommName != null) {
        Preconditions.checkState(executeInAm == false && executeInContainers == false,
            "Uber (in-AM) and container execution cannot be enabled with a custom plugins. TaskScheduler=" +
                taskSchedulerName + ", ContainerLauncher=" + containerLauncherName +
                ", TaskCommunicator=" + taskCommName);
      }
      this.executeInAm = executeInAm;
      this.executeInContainers = executeInContainers;
      this.taskSchedulerName = taskSchedulerName;
      this.containerLauncherName = containerLauncherName;
      this.taskCommName = taskCommName;
    }

    public boolean shouldExecuteInAm() {
      return executeInAm;
    }

    public boolean shouldExecuteInContainers() {
      return executeInContainers;
    }

    public String getTaskSchedulerName() {
      return taskSchedulerName;
    }

    public String getContainerLauncherName() {
      return containerLauncherName;
    }

    public String getTaskCommName() {
      return taskCommName;
    }

    @Override
    public String toString() {
      return "VertexExecutionContext{" +
          "executeInAm=" + executeInAm +
          ", executeInContainers=" + executeInContainers +
          ", taskSchedulerName='" + taskSchedulerName + '\'' +
          ", containerLauncherName='" + containerLauncherName + '\'' +
          ", taskCommName='" + taskCommName + '\'' +
          '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      VertexExecutionContext that = (VertexExecutionContext) o;

      if (executeInAm != that.executeInAm) {
        return false;
      }
      if (executeInContainers != that.executeInContainers) {
        return false;
      }
      if (taskSchedulerName != null ? !taskSchedulerName.equals(that.taskSchedulerName) :
          that.taskSchedulerName != null) {
        return false;
      }
      if (containerLauncherName != null ?
          !containerLauncherName.equals(that.containerLauncherName) :
          that.containerLauncherName != null) {
        return false;
      }
      return !(taskCommName != null ? !taskCommName.equals(that.taskCommName) :
          that.taskCommName != null);

    }

    @Override
    public int hashCode() {
      int result = (executeInAm ? 1 : 0);
      result = 31 * result + (executeInContainers ? 1 : 0);
      result = 31 * result + (taskSchedulerName != null ? taskSchedulerName.hashCode() : 0);
      result = 31 * result + (containerLauncherName != null ? containerLauncherName.hashCode() : 0);
      result = 31 * result + (taskCommName != null ? taskCommName.hashCode() : 0);
      return result;
    }
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

  @Private
  public List<DataSourceDescriptor> getDataSources() {
    return dataSources;
  }
  
  @Private
  public List<DataSinkDescriptor> getDataSinks() {
    return dataSinks;
  }

  @Private
  public VertexExecutionContext getVertexExecutionContext() {
    return this.vertexExecutionContext;
  }

  List<Edge> getInputEdges() {
    return inputEdges;
  }

  List<Edge> getOutputEdges() {
    return outputEdges;
  }
  
  List<RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> getInputs() {
    return Lists.newArrayList(additionalInputs.values());
  }

  List<RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor>> getOutputs() {
    return Lists.newArrayList(additionalOutputs.values());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((vertexName == null) ? 0 : vertexName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Vertex other = (Vertex) obj;
    if (vertexName == null) {
      if (other.vertexName != null)
        return false;
    } else if (!vertexName.equals(other.vertexName))
      return false;
    return true;
  }
}
