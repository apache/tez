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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Container;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.runtime.api.RootInputSpecUpdate;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;

import com.google.common.base.Preconditions;

@Unstable
/**
 * Object with API's to interact with the Tez execution engine
 */
public interface VertexManagerPluginContext {
  
  public class TaskWithLocationHint {
    Integer taskIndex;
    TaskLocationHint locationHint;
    public TaskWithLocationHint(Integer taskIndex, @Nullable TaskLocationHint locationHint) {
      Preconditions.checkNotNull(taskIndex);
      this.taskIndex = taskIndex;
      this.locationHint = locationHint;
    }
    
    public Integer getTaskIndex() {
      return taskIndex;
    }
    
    public TaskLocationHint getTaskLocationHint() {
      return locationHint;
    }
  }
  /**
   * Get the edge properties on the input edges of this vertex. The input edge 
   * is represented by the source vertex name
   * @return Map of source vertex name and edge property
   */
  public Map<String, EdgeProperty> getInputVertexEdgeProperties();
  
  /**
   * Get the name of the vertex
   * @return Vertex name
   */
  public String getVertexName();
  
  /**
   * Get the payload set for the plugin
   * @return user payload
   */
  @Nullable
  public byte[] getUserPayload();
  
  /**
   * Get the number of tasks in the given vertex
   * @param vertexName
   * @return Total number of tasks in this vertex
   */
  public int getVertexNumTasks(String vertexName);
  
  /**
   * Get the resource allocated to a task of this vertex
   * @return Resource
   */
  Resource getVertexTaskResource();
  
  /**
   * Get the container for the successful attempt of the task
   * @return YARN container for the successful task. Maybe null if there is no
   * successful task.
   */
  public Container getTaskContainer(String vertexName, Integer taskIndex);
  
  /**
   * Get the total resource allocated to this vertex. If the DAG is running in 
   * a busy cluster then it may have no resources available dedicated to it. The
   * DAG may divide its available resource among member vertices.
   * @return Resource
   */
  Resource getTotalAvailableResource();
  
  /**
   * Get the number of nodes in the cluster
   * @return Number of nodes
   */
  int getNumClusterNodes();
  
  /**
   * Set the new parallelism (number of tasks) of this vertex,
   * Map of source (input) vertices and edge managers to change the event routing
   * between the source tasks and the new destination tasks and the number of physical inputs for root inputs.
   * This API can change the parallelism only once. Subsequent attempts will be 
   * disallowed
   * @param parallelism New number of tasks in the vertex
   * @param locationHint the placement policy for tasks.
   * @param sourceEdgeManagers Edge Managers to be updated
   * @param rootInputSpecUpdate Updated Root Input specifications, if any.
   *        If none specified, a default of 1 physical input is used
   * @return true if the operation was allowed.
   */
  public boolean setVertexParallelism(int parallelism,
      @Nullable VertexLocationHint locationHint,
      @Nullable Map<String, EdgeManagerDescriptor> sourceEdgeManagers,
      @Nullable Map<String, RootInputSpecUpdate> rootInputSpecUpdate);
  
  /**
   * Allows a VertexManagerPlugin to assign Events for Root Inputs
   * 
   * For regular Event Routing changes - the EdgeManager should be configured
   * via the setVertexParallelism method
   * 
   * @param inputName
   *          The input name associated with the event
   * @param events
   *          The list of Events to be assigned to various tasks belonging to
   *          the Vertex. The target index on individual events represents the
   *          task to which events need to be sent.
   */
  public void addRootInputEvents(String inputName, Collection<RootInputDataInformationEvent> events);
  
  /**
   * Notify the vertex to start the given tasks
   * @param tasks Indices of the tasks to be started
   */
  public void scheduleVertexTasks(List<TaskWithLocationHint> tasks);
  
  /**
   * Get the names of the non-vertex inputs of this vertex. These are primary 
   * sources of data.
   * @return Names of inputs to this vertex. Maybe null if there are no inputs
   */
  @Nullable
  public Set<String> getVertexInputNames();

  /**
   * Set the placement hint for tasks in this vertex
   * 
   * @param locationHint
   */
  public void setVertexLocationHint(VertexLocationHint locationHint);

  /**
   * @return DAG Attempt number
   */
  public int getDAGAttemptNumber();
}