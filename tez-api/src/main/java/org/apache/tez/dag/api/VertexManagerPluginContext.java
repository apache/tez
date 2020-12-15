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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.VertexStatistics;
import org.apache.tez.runtime.api.events.CustomProcessorEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;

import org.apache.tez.common.Preconditions;

/**
 * Object with API's to interact with the Tez execution engine
 */
@Unstable
@Public
public interface VertexManagerPluginContext {
  
  public class ScheduleTaskRequest {
    int taskIndex;
    TaskLocationHint locationHint;

    public static ScheduleTaskRequest create(int taskIndex, @Nullable TaskLocationHint locationHint) {
      return new ScheduleTaskRequest(taskIndex, locationHint); 
    }
    
    private ScheduleTaskRequest(int taskIndex, @Nullable TaskLocationHint locationHint) {
      Preconditions.checkState(taskIndex >= 0);
      this.taskIndex = taskIndex;
      this.locationHint = locationHint;
    }
    
    public int getTaskIndex() {
      return taskIndex;
    }
    
    public TaskLocationHint getTaskLocationHint() {
      return locationHint;
    }    
  }
  
  @Deprecated
  public class TaskWithLocationHint {
    Integer taskIndex;
    TaskLocationHint locationHint;
    public TaskWithLocationHint(Integer taskIndex, @Nullable TaskLocationHint locationHint) {
      Preconditions.checkState(taskIndex != null);
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
   * Get the edge properties on the output edges of this vertex. The output edge 
   * is represented by the destination vertex name
   * @return Map of destination vertex name and edge property
   */
  public Map<String, EdgeProperty> getOutputVertexEdgeProperties();
  
  /**
   * Get a {@link VertexStatistics} object to find out execution statistics
   * about the given {@link Vertex}.
   * <br>This only provides point in time values for statistics (completed tasks)
   * and must be called again to get updated values.
   *
   * @param vertexName
   *          Name of the {@link Vertex}
   * @return {@link VertexStatistics} for the given vertex
   */
  public VertexStatistics getVertexStatistics(String vertexName);

  /**
   * Get the name of the vertex
   * @return Vertex name
   */
  public String getVertexName();
  
  /**
   * Get the payload set for the plugin
   * @return user payload
   */
  public UserPayload getUserPayload();
  
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
   */
  @Deprecated
  public void setVertexParallelism(int parallelism,
      @Nullable VertexLocationHint locationHint,
      @Nullable Map<String, EdgeManagerPluginDescriptor> sourceEdgeManagers,
      @Nullable Map<String, InputSpecUpdate> rootInputSpecUpdate);

  /**
   * API to reconfigure a {@link Vertex} that is reading root inputs based on
   * the data read from the root inputs. Root inputs are external data sources
   * that provide the initial data for the DAG and are added to the
   * {@link Vertex} using the
   * {@link Vertex#addDataSource(String, DataSourceDescriptor)} API. Typically,
   * the parallelism of such vertices is determined at runtime by gathering
   * information about the data source. This API may be used to set the
   * parallelism of the vertex at runtime based on the data sources, as well as
   * changing the specification for those inputs. In addition, changing
   * parallelism is often accompanied by changing the {@link EdgeProperty} of
   * the source {@link Edge} because event routing between source and
   * destination tasks may need to be updated to account for the new task
   * parallelism. This method can be called to update the parallelism multiple
   * times until any of the tasks of the vertex have been scheduled (by invoking
   * {@link #scheduleTasks(List)}. If needed, the original source edge
   * properties may be obtained via {@link #getInputVertexEdgeProperties()}
   * 
   * @param parallelism
   *          New number of tasks in the vertex
   * @param locationHint
   *          the placement policy for tasks specified at
   *          {@link VertexLocationHint}s
   * @param sourceEdgeProperties
   *          Map with Key=name of {@link Edge} to be updated and Value=
   *          {@link EdgeProperty}. The name of the Edge will be the
   *          corresponding source vertex name.
   * @param rootInputSpecUpdate
   *          The key of the map is the name of the data source and the value is
   *          the updated {@link InputSpecUpdate} for that data source. If none
   *          specified, a default value is used. See {@link InputSpecUpdate}
   *          for details.
   */
  public void reconfigureVertex(int parallelism,
      @Nullable VertexLocationHint locationHint,
      @Nullable Map<String, EdgeProperty> sourceEdgeProperties,
      @Nullable Map<String, InputSpecUpdate> rootInputSpecUpdate);
  
  /**
   * API to reconfigure a {@link Vertex} by changing its task parallelism. Task
   * parallelism is often accompanied by changing the {@link EdgeProperty} of
   * the source {@link Edge} because event routing between source and
   * destination tasks may need to be updated to account for the new task
   * parallelism. This method can be called to update the parallelism multiple
   * times until any of the tasks of the vertex have been scheduled (by invoking
   * {@link #scheduleTasks(List)}. If needed, the original source edge
   * properties may be obtained via {@link #getInputVertexEdgeProperties()}
   * 
   * @param parallelism
   *          New number of tasks in the vertex
   * @param locationHint
   *          the placement policy for tasks specified at
   *          {@link VertexLocationHint}s
   * @param sourceEdgeProperties
   *          Map with Key=name of {@link Edge} to be updated and Value=
   *          {@link EdgeProperty}. The name of the Edge will be the 
   *          corresponding source vertex name.
   */
  public void reconfigureVertex(int parallelism,
      @Nullable VertexLocationHint locationHint,
      @Nullable Map<String, EdgeProperty> sourceEdgeProperties);

  /**
   * API to reconfigure a {@link Vertex} that is reading root inputs based on
   * the data read from the root inputs. Root inputs are external data sources
   * that provide the initial data for the DAG and are added to the
   * {@link Vertex} using the
   * {@link Vertex#addDataSource(String, DataSourceDescriptor)} API. Typically,
   * the parallelism of such vertices is determined at runtime by gathering
   * information about the data source. This API may be used to set the
   * parallelism of the vertex at runtime based on the data sources, as well as
   * changing the specification for those inputs.
   * @param rootInputSpecUpdate
   *          The key of the map is the name of the data source and the value is
   *          the updated {@link InputSpecUpdate} for that data source. If none
   *          specified, a default value is used. See {@link InputSpecUpdate}
   *          for details.
   * @param locationHint
   *          the placement policy for tasks specified at
   *          {@link VertexLocationHint}s
   * @param parallelism
   *          New number of tasks in the vertex
   */
  public void reconfigureVertex(@Nullable Map<String, InputSpecUpdate> rootInputSpecUpdate,
      @Nullable VertexLocationHint locationHint,
      int parallelism);

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
  public void addRootInputEvents(String inputName, Collection<InputDataInformationEvent> events);

  /**
   * Allows a VertexManagerPlugin to send events of custom payload to processor
   * of a specific task of managed vertex
   *
   * It's up to user to make sure taskId is valid
   *
   * @param events events to be sent
   * @param taskId id of a task of managed vertex
   */
  public void sendEventToProcessor(Collection<CustomProcessorEvent> events, int taskId);
  
  @Deprecated
  /**
   * Replaced by {@link #scheduleTasks(List)}
   * Notify the vertex to start the given tasks
   * @param tasks Indices of the tasks to be started
   */
  public void scheduleVertexTasks(List<TaskWithLocationHint> tasks);
  
  /**
   * Notify the vertex to schedule the given tasks
   * @param tasks Identifier and metadata for the tasks to schedule
   */
  public void scheduleTasks(List<ScheduleTaskRequest> tasks);

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
  
  /**
   * Register to get notifications on updates to the specified vertex. Notifications will be sent
   * via {@link VertexManagerPlugin#onVertexStateUpdated(org.apache.tez.dag.api.event.VertexStateUpdate)}
   *
   * This method can only be invoked once. Duplicate invocations will result in an error.
   *
   * @param vertexName the vertex name for which notifications are required.
   * @param stateSet   the set of states for which notifications are required. null implies all
   */
  void registerForVertexStateUpdates(String vertexName, @Nullable Set<VertexState> stateSet);
  
  /**
   * Optional API. No need to call this when the vertex is not fully defined to
   * start with. E.g. vertex parallelism is not defined, or edges are not
   * configured. In that case, Tez will assume that the vertex needs
   * reconfiguration. If the vertex is already fully defined, but the
   * {@link VertexManagerPlugin} wants to reconfigure the vertex, then it must
   * use this API to inform Tez about its intention. Without invoking this
   * method, it is invalid to re-configure the vertex if
   * the vertex is already fully defined. This can be invoked at any time until
   * {@link VertexManagerPlugin#initialize()} has completed. Its invalid to
   * invoke this method after {@link VertexManagerPlugin#initialize()} has
   * completed<br>
   * If this API is invoked, then {@link #doneReconfiguringVertex()} must be
   * invoked after the {@link VertexManagerPlugin} is done reconfiguring the
   * vertex, . Actions like scheduling tasks or sending events do not count as
   * reconfiguration.
   */
  public void vertexReconfigurationPlanned();
  
  /**
   * Optional API. This needs to be called only if {@link #vertexReconfigurationPlanned()} has been 
   * invoked. This must be called after {@link #vertexReconfigurationPlanned()} is called.
   */
  public void doneReconfiguringVertex();
  
  /**
   * Optional API. This API can be invoked to declare that the
   * {@link VertexManagerPlugin} is done with its work. After this the system
   * will not invoke the plugin methods any more. Its invalid for the plugin to
   * make further invocations of the context APIs after this. This can be used
   * to stop receiving further {@link VertexState} notifications after the
   * plugin has made all changes.
   */
  // TODO must be done later after TEZ-1714
  //public void vertexManagerDone();

  /**
   * Get input vertex groups of this vertex, including vertex group name and
   * all members vertex name
   *
   * @return map whose key is vertex group name and value is list of members' name,
   *         or empty map if there is no input vertex group.
   */
  Map<String, List<String>> getInputVertexGroups();

}