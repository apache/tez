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

import java.util.Collections;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

import com.google.common.base.Preconditions;

/**
 * Describes the placements hints for tasks.
 * The system will make a best-effort attempt to run the tasks 
 * close to the specified locations.
 */
@Public
@Evolving
public class TaskLocationHint {

  @Unstable
  /**
   * Specifies location affinity to the given vertex and given task in that vertex
   */
  public static class TaskBasedLocationAffinity {
    private String vertexName;
    private int taskIndex;
    public TaskBasedLocationAffinity(String vertexName, int taskIndex) {
      this.vertexName = vertexName;
      this.taskIndex = taskIndex;
    }
    public String getVertexName() {
      return vertexName;
    }
    public int getTaskIndex() {
      return taskIndex;
    }
    @Override
    public String toString() {
      return "[Vertex: " + vertexName + ", TaskIndex: " + taskIndex + "]";
    }
  }

  // Host names if any to be used
  private Set<String> hosts;
  // Rack names if any to be used
  private Set<String> racks;
  
  private TaskBasedLocationAffinity affinitizedTask;

  private TaskLocationHint(String vertexName, int taskIndex) {
    Preconditions.checkNotNull(vertexName);
    Preconditions.checkArgument(taskIndex >= 0);
    this.affinitizedTask = new TaskBasedLocationAffinity(vertexName, taskIndex);
  }

  private TaskLocationHint(Set<String> hosts, Set<String> racks) {
    if (hosts != null) {
      this.hosts = Collections.unmodifiableSet(hosts);
    } else {
      this.hosts = null;
    }
    if (racks != null) {
      this.racks = Collections.unmodifiableSet(racks);
    } else {
      this.racks = null;
    }
  }

  /**
   * Provide a location hint that affinitizes to the given task in the given vertex. Tez will try 
   * to run in the same container as the given task or node local to it. Locality may degrade to
   * rack local or further depending on cluster resource allocations.<br>
   * This is expected to be used only during dynamic optimizations via {@link VertexManagerPlugin}s
   * and not in while creating the dag using the DAG API.
   * @param vertexName
   * @param taskIndex
   * @return the task location hint for the vertex name and task index
   */
  public static TaskLocationHint createTaskLocationHint(String vertexName, int taskIndex) {
    return new TaskLocationHint(vertexName, taskIndex);
  }

  /**
   * Provides a location hint with nodes and racks at which the task may be executed.
   * Tez will try to execute the task at those locations
   */
  public static TaskLocationHint createTaskLocationHint(Set<String> hosts, Set<String> racks) {
    return new TaskLocationHint(hosts, racks);
  }

  public TaskBasedLocationAffinity getAffinitizedTask() {
    return affinitizedTask;
  }

  public Set<String> getHosts() {
    return hosts;
  }

  public Set<String> getRacks() {
    return racks;
  }

  @Override
  public int hashCode() {
    final int prime = 9397;
    int result = 1;
    result = ( hosts != null) ?
        prime * result + hosts.hashCode() :
        result + prime;
    result = ( racks != null) ?
        prime * result + racks.hashCode() :
        result + prime;
    if (affinitizedTask != null) {
      result = prime * result + affinitizedTask.getVertexName().hashCode() + affinitizedTask.getTaskIndex();
    }
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TaskLocationHint other = (TaskLocationHint) obj;
    if (hosts != null) {
      if (!hosts.equals(other.hosts)) {
        return false;
      }
    } else if (other.hosts != null) {
      return false;
    }
    if (racks != null) {
      if (!racks.equals(other.racks)) {
        return false;
      }
    } else if (other.racks != null) {
      return false;
    }
    if (affinitizedTask != null) {
      if (other.affinitizedTask == null) {
        return false;
      }
      if (affinitizedTask.getTaskIndex() != other.affinitizedTask.getTaskIndex()) {
        return false;
      } else if (!affinitizedTask.getVertexName().equals(other.affinitizedTask.getVertexName())) {
        return false;
      }
    } else if (other.affinitizedTask != null){
      return false;
    }
    return true;
  }
}
