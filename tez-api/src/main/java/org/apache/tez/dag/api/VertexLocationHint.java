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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ContainerId;

import com.google.common.base.Preconditions;

public class VertexLocationHint  {

  private final List<TaskLocationHint> taskLocationHints;

  
  public VertexLocationHint(List<TaskLocationHint> taskLocationHints) {
    if (taskLocationHints != null) {
      this.taskLocationHints = Collections.unmodifiableList(taskLocationHints);
    } else {
      this.taskLocationHints = null;
    }
  }

  public List<TaskLocationHint> getTaskLocationHints() {
    return taskLocationHints;
  }

  @Override
  public int hashCode() {
    final int prime = 7883;
    int result = 1;
    if (taskLocationHints != null) {
      result = prime * result + taskLocationHints.hashCode();
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
    VertexLocationHint other = (VertexLocationHint) obj;
    if (taskLocationHints != null) {
      if (!taskLocationHints.equals(other.taskLocationHints)) {
        return false;
      }
    } else if (other.taskLocationHints != null) {
      return false;
    }
    return true;
  }

  public static class TaskLocationHint {

    // Container to which to affinitize
    ContainerId containerId;
    // Host names if any to be used
    private Set<String> hosts;
    // Rack names if any to be used
    private Set<String> racks;

    /**
     * Provide a container to which the task may be affinitized
     * Tez will try to run the task on that container or node local to it
     */
    public TaskLocationHint(ContainerId containerId) {
      Preconditions.checkNotNull(containerId);
      this.containerId = containerId;
    }
    
    /**
     * Provides nodes and racks at which the task may be executed.
     * Tez will try to execute the task at those locations
     */
    public TaskLocationHint(Set<String> hosts, Set<String> racks) {
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

    public ContainerId getAffinitizedContainer() {
      return containerId;
    }
    
    public Set<String> getDataLocalHosts() {
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
      result = ( containerId != null) ?
          prime * result + containerId.hashCode() :
          result + prime;
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
      if (containerId != null) {
        if (!containerId.equals(other.containerId)) {
          return false;
        }
      } else if (other.containerId != null) {
        return false;
      }
      return true;
    }
  }
}
