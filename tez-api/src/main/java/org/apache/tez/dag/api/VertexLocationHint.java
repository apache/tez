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

import org.apache.hadoop.classification.InterfaceAudience.Public;

/**
 * Describes the placements hints for tasks in a vertex.
 * The system will make a best-effort attempt to run the tasks 
 * close to the specified locations.
 */
@Public
public class VertexLocationHint  {

  private final List<TaskLocationHint> taskLocationHints;

  
  private VertexLocationHint(List<TaskLocationHint> taskLocationHints) {
    if (taskLocationHints != null) {
      this.taskLocationHints = Collections.unmodifiableList(taskLocationHints);
    } else {
      this.taskLocationHints = null;
    }
  }

  public static VertexLocationHint create(
      List<TaskLocationHint> taskLocationHints) {
    return new VertexLocationHint(taskLocationHints);
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
}
