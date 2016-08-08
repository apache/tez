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

package org.apache.tez.dag.api.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.tez.dag.api.records.DAGProtos;

/**
 * Subset of information about a Tez vertex.
 */
@Public
public class VertexInformation {

  private List<TaskInformation> taskInformationList = null;
  DAGProtos.VertexInformationProtoOrBuilder proxy = null;
  private AtomicBoolean taskListInitialized = new AtomicBoolean(false);

  @Private
  public VertexInformation(DAGProtos.VertexInformationProtoOrBuilder proxy) {
    this.proxy = proxy;
  }

  public String getName() {
    return proxy.getName();
  }

  public String getId() {
    return proxy.getId();
  }

  public List<TaskInformation> getTaskInformationList() {
    if (taskListInitialized.get()) {
      return taskInformationList;
    }

    taskInformationList = new ArrayList<>(proxy.getTasksCount());
    for(DAGProtos.TaskInformationProto taskProto : proxy.getTasksList()) {
      taskInformationList.add(new TaskInformation(taskProto));
    }

    taskListInitialized.set(true);
    return taskInformationList;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    if (o instanceof VertexInformation) {
      VertexInformation other = (VertexInformation) o;
      List<TaskInformation> taskList = getTaskInformationList();
      List<TaskInformation> otherTaskList = other.getTaskInformationList();

      return getName().equals(other.getName())
        && getId().equals(other.getId())
        &&
        ((taskList == null && otherTaskList == null)
          || taskList.equals(otherTaskList));
    }
    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 46021;
    int result = prime + getName().hashCode();

    String id = getId();
    List<TaskInformation> taskInformationList = getTaskInformationList();

    result = prime * result +
      ((id == null)? 0 : id.hashCode());
    result = prime * result +
      ((taskInformationList == null)? 0 : taskInformationList.hashCode());

    return result;
  }

  @Override
  public String toString() {
    String taskListStr = StringUtils.join(getTaskInformationList(), ",");
    return ("name=" + getName()
      + ", id=" + getId()
      + ", TaskInformationList=" + taskListStr);
  }
}
