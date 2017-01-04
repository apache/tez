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

import org.apache.tez.dag.api.records.DAGProtos.TaskInformationProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexInformationProto;

public class VertexInformationBuilder extends VertexInformation {

  public VertexInformationBuilder() {
    super(VertexInformationProto.newBuilder());
  }

  public void setName(String name) {
    getBuilder().setName(name);
  }

  public void setId(String id) {
    getBuilder().setId(id);
  }

  public void setTaskInformationList(List<TaskInformation> taskInformationList) {
    List<TaskInformationProto> taskProtos = new ArrayList<>(taskInformationList.size());
    for(TaskInformation task : taskInformationList) {
      TaskInformationBuilder taskBuilder = new TaskInformationBuilder();
      taskBuilder.setState(task.getState());
      taskBuilder.setId(task.getID());
      taskBuilder.setScheduledTime(task.getScheduledTime());
      taskBuilder.setStartTime(task.getStartTime());
      taskBuilder.setEndTime(task.getEndTime());
      taskBuilder.setSuccessfulAttemptId(task.getSuccessfulAttemptID());
      taskBuilder.setTaskCounters(task.getTaskCounters());

      taskProtos.add(taskBuilder.getProto());
    }
    getBuilder().addAllTasks(taskProtos);
  }

  public VertexInformationProto getProto() {
    return getBuilder().build();
  }

  private VertexInformationProto.Builder getBuilder() {
    return (VertexInformationProto.Builder) this.proxy;
  }
}
