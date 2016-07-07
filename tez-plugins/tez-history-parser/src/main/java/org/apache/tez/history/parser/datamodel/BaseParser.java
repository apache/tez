/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.history.parser.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;

import java.util.List;
import java.util.Map;

public abstract class BaseParser {

  protected DagInfo dagInfo;
  protected VersionInfo versionInfo;
  protected Map<String, String> config;
  protected final List<VertexInfo> vertexList;
  protected final List<TaskInfo> taskList;
  protected final List<TaskAttemptInfo> attemptList;


  public BaseParser() {
    vertexList = Lists.newLinkedList();
    taskList = Lists.newLinkedList();
    attemptList = Lists.newLinkedList();
  }

  /**
   * link the parsed contents, so that it becomes easier to iterate from DAG-->Task and Task--DAG.
   * e.g Link vertex to dag, task to vertex, attempt to task etc
   */
  protected void linkParsedContents() {
    //Link vertex to DAG
    for (VertexInfo vertexInfo : vertexList) {
      vertexInfo.setDagInfo(dagInfo);
    }

    //Link task to vertex
    for (TaskInfo taskInfo : taskList) {
      //Link vertex to task
      String vertexId = TezTaskID.fromString(taskInfo.getTaskId()).getVertexID().toString();
      VertexInfo vertexInfo = dagInfo.getVertexFromId(vertexId);
      Preconditions.checkState(vertexInfo != null, "VertexInfo for " + vertexId + " can't be "
          + "null");
      taskInfo.setVertexInfo(vertexInfo);
    }

    //Link task attempt to task
    for (TaskAttemptInfo attemptInfo : attemptList) {
      //Link task to task attempt
      TezTaskAttemptID taskAttemptId = TezTaskAttemptID.fromString(attemptInfo
          .getTaskAttemptId());
      VertexInfo vertexInfo = dagInfo.getVertexFromId(taskAttemptId.getTaskID()
          .getVertexID().toString());
      Preconditions.checkState(vertexInfo != null, "Vertex " + taskAttemptId.getTaskID()
          .getVertexID().toString() + " is not present in DAG");
      TaskInfo taskInfo = vertexInfo.getTask(taskAttemptId.getTaskID().toString());
      attemptInfo.setTaskInfo(taskInfo);
    }

    //Set container details
    for (VertexInfo vertexInfo : dagInfo.getVertices()) {
      for (TaskAttemptInfo taskAttemptInfo : vertexInfo.getTaskAttempts()) {
        dagInfo.addContainerMapping(taskAttemptInfo.getContainer(), taskAttemptInfo);
      }
    }


    //Set reference time for all events
    for (VertexInfo vertexInfo : dagInfo.getVertices()) {
      setReferenceTime(vertexInfo.getEvents(), dagInfo.getStartTimeInterval());
      for (TaskInfo taskInfo : vertexInfo.getTasks()) {
        setReferenceTime(taskInfo.getEvents(), dagInfo.getStartTimeInterval());
        for (TaskAttemptInfo taskAttemptInfo : taskInfo.getTaskAttempts()) {
          setReferenceTime(taskAttemptInfo.getEvents(), dagInfo.getStartTimeInterval());
        }
      }
    }

    dagInfo.setVersionInfo(versionInfo);
    dagInfo.setAppConfig(config);
  }

  /**
   * Set reference time to all events
   *
   * @param eventList
   * @param referenceTime
   */
  private void setReferenceTime(List<Event> eventList, final long referenceTime) {
    Iterables.all(eventList, new Predicate<Event>() {
      @Override public boolean apply(Event input) {
        input.setReferenceTime(referenceTime);
        return false;
      }
    });
  }

  protected void setUserName(String userName) {
    Preconditions.checkArgument(dagInfo != null, "DagInfo can not be null");
    dagInfo.setUserName(userName);
  }
}
