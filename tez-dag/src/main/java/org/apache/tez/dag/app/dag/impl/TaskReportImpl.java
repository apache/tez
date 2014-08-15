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

package org.apache.tez.dag.app.dag.impl;

import org.apache.tez.dag.api.oldrecords.TaskReport;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.records.TezTaskID;

public class TaskReportImpl implements TaskReport {

  private TezTaskID taskID;
  private TaskState taskState;
  private float progress;
  private long startTime;
  private long finishTime;

  public TaskReportImpl() {
  }

  public TaskReportImpl(TezTaskID taskID, TaskState taskState,
      float progress, long startTime, long finishTime) {
    this.taskID = taskID;
    this.taskState = taskState;
    this.progress = progress;
    this.startTime = startTime;
    this.finishTime = finishTime;
  }

  @Override
  public TezTaskID getTaskId() {
    return taskID;
  }

  @Override
  public TaskState getTaskState() {
    return taskState;
  }

  @Override
  public float getProgress() {
    return progress;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public long getFinishTime() {
    return finishTime;
  }

  @Override
  public void setTaskId(TezTaskID taskId) {
    this.taskID = taskId;
  }

  @Override
  public void setTaskState(TaskState taskState) {
    this.taskState = taskState;
  }

  @Override
  public void setProgress(float progress) {
    this.progress = progress;
  }

  @Override
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  @Override
  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

}
