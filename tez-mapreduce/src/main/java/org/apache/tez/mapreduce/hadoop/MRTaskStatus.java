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

package org.apache.tez.mapreduce.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.tez.common.TezTaskStatus;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.records.TezTaskAttemptID;

public class MRTaskStatus implements TezTaskStatus {

  static final Log LOG =
      LogFactory.getLog(TaskStatus.class.getName());
  // max task-status string size
  static final int MAX_STRING_SIZE = 1024;

  private TezTaskAttemptID taskAttemptId;
  private State state = State.UNASSIGNED;
  private float progress = 0.0f;
  private String diagnostics = "";
  private String userStatusInfo = "";
  private Phase phase;
  private TezCounters counters;
  
  private long localOutputSize;
  List<TezTaskAttemptID> failedTaskDependencies = 
      new ArrayList<TezTaskAttemptID>();
  
  private long startTime;
  private long finishTime;
  private long sortFinishTime;
  private long mapFinishTime;
  private long shuffleFinishTime;
  
  // For serialization.
  public MRTaskStatus() {
  }
  
  public MRTaskStatus(
      TezTaskAttemptID taskAttemptId,  
      TezCounters counters, Phase phase) {
    this.taskAttemptId = taskAttemptId;
    this.counters = counters;
    this.phase = phase;
  }
  
  @Override
  public TezTaskAttemptID getTaskAttemptId() {
    return taskAttemptId;
  }

  @Override
  public float getProgress() {
    return progress; 
  }

  @Override
  public void setProgress(float progress) {
    this.progress = progress;
  }

  @Override
  public State getRunState() {
    return state;
  }

  @Override
  public void setRunState(State state) {
    this.state = state;
  }

  @Override
  public String getDiagnosticInfo() {
    return diagnostics;
  }

  @Override
  public void setDiagnosticInfo(String info) {
    this.diagnostics = info;
  }

  @Override
  public String getStateString() {
    return userStatusInfo;
  }

  @Override
  public void setStateString(String userStatusInfo) {
    this.userStatusInfo = userStatusInfo;
  }

  @Override
  public long getFinishTime() {
    return finishTime;
  }

  @Override
  public long getShuffleFinishTime() {
    return shuffleFinishTime;
  }

  @Override
  public long getMapFinishTime() {
    return mapFinishTime;
  }

  @Override
  public long getSortFinishTime() {
    return sortFinishTime;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public Phase getPhase() {
    return phase;
  }

  @Override
  public void setPhase(Phase phase) {
    Phase oldPhase = getPhase();
    if (oldPhase != phase) {
      // sort phase started
      if (phase == Phase.SORT){
        if (oldPhase == Phase.MAP) {
          setMapFinishTime(System.currentTimeMillis());
        } else {
          setShuffleFinishTime(System.currentTimeMillis());
        }
      } else if (phase == Phase.REDUCE) {
        setSortFinishTime(System.currentTimeMillis());
      }
      this.phase = phase;
    }
  }

  @Override
  public TezCounters getCounters() {
    return counters;
  }

  @Override
  public void setCounters(TezCounters counters) {
    this.counters = counters;
  }

  @Override
  public long getLocalOutputSize() {
    return localOutputSize;
  }

  @Override
  public List<TezTaskAttemptID> getFailedDependencies() {
    return failedTaskDependencies;
  }

  @Override
  public void addFailedDependency(TezTaskAttemptID taskAttemptId) {
    failedTaskDependencies.add(taskAttemptId);
  }

  @Override
  synchronized public void clearStatus() {
    userStatusInfo = "";
    failedTaskDependencies.clear();
  }

  @Override
  synchronized public void statusUpdate(
      float progress, String userDiagnosticInfo, TezCounters counters) {
    setProgress(progress);
    setDiagnosticInfo(userDiagnosticInfo);
    setCounters(counters);
  }

  @Override
  public void setOutputSize(long localOutputSize) {
    this.localOutputSize = localOutputSize;
  }

  @Override
  public void setFinishTime(long finishTime) {
    if(this.getStartTime() > 0 && finishTime > 0) {
      if (getShuffleFinishTime() == 0) {
        setShuffleFinishTime(finishTime);
      }
      if (getSortFinishTime() == 0){
        setSortFinishTime(finishTime);
      }
      if (getMapFinishTime() == 0) {
        setMapFinishTime(finishTime);
      }
      this.finishTime = finishTime;
    }
  }

  @Override
  public void setShuffleFinishTime(long shuffleFinishTime) {
    this.shuffleFinishTime = shuffleFinishTime;
  }

  @Override
  public void setMapFinishTime(long mapFinishTime) {
    this.mapFinishTime = mapFinishTime;
  }

  @Override
  public void setSortFinishTime(long sortFinishTime) {
    this.sortFinishTime = sortFinishTime;
    if (getShuffleFinishTime() == this.shuffleFinishTime ){
      setShuffleFinishTime(sortFinishTime);
    }
  }

  @Override
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  
  @Override
  public void write(DataOutput out) throws IOException {
    taskAttemptId.write(out);
    WritableUtils.writeEnum(out, state);
    out.writeFloat(progress);
    WritableUtils.writeString(out, diagnostics);
    WritableUtils.writeString(out, userStatusInfo);
    WritableUtils.writeEnum(out, phase);

    counters.write(out);
    
    out.writeLong(localOutputSize);
    out.writeLong(startTime);
    out.writeLong(finishTime);
    out.writeLong(sortFinishTime);
    out.writeLong(mapFinishTime);
    out.writeLong(shuffleFinishTime);

    out.writeInt(failedTaskDependencies.size());
    for(TezTaskAttemptID taskAttemptId : failedTaskDependencies) {
      taskAttemptId.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    taskAttemptId = TezTaskAttemptID.readTezTaskAttemptID(in);
    state = WritableUtils.readEnum(in, State.class);
    progress = in.readFloat();
    diagnostics = WritableUtils.readString(in);
    userStatusInfo = WritableUtils.readString(in);
    phase = WritableUtils.readEnum(in, Phase.class);
    counters = new TezCounters();
    
    counters.readFields(in);
    
    localOutputSize = in.readLong();
    startTime = in.readLong();
    finishTime = in.readLong();
    sortFinishTime = in.readLong();
    mapFinishTime = in.readLong();
    shuffleFinishTime = in.readLong();
    
    int numFailedDependencies = in.readInt();
    for (int i = 0 ; i < numFailedDependencies ; i++) {
      TezTaskAttemptID taskAttemptId = TezTaskAttemptID.readTezTaskAttemptID(in);
      failedTaskDependencies.add(taskAttemptId);
    }
    
  }
  
}
