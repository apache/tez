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

package org.apache.tez.mapreduce.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;

import com.google.common.base.Joiner;

public class DAGJobStatus extends JobStatus {

  private final String jobFile;
  private final DAGStatus dagStatus;
  private final ApplicationReport report;

  public DAGJobStatus(ApplicationReport report, DAGStatus dagStatus, String jobFile) {
    super();
    this.dagStatus = dagStatus;
    this.jobFile = jobFile;
    this.report = report;
  }

  @Override
  protected synchronized void setMapProgress(float p) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void setCleanupProgress(float p) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void setSetupProgress(float p) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void setReduceProgress(float p) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void setPriority(JobPriority jp) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void setFinishTime(long finishTime) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void setHistoryFile(String historyFile) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void setTrackingUrl(String trackingUrl) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void setRetired() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void setState(State state) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void setStartTime(long startTime) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void setUsername(String userName) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void setSchedulingInfo(String schedulingInfo) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void setJobACLs(Map<JobACL, AccessControlList> acls) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void setQueue(String queue) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void setFailureInfo(String failureInfo) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized String getQueue() {
    return report.getQueue();
  }

  @Override
  public synchronized float getMapProgress() {
    if(dagStatus.getVertexProgress() != null) {
      return getProgress(MultiStageMRConfigUtil.getInitialMapVertexName());
    }
    if (dagStatus.getState() == DAGStatus.State.SUCCEEDED) {
      return 1.0f;
    }
    return 0.0f;
  }

  @Override
  public synchronized float getCleanupProgress() {
    if (dagStatus.getState() == DAGStatus.State.SUCCEEDED ||
        dagStatus.getState() == DAGStatus.State.FAILED ||
        dagStatus.getState() == DAGStatus.State.KILLED ||
        dagStatus.getState() == DAGStatus.State.ERROR) {
      return 1.0f;
    }
    return 0.0f;
  }

  @Override
  public synchronized float getSetupProgress() {
    if (dagStatus.getState() == DAGStatus.State.RUNNING) {
      return 1.0f;
    }
    return 0.0f;
  }

  @Override
  public synchronized float getReduceProgress() {
    if(dagStatus.getVertexProgress() != null) {
      return getProgress(MultiStageMRConfigUtil.getFinalReduceVertexName());
    }
    if (dagStatus.getState() == DAGStatus.State.SUCCEEDED) {
      return 1.0f;
    }
    return 0.0f;
  }

  @Override
  public synchronized State getState() {
    switch (dagStatus.getState()) {
    case SUBMITTED:
    case INITING:
      return State.PREP;
    case RUNNING:
      return State.RUNNING;
    case SUCCEEDED:
      return State.SUCCEEDED;
    case KILLED:
      return State.KILLED;
    case FAILED:
    case ERROR:
      return State.FAILED;
    default:
      throw new TezUncheckedException("Unknown value of DAGState.State:"
          + dagStatus.getState());
    }
  }

  @Override
  public synchronized long getStartTime() {
    return report.getStartTime();
  }

  @Override
  public JobID getJobID() {
    return TypeConverter.fromYarn(report.getApplicationId());
  }

  @Override
  public synchronized String getUsername() {
    return report.getUser();
  }

  @Override
  public synchronized String getSchedulingInfo() {
    return report.getTrackingUrl();
  }

  @Override
  public synchronized Map<JobACL, AccessControlList> getJobACLs() {
    // TODO Auto-generated method stub
    return super.getJobACLs();
  }

  @Override
  public synchronized JobPriority getPriority() {
    // TEX-147: return real priority
    return JobPriority.NORMAL;
  }

  @Override
  public synchronized String getFailureInfo() {
    return Joiner.on(". ").join(dagStatus.getDiagnostics());
  }

  @Override
  public synchronized boolean isJobComplete() {
    return (dagStatus.getState() == DAGStatus.State.SUCCEEDED ||
        dagStatus.getState() == DAGStatus.State.FAILED ||
        dagStatus.getState() == DAGStatus.State.KILLED ||
        dagStatus.getState() == DAGStatus.State.ERROR);
  }

  @Override
  public synchronized void write(DataOutput out) throws IOException {
    // FIXME
  }

  @Override
  public synchronized void readFields(DataInput in) throws IOException {
    // FIXME
  }

  @Override
  public String getJobName() {
    return report.getName();
  }

  @Override
  public String getJobFile() {
    return jobFile;
  }

  @Override
  public synchronized String getTrackingUrl() {
    return report.getTrackingUrl();
  }

  @Override
  public synchronized long getFinishTime() {
    return report.getFinishTime();
  }

  @Override
  public synchronized boolean isRetired() {
    // FIXME handle retired jobs?
    return false;
  }

  @Override
  public synchronized String getHistoryFile() {
    // FIXME handle history in status
    return null;
  }

  @Override
  public int getNumUsedSlots() {
    return report.getApplicationResourceUsageReport().getNumUsedContainers();
  }

  @Override
  public void setNumUsedSlots(int n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNumReservedSlots() {
    return report.getApplicationResourceUsageReport().
        getNumReservedContainers();
  }

  @Override
  public void setNumReservedSlots(int n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getUsedMem() {
    return report.getApplicationResourceUsageReport().
        getUsedResources().getMemory();
  }

  @Override
  public void setUsedMem(int m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getReservedMem() {
    return report.getApplicationResourceUsageReport().
        getReservedResources().getMemory();
  }

  @Override
  public void setReservedMem(int r) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNeededMem() {
    return report.getApplicationResourceUsageReport().
        getNeededResources().getMemory();
  }

  @Override
  public void setNeededMem(int n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized boolean isUber() {
    return false;
  }

  @Override
  public synchronized void setUber(boolean isUber) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("job-id : " + getJobID());
    buffer.append("uber-mode : " + isUber());
    buffer.append("map-progress : " + getMapProgress());
    buffer.append("reduce-progress : " + getReduceProgress());
    buffer.append("cleanup-progress : " + getCleanupProgress());
    buffer.append("setup-progress : " + getSetupProgress());
    buffer.append("runstate : " + getState());
    buffer.append("start-time : " + getStartTime());
    buffer.append("user-name : " + getUsername());
    buffer.append("priority : " + getPriority());
    buffer.append("scheduling-info : " + getSchedulingInfo());
    buffer.append("num-used-slots" + getNumUsedSlots());
    buffer.append("num-reserved-slots" + getNumReservedSlots());
    buffer.append("used-mem" + getUsedMem());
    buffer.append("reserved-mem" + getReservedMem());
    buffer.append("needed-mem" + getNeededMem());
    return buffer.toString();
  }

  private float getProgress(String vertexName) {
    Progress progress = dagStatus.getVertexProgress().get(vertexName);
    if(progress == null) {
      // no such stage. return 0 like MR app currently does.
      return 0;
    }
    float totalTasks = (float) progress.getTotalTaskCount();
    if(totalTasks != 0) {
      return progress.getSucceededTaskCount()/totalTasks;
    }
    return 0;
  }

}
