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

package org.apache.hadoop.mapreduce.v2.app2.job.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.job.Job;
import org.apache.hadoop.mapreduce.v2.app2.job.Task;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;

public class NotRunningJob implements Job {

  private final JobId jobId;
  private final AppContext context;
  private final List<AMInfo> amInfos = new ArrayList<AMInfo>();
  private final Configuration conf;
  
  public NotRunningJob(ApplicationAttemptId appAttemptID,
      AppContext context, Configuration conf){
    this.context = context;
    this.jobId = MRBuilderUtils.newJobId(appAttemptID.getApplicationId(),
        appAttemptID.getApplicationId().getId());
    this.conf = conf;
  }

  @Override
  public JobId getID() {
    return jobId;
  }

  @Override
  public String getName() {
    return context.getApplicationName();
  }

  @Override
  public JobState getState() {
    return JobState.NEW;
  }

  @Override
  public JobReport getReport() {
    JobReport jobReport = MRBuilderUtils.newJobReport(jobId,
        "",
        "", JobState.NEW,
        -1, -1,
        -1, 0, 0, 0, 0, "", amInfos, false, "");
    return jobReport;
  }

  @Override
  public Counters getAllCounters() {
    return null;
  }

  @Override
  public Map<TaskId, Task> getTasks() {
    return null;
  }

  @Override
  public Map<TaskId, Task> getTasks(TaskType taskType) {
    return null;
  }

  @Override
  public Task getTask(TaskId taskID) {
    return null;
  }

  @Override
  public List<String> getDiagnostics() {
    return new ArrayList<String>();
  }

  @Override
  public int getTotalMaps() {
    return 0;
  }

  @Override
  public int getTotalReduces() {
    return 0;
  }

  @Override
  public int getCompletedMaps() {
    return 0;
  }

  @Override
  public int getCompletedReduces() {
    return 0;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public boolean isUber() {
    return false;
  }

  @Override
  public String getUserName() {
    return (String) context.getUser();
  }

  @Override
  public String getQueueName() {
    return "";
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public Path getConfFile() {
    throw new NotImplementedException();
  }

  @Override
  public Configuration loadConfFile() throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public Map<JobACL, AccessControlList> getJobACLs() {
    throw new NotImplementedException();
  }

  @Override
  public TaskAttemptCompletionEvent[] getTaskAttemptCompletionEvents(
      int fromEventId, int maxEvents) {
    return null;
  }

  @Override
  public List<AMInfo> getAMInfos() {
    return amInfos;
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI,
      JobACL jobOperation) {
    throw new NotImplementedException();
  }

}
