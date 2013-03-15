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
package org.apache.tez.common;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.crypto.SecretKey;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progress;
import org.apache.tez.api.Partitioner;
import org.apache.tez.api.Processor;
import org.apache.tez.records.TezJobID;
import org.apache.tez.records.TezTaskAttemptID;

public abstract class TezTask implements Writable {

  private TezTaskAttemptID taskAttemptId;
  private String user;
  private String jobName;
  
  protected SecretKey jobTokenSecret; 
  protected TezTaskReporter reporter;
  protected Partitioner partitioner;
  protected Processor combineProcessor;
  protected TezTaskStatus status;
  protected Progress progress = new Progress();
  protected String taskModuleClassName;
  
  public TezTask() {
  }
  
  public TezTask(
      TezTaskAttemptID taskAttemptID, String user, 
      String jobName, String moduleClassName) {
    this.taskAttemptId = taskAttemptID;
    this.user = user;
    this.jobName = jobName;
    this.taskModuleClassName = moduleClassName;
  }
  
  public TezTaskAttemptID getTaskAttemptId() {
    return taskAttemptId;
  }

  public Progress getProgress() {
    return progress;
  }

  public TezJobID getJobID() {
    return taskAttemptId.getJobID();
  }
  
  public String getUser() {
    return user;
  }

  public String getJobName() {
    return jobName;
  }

  public SecretKey getJobTokenSecret() {
    return jobTokenSecret;
  }
  
  public void setJobTokenSecret(SecretKey jobTokenSecret) {
    this.jobTokenSecret = jobTokenSecret;
  }

  public TezTaskStatus getStatus() {
    return status;
  }

  public TezTaskReporter getTaskReporter() {
    return reporter;
  }

  public Processor getCombineProcessor() {
    return combineProcessor;
  }

  public Partitioner getPartitioner() {
    return partitioner;
  }
  
  public String getTaskModuleClassName() {
    return taskModuleClassName;
  }

  public void statusUpdate() throws IOException, InterruptedException {}

  @Override
  public void write(DataOutput out) throws IOException {
    taskAttemptId.write(out);
    Text.writeString(out, user);
    Text.writeString(out, jobName);
    Text.writeString(out, taskModuleClassName);
    // TODO VERIFY skipping, skipRanges
    // Nothing else should require serialization at the moment. The rest is all
    // constructed / initialized during task execution.
  }
  
  public void readFields(DataInput in) throws IOException {
    taskAttemptId = TezTaskAttemptID.read(in);
    user = Text.readString(in);
    jobName = Text.readString(in);
    taskModuleClassName = Text.readString(in);
  }
}
