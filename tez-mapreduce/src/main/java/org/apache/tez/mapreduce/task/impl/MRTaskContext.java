/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.mapreduce.task.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.crypto.SecretKey;

import org.apache.hadoop.io.WritableUtils;
import org.apache.tez.common.TezTask;
import org.apache.tez.records.TezTaskAttemptID;

// TODO XXX Writable serialization for now.
public class MRTaskContext extends TezTask {  
  private String splitLocation;
  private long splitOffset;
  private int numMapTasks;

  // Required for serialization.
  public MRTaskContext() {
  }
  
  // TODO TEZAM5 Remove jobToken from the consturctor.
  public MRTaskContext(TezTaskAttemptID taskAttemptId, String user,
      String jobName, String moduleClassName, SecretKey jobToken,
      String splitLocation, long splitOffset, int numMapTasks) {
    super(taskAttemptId, user, jobName, moduleClassName);
    this.splitLocation = splitLocation;
    this.splitOffset = splitOffset;
    this.numMapTasks = numMapTasks;
  }

  public String getSplitLocation() {
    return splitLocation;
  }

  public long getSplitOffset() {
    return splitOffset;
  }
  
  public int getNumMapTasks() {
    return this.numMapTasks;
  }
  
  public void setNumMapTasks(int numMapTasks) {
    this.numMapTasks = numMapTasks;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    // TODO VERIFY That split location needs to be serialized.
    WritableUtils.writeString(out, splitLocation);
    out.writeLong(splitOffset);
    out.writeInt(numMapTasks);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    splitLocation = WritableUtils.readString(in);
    splitOffset = in.readLong();
    numMapTasks = in.readInt();
  }
}
