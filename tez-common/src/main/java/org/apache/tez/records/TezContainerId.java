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

package org.apache.tez.records;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;

//TODO EVENTUALLY Once everything is on PB, get rid of this.
//Alternately have the PB interfaces implement Writable.
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TezContainerId implements Writable {

  private ContainerId containerId;

  public TezContainerId() {
  }
  
  public TezContainerId(ContainerId containerId) {
    this.containerId = containerId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(containerId.getApplicationAttemptId().getApplicationId()
        .getClusterTimestamp());
    out.writeInt(containerId.getApplicationAttemptId().getApplicationId()
        .getId());
    out.writeInt(containerId.getApplicationAttemptId().getAttemptId());
    out.writeInt(containerId.getId());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    long timestamp = in.readLong();
    int appId = in.readInt();
    int appAttemptId = in.readInt();
    int id = in.readInt();

    ApplicationId applicationId = ApplicationId.newInstance(timestamp, appId);
    ApplicationAttemptId applicationAttemptId = ApplicationAttemptId
        .newInstance(applicationId, appAttemptId);

    this.containerId = ContainerId.newInstance(applicationAttemptId, id);
  }

  @Override
  public String toString() {
    return containerId.toString();
  }
  
  public ContainerId getContainerId() {
    return this.containerId;
  }
}
