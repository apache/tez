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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.records.TezContainerId;

// TODO EVENTUALLY move this over to PB. Fix package/module.
// TODO EVENTUALLY unit tests for functionality.
public class ContainerContext implements Writable {

  ContainerId containerId;
  String pid;

  public ContainerContext() {
    containerId = Records.newRecord(ContainerId.class);
    pid = "";
  }

  public ContainerContext(ContainerId containerId, String pid) {
    this.containerId = containerId;
    this.pid = pid;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public String getPid() {
    return pid;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    TezContainerId tezContainerId = new TezContainerId();
    tezContainerId.readFields(in);
    this.containerId = tezContainerId.getContainerId();
    this.pid = Text.readString(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    TezContainerId tezContainerId = new TezContainerId(containerId);
    tezContainerId.write(out);
    Text.writeString(out, pid);
  }
}
