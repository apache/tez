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

import org.apache.tez.dag.api.records.DAGProtos.ProgressProto;
import org.apache.tez.dag.api.records.DAGProtos.ProgressProto.Builder;

public class ProgressBuilder extends Progress {

  public ProgressBuilder() {
    super(ProgressProto.newBuilder());
  }

  public ProgressProto getProto() {
    return getBuilder().build();
  }

  public void setTotalTaskCount(int count) {
    getBuilder().setTotalTaskCount(count);
  }

  public void setSucceededTaskCount(int count) {
    getBuilder().setSucceededTaskCount(count);
  }

  public void setRunningTaskCount(int count) {
    getBuilder().setRunningTaskCount(count);
  }

  public void setFailedTaskCount(int count) {
    getBuilder().setFailedTaskCount(count);
  }

  public void setKilledTaskCount(int count) {
    getBuilder().setKilledTaskCount(count);
  }

  public void setFailedTaskAttemptCount(int count) {
    getBuilder().setFailedTaskAttemptCount(count);
  }

  public void setKilledTaskAttemptCount(int count) {
    getBuilder().setKilledTaskAttemptCount(count);
  }

  private ProgressProto.Builder getBuilder() {
    return (Builder) this.proxy;
  }
}
