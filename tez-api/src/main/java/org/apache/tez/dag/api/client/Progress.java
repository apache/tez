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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.tez.dag.api.records.DAGProtos.ProgressProtoOrBuilder;

/**
 * Describes the progress made by DAG execution
 */
@Public
@Evolving
public class Progress {
  
  ProgressProtoOrBuilder proxy = null;
  
  Progress(ProgressProtoOrBuilder proxy) {
    this.proxy = proxy;
  }
  
  public int getTotalTaskCount() {
    return proxy.getTotalTaskCount();
  }

  public int getSucceededTaskCount() {
    return proxy.getSucceededTaskCount();
  }

  public int getRunningTaskCount() {
    return proxy.getRunningTaskCount();
  }

  public int getFailedTaskCount() {
    return proxy.getFailedTaskCount();
  }

  public int getKilledTaskCount() {
    return proxy.getKilledTaskCount();
  }

  public int getFailedTaskAttemptCount() {
    return proxy.getFailedTaskAttemptCount();
  }

  public int getKilledTaskAttemptCount() {
    return proxy.getKilledTaskAttemptCount();
  }

  public int getRejectedTaskAttemptCount() {
    return proxy.getRejectedTaskAttemptCount();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Progress){
      Progress other = (Progress)obj;
      return getTotalTaskCount() == other.getTotalTaskCount() 
          && getSucceededTaskCount() == other.getSucceededTaskCount()
          && getRunningTaskCount() == other.getRunningTaskCount()
          && getFailedTaskCount() == other.getFailedTaskCount()
          && getKilledTaskCount() == other.getKilledTaskCount()
          && getFailedTaskAttemptCount() == other.getFailedTaskAttemptCount()
          && getKilledTaskAttemptCount() == other.getKilledTaskAttemptCount()
          && getRejectedTaskAttemptCount() == other.getRejectedTaskAttemptCount();
    }
    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 45007;
    int result = prime + getTotalTaskCount();
    result = prime * result +
        getSucceededTaskCount();
    result = prime * result +
        getRunningTaskCount();
    result = prime * result +
        getFailedTaskCount();
    result = prime * result +
        getKilledTaskCount();
    result = prime * result +
        getFailedTaskAttemptCount();
    result = prime * result +
        getKilledTaskAttemptCount();
    result = prime * result +
        getRejectedTaskAttemptCount();

    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("TotalTasks: ");
    sb.append(getTotalTaskCount());
    sb.append(" Succeeded: ");
    sb.append(getSucceededTaskCount());
    sb.append(" Running: ");
    sb.append(getRunningTaskCount());
    sb.append(" Failed: "); 
    sb.append(getFailedTaskCount());
    sb.append(" Killed: "); 
    sb.append(getKilledTaskCount());
    if (getFailedTaskAttemptCount() > 0) {
      sb.append(" FailedTaskAttempts: ");
      sb.append(getFailedTaskAttemptCount());
    }
    if (getKilledTaskAttemptCount() > 0) {
      sb.append(" KilledTaskAttempts: ");
      sb.append(getKilledTaskAttemptCount());
    }
    if (getRejectedTaskAttemptCount() > 0) {
      sb.append(" RejectedTaskAttempts: ");
      sb.append(getRejectedTaskAttemptCount());
    }
    return sb.toString();
  }

}
