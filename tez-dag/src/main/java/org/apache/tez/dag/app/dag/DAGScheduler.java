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

package org.apache.tez.dag.app.dag;

import java.util.Map;
import java.util.Queue;

import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.records.TezVertexID;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public abstract class DAGScheduler {
  private static class VertexInfo {
    int concurrencyLimit;
    int concurrency;
    Queue<DAGEventSchedulerUpdate> pendingAttempts = Lists.newLinkedList();
    
    VertexInfo(int limit) {
      this.concurrencyLimit = limit;
    }
  }
  
  Map<TezVertexID, VertexInfo> vertexInfo = null;
  
  public void addVertexConcurrencyLimit(TezVertexID vId, int concurrency) {
    if (vertexInfo == null) {
      vertexInfo = Maps.newHashMap();
    }
    if (concurrency > 0) {
      vertexInfo.put(vId, new VertexInfo(concurrency));
    }
  }
  
  public void scheduleTask(DAGEventSchedulerUpdate event) {
    VertexInfo vInfo = null;
    if (vertexInfo != null) {
      vInfo = vertexInfo.get(event.getAttempt().getID().getTaskID().getVertexID());
    }
    scheduleTaskWithLimit(event, vInfo);
  }
  
  private void scheduleTaskWithLimit(DAGEventSchedulerUpdate event, VertexInfo vInfo) {
    if (vInfo != null) {
      if (vInfo.concurrency >= vInfo.concurrencyLimit) {
        vInfo.pendingAttempts.add(event);
        return; // already at max concurrency
      }
      vInfo.concurrency++;
    }
    scheduleTaskEx(event);
  }
  
  public void taskCompleted(DAGEventSchedulerUpdate event) {
    taskCompletedEx(event);
    if (vertexInfo != null) {
      VertexInfo vInfo = vertexInfo.get(event.getAttempt().getID().getTaskID().getVertexID());
      if (vInfo != null) {
        vInfo.concurrency--;
        if (!vInfo.pendingAttempts.isEmpty()) {
          scheduleTaskWithLimit(vInfo.pendingAttempts.poll(), vInfo);
        }
      }
    }
  }
  
  public abstract void scheduleTaskEx(DAGEventSchedulerUpdate event);
  
  public abstract void taskCompletedEx(DAGEventSchedulerUpdate event);
}
