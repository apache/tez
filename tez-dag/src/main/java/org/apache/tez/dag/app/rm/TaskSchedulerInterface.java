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

package org.apache.tez.dag.app.rm;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

public interface TaskSchedulerInterface {

  public abstract Resource getAvailableResources();

  public abstract int getClusterNodeCount();

  public abstract void resetMatchLocalityForAllHeldContainers();

  public abstract Resource getTotalResources();

  public abstract void blacklistNode(NodeId nodeId);

  public abstract void unblacklistNode(NodeId nodeId);

  public abstract void allocateTask(Object task, Resource capability,
      String[] hosts, String[] racks, Priority priority,
      Object containerSignature, Object clientCookie);
  
  /**
   * Allocate affinitized to a specific container
   */
  public abstract void allocateTask(Object task, Resource capability,
      ContainerId containerId, Priority priority, Object containerSignature,
      Object clientCookie);
  
  public abstract boolean deallocateTask(Object task, boolean taskSucceeded);

  public abstract Object deallocateContainer(ContainerId containerId);

  public abstract void setShouldUnregister();
}
