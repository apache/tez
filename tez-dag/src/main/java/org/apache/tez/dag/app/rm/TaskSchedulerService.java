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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

public abstract class TaskSchedulerService extends AbstractService{

  public TaskSchedulerService(String name) {
    super(name);
  }

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

  public abstract boolean hasUnregistered();

  public abstract void initiateStop();

  public interface TaskSchedulerAppCallback {
    public class AppFinalStatus {
      public final FinalApplicationStatus exitStatus;
      public final String exitMessage;
      public final String postCompletionTrackingUrl;
      public AppFinalStatus(FinalApplicationStatus exitStatus,
                             String exitMessage,
                             String posCompletionTrackingUrl) {
        this.exitStatus = exitStatus;
        this.exitMessage = exitMessage;
        this.postCompletionTrackingUrl = posCompletionTrackingUrl;
      }
    }
    // upcall to app must be outside locks
    public void taskAllocated(Object task,
                               Object appCookie,
                               Container container);
    // this may end up being called for a task+container pair that the app
    // has not heard about. this can happen because of a race between
    // taskAllocated() upcall and deallocateTask() downcall
    public void containerCompleted(Object taskLastAllocated,
                                    ContainerStatus containerStatus);
    public void containerBeingReleased(ContainerId containerId);
    public void nodesUpdated(List<NodeReport> updatedNodes);
    public void appShutdownRequested();
    public void setApplicationRegistrationData(
                                Resource maxContainerCapability,
                                Map<ApplicationAccessType, String> appAcls,
                                ByteBuffer clientAMSecretKey
                                );
    public void onError(Throwable t);
    public float getProgress();
    public void preemptContainer(ContainerId containerId);
    public AppFinalStatus getFinalAppStatus();

  }
}
