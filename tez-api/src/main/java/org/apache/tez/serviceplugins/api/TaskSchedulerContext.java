/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.serviceplugins.api;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.ContainerSignatureMatcher;
import org.apache.tez.dag.api.UserPayload;

@InterfaceAudience.Public
@InterfaceStability.Unstable

public interface TaskSchedulerContext {

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

  enum AMState {
    IDLE, RUNNING_APP, COMPLETED
  }

  // TODO TEZ-2003 (post) TEZ-2664. Remove references to YARN constructs like Container, ContainerStatus, NodeReport
  // TODO TEZ-2003 (post) TEZ-2668 Enhancements to TaskScheduler interfaces
  // - setApplicationRegistrationData may not be relevant to non YARN clusters
  // - getAppFinalStatus may not be relevant to non YARN clusters
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

  // TODO Post TEZ-2003, this method specifically needs some cleaning up.
  // ClientAMSecretKey is only relevant when running under YARN. As are ApplicationACLs.
  public void setApplicationRegistrationData(
      Resource maxContainerCapability,
      Map<ApplicationAccessType, String> appAcls,
      ByteBuffer clientAMSecretKey
  );
  public void onError(Throwable t);
  public float getProgress();
  public void preemptContainer(ContainerId containerId);

  public AppFinalStatus getFinalAppStatus();


  // Getters

  public UserPayload getInitialUserPayload();

  public String getAppTrackingUrl();

  /**
   * A custom cluster identifier allocated to schedulers to generate an AppId, if not making
   * use of YARN
   * @return
   */
  public long getCustomClusterIdentifier();

  public ContainerSignatureMatcher getContainerSignatureMatcher();

  /**
   * Get the application attempt id for the running application. Relevant when running under YARN
   * @return
   */
  public ApplicationAttemptId getApplicationAttemptId();

  public String getAppHostName();

  public int getAppClientPort();

  public boolean isSession();

  public AMState getAMState();
}
