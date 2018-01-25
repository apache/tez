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

/**
 * Context for a {@link TaskScheduler}
 * <p/>
 * This provides methods for a scheduler to interact with the Tez framework.
 * <p/>
 * Calls into this should be outside of locks, which may also be obtained by methods in the
 * scheduler
 * which implement the {@link TaskScheduler} interface
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface TaskSchedulerContext extends ServicePluginContextBase {

  class AppFinalStatus {
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

  /**
   * Indicates the state the AM is in.
   */
  enum AMState {
    IDLE,
    RUNNING_APP,
    COMPLETED
  }

  // TODO TEZ-2003 (post) TEZ-2664. Remove references to YARN constructs like Container, ContainerStatus, NodeReport
  // TODO TEZ-2003 (post) TEZ-2668 Enhancements to TaskScheduler interfaces
  // - setApplicationRegistrationData may not be relevant to non YARN clusters
  // - getAppFinalStatus may not be relevant to non YARN clusters


  /**
   * Indicate to the framework that a container is being assigned to a task.
   *
   * @param task      the task for which a container is being assigned. This should be the same
   *                  instance that was provided when requesting for an allocation
   * @param appCookie the cookie which was provided while requesting allocation for this task
   * @param container the actual container assigned to the task
   */
  void taskAllocated(Object task,
                     Object appCookie,
                     Container container);


  /**
   * Indicate to the framework that a container has completed. This is typically used by sources
   * which have
   * a means to indicate a container failure to the scheduler (typically centrally managed
   * schedulers - YARN)
   *
   * @param taskLastAllocated the task that was allocated to this container, if any. This is the
   *                          same instance that was passed in while requesting an allocation
   * @param containerStatus   the status with which the container ended
   */
  void containerCompleted(Object taskLastAllocated,
                          ContainerStatus containerStatus);

  /**
   * Indicates to the framework that a container is being released.
   *
   * @param containerId the id of the container being released
   */
  void containerBeingReleased(ContainerId containerId);


  /**
   * Provide an update to the framework about the status of nodes available to this report
   *
   * @param updatedNodes a list of updated node reports
   */
  void nodesUpdated(List<NodeReport> updatedNodes);

  /**
   * Inform the framework that an app shutdown is required. This should typically not be used, other
   * than
   * by the YARN scheduler.
   */
  void appShutdownRequested();

  /**
   * Provide an update to the framework about specific information about the source managed by this
   * scheduler.
   *
   * @param maxContainerCapability the total resource capability of the source
   * @param appAcls                ACLs for the source
   * @param clientAMSecretKey      a secret key provided by the source
   */
  void setApplicationRegistrationData(
      Resource maxContainerCapability,
      Map<ApplicationAccessType, String> appAcls,
      ByteBuffer clientAMSecretKey,
      String queueName
  );

  /**
   * Inform the framework that the scheduler has determined that a previously allocated container
   * needs to be preempted
   *
   * @param containerId the containerId to be preempted
   */
  void preemptContainer(ContainerId containerId);

  /**
   * Get the final status for the application, which could be provided to the coordinator of the
   * source.
   * Primarily relevant to YARN
   *
   * @return the final Application status
   */
  AppFinalStatus getFinalAppStatus();


  // Getters

  /**
   * Get the tracking URL for the application. Primarily relevant to YARN
   *
   * @return the trackingUrl for the app
   */
  String getAppTrackingUrl();

  /**
   * Request the framework for progress of the running DAG. This value must be between 0 and 1
   *
   * @return progress
   */
  float getProgress();

  /**
   * A custom cluster identifier allocated to schedulers to generate an AppId, if not making
   * use of YARN
   *
   * @return the custom cluster identifier
   */
  long getCustomClusterIdentifier();

  /**
   * Get an instance of {@link ContainerSignatureMatcher} which can be used to check whether the
   * specifications of a container match what is required by a task.
   *
   * @return an instance of {@link ContainerSignatureMatcher}
   */
  ContainerSignatureMatcher getContainerSignatureMatcher();

  /**
   * Get the application attempt id for the running application. Relevant when running under YARN
   *
   * @return the applicationAttemptId for the running app
   */
  ApplicationAttemptId getApplicationAttemptId();

  /**
   * Get the hostname on which the app is running
   *
   * @return the hostname
   */
  String getAppHostName();

  /**
   * Get the port on which the DAG client is listening
   *
   * @return the client port
   */
  int getAppClientPort();

  /**
   * Check whether the AM is running in session mode.
   *
   * @return true if session mode, false otherwise
   */
  boolean isSession();

  /**
   * Get the state of the AppMaster
   *
   * @return the app master state
   */
  AMState getAMState();

  int getVertexIndexForTask(Object task);
}
