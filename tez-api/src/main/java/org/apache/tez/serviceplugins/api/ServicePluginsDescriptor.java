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

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ServicePluginsDescriptor {

  private final boolean enableContainers;
  private final boolean enableUber;

  private TaskSchedulerDescriptor[] taskSchedulerDescriptors;
  private ContainerLauncherDescriptor[] containerLauncherDescriptors;
  private TaskCommunicatorDescriptor[] taskCommunicatorDescriptors;

  private ServicePluginsDescriptor(boolean enableContainers, boolean enableUber,
                                   TaskSchedulerDescriptor[] taskSchedulerDescriptor,
                                   ContainerLauncherDescriptor[] containerLauncherDescriptors,
                                   TaskCommunicatorDescriptor[] taskCommunicatorDescriptors) {
    this.enableContainers = enableContainers;
    this.enableUber = enableUber;
    Preconditions.checkArgument(taskSchedulerDescriptors == null || taskSchedulerDescriptors.length > 0,
        "TaskSchedulerDescriptors should either not be specified or at least 1 should be provided");
    this.taskSchedulerDescriptors = taskSchedulerDescriptor;
    Preconditions.checkArgument(containerLauncherDescriptors == null || containerLauncherDescriptors.length > 0,
        "ContainerLauncherDescriptor should either not be specified or at least 1 should be provided");
    this.containerLauncherDescriptors = containerLauncherDescriptors;
    Preconditions.checkArgument(taskCommunicatorDescriptors == null || taskCommunicatorDescriptors.length > 0,
        "TaskCommunicatorDescriptors should either not be specified or at least 1 should be provided");
    this.taskCommunicatorDescriptors = taskCommunicatorDescriptors;
  }

  public static ServicePluginsDescriptor create(TaskSchedulerDescriptor[] taskSchedulerDescriptor,
                                                ContainerLauncherDescriptor[] containerLauncherDescriptors,
                                                TaskCommunicatorDescriptor[] taskCommunicatorDescriptors) {
    return new ServicePluginsDescriptor(true, false, taskSchedulerDescriptor,
        containerLauncherDescriptors, taskCommunicatorDescriptors);
  }

  public static ServicePluginsDescriptor create(boolean enableUber,
                                                TaskSchedulerDescriptor[] taskSchedulerDescriptor,
                                                ContainerLauncherDescriptor[] containerLauncherDescriptors,
                                                TaskCommunicatorDescriptor[] taskCommunicatorDescriptors) {
    return new ServicePluginsDescriptor(true, enableUber, taskSchedulerDescriptor,
        containerLauncherDescriptors, taskCommunicatorDescriptors);
  }

  public static ServicePluginsDescriptor create(boolean enableContainers, boolean enableUber,
                                                TaskSchedulerDescriptor[] taskSchedulerDescriptor,
                                                ContainerLauncherDescriptor[] containerLauncherDescriptors,
                                                TaskCommunicatorDescriptor[] taskCommunicatorDescriptors) {
    return new ServicePluginsDescriptor(enableContainers, enableUber, taskSchedulerDescriptor,
        containerLauncherDescriptors, taskCommunicatorDescriptors);
  }

  public static ServicePluginsDescriptor create(boolean enableUber) {
    return new ServicePluginsDescriptor(true, enableUber, null, null, null);
  }


  public boolean areContainersEnabled() {
    return enableContainers;
  }

  public boolean isUberEnabled() {
    return enableUber;
  }

  public TaskSchedulerDescriptor[] getTaskSchedulerDescriptors() {
    return taskSchedulerDescriptors;
  }

  public ContainerLauncherDescriptor[] getContainerLauncherDescriptors() {
    return containerLauncherDescriptors;
  }

  public TaskCommunicatorDescriptor[] getTaskCommunicatorDescriptors() {
    return taskCommunicatorDescriptors;
  }
}
