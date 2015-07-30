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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.tez.common.ServicePluginLifecycle;

/**
 * Plugin to allow custom container launchers to be written to launch containers on different types
 * of executors.
 */

@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class ContainerLauncher implements ServicePluginLifecycle {

  private final ContainerLauncherContext containerLauncherContext;

  public ContainerLauncher(ContainerLauncherContext containerLauncherContext) {
    this.containerLauncherContext = containerLauncherContext;
  }

  @Override
  public void initialize() throws Exception {
  }

  @Override
  public void start() throws Exception {
  }

  @Override
  public void shutdown() throws Exception {
  }

  public final ContainerLauncherContext getContext() {
    return this.containerLauncherContext;
  }

  public abstract void launchContainer(ContainerLaunchRequest launchRequest);
  public abstract void stopContainer(ContainerStopRequest stopRequest);
}
