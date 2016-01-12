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

  /**
   * An entry point for initialization.
   * Order of service setup. Constructor, initialize(), start() - when starting a service.
   *
   * @throws Exception
   */
  @Override
  public void initialize() throws Exception {
  }

  /**
   * An entry point for starting the service.
   * Order of service setup. Constructor, initialize(), start() - when starting a service.
   *
   * @throws Exception
   */
  @Override
  public void start() throws Exception {
  }

  /**
   * Stop the service. This could be invoked at any point, when the service is no longer required -
   * including in case of errors.
   *
   * @throws Exception
   */
  @Override
  public void shutdown() throws Exception {
  }

  /**
   * Get the {@link ContainerLauncherContext} associated with this instance of the container
   * launcher, which is used to communicate with the rest of the system
   *
   * @return an instance of {@link ContainerLauncherContext}
   */
  public final ContainerLauncherContext getContext() {
    return this.containerLauncherContext;
  }

  /**
   * Get the {@link ContainerLauncherContext} associated with this instance of the container
   * launcher, which is used to communicate with the rest of the system
   *
   * @param launchRequest the actual launch request
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract void launchContainer(ContainerLaunchRequest launchRequest) throws
      ServicePluginException;

  /**
   * A request to stop a specific container
   *
   * @param stopRequest the actual stop request
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract void stopContainer(ContainerStopRequest stopRequest) throws ServicePluginException;
}
