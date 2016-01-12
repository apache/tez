/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app.launcher;

import org.apache.tez.serviceplugins.api.ContainerLaunchRequest;
import org.apache.tez.serviceplugins.api.ContainerLauncher;
import org.apache.tez.serviceplugins.api.ContainerLauncherContext;
import org.apache.tez.serviceplugins.api.ContainerStopRequest;

public class TezTestServiceContainerLauncherWithErrors extends ContainerLauncher {
  public TezTestServiceContainerLauncherWithErrors(
      ContainerLauncherContext containerLauncherContext) {
    super(containerLauncherContext);
  }

  @Override
  public void launchContainer(ContainerLaunchRequest launchRequest) {
    throw new RuntimeException("Simulated Error");
  }

  @Override
  public void stopContainer(ContainerStopRequest stopRequest) {
    throw new RuntimeException("Simulated Error");
  }
}
