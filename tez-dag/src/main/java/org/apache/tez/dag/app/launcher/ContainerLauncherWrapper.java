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

import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.serviceplugins.api.ContainerLaunchRequest;
import org.apache.tez.serviceplugins.api.ContainerLauncher;
import org.apache.tez.serviceplugins.api.ContainerStopRequest;

public class ContainerLauncherWrapper {

  private final ContainerLauncher real;

  public ContainerLauncherWrapper(ContainerLauncher containerLauncher) {
    this.real = containerLauncher;
  }

  public void launchContainer(ContainerLaunchRequest launchRequest) throws Exception {
    real.launchContainer(launchRequest);
  }

  public void stopContainer(ContainerStopRequest stopRequest) throws Exception {
    real.stopContainer(stopRequest);
  }

  public ContainerLauncher getContainerLauncher() {
    return real;
  }

  public void dagComplete(DAG dag, JobTokenSecretManager jobTokenSecretManager) {
    if (real instanceof TezContainerLauncherImpl) {
      ((TezContainerLauncherImpl)real).dagComplete(dag, jobTokenSecretManager);
    }
    if (real instanceof LocalContainerLauncher) {
      ((LocalContainerLauncher)real).dagComplete(dag, jobTokenSecretManager);
    }
  }
}
