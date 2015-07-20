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

package org.apache.tez.dag.app.launcher;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.tez.serviceplugins.api.ContainerLaunchRequest;
import org.apache.tez.serviceplugins.api.ContainerLauncherOperationBase;
import org.apache.tez.serviceplugins.api.ContainerStopRequest;

@InterfaceAudience.Private
public class ContainerOp {
  enum OPType {
    LAUNCH_REQUEST, STOP_REQUEST
  }

  final ContainerLauncherOperationBase command;
  final OPType opType;

  public ContainerOp(OPType opType, ContainerLauncherOperationBase command) {
    this.opType = opType;
    this.command = command;
  }

  public OPType getOpType() {
    return opType;
  }

  public ContainerLauncherOperationBase getBaseOperation() {
    return command;
  }

  public ContainerLaunchRequest getLaunchRequest() {
    Preconditions.checkState(opType == OPType.LAUNCH_REQUEST);
    return (ContainerLaunchRequest) command;
  }

  public ContainerStopRequest getStopRequest() {
    Preconditions.checkState(opType == OPType.STOP_REQUEST);
    return (ContainerStopRequest) command;
  }

  @Override
  public String toString() {
    return "ContainerOp{" +
        "opType=" + opType +
        ", command=" + command +
        '}';
  }
}