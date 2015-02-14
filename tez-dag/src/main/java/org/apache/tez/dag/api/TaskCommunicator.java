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

package org.apache.tez.dag.api;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;

// TODO TEZ-2003 Move this into the tez-api module
public abstract class TaskCommunicator extends AbstractService {
  public TaskCommunicator(String name) {
    super(name);
  }

  // TODO TEZ-2003 Ideally, don't expose YARN containerId; instead expose a Tez specific construct.
  // TODO When talking to an external service, this plugin implementer may need access to a host:port
  public abstract void registerRunningContainer(ContainerId containerId, String hostname, int port);

  // TODO TEZ-2003 Ideally, don't expose YARN containerId; instead expose a Tez specific construct.
  public abstract void registerContainerEnd(ContainerId containerId);

  // TODO TEZ-2003 TaskSpec breakup into a clean interface
  // TODO TEZ-2003 Add support for priority
  public abstract void registerRunningTaskAttempt(ContainerId containerId, TaskSpec taskSpec,
                                                  Map<String, LocalResource> additionalResources,
                                                  Credentials credentials,
                                                  boolean credentialsChanged);

  // TODO TEZ-2003 Remove reference to TaskAttemptID
  public abstract void unregisterRunningTaskAttempt(TezTaskAttemptID taskAttemptID);

  // TODO TEZ-2003 This doesn't necessarily belong here. A server may not start within the AM.
  public abstract InetSocketAddress getAddress();

  // TODO Eventually. Add methods here to support preemption of tasks.
}
