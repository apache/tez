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

package org.apache.tez.service;

import java.io.IOException;

import org.apache.tez.dag.api.TezException;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.RunContainerRequestProto;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.SubmitWorkRequestProto;

public interface ContainerRunner {

  void queueContainer(RunContainerRequestProto request) throws TezException;

  void submitWork(SubmitWorkRequestProto request) throws TezException;
}
