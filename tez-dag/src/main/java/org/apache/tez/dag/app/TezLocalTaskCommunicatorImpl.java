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

package org.apache.tez.dag.app;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.tez.serviceplugins.api.TaskCommunicatorContext;
import org.apache.tez.dag.api.TezUncheckedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezLocalTaskCommunicatorImpl extends TezTaskCommunicatorImpl {

  private static final Logger LOG = LoggerFactory.getLogger(TezLocalTaskCommunicatorImpl.class);

  public TezLocalTaskCommunicatorImpl(
      TaskCommunicatorContext taskCommunicatorContext) {
    super(taskCommunicatorContext);
  }

  @Override
  protected void startRpcServer() {
    try {
      this.address = new InetSocketAddress(InetAddress.getLocalHost(), 0);
    } catch (UnknownHostException e) {
      throw new TezUncheckedException(e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Not starting TaskAttemptListener RPC in LocalMode");
    }
  }
}
