/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.tez.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.common.records.ProceedToCompletionResponse;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.engine.newapi.impl.TezHeartbeatRequest;
import org.apache.tez.engine.newapi.impl.TezHeartbeatResponse;
import org.apache.tez.engine.records.OutputContext;
import org.apache.tez.engine.records.TezTaskDependencyCompletionEventsUpdate;

public class TestUmbilicalProtocol implements TezTaskUmbilicalProtocol {

  private static final Log LOG = LogFactory.getLog(TestUmbilicalProtocol.class);
  private ProceedToCompletionResponse proceedToCompletionResponse;


  public TestUmbilicalProtocol() {
    proceedToCompletionResponse = new ProceedToCompletionResponse(false, true);
  }

  public TestUmbilicalProtocol(boolean shouldLinger) {
    if (shouldLinger) {
      proceedToCompletionResponse = new ProceedToCompletionResponse(false, false);
    } else {
      proceedToCompletionResponse = new ProceedToCompletionResponse(false, true);
    }
  }

  @Override
  public ProtocolSignature getProtocolSignature(String arg0, long arg1, int arg2)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public TezTaskDependencyCompletionEventsUpdate getDependentTasksCompletionEvents(
      int fromEventIdx, int maxEventsToFetch,
      TezTaskAttemptID reduce) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ContainerTask getTask(ContainerContext containerContext)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public void commitPending(TezTaskAttemptID taskId)
      throws IOException, InterruptedException {
    LOG.info("Got 'commit-pending' from " + taskId);
  }

  @Override
  public boolean canCommit(TezTaskAttemptID taskid) throws IOException {
    LOG.info("Got 'can-commit' from " + taskid);
    return true;
  }

  @Override
  public void outputReady(TezTaskAttemptID taskAttemptId,
      OutputContext outputContext) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public ProceedToCompletionResponse proceedToCompletion(
      TezTaskAttemptID taskAttemptId) throws IOException {
    return proceedToCompletionResponse;
  }

  @Override
  public TezHeartbeatResponse heartbeat(TezHeartbeatRequest request) {
    // TODO Auto-generated method stub
    // TODO TODONEWTEZ
    return null;
  }

}
