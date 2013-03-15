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
import org.apache.tez.common.TezTaskStatus;
import org.apache.tez.mapreduce.hadoop.ContainerContext;
import org.apache.tez.mapreduce.hadoop.ContainerTask;
import org.apache.tez.mapreduce.hadoop.TezTaskUmbilicalProtocol;
import org.apache.tez.mapreduce.hadoop.records.ProceedToCompletionResponse;
import org.apache.tez.records.TezJobID;
import org.apache.tez.records.TezTaskAttemptID;
import org.apache.tez.records.TezTaskDependencyCompletionEventsUpdate;
import org.apache.tez.records.OutputContext;

public class TestUmbilicalProtocol implements TezTaskUmbilicalProtocol {

  private static final Log LOG = LogFactory.getLog(TestUmbilicalProtocol.class);
  private ProceedToCompletionResponse proceedToCompletionResponse;
  private boolean shouldLinger;
  
  public TestUmbilicalProtocol() {
    proceedToCompletionResponse = new ProceedToCompletionResponse(false, true);
  }
  
  public TestUmbilicalProtocol(boolean shouldLinger) {
    if (shouldLinger) {
      proceedToCompletionResponse = new ProceedToCompletionResponse(false, false);
    } else {
      proceedToCompletionResponse = new ProceedToCompletionResponse(false, true);
    }
    this.shouldLinger = shouldLinger;
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
      TezJobID jobID, int fromEventIdx, int maxEventsToFetch,
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
  public boolean statusUpdate(TezTaskAttemptID taskId, TezTaskStatus taskStatus)
      throws IOException, InterruptedException {
    LOG.info("Got 'status-update' from " + taskId + ": status=" + taskStatus);
    return true;
  }

  @Override
  public void reportDiagnosticInfo(TezTaskAttemptID taskid, String trace)
      throws IOException {
    LOG.info("Got 'diagnostic-info' from " + taskid + ": trace=" + trace);
  }

  @Override
  public boolean ping(TezTaskAttemptID taskid) throws IOException {
    LOG.info("Got 'ping' from " + taskid);
    return true;
  }

  @Override
  public void done(TezTaskAttemptID taskid) throws IOException {
    LOG.info("Got 'done' from " + taskid);
  }

  @Override
  public void commitPending(TezTaskAttemptID taskId, TezTaskStatus taskStatus)
      throws IOException, InterruptedException {
    LOG.info("Got 'commit-pending' from " + taskId + ": status=" + taskStatus);
  }

  @Override
  public boolean canCommit(TezTaskAttemptID taskid) throws IOException {
    LOG.info("Got 'can-commit' from " + taskid);
    return !shouldLinger;
  }

  @Override
  public void shuffleError(TezTaskAttemptID taskId, String message)
      throws IOException {
    LOG.info("Got 'shuffle-error' from " + taskId + ": message=" + message);
  }

  @Override
  public void fsError(TezTaskAttemptID taskId, String message)
      throws IOException {
    LOG.info("Got 'fs-error' from " + taskId + ": message=" + message);
  }

  @Override
  public void fatalError(TezTaskAttemptID taskId, String message)
      throws IOException {
    LOG.info("Got 'fatal-error' from " + taskId + ": message=" + message);
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

}
