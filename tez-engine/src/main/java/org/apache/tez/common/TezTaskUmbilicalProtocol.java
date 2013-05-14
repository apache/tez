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

package org.apache.tez.common;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.tez.common.records.ProceedToCompletionResponse;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.engine.api.Master;
import org.apache.tez.engine.records.OutputContext;

/** Protocol that task child process uses to contact its parent process.  The
 * parent is a daemon which which polls the central master for a new map or
 * reduce task and runs it as a child process.  All communication between child
 * and parent is via this protocol. */ 
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface TezTaskUmbilicalProtocol extends Master {

  public static final long versionID = 19L;

  ContainerTask getTask(ContainerContext containerContext) throws IOException;
  
  boolean statusUpdate(TezTaskAttemptID taskId, TezTaskStatus taskStatus) 
  throws IOException, InterruptedException;
  
  void reportDiagnosticInfo(TezTaskAttemptID taskid, String trace) throws IOException;
  
  boolean ping(TezTaskAttemptID taskid) throws IOException;

  void done(TezTaskAttemptID taskid) throws IOException;
  
  void commitPending(TezTaskAttemptID taskId, TezTaskStatus taskStatus) 
  throws IOException, InterruptedException;  

  boolean canCommit(TezTaskAttemptID taskid) throws IOException;

  void shuffleError(TezTaskAttemptID taskId, String message) throws IOException;
  
  void fsError(TezTaskAttemptID taskId, String message) throws IOException;

  void fatalError(TezTaskAttemptID taskId, String message) throws IOException;
  
  // TODO TEZAM5 Can commitPending and outputReady be collapsed into a single
  // call.
  // IAC outputReady followed by commit is a little confusing - since the output
  // isn't really in place till a commit is called. Maybe rename to
  // processingComplete or some such.
  
  // TODO EVENTUALLY This is not the most useful API. Once there's some kind of
  // support for the Task handing output over to the Container, this won't rally
  // be required. i.e. InMemShuffle running as a service in the Container, or
  // the second task in getTask(). ContainerUmbilical would include getTask and
  // getServices...
  
  void outputReady(TezTaskAttemptID taskAttemptId, OutputContext outputContext)
      throws IOException;
  
  ProceedToCompletionResponse
      proceedToCompletion(TezTaskAttemptID taskAttemptId) throws IOException;
}
