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

package org.apache.tez.engine.newapi.impl;

import java.util.List;
import java.util.Map;

import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.engine.newapi.Event;
import org.apache.tez.engine.newapi.rpc.impl.TaskSpec;

/**
 * Interface to the RPC layer ( umbilical ) between the Tez AM and
 * a Tez Container's JVM.
 */
public interface TezEngineContext {

  /**
   * Heartbeat call back to the AM from the Container JVM
   * @param events Events to be sent to the AM
   * @return Events sent by the AM to the Container JVM which in turn will
   * be handled either by the JVM or routed to the approrpiate instances of
   * Input/Processor/Outputs with a particular Task Attempt.
   */
  public Event[] hearbeat(Event[] events);

  /**
   * Hook to ask the Tez AM for the next task to be run on the Container
   * @return Next task to be run
   */
  public TaskSpec getNextTask();

  /**
   * Hook to query the Tez AM whether a particular Task Attempt can commit its
   * output.
   * @param attemptIDs Attempt IDs of the Tasks that are waiting to commit.
   * @return Map of boolean flags indicating whether the respective task
   * attempts can commit.
   */
  public Map<TezTaskAttemptID, Boolean>
      canTaskCommit(List<TezTaskAttemptID> attemptIDs);

  /**
   * Inform the Tez AM that one ore more Task attempts have failed.
   * @param attemptIDs Task Attempt IDs for the failed attempts.
   */
  public void taskFailed(List<TezTaskAttemptID> attemptIDs);

}
