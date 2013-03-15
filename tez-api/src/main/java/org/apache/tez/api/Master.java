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
package org.apache.tez.api;

import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.tez.records.TezJobID;
import org.apache.tez.records.TezTaskAttemptID;
import org.apache.tez.records.TezTaskDependencyCompletionEventsUpdate;

/**
 * {@link Master} represents the master controlling the {@link Task}. 
 */
@ProtocolInfo(protocolName = "Master", protocolVersion = 1)
public interface Master extends VersionedProtocol {

  // TODO TEZAM3 This likely needs to change to be a little more generic.
  // Many output / input relationships cannot be captured via this. The current
  // form works primarily works for the existing MR

  TezTaskDependencyCompletionEventsUpdate getDependentTasksCompletionEvents(
      TezJobID jobID, int fromEventIdx, int maxEventsToFetch,
      TezTaskAttemptID taskAttemptId);

}
