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

package org.apache.tez.dag.api.committer;


import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezVertexID;

public class VertexContext {

  private final TezDAGID tezDAGID;
  private final byte[] userPayload;
  private final ApplicationAttemptId applicationAttemptId;
  private final TezVertexID tezVertexID;

  public VertexContext(TezDAGID tezDAGID, byte[] userPayload,
      TezVertexID tezVertexID,
      ApplicationAttemptId applicationAttemptId) {
    this.tezDAGID = tezDAGID;
    this.userPayload = userPayload;
    this.tezVertexID = tezVertexID;
    this.applicationAttemptId = applicationAttemptId;
  }

  public TezDAGID getDAGID() {
    return tezDAGID;
  }

  public byte[] getUserPayload() {
    return userPayload;
  }

  // TODO get rid of this as part of VertexContext cleanup
  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  public TezVertexID getVertexID() {
    return tezVertexID;
  }


}
