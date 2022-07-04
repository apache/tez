/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.api.client;

import org.apache.tez.dag.api.client.VertexStatus.State;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.app.dag.VertexState;
import org.junit.Assert;
import org.junit.Test;

public class TestVertexStatusBuilder {

  @Test(timeout = 5000)
  public void testVertexStateConversion() {
    for (VertexState state : VertexState.values()) {
      DAGProtos.VertexStatusStateProto stateProto =
          VertexStatusBuilder.getProtoState(state);
      VertexStatus.State clientState =
          VertexStatus.getState(stateProto);
      Assert.assertEquals(state.name(), clientState.name());
    }
  }
}
