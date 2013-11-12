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

package org.apache.tez.dag.app.dag.event;

import org.apache.tez.dag.records.TezVertexID;

public class VertexEventOneToOneSourceSplit extends VertexEvent {
  final int numTasks;
  final TezVertexID originalSplitVertex;
  final TezVertexID senderVertex;
  
  public VertexEventOneToOneSourceSplit(TezVertexID vertexId,
      TezVertexID senderVertex,
      TezVertexID originalSplitVertex,
      int numTasks) {
    super(vertexId, VertexEventType.V_ONE_TO_ONE_SOURCE_SPLIT);
    this.numTasks = numTasks;
    this.senderVertex = senderVertex;
    this.originalSplitVertex = originalSplitVertex;
  }
  
  public int getNumTasks() {
    return numTasks;
  }
  
  public TezVertexID getOriginalSplitSource() {
    return originalSplitVertex;
  }
  
  public TezVertexID getSenderVertex() {
    return senderVertex;
  }

}
