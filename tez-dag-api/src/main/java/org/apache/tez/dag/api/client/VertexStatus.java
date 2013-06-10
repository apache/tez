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

package org.apache.tez.dag.api.client;

import java.util.List;

import org.apache.tez.dag.api.records.DAGProtos.VertexStatusProtoOrBuilder;
import org.apache.tez.dag.api.TezUncheckedException;

public class VertexStatus {
  
  public enum State {
    INITED,
    RUNNING,
    SUCCEEDED,
    KILLED,
    FAILED,
    ERROR
  };
  
  VertexStatusProtoOrBuilder proxy = null;
  Progress progress = null;
  
  public VertexStatus(VertexStatusProtoOrBuilder proxy) {
    this.proxy = proxy;
  }

  public State getState() {
    switch(proxy.getState()) {
    case VERTEX_INITED:
      return VertexStatus.State.INITED;
    case VERTEX_RUNNING:
      return VertexStatus.State.RUNNING;
    case VERTEX_SUCCEEDED:
      return VertexStatus.State.SUCCEEDED;
    case VERTEX_FAILED:
      return VertexStatus.State.FAILED;
    case VERTEX_KILLED:
      return VertexStatus.State.KILLED;
    case VERTEX_ERROR:
      return VertexStatus.State.ERROR;
    default:
      throw new TezUncheckedException("Unsupported value for VertexStatus.State : " + 
                              proxy.getState());
    }    
  }

  public List<String> getDiagnostics() {
    return proxy.getDiagnosticsList();
  }

  public Progress getProgress() {
    if(progress == null && proxy.hasProgress()) {
      progress = new Progress(proxy.getProgress());
    }
    return progress;    
  }

}
