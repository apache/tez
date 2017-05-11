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
package org.apache.tez.runtime.library.cartesianproduct;

import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.CartesianProductConfigProto;

import java.io.IOException;
import java.util.List;

/**
 * base class of cartesian product vertex manager implementation
 */
abstract class CartesianProductVertexManagerReal {
  private final VertexManagerPluginContext context;

  public CartesianProductVertexManagerReal(VertexManagerPluginContext context) {
    this.context = context;
  }

  public final VertexManagerPluginContext getContext() {
    return this.context;
  }

  public abstract void initialize(CartesianProductConfigProto config) throws Exception;

  public abstract void onVertexManagerEventReceived(VertexManagerEvent vmEvent) throws IOException;

  public abstract void onVertexStarted(List<TaskAttemptIdentifier> completions) throws Exception;

  public abstract void onVertexStateUpdated(VertexStateUpdate stateUpdate) throws Exception;

  public abstract void onSourceTaskCompleted(TaskAttemptIdentifier attempt) throws Exception;
}
