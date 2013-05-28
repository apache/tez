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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.tez.dag.api.client.VertexStatus;

@Public
@Unstable
public abstract class VertexOutputCommitter {

  /**
   * Setup up the vertex output committer.
   *
   * @param context Context of the vertex whose output is being written.
   * @throws IOException
   */
  public abstract void init(VertexContext context) throws IOException;

  /**
   * For the framework to setup the vertex output during initialization. This is
   * called from the application master process for the vertex. This will be
   * called multiple times, once per dag attempt for each vertex.
   *
   * @throws IOException if setup fails
   */
  public abstract void setupVertex() throws IOException;

  /**
   * For committing vertex's output after successful vertex completion.
   * Note that this is invoked for vertices with a successful final state.
   * This is called from the application master process for the entire vertex.
   * This is guaranteed to only be called once.
   * If it throws an exception the entire vertex will fail.
   *
   * @throws IOException
   */
  public abstract void commitVertex() throws IOException;

  /**
   * For aborting an unsuccessful vertex's output. Note that this is invoked for
   * vertices with a final failed state. This is called from the application
   * master process for the entire vertex. This may be called multiple times.
   *
   * @param state final runstate of the vertex
   * @throws IOException
   */
  public abstract void abortVertex(VertexStatus.State finalState)
      throws IOException;

}
