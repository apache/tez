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

package org.apache.tez.runtime.api;

import org.apache.tez.dag.api.client.VertexStatus;

import java.io.IOException;

/**
 * OutputCommitter to "finalize" the output and make it user-visible if needed.
 * The OutputCommitter is allowed only on a terminal Output.
 */
public abstract class OutputCommitter {

  /**
   * Setup up the Output committer.
   *
   * @param context Context of the output that is being acted upon
   * @throws java.lang.Exception
   */
  public abstract void initialize(OutputCommitterContext context)
      throws Exception;

  /**
   * For the framework to setup the output during initialization. This is
   * called from the application master process for the vertex. This will be
   * called multiple times, once per dag attempt.
   *
   * @throws java.lang.Exception if setup fails
   */
  public abstract void setupOutput() throws Exception;

  /**
   * For committing the output after a successful vertex completion.
   * Note that this is invoked for the outputs of vertices with a successful
   * final state. This is called from the application master process.
   * This is guaranteed to only be called once.
   * If it throws an exception the entire vertex will fail.
   *
   * @throws java.lang.Exception
   */
  public abstract void commitOutput() throws Exception;

  /**
   * For aborting an unsuccessful vertex's output. Note that this is invoked for
   * vertices with a final failed state. This is called from the application
   * master process. This may be called multiple times.
   *
   * @param finalState final run-state of the vertex
   * @throws java.lang.Exception
   */
  public abstract void abortOutput(VertexStatus.State finalState)
    throws Exception;

}
