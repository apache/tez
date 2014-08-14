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

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.tez.dag.api.client.VertexStatus;

/**
 * OutputCommitter to "finalize" the output and make it user-visible if needed.
 * The OutputCommitter is allowed only on a terminal Output.
 */
@InterfaceStability.Evolving
@Public
public abstract class OutputCommitter {

  private final OutputCommitterContext committerContext;

  /**
   * Constructor an instance of the OutputCommitter. Classes extending this to create a
   * OutputCommitter, must provide the same constructor so that Tez can create an instance of
   * the class at runtime.
   *
   * @param committerContext committer context which can be used to access the payload, vertex
   *                         properties, etc
   */
  public OutputCommitter(OutputCommitterContext committerContext) {
    this.committerContext = committerContext;
  }

  /**
   * Setup up the Output committer.
   *
   * @throws java.lang.Exception
   */
  public abstract void initialize() throws Exception;

  /**
   * For the framework to setup the output during initialization. This is
   * called from the application master process for the vertex. This will be
   * called multiple times, once per dag attempt.
   *
   * @throws java.lang.Exception if setup fails
   */
  public abstract void setupOutput() throws Exception;

  /**
   * For committing the output after successful completion of tasks that write
   * the output. Note that this is invoked for the outputs of vertices whose
   * tasks have successfully completed. This is called from the application
   * master process. Based on user configuration, commit is called at the end of
   * the DAG execution for all outputs or immediately upon completion of all the
   * tasks that produced the output. This is guaranteed to only be called once.
   * 
   * @throws java.lang.Exception
   */
  public abstract void commitOutput() throws Exception;

  /**
   * For aborting an output. Note that this is invoked for vertices with a final
   * non-successful state. This is also called to abort a previously committed
   * output in the case of a post-commit failure. This is called from the
   * application master process. This may be called multiple times.
   * 
   * @param finalState
   *          final run-state of the vertex
   * @throws java.lang.Exception
   */
  public abstract void abortOutput(VertexStatus.State finalState)
    throws Exception;

  /**
   * Whether the OutputCommitter supports recovery of output from a Task
   * that completed in a previous DAG attempt
   * @return True if recovery supported
   */
  public boolean isTaskRecoverySupported() {
    return true;
  }

  /**
   * Recover task output from a previous DAG attempt
   * @param taskIndex Index of task to be recovered
   * @param previousDAGAttempt Previous DAG Attempt Number
   * @throws java.lang.Exception
   */
  public void recoverTask(int taskIndex, int previousDAGAttempt)  throws Exception {
  }

  /**
   * Return ahe {@link org.apache.tez.runtime.api.OutputCommitterContext} for this specific instance of
   * the Committer.
   *
   * @return the {@link org.apache.tez.runtime.api.OutputCommitterContext} for the input
   */
  public final OutputCommitterContext getContext() {
    return this.committerContext;
  }

}
