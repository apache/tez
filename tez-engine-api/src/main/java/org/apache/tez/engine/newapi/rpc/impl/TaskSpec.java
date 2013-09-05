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

package org.apache.tez.engine.newapi.rpc.impl;

import java.util.List;

import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.records.TezTaskAttemptID;

/**
 * Serializable Task information that is sent across the Umbilical from the
 * Tez AM to the Tez Container's JVM.
 */
public interface TaskSpec {

  /**
   * Get the vertex name for the current task.
   * @return the vertex name set by the user.
   */
  public String getVertexName();
  
  /**
   * Get the task attempt id for the current task.
   * @return the {@link TaskAttemptID}
   */
  public TezTaskAttemptID getTaskAttemptID();
  
  /**
   * Get the owner of the job.
   * @return the owner.
   */
  public String getUser();
  /**
   * The Processor definition for the given Task
   * @return {@link ProcessorDescriptor}
   */
  public ProcessorDescriptor getProcessorDescriptor();

  /**
   * The List of Inputs for this Task.
   * @return {@link Input}
   */
  public List<InputSpec> getInputs();

  /**
   * The List of Outputs for this Task.
   * @return {@link Output}
   */
  public List<OutputSpec> getOutputs();
}
