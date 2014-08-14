/*
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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;

/**
 * An abstract class which should be the base class for all implementations of LogicalOutput.
 *
 * This class implements the framework facing as well as user facing methods which need to be
 * implemented by all LogicalOutputs.
 *
 * This includes default implementations of a new method for convenience.
 *
 */
@Public
public abstract class AbstractLogicalOutput implements LogicalOutput, LogicalOutputFrameworkInterface {

  private final int numPhysicalOutputs;
  private final OutputContext outputContext;

  /**
   * Constructor an instance of the LogicalOutput. Classes extending this one to create a
   * LogicalOutput, must provide the same constructor so that Tez can create an instance of the
   * class at runtime.
   *
   * @param outputContext      the {@link org.apache.tez.runtime.api.OutputContext} which
   *                           provides
   *                           the Output with context information within the running task.
   * @param numPhysicalOutputs the number of physical outputs that the logical output will
   *                           generate. This is typically determined by Edge Routing.
   */
  public AbstractLogicalOutput(OutputContext outputContext, int numPhysicalOutputs) {
    this.outputContext = outputContext;
    this.numPhysicalOutputs = numPhysicalOutputs;
  }

  public abstract List<Event> initialize() throws Exception;

  /**
   * Get the number of physical outputs that this LogicalOutput is expected to generate. This is
   * typically determined by Edge routing, and number of downstream tasks
   *
   * @return the number of physical outputs
   */
  public final int getNumPhysicalOutputs() {
    return numPhysicalOutputs;
  }

  /**
   * Return the {@link org.apache.tez.runtime.api.OutputContext} for this specific instance of
   * the LogicalOutput
   *
   * @return the {@link org.apache.tez.runtime.api.OutputContext} for the output
   */
  public final OutputContext getContext() {
    return outputContext;
  }
}
