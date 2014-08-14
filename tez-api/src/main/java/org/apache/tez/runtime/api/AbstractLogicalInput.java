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
 * An abstract class which should be the base class for all implementations of LogicalInput.
 *
 * This class implements the framework facing as well as user facing methods which need to be
 * implemented by all LogicalInputs.
 *
 * This includes default implementations of a new method for convenience.
 *
 * <code>Input</code> classes must provide a 2 argument public constructor for Tez to create the
 * Input. The parameters to this constructor are 1) an instance of
 * {@link org.apache.tez.runtime.api.InputContext} and 2) an integer which is used to
 * setup the number of physical inputs that the logical input will see.
 * Tez will take care of initializing and closing the Input after a {@link Processor} completes. </p>
 * <p/>
 *
 */
@Public
public abstract class AbstractLogicalInput implements LogicalInput, LogicalInputFrameworkInterface {

  private final int numPhysicalInputs;
  private final InputContext inputContext;

  /**
   * Constructor an instance of the LogicalInput. Classes extending this one to create a
   * LogicalInput, must provide the same constructor so that Tez can create an instance of the
   * class at runtime.
   *
   * @param inputContext      the {@link org.apache.tez.runtime.api.InputContext} which provides
   *                          the Input with context information within the running task.
   * @param numPhysicalInputs the number of physical inputs that the logical input will
   *                          receive. This is typically determined by Edge Routing.
   */
  public AbstractLogicalInput(InputContext inputContext, int numPhysicalInputs) {
    this.inputContext = inputContext;
    this.numPhysicalInputs = numPhysicalInputs;
  }

  @Override
  public abstract List<Event> initialize() throws Exception;

  /**
   * Get the number of physical inputs that this LogicalInput will receive. This is
   * typically determined by Edge routing, and number of upstream tasks
   *
   * @return the number of physical inputs
   */
  public final int getNumPhysicalInputs() {
    return numPhysicalInputs;
  }

  /**
   * Return ahe {@link org.apache.tez.runtime.api.InputContext} for this specific instance of
   * the LogicalInput
   *
   * @return the {@link org.apache.tez.runtime.api.InputContext} for the input
   */
  public final InputContext getContext() {
    return inputContext;
  }
}
