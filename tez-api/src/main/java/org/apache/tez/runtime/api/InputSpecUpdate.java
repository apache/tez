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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;

import com.google.common.collect.Lists;

/**
 * Update Input specs for Inputs running in a task. Allows setting the number of physical
 * inputs for all work units if they have the same number of physical inputs, or individual
 * numPhysicalInputs for each work unit.
 * 
 */
@Unstable
@Public
public class InputSpecUpdate {

  private final boolean forAllWorkUnits;
  private final List<Integer> numPhysicalInputs;

  private final static InputSpecUpdate DEFAULT_SINGLE_PHYSICAL_INPUT_SPEC = createAllTaskInputSpecUpdate(1);
  
  /**
   * Create an update instance where all work units (typically represented by
   * {@link InputDataInformationEvent}) will have the same number of physical inputs.
   * 
   * @param numPhysicalInputs
   *          the number of physical inputs for all work units which will use the LogicalInput
   * @return a new InputSpecUpdate instance given the parameters
   */
  public static InputSpecUpdate createAllTaskInputSpecUpdate(int numPhysicalInputs) {
    return new InputSpecUpdate(numPhysicalInputs);
  }

  /**
   * Create an update instance where all work units (typically represented by
   * {@link InputDataInformationEvent}) will have the same number of physical inputs.
   * 
   * @param perWorkUnitNumPhysicalInputs
   *          A list containing one entry per work unit. The order in the list corresponds to task
   *          index or equivalently the order of {@link InputDataInformationEvent}s being sent.
   * @return a new InputSpecUpdate instance given the parameters
   */
  public static InputSpecUpdate createPerTaskInputSpecUpdate(
      List<Integer> perWorkUnitNumPhysicalInputs) {
    return new InputSpecUpdate(perWorkUnitNumPhysicalInputs);
  }
  
  public static InputSpecUpdate getDefaultSinglePhysicalInputSpecUpdate() {
    return DEFAULT_SINGLE_PHYSICAL_INPUT_SPEC;
  }

  private InputSpecUpdate(int numPhysicalInputs) {
    this.forAllWorkUnits = true;
    this.numPhysicalInputs = Lists.newArrayList(numPhysicalInputs);
  }

  private InputSpecUpdate(List<Integer> perWorkUnitNumPhysicalInputs) {
    this.forAllWorkUnits = false;
    this.numPhysicalInputs = Lists.newArrayList(perWorkUnitNumPhysicalInputs);
  }

  @Private
  public int getNumPhysicalInputsForWorkUnit(int index) {
    if (this.forAllWorkUnits) {
      return numPhysicalInputs.get(0);
    } else {
      return numPhysicalInputs.get(index);
    }
  }
  
  @Private
  /* Used for recovery serialization */
  public boolean isForAllWorkUnits() {
    return this.forAllWorkUnits;
  }
  
  @Private
  /* Used for recovery serialization */
  public List<Integer> getAllNumPhysicalInputs() {
    return numPhysicalInputs;
  }

  @Override
  public String toString() {
    return "forAllWorkUnits=" + forAllWorkUnits + ", update=" + numPhysicalInputs.toString();
  }
}
