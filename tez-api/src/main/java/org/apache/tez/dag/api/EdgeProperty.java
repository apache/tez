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
package org.apache.tez.dag.api;

import org.apache.hadoop.classification.InterfaceAudience.Private;

import com.google.common.base.Preconditions;

public class EdgeProperty {
  
  /**
   * Defines the manner of data movement between source and destination tasks.
   * Determines which destination tasks have access to data produced on this
   * edge by a source task. A destination task may choose to read any portion of
   * the data available to it.
   */
  public enum DataMovementType {
    /**
     * Output on this edge produced by the i-th source task is available to the 
     * i-th destination task.
     */
    ONE_TO_ONE,
    /**
     * Output on this edge produced by any source task is available to all
     * destination tasks.
     */
    BROADCAST,
    /**
     * The i-th output on this edge produced by all source tasks is available to
     * the same destination task. Source tasks scatter their outputs and they
     * are gathered by designated destination tasks.
     */
    SCATTER_GATHER,
    
    /**
     * Custom routing defined by the user.
     */
    CUSTOM
  }
  
  /**
   * Determines the lifetime of the data produced on this edge by a source task.
   */
  public enum DataSourceType {
    /**
     * Data produced by the source is persisted and available even when the
     * task is not running. The data may become unavailable and may cause the 
     * source task to be re-executed.
     */
    PERSISTED,
    /**
     * Source data is stored reliably and will always be available
     */
    PERSISTED_RELIABLE,
    /**
     * Data produced by the source task is available only while the source task
     * is running. This requires the destination task to run concurrently with 
     * the source task.
     */
    EPHEMERAL
  }
  
  /**
   * Determines when the destination task is eligible to run, once the source  
   * task is eligible to run.
   */
  public enum SchedulingType {
    /**
     * Destination task is eligible to run after one or more of its source tasks 
     * have started or completed.
     */
    SEQUENTIAL,
    /**
     * Destination task must run concurrently with the source task
     */
    CONCURRENT
  }
  
  final DataMovementType dataMovementType;
  final DataSourceType dataSourceType;
  final SchedulingType schedulingType;
  final InputDescriptor inputDescriptor;
  final OutputDescriptor outputDescriptor;
  final EdgeManagerPluginDescriptor edgeManagerDescriptor;
  
  /**
   * @param dataMovementType
   * @param dataSourceType
   * @param schedulingType
   * @param edgeSource
   *          The {@link OutputDescriptor} that generates data on the edge.
   * @param edgeDestination
   *          The {@link InputDescriptor} which will consume data from the edge.
   */
  public EdgeProperty(DataMovementType dataMovementType, 
                       DataSourceType dataSourceType,
                       SchedulingType schedulingType,
                       OutputDescriptor edgeSource,
                       InputDescriptor edgeDestination) {
    Preconditions.checkArgument(dataMovementType != DataMovementType.CUSTOM,
        DataMovementType.CUSTOM + " cannot be used with this constructor");
    this.dataMovementType = dataMovementType;
    this.dataSourceType = dataSourceType;
    this.schedulingType = schedulingType;
    this.outputDescriptor = edgeSource;
    this.inputDescriptor = edgeDestination;
    this.edgeManagerDescriptor = null;
  }
  
  /**
   * Setup an Edge which uses a custom EdgeManager
   * 
   * @param edgeManagerDescriptor
   *          the EdgeManager specifications. This can be null if the edge
   *          manager will be setup at runtime
   * @param dataSourceType
   * @param schedulingType
   * @param edgeSource
   *          The {@link OutputDescriptor} that generates data on the edge.
   * @param edgeDestination
   *          The {@link InputDescriptor} which will consume data from the edge.
   */
  public EdgeProperty(EdgeManagerPluginDescriptor edgeManagerDescriptor,
                      DataSourceType dataSourceType,
                      SchedulingType schedulingType,
                      OutputDescriptor edgeSource,
                      InputDescriptor edgeDestination) {
    this.dataMovementType = DataMovementType.CUSTOM;
    this.edgeManagerDescriptor = edgeManagerDescriptor;
    this.dataSourceType = dataSourceType;
    this.schedulingType = schedulingType;
    this.inputDescriptor = edgeDestination;
    this.outputDescriptor = edgeSource;
  }
  
  public DataMovementType getDataMovementType() {
    return dataMovementType;
  }
  
  public DataSourceType getDataSourceType() {
    return dataSourceType;
  }
  
  public SchedulingType getSchedulingType() {
    return schedulingType;
  }
  
  /**
   * @return the {@link InputDescriptor} which will consume data from the edge.
   */
  public InputDescriptor getEdgeDestination() {
    return inputDescriptor;
  }
  
  /**
   * @return the {@link OutputDescriptor} which produces data on the edge.
   */
  public OutputDescriptor getEdgeSource() {
    return outputDescriptor;
  }
  
  /**
   * Returns the Edge Manager specifications for this edge.  
   * @return @link {@link EdgeManagerPluginDescriptor} if a custom edge was setup, null otherwise.
   */
  @Private
  public EdgeManagerPluginDescriptor getEdgeManagerDescriptor() {
    return edgeManagerDescriptor;
  }
  
  @Override
  public String toString() {
    return "{ " + dataMovementType + " : " + inputDescriptor.getClassName()
        + " >> " + dataSourceType + " >> " + outputDescriptor.getClassName()
        + " >> " + (edgeManagerDescriptor == null ? "NullEdgeManager" : edgeManagerDescriptor.getClassName())
        + " }";
  }
  
}
