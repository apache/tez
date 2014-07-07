/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.tez.runtime.library.conf;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.runtime.library.input.ShuffledUnorderedKVInput;
import org.apache.tez.runtime.library.output.OnFileUnorderedKVOutput;

/**
 * Configure payloads for the OnFileUnorderedKVOutput and ShuffledUnorderedKVInput pair
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class UnorderedUnpartitionedKVEdgeConfiguration extends HadoopKeyValuesBasedBaseConf {
  private final OnFileUnorderedKVOutputConfiguration outputConf;
  private final ShuffledUnorderedKVInputConfiguration inputConf;

  private UnorderedUnpartitionedKVEdgeConfiguration(
      OnFileUnorderedKVOutputConfiguration outputConfiguration,
      ShuffledUnorderedKVInputConfiguration inputConfiguration) {
    this.outputConf = outputConfiguration;
    this.inputConf = inputConfiguration;

  }

  /**
   * Create a builder to configure the relevant Input and Output
   * @param keyClassName the key class name
   * @param valueClassName the value class name
   * @return a builder to configure the edge
   */
  public static Builder newBuilder(String keyClassName, String valueClassName) {
    return new Builder(keyClassName, valueClassName);
  }

  @Override
  public byte[] getOutputPayload() {
    return outputConf.toByteArray();
  }

  @Override
  public String getOutputClassName() {
    return OnFileUnorderedKVOutput.class.getName();
  }

  @Override
  public byte[] getInputPayload() {
    return inputConf.toByteArray();
  }

  @Override
  public String getInputClassName() {
    return ShuffledUnorderedKVInput.class.getName();
  }

  /**
   * This is a convenience method for the typical usage of this edge, and creates an instance of
   * {@link org.apache.tez.dag.api.EdgeProperty} which is likely to be used. </p>
   * If custom edge properties are required, the methods to get the relevant payloads should be
   * used. </p>
   * * In this case - DataMovementType.BROADCAST, EdgeProperty.DataSourceType.PERSISTED,
   * EdgeProperty.SchedulingType.SEQUENTIAL
   *
   * @return an {@link org.apache.tez.dag.api.EdgeProperty} instance
   */
  public EdgeProperty createDefaultBroadcastEdgeProperty() {
    EdgeProperty edgeProperty = new EdgeProperty(EdgeProperty.DataMovementType.BROADCAST,
        EdgeProperty.DataSourceType.PERSISTED, EdgeProperty.SchedulingType.SEQUENTIAL,
        new OutputDescriptor(
            getOutputClassName()).setUserPayload(getOutputPayload()),
        new InputDescriptor(
            getInputClassName()).setUserPayload(getInputPayload()));
    return edgeProperty;
  }

  /**
   * This is a convenience method for the typical usage of this edge, and creates an instance of
   * {@link org.apache.tez.dag.api.EdgeProperty} which is likely to be used. </p>
   * If custom edge properties are required, the methods to get the relevant payloads should be
   * used. </p>
   * * In this case - DataMovementType.ONE_TO_ONE, EdgeProperty.DataSourceType.PERSISTED,
   * EdgeProperty.SchedulingType.SEQUENTIAL
   *
   * @return an {@link org.apache.tez.dag.api.EdgeProperty} instance
   */
  public EdgeProperty createDefaultOneToOneEdgeProperty() {
    EdgeProperty edgeProperty = new EdgeProperty(EdgeProperty.DataMovementType.ONE_TO_ONE,
        EdgeProperty.DataSourceType.PERSISTED, EdgeProperty.SchedulingType.SEQUENTIAL,
        new OutputDescriptor(
            getOutputClassName()).setUserPayload(getOutputPayload()),
        new InputDescriptor(
            getInputClassName()).setUserPayload(getInputPayload()));
    return edgeProperty;
  }

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class Builder extends HadoopKeyValuesBasedBaseConf.Builder<Builder> {

    private final OnFileUnorderedKVOutputConfiguration.Builder outputBuilder =
        new OnFileUnorderedKVOutputConfiguration.Builder();
    private final OnFileUnorderedKVOutputConfiguration.SpecificBuilder<UnorderedUnpartitionedKVEdgeConfiguration.Builder>
        specificOutputBuilder =
        new OnFileUnorderedKVOutputConfiguration.SpecificBuilder<UnorderedUnpartitionedKVEdgeConfiguration.Builder>(
            this, outputBuilder);

    private final ShuffledUnorderedKVInputConfiguration.Builder inputBuilder =
        new ShuffledUnorderedKVInputConfiguration.Builder();
    private final ShuffledUnorderedKVInputConfiguration.SpecificBuilder<UnorderedUnpartitionedKVEdgeConfiguration.Builder>
        specificInputBuilder =
        new ShuffledUnorderedKVInputConfiguration.SpecificBuilder<UnorderedUnpartitionedKVEdgeConfiguration.Builder>(
            this, inputBuilder);

    @InterfaceAudience.Private
    Builder(String keyClassName, String valueClassName) {
      outputBuilder.setKeyClassName(keyClassName);
      outputBuilder.setValueClassName(valueClassName);
      inputBuilder.setKeyClassName(keyClassName);
      inputBuilder.setValueClassName(valueClassName);
    }

    @Override
    public Builder enableCompression(String compressionCodec) {
      outputBuilder.enableCompression(compressionCodec);
      inputBuilder.enableCompression(compressionCodec);
      return this;
    }

    @Override
    public Builder setAdditionalConfiguration(String key, String value) {
      outputBuilder.setAdditionalConfiguration(key, value);
      inputBuilder.setAdditionalConfiguration(key, value);
      return this;
    }

    @Override
    public Builder setAdditionalConfiguration(Map<String, String> confMap) {
      outputBuilder.setAdditionalConfiguration(confMap);
      inputBuilder.setAdditionalConfiguration(confMap);
      return this;
    }

    @Override
    public Builder setFromConfiguration(Configuration conf) {
      outputBuilder.setFromConfiguration(conf);
      inputBuilder.setFromConfiguration(conf);
      return this;
    }

    /**
     * Configure the specific output
     * @return a builder to configure the output
     */
    public OnFileUnorderedKVOutputConfiguration.SpecificBuilder<Builder> configureOutput() {
      return specificOutputBuilder;
    }

    /**
     * Configure the specific input
     * @return a builder to configure the input
     */
    public ShuffledUnorderedKVInputConfiguration.SpecificBuilder<Builder> configureInput() {
      return specificInputBuilder;
    }

    /**
     * Build and return an instance of the configuration
     * @return an instance of the acatual configuration
     */
    public UnorderedUnpartitionedKVEdgeConfiguration build() {
      return new UnorderedUnpartitionedKVEdgeConfiguration(outputBuilder.build(), inputBuilder.build());
    }

  }
}
