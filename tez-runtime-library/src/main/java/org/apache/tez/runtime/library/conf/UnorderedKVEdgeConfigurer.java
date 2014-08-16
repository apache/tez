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

import javax.annotation.Nullable;

import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.library.input.UnorderedKVInput;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;

/**
 * Configure payloads for the UnorderedKVOutput and UnorderedKVInput pair </p>
 *
 * Values will be picked up from tez-site if not specified, otherwise defaults from
 * {@link org.apache.tez.runtime.library.api.TezRuntimeConfiguration} will be used.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class UnorderedKVEdgeConfigurer extends HadoopKeyValuesBasedBaseEdgeConfigurer {
  private final UnorderedKVOutputConfigurer outputConf;
  private final UnorderedKVInputConfigurer inputConf;

  private UnorderedKVEdgeConfigurer(
      UnorderedKVOutputConfigurer outputConfiguration,
      UnorderedKVInputConfigurer inputConfiguration) {
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
  public UserPayload getOutputPayload() {
    return outputConf.toUserPayload();
  }

  @Override
  public String getOutputClassName() {
    return UnorderedKVOutput.class.getName();
  }

  @Override
  public UserPayload getInputPayload() {
    return inputConf.toUserPayload();
  }

  @Override
  public String getInputClassName() {
    return UnorderedKVInput.class.getName();
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

  /**
   * This is a convenience method for creating an Edge descriptor based on the specified
   * EdgeManagerDescriptor.
   *
   * @param edgeManagerDescriptor the custom edge specification
   * @return an {@link org.apache.tez.dag.api.EdgeProperty} instance
   */
  public EdgeProperty createDefaultCustomEdgeProperty(EdgeManagerPluginDescriptor edgeManagerDescriptor) {
    Preconditions.checkNotNull(edgeManagerDescriptor, "EdgeManagerDescriptor cannot be null");
    EdgeProperty edgeProperty =
        new EdgeProperty(edgeManagerDescriptor, EdgeProperty.DataSourceType.PERSISTED,
            EdgeProperty.SchedulingType.SEQUENTIAL,
            new OutputDescriptor(getOutputClassName()).setUserPayload(getOutputPayload()),
            new InputDescriptor(getInputClassName()).setUserPayload(getInputPayload()));
    return edgeProperty;
  }

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class Builder extends HadoopKeyValuesBasedBaseEdgeConfigurer.Builder<Builder> {

    private final UnorderedKVOutputConfigurer.Builder outputBuilder =
        new UnorderedKVOutputConfigurer.Builder();
    private final UnorderedKVOutputConfigurer.SpecificBuilder<UnorderedKVEdgeConfigurer.Builder>
        specificOutputBuilder =
        new UnorderedKVOutputConfigurer.SpecificBuilder<UnorderedKVEdgeConfigurer.Builder>(
            this, outputBuilder);

    private final UnorderedKVInputConfigurer.Builder inputBuilder =
        new UnorderedKVInputConfigurer.Builder();
    private final UnorderedKVInputConfigurer.SpecificBuilder<UnorderedKVEdgeConfigurer.Builder>
        specificInputBuilder =
        new UnorderedKVInputConfigurer.SpecificBuilder<UnorderedKVEdgeConfigurer.Builder>(
            this, inputBuilder);

    @InterfaceAudience.Private
    Builder(String keyClassName, String valueClassName) {
      outputBuilder.setKeyClassName(keyClassName);
      outputBuilder.setValueClassName(valueClassName);
      inputBuilder.setKeyClassName(keyClassName);
      inputBuilder.setValueClassName(valueClassName);
    }

    @Override
    public Builder setCompression(boolean enabled, @Nullable String compressionCodec,
                                  @Nullable Map<String, String> codecConf) {
      outputBuilder.setCompression(enabled, compressionCodec, codecConf);
      inputBuilder.setCompression(enabled, compressionCodec, codecConf);
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
     * Set serialization class responsible for providing serializer/deserializer for key/value and
     * the corresponding comparator class to be used as key comparator.
     *
     * @param serializationClassName
     * @param serializerConf         the serializer configuration. This can be null, and is a
     *                               {@link java.util.Map} of key-value pairs. The keys should be limited
     *                               to the ones required by the comparator.
     * @return
     */
    public Builder setKeySerializationClass(String serializationClassName,
                                            @Nullable Map<String, String> serializerConf) {
      outputBuilder.setKeySerializationClass(serializationClassName, serializerConf);
      inputBuilder.setKeySerializationClass(serializationClassName, serializerConf);
      return this;
    }

    /**
     * Set serialization class responsible for providing serializer/deserializer for values.
     *
     * @param serializationClassName
     * @param serializerConf         the serializer configuration. This can be null, and is a
     *                               {@link java.util.Map} of key-value pairs. The keys should be limited
     *                               to the ones required by the comparator.
     * @return
     */
    public Builder setValueSerializationClass(String serializationClassName,
                                              @Nullable Map<String, String> serializerConf) {
      outputBuilder.setValueSerializationClass(serializationClassName, serializerConf);
      inputBuilder.setValueSerializationClass(serializationClassName, serializerConf);
      return this;
    }

    /**
     * Configure the specific output
     * @return a builder to configure the output
     */
    public UnorderedKVOutputConfigurer.SpecificBuilder<Builder> configureOutput() {
      return specificOutputBuilder;
    }

    /**
     * Configure the specific input
     * @return a builder to configure the input
     */
    public UnorderedKVInputConfigurer.SpecificBuilder<Builder> configureInput() {
      return specificInputBuilder;
    }

    /**
     * Build and return an instance of the configuration
     * @return an instance of the acatual configuration
     */
    public UnorderedKVEdgeConfigurer build() {
      return new UnorderedKVEdgeConfigurer(outputBuilder.build(), inputBuilder.build());
    }

  }
}
