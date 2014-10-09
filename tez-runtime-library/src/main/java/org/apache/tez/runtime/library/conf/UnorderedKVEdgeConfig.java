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
public class UnorderedKVEdgeConfig extends HadoopKeyValuesBasedBaseEdgeConfig {
  private final UnorderedKVOutputConfig outputConf;
  private final UnorderedKVInputConfig inputConf;

  private UnorderedKVEdgeConfig(
      UnorderedKVOutputConfig outputConfiguration,
      UnorderedKVInputConfig inputConfiguration) {
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
    EdgeProperty edgeProperty = EdgeProperty.create(EdgeProperty.DataMovementType.BROADCAST,
        EdgeProperty.DataSourceType.PERSISTED, EdgeProperty.SchedulingType.SEQUENTIAL,
        OutputDescriptor.create(
            getOutputClassName()).setUserPayload(getOutputPayload()),
        InputDescriptor.create(
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
    EdgeProperty edgeProperty = EdgeProperty.create(EdgeProperty.DataMovementType.ONE_TO_ONE,
        EdgeProperty.DataSourceType.PERSISTED, EdgeProperty.SchedulingType.SEQUENTIAL,
        OutputDescriptor.create(
            getOutputClassName()).setUserPayload(getOutputPayload()),
        InputDescriptor.create(
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
        EdgeProperty.create(edgeManagerDescriptor, EdgeProperty.DataSourceType.PERSISTED,
            EdgeProperty.SchedulingType.SEQUENTIAL,
            OutputDescriptor.create(getOutputClassName()).setUserPayload(getOutputPayload()),
            InputDescriptor.create(getInputClassName()).setUserPayload(getInputPayload()));
    return edgeProperty;
  }

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class Builder extends HadoopKeyValuesBasedBaseEdgeConfig.Builder<Builder> {

    private final UnorderedKVOutputConfig.Builder outputBuilder =
        new UnorderedKVOutputConfig.Builder();
    private final UnorderedKVOutputConfig.SpecificBuilder<UnorderedKVEdgeConfig.Builder>
        specificOutputBuilder =
        new UnorderedKVOutputConfig.SpecificBuilder<UnorderedKVEdgeConfig.Builder>(
            this, outputBuilder);

    private final UnorderedKVInputConfig.Builder inputBuilder =
        new UnorderedKVInputConfig.Builder();
    private final UnorderedKVInputConfig.SpecificBuilder<UnorderedKVEdgeConfig.Builder>
        specificInputBuilder =
        new UnorderedKVInputConfig.SpecificBuilder<UnorderedKVEdgeConfig.Builder>(
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
    /**
     * Edge config options are derived from client-side tez-site.xml (recommended).
     * Optionally invoke setFromConfiguration to override these config options via commandline arguments.
     *
     * @param conf
     * @return this object for further chained method calls
     */
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
     * @return this object for further chained method calls
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
     * @return this object for further chained method calls
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
    public UnorderedKVOutputConfig.SpecificBuilder<Builder> configureOutput() {
      return specificOutputBuilder;
    }

    /**
     * Configure the specific input
     * @return a builder to configure the input
     */
    public UnorderedKVInputConfig.SpecificBuilder<Builder> configureInput() {
      return specificInputBuilder;
    }

    /**
     * Build and return an instance of the configuration
     * @return an instance of the acatual configuration
     */
    public UnorderedKVEdgeConfig build() {
      return new UnorderedKVEdgeConfig(outputBuilder.build(), inputBuilder.build());
    }

  }
}
