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
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;

/**
 * Configure payloads for the OrderedPartitionedKVOutput and OrderedGroupedKVInput pair </p>
 *
 * Values will be picked up from tez-site if not specified, otherwise defaults from
 * {@link org.apache.tez.runtime.library.api.TezRuntimeConfiguration} will be used.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OrderedPartitionedKVEdgeConfig
    extends HadoopKeyValuesBasedBaseEdgeConfig {

  private final OrderedPartitionedKVOutputConfig outputConf;
  private final OrderedGroupedKVInputConfig inputConf;

  private OrderedPartitionedKVEdgeConfig(
      OrderedPartitionedKVOutputConfig outputConfiguration,
      OrderedGroupedKVInputConfig inputConfiguration) {
    this.outputConf = outputConfiguration;
    this.inputConf = inputConfiguration;
  }

  /**
   * Create a builder to configure the relevant Input and Output. </p> This method should only be
   * used when using a custom Partitioner which requires specific Configuration. {@link
   * #newBuilder(String, String, String)} is the preferred method to crate an instance of the
   * Builder
   *
   * @param keyClassName         the key class name
   * @param valueClassName       the value class name
   * @param partitionerClassName the partitioner class name
   * @param partitionerConf      the partitioner configuration. This can be null, and is a {@link
   *                             java.util.Map} of key-value pairs. The keys should be limited to
   *                             the ones required by the partitioner.
   * @return a builder to configure the edge
   */
  public static Builder newBuilder(String keyClassName, String valueClassName,
                                   String partitionerClassName,
                                   @Nullable Map<String, String> partitionerConf) {
    return new Builder(keyClassName, valueClassName, partitionerClassName, partitionerConf);
  }

  /**
   * Create a builder to configure the relevant Input and Output
   *
   * @param keyClassName         the key class name
   * @param valueClassName       the value class name
   * @param partitionerClassName the partitioner class name
   * @return a builder to configure the edge
   */
  public static Builder newBuilder(String keyClassName, String valueClassName,
                                   String partitionerClassName) {
    return newBuilder(keyClassName, valueClassName, partitionerClassName, null);
  }

  @Override
  public UserPayload getOutputPayload() {
    return outputConf.toUserPayload();
  }

  @Override
  public String getOutputClassName() {
    return OrderedPartitionedKVOutput.class.getName();
  }

  @Override
  public UserPayload getInputPayload() {
    return inputConf.toUserPayload();
  }

  @Override
  public String getInputClassName() {
    return inputConf.getInputClassName();
  }

  /**
   * This is a convenience method for the typical usage of this edge, and creates an instance of
   * {@link org.apache.tez.dag.api.EdgeProperty} which is likely to be used. </p>
   * * In this case - DataMovementType.SCATTER_GATHER, EdgeProperty.DataSourceType.PERSISTED,
   * EdgeProperty.SchedulingType.SEQUENTIAL
   *
   * @return an {@link org.apache.tez.dag.api.EdgeProperty} instance
   */
  public EdgeProperty createDefaultEdgeProperty() {
    EdgeProperty edgeProperty = EdgeProperty.create(EdgeProperty.DataMovementType.SCATTER_GATHER,
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

    private final OrderedPartitionedKVOutputConfig.Builder outputBuilder =
        new OrderedPartitionedKVOutputConfig.Builder();
    private final OrderedPartitionedKVOutputConfig.SpecificBuilder<OrderedPartitionedKVEdgeConfig.Builder>
        specificOutputBuilder =
        new OrderedPartitionedKVOutputConfig.SpecificBuilder<OrderedPartitionedKVEdgeConfig.Builder>(
            this, outputBuilder);

    private final OrderedGroupedKVInputConfig.Builder inputBuilder =
        new OrderedGroupedKVInputConfig.Builder();
    private final OrderedGroupedKVInputConfig.SpecificBuilder<OrderedPartitionedKVEdgeConfig.Builder>
        specificInputBuilder =
        new OrderedGroupedKVInputConfig.SpecificBuilder<OrderedPartitionedKVEdgeConfig.Builder>(this,
            inputBuilder);

    @InterfaceAudience.Private
    Builder(String keyClassName, String valueClassName, String partitionerClassName,
            Map<String, String> partitionerConf) {
      outputBuilder.setKeyClassName(keyClassName);
      outputBuilder.setValueClassName(valueClassName);
      outputBuilder.setPartitioner(partitionerClassName, partitionerConf);
      inputBuilder.setKeyClassName(keyClassName);
      inputBuilder.setValueClassName(valueClassName);
    }

    /**
     * Set the key comparator class
     *
     * @param comparatorClassName the key comparator class name
     * @return instance of the current builder
     */
    public Builder setKeyComparatorClass(String comparatorClassName) {
      return setKeyComparatorClass(comparatorClassName, null);
    }

    /**
     * Set the key comparator class and it's associated configuration. This method should only be
     * used if the comparator requires some specific configuration, which is typically not the
     * case. {@link #setKeyComparatorClass(String)} is the preferred method for setting a
     * comparator.
     *
     * @param comparatorClassName the key comparator class name
     * @param comparatorConf      the comparator configuration. This can be null, and is a {@link
     *                            java.util.Map} of key-value pairs. The keys should be limited to
     *                            the ones required by the comparator.
     * @return instance of the current builder
     */
    public Builder setKeyComparatorClass(String comparatorClassName,
                                         @Nullable Map<String, String> comparatorConf) {
      outputBuilder.setKeyComparatorClass(comparatorClassName, comparatorConf);
      inputBuilder.setKeyComparatorClass(comparatorClassName, comparatorConf);
      return this;
    }

    /**
     * Set serialization class and the relevant comparator to be used for sorting.
     * Providing custom serialization class could change the way, keys needs to be compared in
     * sorting. Providing invalid comparator here could create invalid results.
     *
     * @param serializationClassName
     * @param comparatorClassName
     * @param serializerConf         the serializer configuration. This can be null, and is a
     *                               {@link java.util.Map} of key-value pairs. The keys should be limited
     *                               to the ones required by the comparator.
     * @return this object for further chained method calls
     */
    public Builder setKeySerializationClass(String serializationClassName,
        String comparatorClassName, @Nullable Map<String, String> serializerConf) {
      outputBuilder.setKeySerializationClass(serializationClassName, comparatorClassName, serializerConf);
      inputBuilder.setKeySerializationClass(serializationClassName, comparatorClassName, serializerConf);
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
     * Configure the specific output
     * @return a builder to configure the output
     */
    public OrderedPartitionedKVOutputConfig.SpecificBuilder<Builder> configureOutput() {
      return specificOutputBuilder;
    }

    /**
     * Configure the specific input
     * @return a builder to configure the input
     */
    public OrderedGroupedKVInputConfig.SpecificBuilder<Builder> configureInput() {
      return specificInputBuilder;
    }

    /**
     * Build and return an instance of the configuration
     * @return an instance of the acatual configuration
     */
    public OrderedPartitionedKVEdgeConfig build() {
      return new OrderedPartitionedKVEdgeConfig(outputBuilder.build(), inputBuilder.build());
    }

  }
}
