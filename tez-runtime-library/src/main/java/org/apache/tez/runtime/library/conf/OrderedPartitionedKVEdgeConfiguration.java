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

import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.runtime.library.input.ShuffledMergedInput;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;

/**
 * Configure payloads for the OnFileSortedOutput and ShuffledMergedInput pair
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OrderedPartitionedKVEdgeConfiguration extends HadoopKeyValuesBasedBaseConf {

  private final OnFileSortedOutputConfiguration outputConf;
  private final ShuffledMergedInputConfiguration inputConf;

  private OrderedPartitionedKVEdgeConfiguration(
      OnFileSortedOutputConfiguration outputConfiguration,
      ShuffledMergedInputConfiguration inputConfiguration) {
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
    return OnFileSortedOutput.class.getName();
  }

  @Override
  public byte[] getInputPayload() {
    return inputConf.toByteArray();
  }

  @Override
  public String getInputClassName() {
    return ShuffledMergedInput.class.getName();
  }

  /**
   * This is a convenience method for the typical usage of this edge, and creates an instance of
   * {@link org.apache.tez.dag.api.EdgeProperty} which is likely to be used. </p>
   * If custom edge properties are required, the methods to get the relevant payloads should be
   * used. </p>
   * * In this case - DataMovementType.SCATTER_GATHER, EdgeProperty.DataSourceType.PERSISTED,
   * EdgeProperty.SchedulingType.SEQUENTIAL
   *
   * @return an {@link org.apache.tez.dag.api.EdgeProperty} instance
   */
  public EdgeProperty createDefaultEdgeProperty() {
    EdgeProperty edgeProperty = new EdgeProperty(EdgeProperty.DataMovementType.SCATTER_GATHER,
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

    private boolean outputConfigured = false;

    private final OnFileSortedOutputConfiguration.Builder outputBuilder =
        new OnFileSortedOutputConfiguration.Builder();
    private final OnFileSortedOutputConfiguration.SpecificBuilder<OrderedPartitionedKVEdgeConfiguration.Builder>
        specificOutputBuilder =
        new OnFileSortedOutputConfiguration.SpecificBuilder<OrderedPartitionedKVEdgeConfiguration.Builder>(
            this, outputBuilder);

    private final ShuffledMergedInputConfiguration.Builder inputBuilder =
        new ShuffledMergedInputConfiguration.Builder();
    private final ShuffledMergedInputConfiguration.SpecificBuilder<OrderedPartitionedKVEdgeConfiguration.Builder>
        specificInputBuilder =
        new ShuffledMergedInputConfiguration.SpecificBuilder<OrderedPartitionedKVEdgeConfiguration.Builder>(this,
            inputBuilder);

    @InterfaceAudience.Private
    Builder(String keyClassName, String valueClassName) {
      outputBuilder.setKeyClassName(keyClassName);
      outputBuilder.setValueClassName(valueClassName);
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
      outputBuilder.setKeyComparatorClass(comparatorClassName);
      inputBuilder.setKeyComparatorClass(comparatorClassName);
      return this;
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
     * @param partitionerClassName the partitioner class name
     * @param partitionerConf configuration for the partitioner. This can be null
     * @return a builder to configure the output
     */
    public OnFileSortedOutputConfiguration.SpecificBuilder<Builder> configureOutput(
        String partitionerClassName, Configuration partitionerConf) {
      outputConfigured = true;
      outputBuilder.setPartitioner(partitionerClassName, partitionerConf);
      return specificOutputBuilder;
    }

    /**
     * Configure the specific input
     * @return a builder to configure the input
     */
    public ShuffledMergedInputConfiguration.SpecificBuilder<Builder> configureInput() {
      return specificInputBuilder;
    }

    /**
     * Build and return an instance of the configuration
     * @return an instance of the acatual configuration
     */
    public OrderedPartitionedKVEdgeConfiguration build() {
      Preconditions.checkState(outputConfigured == true, "Output must be configured - partitioner required");
      return new OrderedPartitionedKVEdgeConfiguration(outputBuilder.build(), inputBuilder.build());
    }

  }

  public static void main(String[] args) {
    // Sample usage;
    OrderedPartitionedKVEdgeConfiguration
        conf = OrderedPartitionedKVEdgeConfiguration.newBuilder("keyClass", "valClass")
        .setKeyComparatorClass("comparatorClass")
        .enableCompression(null)
        .configureOutput("partitionerClassName", null)
        .setSortBufferSize(1024)
        .setSorterNumThreads(1)
        .done()
        .configureInput()
        .setCombiner("Combiner", null)
        .setShuffleBufferFraction(0.25f)
        .done()
        .build();

    conf.getInputPayload();
    conf.getOutputPayload();


  }
}
