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

import java.io.IOException;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.input.OrderedGroupedKVInput;
import org.apache.tez.runtime.library.input.OrderedGroupedInputLegacy;

@InterfaceAudience.Public
@InterfaceStability.Evolving
/**
 * Configure {@link org.apache.tez.runtime.library.input.OrderedGroupedKVInput} </p>
 *
 * Values will be picked up from tez-site if not specified, otherwise defaults from
 * {@link org.apache.tez.runtime.library.api.TezRuntimeConfiguration} will be used.
 */
public class OrderedGroupedKVInputConfig {

  /**
   * Configure parameters which are specific to the Input.
   */
  @InterfaceAudience.Private
  public static interface SpecificConfigBuilder<T> extends BaseConfigBuilder<T> {

    /**
     * Specifies whether the legacy version of this input should be used.
     * @return instance of the current builder
     */
    public T useLegacyInput();
    /**
     * Sets the buffer fraction, as a fraction of container size, to be used while fetching remote
     * data.
     *
     * @param shuffleBufferFraction fraction of container size
     * @return instance of the current builder
     */
    public T setShuffleBufferFraction(float shuffleBufferFraction);

    /**
     * Sets the buffer fraction, as a fraction of container size, to be used after the fetch and
     * merge are complete. This buffer is used to cache merged data and avoids writing it out to
     * disk.
     *
     * @param postMergeBufferFraction fraction of container size
     * @return instance of the current builder
     */
    public T setPostMergeBufferFraction(float postMergeBufferFraction);

    /**
     * Sets a size limit on the maximum segment size to be shuffled to disk. This is a fraction of
     * the shuffle buffer.
     *
     * @param maxSingleSegmentFraction fraction of memory determined by ShuffleBufferFraction
     * @return instance of the current builder
     */
    public T setMaxSingleMemorySegmentFraction(float maxSingleSegmentFraction);

    /**
     * Enable the memory to memory merger
     *
     * @param enable whether to enable the memory to memory merger
     * @return instance of the current builder
     */
    public T setMemToMemMerger(boolean enable); // Not super useful until additional params are used.

    /**
     * Configure the point at which in memory segments will be merged. This is specified as a
     * fraction of the shuffle buffer.
     *
     * @param mergeFraction fraction of memory determined by ShuffleBufferFraction, which when
     *                      filled, will
     *                      trigger a merge
     * @return instance of the current builder
     */
    public T setMergeFraction(float mergeFraction);

    /**
     * Configure the combiner class
     *
     * @param combinerClassName the combiner class name
     * @return instance of the current builder
     */
    public T setCombiner(String combinerClassName);

    /**
     * Configure the combiner class and it's associated configuration (specified as key-value
     * pairs). This method should only be used if the combiner requires some specific configuration.
     * {@link #setCombiner(String)} is the preferred method for setting a combiner.
     *
     * @param combinerClassName the combiner class name
     * @param combinerConf      the combiner configuration. This can be null, and otherwise
     *                          is a {@link java.util.Map} of key-value pairs. The keys should
     *                          be limited to the ones required by the combiner.
     * @return instance of the current builder
     */
    public T setCombiner(String combinerClassName, @Nullable Map<String, String> combinerConf);

  }

  @SuppressWarnings("rawtypes")
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class SpecificBuilder<E extends HadoopKeyValuesBasedBaseEdgeConfig.Builder> implements
      SpecificConfigBuilder<SpecificBuilder> {

    private final E edgeBuilder;
    private final OrderedGroupedKVInputConfig.Builder builder;


    @InterfaceAudience.Private
    SpecificBuilder(E edgeBuilder, OrderedGroupedKVInputConfig.Builder builder) {
      this.edgeBuilder = edgeBuilder;
      this.builder = builder;
    }

    public SpecificBuilder<E> useLegacyInput() {
      builder.useLegacyInput();
      return this;
    }

    @Override
    public SpecificBuilder<E> setShuffleBufferFraction(float shuffleBufferFraction) {
      builder.setShuffleBufferFraction(shuffleBufferFraction);
      return this;
    }

    @Override
    public SpecificBuilder<E> setPostMergeBufferFraction(float postMergeBufferFraction) {
      builder.setPostMergeBufferFraction(postMergeBufferFraction);
      return this;
    }

    @Override
    public SpecificBuilder<E> setMaxSingleMemorySegmentFraction(float maxSingleSegmentFraction) {
      builder.setMaxSingleMemorySegmentFraction(maxSingleSegmentFraction);
      return this;
    }

    @Override
    public SpecificBuilder<E> setMemToMemMerger(boolean enable) {
      builder.setMemToMemMerger(enable);
      return this;
    }

    @Override
    public SpecificBuilder<E> setMergeFraction(float mergeFraction) {
      builder.setMergeFraction(mergeFraction);
      return this;
    }

    @Override
    public SpecificBuilder<E> setCombiner(String combinerClassName) {
      return setCombiner(combinerClassName, null);
    }

    @Override
    public SpecificBuilder<E> setCombiner(String combinerClassName, Map<String, String> combinerConf) {
      builder.setCombiner(combinerClassName, combinerConf);
      return this;
    }


    @Override
    public SpecificBuilder<E> setAdditionalConfiguration(String key, String value) {
      builder.setAdditionalConfiguration(key, value);
      return this;
    }

    @Override
    public SpecificBuilder<E> setAdditionalConfiguration(Map<String, String> confMap) {
      builder.setAdditionalConfiguration(confMap);
      return this;
    }

    @Override
    public SpecificBuilder<E> setFromConfiguration(Configuration conf) {
      builder.setFromConfiguration(conf);
      return this;
    }

    public E done() {
      return edgeBuilder;
    }

  }

  @InterfaceAudience.Private
  @VisibleForTesting
  Configuration conf;

  private String inputClassName;

  @InterfaceAudience.Private
  @VisibleForTesting
  OrderedGroupedKVInputConfig() {
  }

  private OrderedGroupedKVInputConfig(Configuration conf, boolean useLegacyInput) {
    this.conf = conf;
    if (useLegacyInput) {
      inputClassName = OrderedGroupedInputLegacy.class.getName();
    } else {
      inputClassName = OrderedGroupedKVInput.class.getName();
    }
  }

  /**
   * Get a UserPayload representation of the Configuration
   * @return a {@link org.apache.tez.dag.api.UserPayload} instance
   */
  public UserPayload toUserPayload() {
    try {
      return TezUtils.createUserPayloadFromConf(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @InterfaceAudience.Private
  public void fromUserPayload(UserPayload payload) {
    try {
      this.conf = TezUtils.createConfFromUserPayload(payload);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @InterfaceAudience.Private
  String toHistoryText() {
    if (conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT,
        TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT_DEFAULT)) {
      return TezUtils.convertToHistoryText(conf);
    }
    return null;
  }

  public String getInputClassName() {
    return inputClassName;
  }

  public static Builder newBuilder(String keyClass, String valueClass) {
    return new Builder(keyClass, valueClass);
  }

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class Builder implements SpecificConfigBuilder<Builder> {

    private final Configuration conf = new Configuration(false);
    private boolean useLegacyInput = false;

    /**
     * Create a configuration builder for {@link org.apache.tez.runtime.library.input.OrderedGroupedKVInput}
     *
     * @param keyClassName         the key class name
     * @param valueClassName       the value class name
     */
    @InterfaceAudience.Private
    Builder(String keyClassName, String valueClassName) {
      this();
      Preconditions.checkNotNull(keyClassName, "Key class name cannot be null");
      Preconditions.checkNotNull(valueClassName, "Value class name cannot be null");
      setKeyClassName(keyClassName);
      setValueClassName(valueClassName);
    }

    @InterfaceAudience.Private
    Builder() {
      Map<String, String> tezDefaults = ConfigUtils
          .extractConfigurationMap(TezRuntimeConfiguration.getTezRuntimeConfigDefaults(),
              OrderedGroupedKVInput.getConfigurationKeySet());
      ConfigUtils.addConfigMapToConfiguration(this.conf, tezDefaults);
      ConfigUtils.addConfigMapToConfiguration(this.conf, TezRuntimeConfiguration.getOtherConfigDefaults());
    }

    @InterfaceAudience.Private
    Builder setKeyClassName(String keyClassName) {
      Preconditions.checkNotNull(keyClassName, "Key class name cannot be null");
      this.conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, keyClassName);
      return this;
    }

    @InterfaceAudience.Private
    Builder setValueClassName(String valueClassName) {
      Preconditions.checkNotNull(valueClassName, "Value class name cannot be null");
      this.conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, valueClassName);
      return this;
    }

    public Builder useLegacyInput() {
      this.useLegacyInput = true;
      return this;
    }

    @Override
    public Builder setShuffleBufferFraction(float shuffleBufferFraction) {
      this.conf
          .setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, shuffleBufferFraction);
      return this;
    }

    @Override
    public Builder setPostMergeBufferFraction(float postMergeBufferFraction) {
      this.conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT, postMergeBufferFraction);
      return this;
    }

    @Override
    public Builder setMaxSingleMemorySegmentFraction(float maxSingleSegmentFraction) {
      this.conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT,
          maxSingleSegmentFraction);
      return this;
    }

    @Override
    public Builder setMemToMemMerger(boolean enable) {
      this.conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM, enable);
      return this;
    }

    @Override
    public Builder setMergeFraction(float mergeFraction) {
      this.conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, mergeFraction);
      return this;
    }

    public Builder setCombiner(String combinerClassName) {
      return setCombiner(combinerClassName, null);
    }

    @Override
    public Builder setCombiner(String combinerClassName, Map<String, String> combinerConf) {
      this.conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS, combinerClassName);
      if (combinerConf != null) {
        // Merging the confs for now. Change to be specific in the future.
        ConfigUtils.mergeConfsWithExclusions(this.conf, combinerConf,
            TezRuntimeConfiguration.getRuntimeConfigKeySet());
      }
      return this;
    }

    /**
     * Set the key comparator class
     *
     * @param comparatorClassName the key comparator class name
     * @return instance of the current builder
     */
    public Builder setKeyComparatorClass(String comparatorClassName) {
      return this.setKeyComparatorClass(comparatorClassName, null);
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
      Preconditions.checkNotNull(comparatorClassName, "Comparator class name cannot be null");
      this.conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS,
          comparatorClassName);
      if (comparatorConf != null) {
        // Merging the confs for now. Change to be specific in the future.
        ConfigUtils.mergeConfsWithExclusions(this.conf, comparatorConf,
            TezRuntimeConfiguration.getRuntimeConfigKeySet());
      }
      return this;
    }
    @Override
    public Builder setAdditionalConfiguration(String key, String value) {
      Preconditions.checkNotNull(key, "Key cannot be null");
      if (ConfigUtils.doesKeyQualify(key,
          Lists.newArrayList(OrderedGroupedKVInput.getConfigurationKeySet(),
              TezRuntimeConfiguration.getRuntimeAdditionalConfigKeySet()),
          TezRuntimeConfiguration.getAllowedPrefixes())) {
        if (value == null) {
          this.conf.unset(key);
        } else {
          this.conf.set(key, value);
        }
      }
      return this;
    }

    @Override
    public Builder setAdditionalConfiguration(Map<String, String> confMap) {
      Preconditions.checkNotNull(confMap, "ConfMap cannot be null");
      Map<String, String> map = ConfigUtils.extractConfigurationMap(confMap,
          Lists.newArrayList(OrderedGroupedKVInput.getConfigurationKeySet(),
              TezRuntimeConfiguration.getRuntimeAdditionalConfigKeySet()), TezRuntimeConfiguration.getAllowedPrefixes());
      ConfigUtils.addConfigMapToConfiguration(this.conf, map);
      return this;
    }

    @Override
    public Builder setFromConfiguration(Configuration conf) {
      // Maybe ensure this is the first call ? Otherwise this can end up overriding other parameters
      Preconditions.checkArgument(conf != null, "Configuration cannot be null");
      Map<String, String> map = ConfigUtils.extractConfigurationMap(conf,
          Lists.newArrayList(OrderedGroupedKVInput.getConfigurationKeySet(),
              TezRuntimeConfiguration.getRuntimeAdditionalConfigKeySet()), TezRuntimeConfiguration.getAllowedPrefixes());
      ConfigUtils.addConfigMapToConfiguration(this.conf, map);
      return this;
    }

    public Builder setCompression(boolean enabled, @Nullable String compressionCodec,
                                  @Nullable Map<String, String> codecConf) {
      this.conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, enabled);
      if (enabled && compressionCodec != null) {
        this.conf
            .set(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, compressionCodec);
      }
      if (codecConf != null) {
        // Merging the confs for now. Change to be specific in the future.
        ConfigUtils.mergeConfsWithExclusions(this.conf, codecConf,
            TezRuntimeConfiguration.getRuntimeConfigKeySet());
      }
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
      Preconditions.checkArgument(serializationClassName != null,
          "serializationClassName cannot be null");
      Preconditions.checkArgument(comparatorClassName != null,
          "comparator cannot be null");
      this.conf.set(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, serializationClassName + ","
          + conf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY));
      setKeyComparatorClass(comparatorClassName, null);
      if (serializerConf != null) {
        // Merging the confs for now. Change to be specific in the future.
        ConfigUtils.mergeConfsWithExclusions(this.conf, serializerConf,
            TezRuntimeConfiguration.getRuntimeConfigKeySet());
      }
      return this;
    }

    /**
     * Serialization class to be used for serializing values.
     *
     * @param serializationClassName
     * @param serializerConf         the serializer configuration. This can be null, and is a
     *                               {@link java.util.Map} of key-value pairs. The keys should be limited
     *                               to the ones required by the comparator.
     * @return this object for further chained method calls
     */
    public Builder setValueSerializationClass(String serializationClassName,
                                              @Nullable Map<String, String> serializerConf) {
      Preconditions.checkArgument(serializationClassName != null,
          "serializationClassName cannot be null");
      this.conf.set(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, serializationClassName + ","
          + conf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY));
      if (serializerConf != null) {
        // Merging the confs for now. Change to be specific in the future.
        ConfigUtils.mergeConfsWithExclusions(this.conf, serializerConf,
            TezRuntimeConfiguration.getRuntimeConfigKeySet());
      }
      return this;
    }

    /**
     * Create the actual configuration instance.
     *
     * @return an instance of the Configuration
     */
    public OrderedGroupedKVInputConfig build() {
      return new OrderedGroupedKVInputConfig(this.conf, this.useLegacyInput);
    }
  }

}
