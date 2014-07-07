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

import java.io.IOException;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.input.ShuffledMergedInput;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ShuffledMergedInputConfiguration {

  /**
   * Configure parameters which are specific to the Input.
   */
  @InterfaceAudience.Private
  public static interface SpecificConfigurer<T> extends BaseConfigurer<T> {

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
     * @return
     */
    public T enableMemToMemMerger(); // Not super useful until additional params are used.

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
     * Enable encrypted data transfer
     *
     * @return instance of the current builder
     */
    public T enableEncryptedTransfer();

    /**
     * Configure the combiner class
     *
     * @param combinerClassName the combiner class name
     * @param combinerConf configuration specific to the Combiner. This can be null
     * @return instance of the current builder
     */
    public T setCombiner(String combinerClassName, Configuration combinerConf);

  }

  @SuppressWarnings("rawtypes")
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class SpecificBuilder<E extends HadoopKeyValuesBasedBaseConf.Builder> implements
      SpecificConfigurer<SpecificBuilder> {

    private final E edgeBuilder;
    private final ShuffledMergedInputConfiguration.Builder builder;


    @InterfaceAudience.Private
    SpecificBuilder(E edgeBuilder, ShuffledMergedInputConfiguration.Builder builder) {
      this.edgeBuilder = edgeBuilder;
      this.builder = builder;
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
    public SpecificBuilder<E> enableMemToMemMerger() {
      builder.enableMemToMemMerger();
      return this;
    }

    @Override
    public SpecificBuilder<E> setMergeFraction(float mergeFraction) {
      builder.setMergeFraction(mergeFraction);
      return this;
    }

    @Override
    public SpecificBuilder<E> enableEncryptedTransfer() {
      builder.enableEncryptedTransfer();
      return this;
    }

    @Override
    public SpecificBuilder<E> setCombiner(String combinerClassName, Configuration combinerConf) {
      builder.setCombiner(combinerClassName, combinerConf);
      return this;
    }


    @Override
    public SpecificBuilder setAdditionalConfiguration(String key, String value) {
      builder.setAdditionalConfiguration(key, value);
      return this;
    }

    @Override
    public SpecificBuilder setAdditionalConfiguration(Map<String, String> confMap) {
      builder.setAdditionalConfiguration(confMap);
      return this;
    }

    @Override
    public SpecificBuilder setFromConfiguration(Configuration conf) {
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

  @InterfaceAudience.Private
  @VisibleForTesting
  ShuffledMergedInputConfiguration() {
  }

  private ShuffledMergedInputConfiguration(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Get a byte array representation of the configuration
   * @return a byte array which can be used as the payload
   */
  public byte[] toByteArray() {
    try {
      return TezUtils.createUserPayloadFromConf(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void fromByteArray(byte[] payload) {
    try {
      this.conf = TezUtils.createConfFromUserPayload(payload);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Builder newBuilder(String keyClass, String valueClass) {
    return new Builder(keyClass, valueClass);
  }

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class Builder implements SpecificConfigurer<Builder> {

    private final Configuration conf = new Configuration(false);

    /**
     * Create a configuration builder for {@link org.apache.tez.runtime.library.input.ShuffledMergedInput}
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
          .extractConfigurationMap(TezJobConfig.getTezRuntimeConfigDefaults(),
              ShuffledMergedInput.getConfigurationKeySet());
      ConfigUtils.addConfigMapToConfiguration(this.conf, tezDefaults);
      ConfigUtils.addConfigMapToConfiguration(this.conf, TezJobConfig.getOtherConfigDefaults());
    }

    @InterfaceAudience.Private
    Builder setKeyClassName(String keyClassName) {
      Preconditions.checkNotNull(keyClassName, "Key class name cannot be null");
      this.conf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_KEY_CLASS, keyClassName);
      return this;
    }

    @InterfaceAudience.Private
    Builder setValueClassName(String valueClassName) {
      Preconditions.checkNotNull(valueClassName, "Value class name cannot be null");
      this.conf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_VALUE_CLASS, valueClassName);
      return this;
    }

    @Override
    public Builder setShuffleBufferFraction(float shuffleBufferFraction) {
      this.conf
          .setFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT, shuffleBufferFraction);
      return this;
    }

    @Override
    public Builder setPostMergeBufferFraction(float postMergeBufferFraction) {
      this.conf.setFloat(TezJobConfig.TEZ_RUNTIME_INPUT_BUFFER_PERCENT, postMergeBufferFraction);
      return this;
    }

    @Override
    public Builder setMaxSingleMemorySegmentFraction(float maxSingleSegmentFraction) {
      this.conf.setFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT,
          maxSingleSegmentFraction);
      return this;
    }

    @Override
    public Builder enableMemToMemMerger() {
      this.conf.setBoolean(TezJobConfig.TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM, true);
      return this;
    }

    @Override
    public Builder setMergeFraction(float mergeFraction) {
      this.conf.setFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, mergeFraction);
      return this;
    }

    @Override
    public Builder enableEncryptedTransfer() {
      this.conf.setBoolean(TezJobConfig.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL, true);
      return this;
    }

    @Override
    public Builder setCombiner(String combinerClassName, Configuration combinerConf) {
      this.conf.set(TezJobConfig.TEZ_RUNTIME_COMBINER_CLASS, combinerClassName);
      if (combinerConf != null) {
        // Merging the confs for now. Change to be specific in the future.
        ConfigUtils.mergeConfsWithExclusions(this.conf, combinerConf,
            TezJobConfig.getRuntimeConfigKeySet());
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
      this.conf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_KEY_COMPARATOR_CLASS,
          comparatorClassName);
      return this;
    }

    @Override
    public Builder setAdditionalConfiguration(String key, String value) {
      Preconditions.checkNotNull(key, "Key cannot be null");
      if (ConfigUtils.doesKeyQualify(key,
          Lists.newArrayList(ShuffledMergedInput.getConfigurationKeySet(),
              TezJobConfig.getRuntimeAdditionalConfigKeySet()),
          TezJobConfig.getAllowedPrefixes())) {
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
          Lists.newArrayList(ShuffledMergedInput.getConfigurationKeySet(),
              TezJobConfig.getRuntimeAdditionalConfigKeySet()), TezJobConfig.getAllowedPrefixes());
      ConfigUtils.addConfigMapToConfiguration(this.conf, map);
      return this;
    }

    @Override
    public Builder setFromConfiguration(Configuration conf) {
      // Maybe ensure this is the first call ? Otherwise this can end up overriding other parameters
      Preconditions.checkArgument(conf != null, "Configuration cannot be null");
      Map<String, String> map = ConfigUtils.extractConfigurationMap(conf,
          Lists.newArrayList(ShuffledMergedInput.getConfigurationKeySet(),
              TezJobConfig.getRuntimeAdditionalConfigKeySet()), TezJobConfig.getAllowedPrefixes());
      ConfigUtils.addConfigMapToConfiguration(this.conf, map);
      return this;
    }

    public Builder enableCompression(String compressionCodec) {
      this.conf.setBoolean(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_IS_COMPRESSED, true);
      if (compressionCodec != null) {
        this.conf
            .set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_COMPRESS_CODEC, compressionCodec);
      }
      return this;
    }

    /**
     * Create the actual configuration instance.
     *
     * @return an instance of the Configuration
     */
    public ShuffledMergedInputConfiguration build() {
      return new ShuffledMergedInputConfiguration(this.conf);
    }
  }

}
