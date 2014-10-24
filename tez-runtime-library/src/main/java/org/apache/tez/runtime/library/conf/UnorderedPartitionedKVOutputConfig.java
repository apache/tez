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
import org.apache.tez.runtime.library.output.UnorderedPartitionedKVOutput;

@InterfaceAudience.Public
@InterfaceStability.Evolving
/**
 * Configure {@link org.apache.tez.runtime.library.output.UnorderedPartitionedKVOutput} </p>
 *
 * Values will be picked up from tez-site if not specified, otherwise defaults from
 * {@link org.apache.tez.runtime.library.api.TezRuntimeConfiguration} will be used.
 */
public class UnorderedPartitionedKVOutputConfig {
  /**
   * Configure parameters which are specific to the Output.
   */
  @InterfaceAudience.Private
  public static interface SpecificConfigBuilder<T> extends BaseConfigBuilder<T> {
    /**
     * Set the buffer size to use
     *
     * @param availableBufferSize the size of the buffer in MB
     * @return instance of the current builder
     */
    public T setAvailableBufferSize(int availableBufferSize);
  }

  @SuppressWarnings("rawtypes")
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class SpecificBuilder<E extends HadoopKeyValuesBasedBaseEdgeConfig.Builder> implements
      SpecificConfigBuilder<SpecificBuilder> {

    private final E edgeBuilder;
    private final Builder builder;

    SpecificBuilder(E edgeBuilder, Builder builder) {
      this.edgeBuilder = edgeBuilder;
      this.builder = builder;
    }

    @Override
    public SpecificBuilder<E> setAvailableBufferSize(int availableBufferSize) {
      builder.setAvailableBufferSize(availableBufferSize);
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

  @InterfaceAudience.Private
  @VisibleForTesting
  UnorderedPartitionedKVOutputConfig() {
  }

  private UnorderedPartitionedKVOutputConfig(Configuration conf) {
    this.conf = conf;
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


  public static Builder newBuilder(String keyClass, String valClass, String partitionerClassName) {
    return newBuilder(keyClass, valClass, partitionerClassName, null);
  }

  public static Builder newBuilder(String keyClass, String valClass, String partitionerClassName,
                                   Map<String, String> partitionerConf) {
    return new Builder(keyClass, valClass, partitionerClassName, partitionerConf);
  }

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class Builder implements SpecificConfigBuilder<Builder> {

    private final Configuration conf = new Configuration(false);

    /**
     * Create a configuration builder for {@link org.apache.tez.runtime.library.output.UnorderedPartitionedKVOutput}
     *
     * @param keyClassName         the key class name
     * @param valueClassName       the value class name
     * @param partitionerClassName the partitioner class name
     * @param partitionerConf      configuration for the partitioner specified as a map of key-value
     *                             pairs. This can be null
     */
    @InterfaceAudience.Private
    Builder(String keyClassName, String valueClassName, String partitionerClassName,
                   Map<String, String> partitionerConf) {
      this();
      Preconditions.checkNotNull(keyClassName, "Key class name cannot be null");
      Preconditions.checkNotNull(valueClassName, "Value class name cannot be null");
      Preconditions.checkNotNull(partitionerClassName, "Partitioner class name cannot be null");
      setKeyClassName(keyClassName);
      setValueClassName(valueClassName);
      setPartitioner(partitionerClassName, partitionerConf);
    }

    @InterfaceAudience.Private
    Builder() {
      Map<String, String> tezDefaults = ConfigUtils
          .extractConfigurationMap(TezRuntimeConfiguration.getTezRuntimeConfigDefaults(),
              UnorderedPartitionedKVOutput.getConfigurationKeySet());
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

    @InterfaceAudience.Private
    Builder setPartitioner(String partitionerClassName, Map<String, String> partitionerConf) {
      Preconditions.checkNotNull(partitionerClassName, "Partitioner class name cannot be null");
      this.conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, partitionerClassName);
      if (partitionerConf != null) {
        // Merging the confs for now. Change to be specific in the future.
        ConfigUtils.mergeConfsWithExclusions(this.conf, partitionerConf,
            TezRuntimeConfiguration.getRuntimeConfigKeySet());
      }
      return this;
    }

    @Override
    public Builder setAvailableBufferSize(int availableBufferSize) {
      this.conf
          .setInt(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB, availableBufferSize);
      return this;
    }

    @Override
    public Builder setAdditionalConfiguration(String key, String value) {
      Preconditions.checkNotNull(key, "Key cannot be null");
      if (ConfigUtils.doesKeyQualify(key,
          Lists.newArrayList(UnorderedPartitionedKVOutput.getConfigurationKeySet(),
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
          Lists.newArrayList(UnorderedPartitionedKVOutput.getConfigurationKeySet(),
              TezRuntimeConfiguration.getRuntimeAdditionalConfigKeySet()), TezRuntimeConfiguration.getAllowedPrefixes());
      ConfigUtils.addConfigMapToConfiguration(this.conf, map);
      return this;
    }

    @Override
    public Builder setFromConfiguration(Configuration conf) {
      // Maybe ensure this is the first call ? Otherwise this can end up overriding other parameters
      Preconditions.checkArgument(conf != null, "Configuration cannot be null");
      Map<String, String> map = ConfigUtils.extractConfigurationMap(conf,
          Lists.newArrayList(UnorderedPartitionedKVOutput.getConfigurationKeySet(),
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
     * Set serialization class responsible for providing serializer/deserializer for keys.
     *
     * @param serializationClassName
     * @param serializerConf         the serializer configuration. This can be null, and is a
     *                               {@link java.util.Map} of key-value pairs. The keys should be limited
     *                               to the ones required by the comparator.
     * @return this object for further chained method calls
     */
    public Builder setKeySerializationClass(String serializationClassName,
                                            @Nullable Map<String, String> serializerConf) {
      Preconditions.checkArgument(serializationClassName != null,
          "serializationClassName cannot be null");
      this.conf.set(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, serializationClassName + ","
          + conf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY));
      if (serializerConf != null) {
        // Merging the confs for now. Change to be specific in the future.
        ConfigUtils.mergeConfsWithExclusions(this.conf, serializerConf,
            TezRuntimeConfiguration.getRuntimeConfigKeySet());
      }      return this;
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
    public UnorderedPartitionedKVOutputConfig build() {
      return new UnorderedPartitionedKVOutputConfig(this.conf);
    }
  }
}
