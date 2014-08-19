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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.tez.dag.api.UserPayload;

@InterfaceAudience.Private
abstract class HadoopKeyValuesBasedBaseEdgeConfig {

  /**
   * Get the payload for the configured Output
   * @return output configuration as a byte array
   */
  public abstract UserPayload getOutputPayload();

  /**
   * Get the output class name
   * @return the output class name
   */
  public abstract String getOutputClassName();

  /**
   * Get the payload for the configured Input
   * @return input configuration as a byte array
   */
  public abstract UserPayload getInputPayload();

  /**
   * Get the input class name
   * @return the input class name
   */
  public abstract String getInputClassName();

  public abstract static class Builder<T extends Builder<T>> implements BaseConfigBuilder<T> {

    /**
     * Enable compression for the specific Input / Output / Edge
     *
     * @param enabled          whether to enable compression or not
     * @param compressionCodec the codec to be used if compression is enabled. null implies using
     *                         the default
     * @param codecConf        the codec configuration. This can be null, and is a {@link
     *                         java.util.Map} of key-value pairs. The keys should be limited to
     *                         the ones required by the comparator.
     * @return instance of the current builder
     */
    public abstract T setCompression(boolean enabled, @Nullable String compressionCodec,
                            @Nullable Map<String, String> codecConf);

  }

}
