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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.tez.dag.api.EdgeProperty;

@InterfaceAudience.Private
abstract class HadoopKeyValuesBasedBaseConf {

  /**
   * Get the payload for the configured Output
   * @return output configuration as a byte array
   */
  public abstract byte[] getOutputPayload();

  /**
   * Get the output class name
   * @return the output class name
   */
  public abstract String getOutputClassName();

  /**
   * Get the payload for the configured Input
   * @return input configuration as a byte array
   */
  public abstract byte[] getInputPayload();

  /**
   * Get the input class name
   * @return the input class name
   */
  public abstract String getInputClassName();

  public abstract static class Builder<T extends Builder<T>> implements BaseConfigurer<T> {

    /**
     * Enable compression for the specific Input / Output / Edge
     *
     * @param compressionCodec the codec to be used. null implies using the default
     * @return instance of the current builder
     */
    public T enableCompression(String compressionCodec) {
      return (T) this;
    }

  }

}
