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
import org.apache.hadoop.conf.Configuration;

@InterfaceAudience.Private
interface BaseConfigBuilder<T> {
  /**
   * Used to set additional configuration parameters which are not set via API methods. This is
   * primarily meant for rarely used configuration options such as IFile read-ahead, configuring
   * the number of parallel files to merge etc.
   *
   * @param key   the key to set
   * @param value the corresponding value
   * @return this object for further chained method calls
   */
  public T setAdditionalConfiguration(String key, String value);

  /**
   * Used to set additional configuration parameters which are not set via API methods. This is
   * primarily meant for rarely used configuration options such as IFile read-ahead, configuring
   * the number of parallel files to merge etc. </p> Additionally keys set via this method are
   * made available to the combiner.
   *
   * @param confMap map of configuration key-value pairs
   * @return this object for further chained method calls
   */
  public T setAdditionalConfiguration(Map<String, String> confMap);

  /**
   * Used to build out a configuration from an existing Hadoop {@link
   * org.apache.hadoop.conf.Configuration}. This is a private API is present only for
   * compatibility and ease of use for existing systems which rely heavily on Configuration.
   *
   * @param conf
   * @return this object for further chained method calls
   */
  @InterfaceAudience.LimitedPrivate({"hive, pig"})
  public T setFromConfiguration(Configuration conf);
}
