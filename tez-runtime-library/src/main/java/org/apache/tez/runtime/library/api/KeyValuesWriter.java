/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.library.api;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * key/value(s) based {@link KeyValueWriter}
 */
@Public
@Evolving
public abstract class KeyValuesWriter extends KeyValueWriter {

  /**
   * Writes a key and its associated values
   *
   * @param key
   *          the key to write
   * @param values
   *          values to write
   * @throws java.io.IOException
   * @throws {@link IOInterruptedException} if IO was interrupted
   * @throws {@link IOInterruptedException} if IO was performing a blocking operation and was interrupted
   */
  public abstract void write(Object key, Iterable<Object> values) throws IOException;
}
