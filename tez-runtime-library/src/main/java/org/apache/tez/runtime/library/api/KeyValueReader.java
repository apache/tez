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

package org.apache.tez.runtime.library.api;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.tez.runtime.api.Reader;

/**
 * A key/value(s) pair based {@link Reader}.
 * 
 * Example usage
 * <code>
 * while (kvReader.next()) {
 *   Object key =  kvReader.getCurrentKey();
 *   Object value = kvReader.getCurrentValue();
 * </code>
 *
 */
@Public
@Evolving
public abstract class KeyValueReader extends Reader {

  /**
   * Moves to the next key/values(s) pair
   * 
   * @return true if another key/value(s) pair exists, false if there are no more.
   * @throws IOException
   *           if an error occurs
   */
  public abstract boolean next() throws IOException;

  
  /**
   * Returns the current key
   * @return the current key
   */
  public abstract Object getCurrentKey() throws IOException;
  
  
  /**
   * Returns the current value
   * @return the current value
   * @throws IOException
   */
  public abstract Object getCurrentValue() throws IOException;
}
