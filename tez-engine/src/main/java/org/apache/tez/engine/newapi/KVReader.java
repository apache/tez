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

package org.apache.tez.engine.newapi;

import java.io.IOException;

/**
 * A key/value(s) pair based {@link Reader}.
 */
public interface KVReader extends Reader {

  /**
   * Check if there is another key/value(s) pair
   * 
   * @return true if another key/value(s) pair exists
   * @throws IOException
   *           if an error occurs
   */
  public boolean hasNext() throws IOException;

  /**
   * Gets the next key.
   * 
   * @return the next key, or null if none exists
   * @throws IOException
   *           if an error occurs
   */
  public Object getNextKey() throws IOException;

  /**
   * Get the next values.
   * 
   * @return an <code>Iterable</code> view of the values for the current key
   * @throws IOException
   *           if an error occurs
   */
  public Iterable<Object> getNextValues() throws IOException;
}