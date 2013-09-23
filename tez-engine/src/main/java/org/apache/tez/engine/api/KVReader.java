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

package org.apache.tez.engine.api;

import java.io.IOException;

import org.apache.tez.engine.newapi.Reader;

/**
 * A key/value(s) pair based {@link Reader}.
 * 
 * Example usage
 * <code>
 * while (kvReader.next()) {
 *   KVRecord kvRecord = getCurrentKV();
 *   Object key =  kvRecord.getKey();
 *   Iterable values = kvRecord.getValues();
 * </code>
 *
 */
public interface KVReader extends Reader {

  /**
   * Moves to the next key/values(s) pair
   * 
   * @return true if another key/value(s) pair exists, false if there are no more.
   * @throws IOException
   *           if an error occurs
   */
  public boolean next() throws IOException;

  /**
   * Return the current key/value(s) pair. Use moveToNext() to advance.
   * @return
   * @throws IOException
   */
  public KVRecord getCurrentKV() throws IOException;
  
  // TODO NEWTEZ Move this to getCurrentKey and getCurrentValue independently. Otherwise usage pattern seems to be getCurrentKV.getKey, getCurrentKV.getValue
  
  // TODO NEWTEZ KVRecord which does not need to return a list!
  // TODO NEWTEZ Parameterize this
  /**
   * Represents a key and an associated set of values
   *
   */
  public static class KVRecord {

    private Object key;
    private Iterable<Object> values;

    public KVRecord(Object key, Iterable<Object> values) {
      this.key = key;
      this.values = values;
    }

    public Object getKey() {
      return this.key;
    }

    public Iterable<Object> getValues() {
      return this.values;
    }
  }
}