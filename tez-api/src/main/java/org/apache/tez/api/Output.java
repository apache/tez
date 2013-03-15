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
package org.apache.tez.api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.records.OutputContext;

/**
 * {@link Output} represents a pipe through which an <em>tez</em> task
 * can send out outputs.
 */
public interface Output {

  /**
   * Initialize <code>Output</code>.
   * 
   * @param conf job configuration
   * @param master master process controlling the task
   * @throws IOException
   * @throws InterruptedException
   */
  public void initialize(Configuration conf, Master master) 
      throws IOException, InterruptedException;

  /** 
   * Writes a key/value pair.
   *
   * @param key the key to write.
   * @param value the value to write.
   * @throws IOException
   */      
  public void write(Object key, Object value
                             ) throws IOException, InterruptedException;

  /**
   * Returns the OutputContext for the particular <code>Output</code>. 
   * 
   * @return the OutputContext for this Output if it exists, otherwise null.
   */
  public OutputContext getOutputContext();
  
  /** 
   * Close this <code>Output</code> for future operations.
   * 
   * @throws IOException
   */ 
  public void close() throws IOException, InterruptedException;
}
