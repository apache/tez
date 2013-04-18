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

import org.apache.hadoop.conf.Configuration;

/**
 * {@link Input} represents a pipe through which an <em>tez</em> task
 * can get input key/value pairs.
 */
public interface Input {
  
  /**
   * Initialize <code>Input</code>.
   * 
   * @param conf job configuration
   * @param master master process controlling the task
   * @throws IOException
   * @throws InterruptedException
   */
  public void initialize(Configuration conf, Master master) 
      throws IOException, InterruptedException;
  
  /**
   * Check if there is another key/value pair.
   * 
   * @return true if a key/value pair was read
   * @throws IOException
   * @throws InterruptedException
   */
  public boolean hasNext() throws IOException, InterruptedException;

  /**
   * Get the next key.
   * 
   * @return the current key or null if there is no current key
   * @throws IOException
   * @throws InterruptedException
   */
  public Object getNextKey() throws IOException, InterruptedException;
  
  /**
   * Get the next values.
   * 
   * @return the object that was read
   * @throws IOException
   * @throws InterruptedException
   */
  public Iterable<Object> getNextValues() 
      throws IOException, InterruptedException;
  
  /**
   * The current progress of the {@link Input} through its data.
   * 
   * @return a number between 0.0 and 1.0 that is the fraction of the data read
   * @throws IOException
   * @throws InterruptedException
   */
  public float getProgress() throws IOException, InterruptedException;
  
  /**
   * Close this <code>Input</code> for future operations.
   */
  public void close() throws IOException;

}
