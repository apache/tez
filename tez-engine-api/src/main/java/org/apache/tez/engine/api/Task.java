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
 * {@link Task} is the base <em>tez</em> entity which consumes 
 * input key/value pairs through an {@link Input} pipe, 
 * processes them via a {@link Processor} and 
 * produces output key/value pairs for an {@link Output} pipe.
 */
public interface Task {
  
  /**
   * Initialize the {@link Task}.
   * 
   * @param conf task configuration
   * @param master master controlling the task
   * @throws IOException
   * @throws InterruptedException
   */
  public void initialize(Configuration conf, Master master) 
      throws IOException, InterruptedException;
  
  /**
   * Get {@link Input} of the task.
   * @return <code>Input</code> of the task
   */
  public Input getInput();

  /**
   * Get {@link Processor} of the task.
   * @return <code>Processor</code> of the task
   */
  public Processor getProcessor();

  /**
   * Get {@link Output} of the task.
   * @return <code>Output</code> of the task
   */
  public Output getOutput();

  /**
   * Run the {@link Task}.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  public void run() throws IOException, InterruptedException;
  
  /**
   * Stop the {@link Task}.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  public void close() throws IOException, InterruptedException;
  
}
