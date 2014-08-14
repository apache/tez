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

package org.apache.tez.runtime.api;

import org.apache.hadoop.classification.InterfaceAudience.Public;

/**
 * Represents an Output through which a TezProcessor writes information to an edge.
 * </p>
 *
 * This interface has methods which can be used by a {@link org.apache.tez.runtime.api.Processor}
 * to control execution of this Output and write data to it.
 * 
 * Users are expected to derive from {@link AbstractLogicalOutput}
 *
 */
@Public
public interface Output {



  /**
   * Start any processing that the Output may need to perform. It is the
   * responsibility of the Processor to start Outputs.
   * 
   * This typically acts as a signal to Outputs to start any Processing that they
   * may required.
   * 
   * This method may be invoked by the framework under certain circumstances,
   * and as such requires the implementation to be non-blocking.   
   * 
   * Outputs must be written to handle multiple start invocations - typically
   * honoring only the first one.
   * 
   * @throws Exception
   */
  public void start() throws Exception;

  /**
   * Gets an instance of the {@link Writer} in an <code>Output</code>
   *
   * @return Gets an instance of the {@link Writer} in an <code>Output</code>
   * @throws Exception
   *           if an error occurs
   */
  public Writer getWriter() throws Exception;

}