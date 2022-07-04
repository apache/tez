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

package org.apache.tez.runtime.api;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

import java.util.List;

/**
 * Represents the Tez framework part of an {@link org.apache.tez.runtime.api.Processor}.
 * <p/>
 *
 * This interface has methods which are used by the Tez framework to control the Processor.
 * <p/>
 * Users are expected to derive from {@link AbstractLogicalIOProcessor}
 */
@Public
public interface ProcessorFrameworkInterface {

  /**
   * Initializes the <code>Processor</code>
   *
   * @throws java.io.IOException
   *           if an error occurs
   */
  public void initialize() throws Exception;

  /**
   * Handles user and system generated {@link Event}s.
   *
   * @param processorEvents
   *          the list of {@link Event}s
   */
  public void handleEvents(List<Event> processorEvents);

  /**
   * Closes the <code>Processor</code>
   *
   * @throws java.io.IOException
   *           if an error occurs
   */
  public void close() throws Exception;

  /**
   * Indicates <code>Processor</code> to abort. Cleanup can be done.
   *
   */
  @Unstable
  public void abort();
}
