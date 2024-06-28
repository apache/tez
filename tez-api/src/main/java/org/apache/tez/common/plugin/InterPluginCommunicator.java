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

package org.apache.tez.common.plugin;

import org.apache.tez.serviceplugins.api.InterPluginId;

/**
 * Defines a storage for communicating between plugins.
 */
public interface InterPluginCommunicator {

  /**
   * Put value in the storage subsection defined by key.
   * @param key to find the right storage subsection
   * @param value value to put in the storage subsection
   */
  void put(InterPluginId key, Object value);

  /**
   * @param key determines the storage subsection.
   * @return the first element of the storage subsection or null
   * if it doesn't exist. It doesn't remove the element
   * from the storage
   */
  Object peek(InterPluginId key);

  /**
   * @param key determines the storage subsection.
   * @return the first element of the storage subsection or null
   * if it doesn't exist. It removes the element from the storage
   */
  Object get(InterPluginId key);

  /**
   * Send to the listeners of a particular key a value.
   * @param key key that determines the listeners
   * @param value value to send
   */
  void send(InterPluginId key, Object value);

  /**
   * Subscribe to a value for a particular key being sent.
   * @param key subscribe to this key
   * @param interPluginListener subscriber
   */
  void subscribe(InterPluginId key, InterPluginListener interPluginListener);

  /**
   * Unsubscribe.
   * @param key to unsubscribe from.
   * @param interPluginListener to unsubscribe.
   */
  void unsubscribe(InterPluginId key, InterPluginListener interPluginListener);

}
