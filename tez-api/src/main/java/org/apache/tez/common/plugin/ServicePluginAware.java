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

import org.apache.tez.common.ServicePluginLifecycle;
import org.apache.tez.serviceplugins.api.InterPluginId;


/**
 * Class that gives awareness to plugins that there are other plugins. This
 * can be useful when plugins work together like it could happen in the
 * plugins in
 * {@link org.apache.tez.serviceplugins.api.ServicePluginsDescriptor}.
 * It provides a way of communicating between them in a queue-like manner.
 */
public abstract class ServicePluginAware implements ServicePluginLifecycle {
  /**
   * object to communicate between plugins.
   */
  private InterPluginCommunicator interPluginCommunicator;

  /**
   * It's going to be a queue-like implementation so several
   * values can go under the same key.
   * @param key to identify the subsection in the storage
   * @param value the value to store
   */
  public final void put(final InterPluginId key, final Object value) {
    if (interPluginCommunicator == null) {
      return;
    }
    interPluginCommunicator.put(key, value);
  }

  /**
   * Get one of the values under key.
   * @param key to identify the subsection in the storage
   * @return the first value in the subsection identified
   * by key. It removes the value from the storage
   */
  public final Object get(final InterPluginId key) {
    if (interPluginCommunicator == null) {
      return null;
    }
    return interPluginCommunicator.get(key);
  }

  /**
   * Get one of the values under key.
   * @param key to identify the subsection in the storage
   * @return the first value in the subsection identified
   * by key. It doesn't removes the value from the storage
   */
  public final Object peek(final InterPluginId key) {
    if (interPluginCommunicator == null) {
      return null;
    }
    return interPluginCommunicator.peek(key);
  }

  /**
   * Send to the listeners of a particular key a value.
   * @param key key that determines the listeners
   * @param value value to send
   */
  public final void send(final InterPluginId key, final Object value) {
    if (interPluginCommunicator == null) {
      return;
    }
    interPluginCommunicator.send(key, value);
  }

  /**
   * Subscribe to a value for a particular key being stored.
   * @param key subscribe to this key
   * @param interPluginListener subscriber
   */
  public final void subscribe(final InterPluginId key,
      final InterPluginListener interPluginListener) {
    if (interPluginCommunicator == null) {
      return;
    }
    interPluginCommunicator.subscribe(key, interPluginListener);
  }

  /**
   * Unsubscribe from a particular key.
   * @param key unsubscribe from this key
   * @param interPluginListener subscriber
   */
  public final void unsubscribe(final InterPluginId key,
      final InterPluginListener interPluginListener) {
    if (interPluginCommunicator == null) {
      return;
    }
    interPluginCommunicator.unsubscribe(key, interPluginListener);
  }

  /**
   * Set the inter plugin storage that will be used by this
   * plugin. It has to be set on the different plugins that
   * will share objects.
   * @param pluginCommunicator storage to send objects from one
   *                           plugin to another
   */
  public final void setInterPluginCommunicator(final InterPluginCommunicator
      pluginCommunicator) {
    this.interPluginCommunicator = pluginCommunicator;
  }
}
