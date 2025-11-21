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

package org.apache.tez.client.registry;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client-side interface for discovering Application Master (AM) instances
 * registered in the AM registry.
 *
 * <p>Implementations are responsible for locating AM endpoints and returning
 * their metadata. This API is used by client components to discover running
 * Tez AMs.</p>
 *
 * <p>Listeners may be registered to receive notifications when AM records
 * appear or are removed.</p>
 */
public abstract class AMRegistryClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AMRegistryClient.class);

  private final List<AMRegistryClientListener> listeners = new ArrayList<>();

  /**
   * Lookup AM metadata for the given application ID.
   *
   * @param appId the application ID
   * @return the AM record if found, otherwise {@code null}
   * @throws IOException if the lookup fails
   */
  public abstract AMRecord getRecord(ApplicationId appId) throws IOException;

  /**
   * Retrieve all AM records known in the registry.
   *
   * @return a list of AM records (possibly empty)
   * @throws IOException if the fetch fails
   */
  public abstract List<AMRecord> getAllRecords() throws IOException;

  /**
   * Register a listener for AM registry events.
   * The listener will be notified when AM records are added or removed.
   *
   * @param listener the listener to add
   */
  public synchronized void addListener(AMRegistryClientListener listener) {
    listeners.add(listener);
  }

  /**
   * Notify listeners of a newly added AM record.
   *
   * @param record the added AM record
   */
  protected synchronized void notifyOnAdded(AMRecord record) {
    for (AMRegistryClientListener listener : listeners) {
      try {
        listener.onAdd(record);
      } catch (Exception e) {
        LOG.warn("Exception while calling AM add listener, AM record {}", record, e);
      }
    }
  }

  /**
   * Notify listeners of an updated AM record.
   *
   * @param record the updated AM record
   */
  protected synchronized void notifyOnUpdated(AMRecord record) {
    for (AMRegistryClientListener listener : listeners) {
      try {
        listener.onUpdate(record);
      } catch (Exception e) {
        LOG.warn("Exception while calling AM update listener, AM record {}", record, e);
      }
    }
  }

  /**
   * Notify listeners of a removed AM record.
   *
   * @param record the removed AM record
   */
  protected synchronized void notifyOnRemoved(AMRecord record) {
    for (AMRegistryClientListener listener : listeners) {
      try {
        listener.onRemove(record);
      } catch (Exception e) {
        LOG.warn("Exception while calling AM remove listener, AM record {}", record, e);
      }
    }
  }
}
