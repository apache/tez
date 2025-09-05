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

/**
 * Interface for client-side AM discovery
 */
public abstract class AMRegistryClient implements Closeable {

  protected List<AMRegistryClientListener> listeners = new ArrayList<>();

  //Get AM info given an appId
  public abstract AMRecord getRecord(String appId) throws IOException;

  //Get all AM infos in the registry
  public abstract List<AMRecord> getAllRecords() throws IOException;

  public synchronized void addListener(AMRegistryClientListener listener) {
    listeners.add(listener);
  }

  protected synchronized void notifyOnAdded(AMRecord record) {
    for(AMRegistryClientListener listener : listeners) {
      listener.onAdd(record);
    }
  }

  protected synchronized void notifyOnRemoved(AMRecord record) {
    for(AMRegistryClientListener listener : listeners) {
      listener.onRemove(record);
    }
  }

}
