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

package org.apache.tez.runtime.common.objectregistry;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.tez.runtime.api.ObjectRegistry;

public class ObjectRegistryImpl implements ObjectRegistry {
  
  public enum ObjectLifeCycle {
    /** Objects are valid for the lifetime of the Tez JVM/Session
     */
    SESSION,
    /** Objects are valid for the lifetime of the DAG.
     */
    DAG,
    /** Objects are valid for the lifetime of the Vertex.
     */
    VERTEX,
  }

  private Map<String, Map.Entry<Object, ObjectLifeCycle>> objectCache =
      new HashMap<String, Entry<Object, ObjectLifeCycle>>();

  private synchronized Object add(ObjectLifeCycle lifeCycle,
      String key, Object value) {
    Map.Entry<Object, ObjectLifeCycle> oldEntry =
        objectCache.put(key,
            new AbstractMap.SimpleImmutableEntry<Object, ObjectLifeCycle>(
                value, lifeCycle));
    return oldEntry != null ? oldEntry.getKey() : null;
  }

  @Override
  public synchronized Object get(String key) {
    Map.Entry<Object, ObjectLifeCycle> entry =
        objectCache.get(key);
    return entry != null ? entry.getKey() : null;
  }

  @Override
  public synchronized boolean delete(String key) {
    return (null != objectCache.remove(key));
  }

  public synchronized void clearCache(ObjectLifeCycle lifeCycle) {
    Iterator<Entry<String, Entry<Object, ObjectLifeCycle>>> it =
      objectCache.entrySet().iterator();
    while (it.hasNext()) {
      Entry<String, Entry<Object, ObjectLifeCycle>> entry = it.next();
      if (entry.getValue().getValue().equals(lifeCycle)) {
        it.remove();
      }
    }
  }

  @Override
  public synchronized Object cacheForVertex(String key, Object value) {
    return add(ObjectLifeCycle.VERTEX, key, value);
  }

  @Override
  public synchronized Object cacheForDAG(String key, Object value) {
    return add(ObjectLifeCycle.DAG, key, value);
  }

  @Override
  public synchronized Object cacheForSession(String key, Object value) {
    return add(ObjectLifeCycle.SESSION, key, value);
  }

}
