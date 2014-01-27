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

/**
 * Preliminary version of a simple shared object cache to re-use
 * objects across multiple tasks within the same container/JVM.
 */
public interface ObjectRegistry {

  /**
   * Insert or update object into the registry. This will remove an object
   * associated with the same key with a different life-cycle as there is only
   * one instance of an Object stored for a given key irrespective of the
   * life-cycle attached to the Object.
   * @param lifeCycle What life-cycle is the Object valid for
   * @param key Key to identify the Object
   * @param value Object to be inserted
   * @return Previous Object associated with the key attached if present
   * else null. Could return the same object if the object was associated with
   * the same key for a different life-cycle.
   */
  public Object add(ObjectLifeCycle lifeCycle, String key, Object value);

  /**
   * Return the object associated with the provided key
   * @param key Key to find object
   * @return Object if found else null
   */
  public Object get(String key);

  /**
   * Delete the object associated with the provided key
   * @param key Key to find object
   * @return True if an object was found and removed
   */
  public boolean delete(String key);

}
