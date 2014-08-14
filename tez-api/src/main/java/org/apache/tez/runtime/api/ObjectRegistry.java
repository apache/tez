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
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * A simple shared object registry to cache objects in the memory of the
 * container running the task. This registry can be used to avoid re-creating
 * objects like common hash-tables, dictionaries etc. and provide performance
 * improvements. The cached objects have different life-cycles. If a task adds
 * an object to the cache with Vertex life-cycle then that object is in the
 * cache while the Vertex (to which the task belongs) is running. DAG life-cycle
 * is while the DAG (to which that task belongs) is running. Session life-cycle
 * is while the session (to which that task belongs) is running. <br>
 * This interface is not supposed to be implemented by users.
 */
@Public
@Evolving
public interface ObjectRegistry {

  /**
   * Insert or update object into the registry with Vertex life-cycle. This will
   * remove an object associated with the same key with a different life-cycle
   * as there is only one instance of an Object stored for a given key
   * irrespective of the life-cycle attached to the Object. The object may stay
   * in the cache while the Vertex (to which the task belongs) is running.
   * 
   * @param key
   *          Key to identify the Object
   * @param value
   *          Object to be inserted
   * @return Previous Object associated with the key attached if present else
   *         null. Could return the same object if the object was associated
   *         with the same key for a different life-cycle.
   */  
  public Object cacheForVertex(String key, Object value);
  
  /**
   * Insert or update object into the registry with DAG life-cycle. This will
   * remove an object associated with the same key with a different life-cycle
   * as there is only one instance of an Object stored for a given key
   * irrespective of the life-cycle attached to the Object. The object may stay
   * in the cache while the DAG (to which the task belongs) is running.
   * 
   * @param key
   *          Key to identify the Object
   * @param value
   *          Object to be inserted
   * @return Previous Object associated with the key attached if present else
   *         null. Could return the same object if the object was associated
   *         with the same key for a different life-cycle.
   */  
  public Object cacheForDAG(String key, Object value);
  
  /**
   * Insert or update object into the registry with Session life-cycle. This
   * will remove an object associated with the same key with a different
   * life-cycle as there is only one instance of an Object stored for a given
   * key irrespective of the life-cycle attached to the Object. The object may stay
   * in the cache while the Session (to which the task belongs) is running.
   * 
   * @param key
   *          Key to identify the Object
   * @param value
   *          Object to be inserted
   * @return Previous Object associated with the key attached if present else
   *         null. Could return the same object if the object was associated
   *         with the same key for a different life-cycle.
   */  
  public Object cacheForSession(String key, Object value);

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
