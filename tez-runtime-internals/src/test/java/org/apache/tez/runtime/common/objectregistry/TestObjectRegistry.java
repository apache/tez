/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.runtime.common.objectregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.tez.runtime.api.ObjectRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestObjectRegistry {

  private void testCRUD(ObjectRegistry objectRegistry) {
    assertNotNull(objectRegistry);

    assertNull(objectRegistry.get("foo"));
    assertFalse(objectRegistry.delete("foo"));
    Integer one = 1;
    Integer twoFirst = 2;
    Integer twoSecond = 3;
    assertNull(objectRegistry.cacheForDAG("one", one));
    assertEquals(one, objectRegistry.get("one"));
    assertNull(objectRegistry.cacheForDAG("two", twoFirst));
    assertNotNull(objectRegistry.cacheForSession("two", twoSecond));
    assertNotEquals(twoFirst, objectRegistry.get("two"));
    assertEquals(twoSecond, objectRegistry.get("two"));
    assertTrue(objectRegistry.delete("one"));
    assertFalse(objectRegistry.delete("one"));

  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testBasicCRUD() {
    ObjectRegistry objectRegistry = new ObjectRegistryImpl();
    testCRUD(objectRegistry);
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testClearCache() {
    ObjectRegistry objectRegistry = new ObjectRegistryImpl();
    testCRUD(objectRegistry);

    String one = "one";
    String two = "two";
    objectRegistry.cacheForVertex(one, one);
    objectRegistry.cacheForDAG(two, two);

    ((ObjectRegistryImpl)objectRegistry).clearCache(ObjectRegistryImpl.ObjectLifeCycle.VERTEX);
    assertNull(objectRegistry.get(one));
    assertNotNull(objectRegistry.get(two));

    objectRegistry.cacheForVertex(one, one);
    ((ObjectRegistryImpl)objectRegistry).clearCache(ObjectRegistryImpl.ObjectLifeCycle.DAG);
    assertNotNull(objectRegistry.get(one));
    assertNull(objectRegistry.get(two));
  }
}
