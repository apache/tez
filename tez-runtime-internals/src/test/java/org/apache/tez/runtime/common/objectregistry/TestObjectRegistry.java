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

import org.apache.tez.runtime.api.ObjectRegistry;
import org.junit.Assert;
import org.junit.Test;

public class TestObjectRegistry {

  private void testCRUD(ObjectRegistry objectRegistry) {
    Assert.assertNotNull(objectRegistry);

    Assert.assertNull(objectRegistry.get("foo"));
    Assert.assertFalse(objectRegistry.delete("foo"));
    Integer one = new Integer(1);
    Integer two_1 = new Integer(2);
    Integer two_2 = new Integer(3);
    Assert.assertNull(objectRegistry.cacheForDAG("one", one));
    Assert.assertEquals(one, objectRegistry.get("one"));
    Assert.assertNull(objectRegistry.cacheForDAG("two", two_1));
    Assert.assertNotNull(objectRegistry.cacheForSession("two", two_2));
    Assert.assertNotEquals(two_1, objectRegistry.get("two"));
    Assert.assertEquals(two_2, objectRegistry.get("two"));
    Assert.assertTrue(objectRegistry.delete("one"));
    Assert.assertFalse(objectRegistry.delete("one"));

  }

  @Test
  public void testBasicCRUD() {
    ObjectRegistry objectRegistry = new ObjectRegistryImpl();
    testCRUD(objectRegistry);
  }

  @Test
  public void testClearCache() {
    ObjectRegistry objectRegistry = new ObjectRegistryImpl();
    testCRUD(objectRegistry);

    String one = "one";
    String two = "two";
    objectRegistry.cacheForVertex(one, one);
    objectRegistry.cacheForDAG(two, two);

    ((ObjectRegistryImpl)objectRegistry).clearCache(ObjectRegistryImpl.ObjectLifeCycle.VERTEX);
    Assert.assertNull(objectRegistry.get(one));
    Assert.assertNotNull(objectRegistry.get(two));

    objectRegistry.cacheForVertex(one, one);
    ((ObjectRegistryImpl)objectRegistry).clearCache(ObjectRegistryImpl.ObjectLifeCycle.DAG);
    Assert.assertNotNull(objectRegistry.get(one));
    Assert.assertNull(objectRegistry.get(two));
  }
}
