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

import org.apache.tez.runtime.common.objectregistry.ObjectLifeCycle;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistry;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryFactory;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryModule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestObjectRegistry {

  @SuppressWarnings("unused")
  @Before
  public void setup() {
    Injector injector = Guice.createInjector(new ObjectRegistryModule());
  }

  private void testCRUD(ObjectRegistry objectRegistry) {
    Assert.assertNotNull(objectRegistry);

    Assert.assertNull(objectRegistry.get("foo"));
    Assert.assertFalse(objectRegistry.delete("foo"));
    Integer one = new Integer(1);
    Integer two_1 = new Integer(2);
    Integer two_2 = new Integer(3);
    Assert.assertNull(objectRegistry.add(ObjectLifeCycle.DAG, "one", one));
    Assert.assertEquals(one, objectRegistry.get("one"));
    Assert.assertNull(objectRegistry.add(ObjectLifeCycle.DAG, "two", two_1));
    Assert.assertNotNull(objectRegistry.add(ObjectLifeCycle.SESSION, "two", two_2));
    Assert.assertNotEquals(two_1, objectRegistry.get("two"));
    Assert.assertEquals(two_2, objectRegistry.get("two"));
    Assert.assertTrue(objectRegistry.delete("one"));
    Assert.assertFalse(objectRegistry.delete("one"));

  }

  @Test
  public void testBasicCRUD() {
    ObjectRegistry objectRegistry =
        ObjectRegistryFactory.getObjectRegistry();
    testCRUD(objectRegistry);
  }

  @Test
  public void testClearCache() {
    ObjectRegistry objectRegistry = new ObjectRegistryImpl();
    testCRUD(objectRegistry);

    String one = "one";
    String two = "two";
    objectRegistry.add(ObjectLifeCycle.VERTEX, one, one);
    objectRegistry.add(ObjectLifeCycle.DAG, two, two);

    ((ObjectRegistryImpl)objectRegistry).clearCache(ObjectLifeCycle.VERTEX);
    Assert.assertNull(objectRegistry.get(one));
    Assert.assertNotNull(objectRegistry.get(two));

    objectRegistry.add(ObjectLifeCycle.VERTEX, one, one);
    ((ObjectRegistryImpl)objectRegistry).clearCache(ObjectLifeCycle.DAG);
    Assert.assertNotNull(objectRegistry.get(one));
    Assert.assertNull(objectRegistry.get(two));
  }
}
