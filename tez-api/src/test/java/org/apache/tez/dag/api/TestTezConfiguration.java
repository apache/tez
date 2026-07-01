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
package org.apache.tez.dag.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestTezConfiguration {

  private static final String expectedValue = "tez.tar.gz";

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testConstruction() {
    TezConfiguration tezConf1 = new TezConfiguration();
    assertEquals(expectedValue, tezConf1.get(TezConfiguration.TEZ_LIB_URIS));

    Configuration tezConf2 = new Configuration(true);
    assertNull(tezConf2.get(TezConfiguration.TEZ_LIB_URIS));

    TezConfiguration tezConf3 = new TezConfiguration(new Configuration());
    assertEquals(expectedValue, tezConf3.get(TezConfiguration.TEZ_LIB_URIS));

    TezConfiguration tezConf4 = new TezConfiguration(false);
    assertNull(tezConf4.get(TezConfiguration.TEZ_LIB_URIS));

    Configuration tezConf5 = new Configuration(true);
    assertNull(tezConf5.get(TezConfiguration.TEZ_LIB_URIS));
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testKeySet() throws IllegalAccessException {
    Class<?> c = TezConfiguration.class;
    Set<String> expectedKeys = new HashSet<String>();
    for (Field f : c.getFields()) {
      if (!f.getName().endsWith("DEFAULT")
          && f.getType() == String.class
          && !f.getName().equals("TEZ_SITE_XML")) {
        String value = (String) f.get(null);
        // not prefix
        if (!value.endsWith(".")) {
          expectedKeys.add((String) f.get(null));
          assertNotNull(f.getAnnotation(ConfigurationScope.class),
              "field " + f.getName() + " do not have annotation of ConfigurationScope.");
        }
      }
    }

    Set<String> actualKeySet = TezConfiguration.getPropertySet();
    for (String key : actualKeySet) {
      if (!expectedKeys.remove(key)) {
        fail("Found unexpected key: " + key + " in key set");
      }
    }
    assertEquals(0, expectedKeys.size(), "Missing keys in key set: " + expectedKeys);
  }
}
