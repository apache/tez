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

package org.apache.tez.dag.api;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class TestTezConfiguration {

  private static final String expectedValue = "tez.tar.gz";

  @Test(timeout = 5000)
  public void testConstruction() {
    TezConfiguration tezConf1 = new TezConfiguration();
    Assert.assertEquals(expectedValue, tezConf1.get(TezConfiguration.TEZ_LIB_URIS));

    Configuration tezConf2 = new Configuration(true);
    Assert.assertNull(tezConf2.get(TezConfiguration.TEZ_LIB_URIS));

    TezConfiguration tezConf3 = new TezConfiguration(new Configuration());
    Assert.assertEquals(expectedValue, tezConf3.get(TezConfiguration.TEZ_LIB_URIS));

    TezConfiguration tezConf4 = new TezConfiguration(false);
    Assert.assertNull(tezConf4.get(TezConfiguration.TEZ_LIB_URIS));

    Configuration tezConf5 = new Configuration(true);
    Assert.assertNull(tezConf5.get(TezConfiguration.TEZ_LIB_URIS));
  }

  @Test(timeout = 5000)
  public void testKeySet() throws IllegalAccessException {
    Class<?> c = TezConfiguration.class;
    Set<String> expectedKeys = new HashSet<String>();
    for (Field f : c.getFields()) {
      if (!f.getName().endsWith("DEFAULT") && f.getType() == String.class) {
        String value = (String)f.get(null);
        // not prefix
        if (!value.endsWith(".")) {
          expectedKeys.add((String) f.get(null));
          Assert.assertNotNull("field " + f.getName() + " do not have annotation of ConfigurationScope.",
              f.getAnnotation(ConfigurationScope.class));
        }
      }
    }

    Set<String> actualKeySet = TezConfiguration.getPropertySet();
    for (String key : actualKeySet) {
      if (!expectedKeys.remove(key)) {
        fail("Found unexpected key: " + key + " in key set");
      }
    }
    assertTrue("Missing keys in key set: " + expectedKeys, expectedKeys.size() == 0);
  }
}
