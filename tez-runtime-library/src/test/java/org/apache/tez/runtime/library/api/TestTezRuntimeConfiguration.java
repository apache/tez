/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.tez.runtime.library.api;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.junit.Test;

public class TestTezRuntimeConfiguration {


  @Test(timeout = 5000)
  public void testKeySet() throws IllegalAccessException {
    Class<?> c = TezRuntimeConfiguration.class;
    Set<String> expectedKeys = new HashSet<String>();
    for (Field f : c.getFields()) {
      if (!f.getName().endsWith("DEFAULT") && f.getType() == String.class) {
        expectedKeys.add((String) f.get(null));
      }
    }

    Set<String> actualKeySet = TezRuntimeConfiguration.getRuntimeConfigKeySet();
    for (String key : actualKeySet) {
      if (!expectedKeys.remove(key)) {
        fail("Found unexpected key: " + key + " in key set");
      }
    }
    assertTrue("Missing keys in key set: " + expectedKeys, expectedKeys.size() == 0);
  }

}
