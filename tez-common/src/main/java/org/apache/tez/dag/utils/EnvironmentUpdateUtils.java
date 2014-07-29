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

package org.apache.tez.dag.utils;

import org.apache.hadoop.classification.InterfaceAudience;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A utility class which allows one to dynamically update/change Environment variables
 */
@InterfaceAudience.Private
public class EnvironmentUpdateUtils {

  /**
   * Allows dynamic update to the environment variables. After calling put,
   * System.getenv(key) will then return value.
   *
   * @param key System environment variable
   * @param value Value to assign to system environment variable
   */
  public static void put(String key, String value){
    Map<String, String> environment = new HashMap<String, String>(System.getenv());
    environment.put(key, value);
    updateEnvironment(environment);
  }

  /**
   * Allows dynamic update to a collection of environment variables. After
   * calling putAll, System.getenv(key) will then return value for each entry
   * in the map
   *
   * @param additionalEnvironment Collection where the key is the System
   * environment variable and the value is the value to assign the system
   * environment variable
   */
  public static void putAll(Map<String, String> additionalEnvironment) {
    Map<String, String> environment = new HashMap<String, String>(System.getenv());
    environment.putAll(additionalEnvironment);
    updateEnvironment(environment);
  }

  /**
   * Finds and modifies internal storage for system environment variables using
   * reflection
   *
   * @param environment Collection where the key is the System
   * environment variable and the value is the value to assign the system
   * environment variable
   */
  @SuppressWarnings("unchecked")
  private static void updateEnvironment(Map<String, String> environment) {
    try {
      Class<?>[] classes = Collections.class.getDeclaredClasses();
      for (Class<?> clazz : classes) {
        if ("java.util.Collections$UnmodifiableMap".equals(clazz.getName())) {
          Field field = clazz.getDeclaredField("m");
          field.setAccessible(true);
          Object obj = field.get(System.getenv());
          Map<String, String> map = (Map<String, String>)obj;
          map.clear();
          map.putAll(environment);
        }
      }
    }
    catch (NoSuchFieldException e) {
      throw new IllegalStateException("Failed to update Environment variables", e);
    }
    catch (IllegalAccessException e) {
      throw new IllegalStateException("Failed to update Environment variables", e);
    }
  }
}
