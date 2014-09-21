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

package org.apache.tez.common;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.Shell;

import java.lang.reflect.Field;
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
    if (!Shell.WINDOWS) {
      updateEnvironment(environment);
    } else {
      updateEnvironmentOnWindows(environment);
    }
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
    if (!Shell.WINDOWS) {
      updateEnvironment(environment);
    } else {
      updateEnvironmentOnWindows(environment);
    }
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
    final Map<String, String> currentEnv = System.getenv();
    copyMapValuesToPrivateField(currentEnv.getClass(), currentEnv, "m", environment);
  }

  /**
   * Finds and modifies internal storage for system environment variables using reflection. This
   * method works only on windows. Note that the actual env is not modified, rather the copy of env
   * which the JVM creates at the beginning of execution is.
   *
   * @param environment Collection where the key is the System
   * environment variable and the value is the value to assign the system
   * environment variable
   */
  @SuppressWarnings("unchecked")
  private static void updateEnvironmentOnWindows(Map<String, String> environment) {
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      copyMapValuesToPrivateField(processEnvironmentClass, null, "theEnvironment", environment);
      copyMapValuesToPrivateField(processEnvironmentClass, null, "theCaseInsensitiveEnvironment",
          environment);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Failed to update Environment variables", e);
    }
  }

  /**
   * Copies the given map values to the field specified by {@code fieldName}
   * @param klass The {@code Class} of the object
   * @param object The object to modify or null if the field is static
   * @param fieldName The name of the field to set
   * @param newMapValues The values to replace the current map.
   */
  @SuppressWarnings("unchecked")
  private static void copyMapValuesToPrivateField(Class<?> klass, Object object, String fieldName,
                                                  Map<String, String> newMapValues) {
    try {
      Field field = klass.getDeclaredField(fieldName);
      field.setAccessible(true);
      Map<String, String> currentMap = (Map<String, String>) field.get(object);
      currentMap.clear();
      currentMap.putAll(newMapValues);
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException("Failed to update Environment variables", e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Failed to update Environment variables", e);
    }
  }
}
