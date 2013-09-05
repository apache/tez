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

package org.apache.tez.engine.newruntime;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tez.dag.api.TezUncheckedException;

public class RuntimeUtils {

  private static final Map<String, Class<?>> CLAZZ_CACHE = new ConcurrentHashMap<String, Class<?>>();

  private static Class<?> getClazz(String className) {
    Class<?> clazz = CLAZZ_CACHE.get(className);
    if (clazz == null) {
      try {
        clazz = Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new TezUncheckedException("Unable to load class: " + className, e);
      }
    }
    return clazz;
  }

  private static <T> T getNewInstance(Class<T> clazz) {
    T instance;
    try {
      instance = clazz.newInstance();
    } catch (InstantiationException e) {
      throw new TezUncheckedException(
          "Unable to instantiate class with 0 arguments: " + clazz.getName(), e);
    } catch (IllegalAccessException e) {
      throw new TezUncheckedException(
          "Unable to instantiate class with 0 arguments: " + clazz.getName(), e);
    }
    return instance;
  }

  public static <T> T createClazzInstance(String className) {
    Class<?> clazz = getClazz(className);
    @SuppressWarnings("unchecked")
    T instance = (T) getNewInstance(clazz);
    return instance;
  }
}
