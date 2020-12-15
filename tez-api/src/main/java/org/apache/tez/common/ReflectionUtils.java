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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.tez.dag.api.TezReflectionException;

@Private
public class ReflectionUtils {

  private static final Map<String, Class<?>> CLAZZ_CACHE = new ConcurrentHashMap<String, Class<?>>();

  @Private
  public static Class<?> getClazz(String className) throws TezReflectionException {
    Class<?> clazz = CLAZZ_CACHE.get(className);
    if (clazz == null) {
      try {
        clazz = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
      } catch (ClassNotFoundException e) {
        throw new TezReflectionException("Unable to load class: " + className, e);
      }
    }
    return clazz;
  }

  private static <T> T getNewInstance(Class<T> clazz) throws TezReflectionException {
    T instance;
    try {
      instance = clazz.newInstance();
    } catch (Exception e) {
      throw new TezReflectionException(
          "Unable to instantiate class with 0 arguments: " + clazz.getName(), e);
    }
    return instance;
  }

  private static <T> T getNewInstance(Class<T> clazz, Class<?>[] parameterTypes, Object[] parameters)
    throws TezReflectionException {
    T instance;
    try {
      Constructor<T> constructor = clazz.getConstructor(parameterTypes);
      instance = constructor.newInstance(parameters);
    } catch (Exception e) {
      throw new TezReflectionException(
          "Unable to instantiate class with " + parameters.length + " arguments: " + clazz.getName(), e);
    }
    return instance;
  }

  @Private
  public static <T> T createClazzInstance(String className) throws TezReflectionException {
    Class<?> clazz = getClazz(className);
    @SuppressWarnings("unchecked")
    T instance = (T) getNewInstance(clazz);
    return instance;
  }

  @Private
  public static <T> T createClazzInstance(String className, Class<?>[] parameterTypes, Object[] parameters)
    throws TezReflectionException {
    Class<?> clazz = getClazz(className);
    @SuppressWarnings("unchecked")
    T instance = (T) getNewInstance(clazz, parameterTypes, parameters);
    return instance;
  }

  @Private
  @SuppressWarnings("unchecked")
  public static <T> T invokeMethod(Object target, Method method, Object... args) throws TezReflectionException {
    try {
      return (T) method.invoke(target, args);
    } catch (Exception e) {
      throw new TezReflectionException(e);
    }
  }

  @Private
  public static Method getMethod(Class<?> targetClazz, String methodName, Class<?>... parameterTypes) throws TezReflectionException {
    try {
      return targetClazz.getMethod(methodName, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new TezReflectionException(e);
    }
  }

  @Private
  public static synchronized void addResourcesToSystemClassLoader(List<URL> urls) {
    TezClassLoader classLoader = TezClassLoader.getInstance();
    for (URL url : urls) {
      classLoader.addURL(url);
    }
  }
}
