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

import com.google.common.util.concurrent.MoreExecutors;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Executor;

/**
 * A interoperability layer to work with multiple versions of guava.
 */
public final class GuavaShim {

  static {
    try {
      executorMethod = MoreExecutors.class.getDeclaredMethod("directExecutor");
    } catch (NoSuchMethodException nsme) {
      try {
        executorMethod = MoreExecutors.class.getDeclaredMethod("sameThreadExecutor");
      } catch (NoSuchMethodException nsmeSame) {
      }
    }
  }

  private GuavaShim() {
  }

  private static Method executorMethod;

  public static Executor directExecutor() {
    try {
      return (Executor) executorMethod.invoke(null);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}