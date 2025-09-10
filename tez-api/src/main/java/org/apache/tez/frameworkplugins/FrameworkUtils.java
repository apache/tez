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
package org.apache.tez.frameworkplugins;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezReflectionException;

public final class FrameworkUtils {

  private FrameworkUtils() {}

  /*
    Searches for a FrameworkService provider which implements a target interface.
    The interface should be either ClientFrameworkService or ServerFrameworkService.
    Depending on which interface is used, either the client or server class of a
    matching FrameworkMode will be used as the implementation.

    NOTE: Layering of FrameworkServices in a decorator-style is currently not supported

    An implementation is searched in the following order:
     1. If conf is not null and the parameter TEZ_FRAMEWORK_MODE is set:
       the value of TEZ_FRAMEWORK_MODE from the conf will be used
     2. If conf is null or the parameter TEZ_FRAMEWORK_MODE is not set
        and the environment var TEZ_FRAMEWORK_MODE is not empty:
            the value of the environment var will be used
     3. Otherwise:
       the default java.util.ServiceLoader behavior will be used,
       i.e. the implementation classname should appear in a file on the classpath at the location
            META-INF/services/org.apache.tez.frameworkplugins.ClientFrameworkService
            or META-INF/services/org.apache.tez.frameworkplugins.ServerFrameworkService
   */
  public static <T extends FrameworkService> T get(Class<T> interfaze, @Nullable Configuration conf) {
    try {
      if ((conf != null) && (conf.get(TezConfiguration.TEZ_FRAMEWORK_MODE) != null)) {
        return getByMode(interfaze, conf.get(TezConfiguration.TEZ_FRAMEWORK_MODE));
      } else if (System.getenv(TezConstants.TEZ_FRAMEWORK_MODE) != null) {
        return getByMode(interfaze, System.getenv(TezConstants.TEZ_FRAMEWORK_MODE));
      } else {
        return getByServiceLoader(interfaze);
      }
    } catch (TezReflectionException e) {
      throw new RuntimeException("Failed to load framework service for interface: " + interfaze.getName(), e);
    }
  }

  private static <T extends FrameworkService> T getByServiceLoader(Class<T> interfaze) {
    List<T> services = new ArrayList<>();
    ServiceLoader<T> frameworkService = ServiceLoader.load(interfaze);
    for (T service : frameworkService) {
      services.add(service);
    }
    if (services.isEmpty()) {
      return null;
    } else if (services.size() > 1) {
      throw new RuntimeException("Layering of multiple framework services is not supported."
          + " Please provide only one implementation class in configuration.");
    }
    //services is guaranteed to have one element at this point
    return services.getFirst();
  }

  private static <T> T getByMode(Class<T> interfaze, String mode) throws TezReflectionException {
    mode = mode.toUpperCase();
    String clazz;
    if (interfaze == ClientFrameworkService.class) {
      clazz = FrameworkMode.valueOf(mode).getClientClassName();
    } else {
      clazz = FrameworkMode.valueOf(mode).getServerClassName();
    }
    return ReflectionUtils.createClazzInstance(clazz);
  }
}
