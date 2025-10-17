/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.frameworkplugins;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.frameworkplugins.yarn.YarnServerFrameworkService;

/**
 * A container for framework-related objects used by the AM process.
 * Provides default implementations when no framework service is available.
 */
public final class FrameworkContext {

  private final ServerFrameworkService frameworkService;
  private final AmExtensions amExtensions;

  /**
   * Constructs a new {@code FrameworkContext} using the provided configuration.
   * <p>
   * This constructor initializes the {@link #frameworkService} by attempting to
   * retrieve an instance of {@link ServerFrameworkService} from the given
   * configuration via {@link FrameworkUtils#get(Class, Configuration)}.
   * If no such service is available, a new {@link YarnServerFrameworkService} is used
   * as the default implementation.
   * </p>
   * <p>
   * It also initializes the {@link #amExtensions} by invoking
   * {@link FrameworkService#createAmExtensions()} on the resolved framework service.
   * </p>
   *
   * @param conf the Hadoop {@link Configuration} used to initialize the framework context
   */
  public FrameworkContext(Configuration conf) {
    this.frameworkService = FrameworkUtils.get(ServerFrameworkService.class, conf);
    this.amExtensions = frameworkService.createAmExtensions();
  }

  public ServerFrameworkService getFrameworkService() {
    return frameworkService;
  }

  public AmExtensions getAmExtensions() {
    return amExtensions;
  }
}
