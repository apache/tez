/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.dag.app;

import java.util.function.Supplier;

import org.apache.hadoop.yarn.api.ApplicationConstants;

/**
 * YARN specific implementation of NodeContext. Uses Suppliers to lazily resolve YARN environment
 * variables only when they are first requested, avoiding eager resolution during DAGAppMaster
 * startup.
 */
public final class YarnNodeManagerContext implements NodeContext {

  // Preserving original variable names conceptually inside the suppliers
  private final Supplier<String> nodeHostStringSupplier;
  private final Supplier<String> nodePortStringSupplier;
  private final Supplier<String> nodeHttpPortStringSupplier;

  public YarnNodeManagerContext() {
    this.nodeHostStringSupplier =
        () -> System.getenv(ApplicationConstants.Environment.NM_HOST.name());
    this.nodePortStringSupplier =
        () -> System.getenv(ApplicationConstants.Environment.NM_PORT.name());
    this.nodeHttpPortStringSupplier =
        () -> System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.name());
  }

  @Override
  public String getNodeHostString() {
    return nodeHostStringSupplier.get();
  }

  @Override
  public String getNodePortString() {
    return nodePortStringSupplier.get();
  }

  @Override
  public String getNodeHttpPortString() {
    return nodeHttpPortStringSupplier.get();
  }
}
