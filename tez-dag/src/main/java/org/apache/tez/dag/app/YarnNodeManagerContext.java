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

import org.apache.hadoop.yarn.api.ApplicationConstants;

/** YARN specific implementation of NodeContext. Resolves YARN environment variables on demand. */
public final class YarnNodeManagerContext implements NodeContext {

  @Override
  public String nodeHost() {
    return System.getenv(ApplicationConstants.Environment.NM_HOST.name());
  }

  @Override
  public int nodePort() {
    String port = System.getenv(ApplicationConstants.Environment.NM_PORT.name());
    return Integer.parseInt(port);
  }

  @Override
  public int nodeHttpPort() {
    String port = System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.name());
    return Integer.parseInt(port);
  }
}
