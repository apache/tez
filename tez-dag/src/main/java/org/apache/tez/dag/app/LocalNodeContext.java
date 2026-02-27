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

/** Local implementation of NodeContext. */
public final class LocalNodeContext implements NodeContext {

  private final String nodeHostString;
  private final String nodePortString;
  private final String nodeHttpPortString;

  public LocalNodeContext(String nodeHostString, int nodePortString, int nmHttpPort) {
    this.nodeHostString = nodeHostString;
    this.nodePortString = String.valueOf(nodePortString);
    this.nodeHttpPortString = String.valueOf(nmHttpPort);
  }

  @Override
  public String getNodeHostString() {
    return nodeHostString;
  }

  @Override
  public String getNodePortString() {
    return nodePortString;
  }

  @Override
  public String getNodeHttpPortString() {
    return nodeHttpPortString;
  }
}
