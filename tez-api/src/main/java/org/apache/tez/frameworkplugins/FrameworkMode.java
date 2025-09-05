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

/*
 A FrameworkMode is a pair of classes implementing (ClientFrameworkService, ServerFrameworkService)
 Clients using one FrameworkMode should only connect to AMs using the same FrameworkMode
 It is the responsibility of the user to setup their environment/configs to ensure matching modes
 e.g. a client using a mode that requires a Zookeeper-based registry should not be configured
      to interact with AMs that do not keep a Zookeeper-based registry
 */
public enum FrameworkMode {

  STANDALONE_ZOOKEEPER(
      "org.apache.tez.frameworkplugins.zookeeper.ZookeeperStandaloneClientFrameworkService",
      "org.apache.tez.frameworkplugins.zookeeper.ZookeeperStandaloneServerFrameworkService");

  String clientClassName;
  String serverClassName;

  FrameworkMode(String clientClassName, String serverClassName) {
    this.clientClassName = clientClassName;
    this.serverClassName = serverClassName;
  }
}
