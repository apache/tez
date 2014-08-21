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

package org.apache.tez.client;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezConfiguration;

import com.google.common.collect.Maps;

@Private
class AMConfiguration {

  private Map<String, LocalResource> amLocalResources = Maps.newHashMap();
  private TezConfiguration tezConf;
  private Credentials credentials;
  private YarnConfiguration yarnConfig;
  private Map<String, String> env;
  private LocalResource binaryConfLRsrc;

  AMConfiguration(TezConfiguration tezConf, Map<String, LocalResource> localResources,
      Credentials credentials) {
    this.tezConf = tezConf;
    if (localResources != null) {
      addAMLocalResources(localResources);
    }
    if (credentials != null) {
      setCredentials(credentials);
    }

  }

  void addAMLocalResources(Map<String, LocalResource> localResources) {
    this.amLocalResources.putAll(localResources);
  }
  
  void clearAMLocalResources() {
    this.amLocalResources.clear();
  }
  
  void setCredentials(Credentials credentials) {
    this.credentials = credentials;
  }
  
  void setTezConfiguration(TezConfiguration tezConf) {
    this.tezConf = tezConf;
  }
  
  void setYarnConfiguration(YarnConfiguration yarnConf) {
    this.yarnConfig = yarnConf;
  }

  String getQueueName() {
    return this.tezConf.get(TezConfiguration.TEZ_QUEUE_NAME);
  }

  Map<String, LocalResource> getAMLocalResources() {
    return amLocalResources;
  }

  TezConfiguration getTezConfiguration() {
    return tezConf;
  }

  YarnConfiguration getYarnConfiguration() {
    return yarnConfig;
  }
  
  Credentials getCredentials() {
    return credentials;
  }
  
  Map<String, String> getEnv() {
    return env;
  }
  
  void setBinaryConfLR(LocalResource binaryConfLRsrc) {
    this.binaryConfLRsrc = binaryConfLRsrc;
  }
  
  LocalResource getBinaryConfLR() {
    return binaryConfLRsrc;
  }

}
