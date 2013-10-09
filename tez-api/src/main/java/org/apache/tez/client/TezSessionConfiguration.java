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

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezConfiguration;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class TezSessionConfiguration {

  private final AMConfiguration amConfiguration;
  private final YarnConfiguration yarnConfig;
  private final TezConfiguration tezConfig;
  private final Map<String, LocalResource> sessionResources;

  public TezSessionConfiguration(AMConfiguration amConfiguration,
      TezConfiguration tezConfig) {
    this(amConfiguration, tezConfig, new YarnConfiguration(tezConfig));
  }

  TezSessionConfiguration(AMConfiguration amConfiguration,
                          TezConfiguration tezConfig,
                          YarnConfiguration yarnConf) {
    this(amConfiguration, tezConfig, yarnConf,
      new TreeMap<String, LocalResource>());
  }

  /**
   * TezSessionConfiguration constructor
   * @param amConfiguration AM Configuration @see AMConfiguration
   * @param tezConfig Tez Configuration
   * @param yarnConf Yarn Configuration
   * @param sessionResources LocalResources accessible to all tasks that are
   *                         launched within this session.
   */
  TezSessionConfiguration(AMConfiguration amConfiguration,
      TezConfiguration tezConfig,
      YarnConfiguration yarnConf,
      Map<String, LocalResource> sessionResources) {
    this.amConfiguration = amConfiguration;
    this.tezConfig = tezConfig;
    this.yarnConfig = yarnConf;
    this.sessionResources = sessionResources;
  }

  public AMConfiguration getAMConfiguration() {
    return amConfiguration;
  }

  public YarnConfiguration getYarnConfiguration() {
    return yarnConfig;
  }

  public TezConfiguration getTezConfiguration() {
    return tezConfig;
  }

  public Map<String, LocalResource> getSessionResources() {
    return Collections.unmodifiableMap(sessionResources);
  }

}
