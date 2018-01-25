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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.yarn.util.RackResolver;

/**
 * Mock RackResolver
 * Overrides CachedDNSToSwitchMapping to ensure that it does not try to resolve hostnames
 */
public class MockDNSToSwitchMapping extends CachedDNSToSwitchMapping implements DNSToSwitchMapping {
  private static final Map<String, String> rackMap =
      Collections.synchronizedMap(new HashMap<String, String>());

  private final String defaultRack = "/default-rack";

  public MockDNSToSwitchMapping() {
    super(null);
  }

  @Override
  public List<String> resolve(List<String> strings) {
    List<String> resolvedHosts = new ArrayList<String>();
    for (String h : strings) {
      String rack = rackMap.get(h);
      if (rack == null) {
        rack = defaultRack;
      }
      resolvedHosts.add(rack);
    }
    return resolvedHosts;
  }

  @Override
  public void reloadCachedMappings() {
  }

  public void reloadCachedMappings(List<String> strings) {
  }

  public static void initializeMockRackResolver() {
    Configuration rackResolverConf = new Configuration(false);
    rackResolverConf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MockDNSToSwitchMapping.class.getName());
    RackResolver.init(rackResolverConf);
  }

  public static void addRackMapping(String host, String rack) {
    rackMap.put(host, rack);
  }
}
