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

package org.apache.tez.common.security;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;

import com.google.common.collect.Sets;

/**
 * Parser for extracting ACL information from Configs
 */
@Private
public class ACLConfigurationParser {

  private static final Logger LOG = LoggerFactory.getLogger(ACLConfigurationParser.class);

  private final Configuration conf;
  private final Map<ACLType, Set<String>> allowedUsers;
  private final Map<ACLType, Set<String>> allowedGroups;
  private static final Pattern splitPattern = Pattern.compile("\\s+");

  public ACLConfigurationParser(Configuration conf) {
    this(conf, false);
  }

  public ACLConfigurationParser(Configuration conf, boolean dagACLs) {
    this.conf = conf;
    allowedUsers = new HashMap<ACLType, Set<String>>(2);
    allowedGroups = new HashMap<ACLType, Set<String>>(2);
    parse(dagACLs);
  }


  private void parse(boolean dagACLs) {
    if (!dagACLs) {
      parseACLType(TezConfiguration.TEZ_AM_VIEW_ACLS, ACLType.AM_VIEW_ACL);
      parseACLType(TezConfiguration.TEZ_AM_MODIFY_ACLS, ACLType.AM_MODIFY_ACL);
    } else {
      parseACLType(TezConstants.TEZ_DAG_VIEW_ACLS, ACLType.DAG_VIEW_ACL);
      parseACLType(TezConstants.TEZ_DAG_MODIFY_ACLS, ACLType.DAG_MODIFY_ACL);
    }
  }

  private boolean isWildCard(String aclStr) {
    return aclStr.trim().equals(ACLManager.WILDCARD_ACL_VALUE);
  }

  private void parseACLType(String configProperty, ACLType aclType) {
    String aclsStr = conf.get(configProperty);
    if (aclsStr == null || aclsStr.isEmpty()) {
      return;
    }
    if (isWildCard(aclsStr)) {
      allowedUsers.put(aclType, Sets.newHashSet(ACLManager.WILDCARD_ACL_VALUE));
      return;
    }

    final String[] splits = splitPattern.split(aclsStr);
    int counter = -1;
    String userListStr = null;
    String groupListStr = null;
    for (String s : splits) {
      if (s.isEmpty()) {
        if (userListStr != null) {
          continue;
        }
      }
      ++counter;
      if (counter == 0) {
        userListStr = s;
      } else if (counter == 1) {
        groupListStr = s;
      } else {
        LOG.warn("Invalid configuration specified for " + configProperty
            + ", ignoring configured ACLs, value=" + aclsStr);
        return;
      }
    }

    if (userListStr == null) {
      return;
    }
    if (userListStr.length() >= 1) {
      allowedUsers.put(aclType,
          Sets.newLinkedHashSet(Arrays.asList(TezCommonUtils.getTrimmedStrings(userListStr))));
    }
    if (groupListStr != null && groupListStr.length() >= 1) {
      allowedGroups.put(aclType,
          Sets.newLinkedHashSet(Arrays.asList(TezCommonUtils.getTrimmedStrings(groupListStr))));
    }

  }

  public Map<ACLType, Set<String>> getAllowedUsers() {
    return Collections.unmodifiableMap(allowedUsers);
  }

  public Map<ACLType, Set<String>> getAllowedGroups() {
    return Collections.unmodifiableMap(allowedGroups);
  }

  public void addAllowedUsers(Map<ACLType, Set<String>> additionalAllowedUsers) {
    for (Entry<ACLType, Set<String>> entry : additionalAllowedUsers.entrySet()) {
      if (allowedUsers.containsKey(entry.getKey())) {
        allowedUsers.get(entry.getKey()).addAll(entry.getValue());
      } else {
        allowedUsers.put(entry.getKey(), entry.getValue());
      }
    }
  }

  public void addAllowedGroups(Map<ACLType, Set<String>> additionalAllowedGroups) {
    for (Entry<ACLType, Set<String>> entry : additionalAllowedGroups.entrySet()) {
      if (allowedGroups.containsKey(entry.getKey())) {
        allowedGroups.get(entry.getKey()).addAll(entry.getValue());
      } else {
        allowedGroups.put(entry.getKey(), entry.getValue());
      }
    }
  }

}
