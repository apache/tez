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

package org.apache.tez.dag.app.security;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class to manage ACLs for the Tez AM and DAGs and provides functionality to check whether
 * a user is authorized to take certain actions.
 */
public class ACLManager {

  private static final Log LOG = LogFactory.getLog(ACLManager.class);
  static final String WILDCARD_ACL_VALUE = "*";

  static enum ACLType {
    AM_VIEW_ACL,
    AM_MODIFY_ACL,
    DAG_VIEW_ACL,
    DAG_MODIFY_ACL
  }

  private final String dagUser;
  private final String amUser;
  private final Map<ACLType, Set<String>> users;
  private final Map<ACLType, Set<String>> groups;
  private final Groups userGroupMapping;
  private final boolean aclsEnabled;

  public ACLManager(Groups userGroupMapping, String amUser) {
    this(userGroupMapping, amUser, null);
  }

  public ACLManager(Groups userGroupMapping, String amUser, Configuration conf) {
    this.userGroupMapping = userGroupMapping;
    this.amUser = amUser;
    this.dagUser = null;
    this.users = new HashMap<ACLType, Set<String>>();
    this.groups = new HashMap<ACLType, Set<String>>();
    if (conf != null) {
      aclsEnabled = conf.getBoolean(TezConfiguration.TEZ_AM_ACLS_ENABLED,
          TezConfiguration.TEZ_AM_ACLS_ENABLED_DEFAULT);
      if (!aclsEnabled) {
        return;
      }
      ACLConfigurationParser parser = new ACLConfigurationParser(conf);
      if (parser.getAllowedUsers() != null) {
        this.users.putAll(parser.getAllowedUsers());
      }
      if (parser.getAllowedGroups() != null) {
        this.groups.putAll(parser.getAllowedGroups());
      }
    } else {
      aclsEnabled = true;
    }
  }

  public ACLManager(ACLManager amACLManager, String dagUser, Configuration dagConf) {
    this.userGroupMapping = amACLManager.userGroupMapping;
    this.amUser = amACLManager.amUser;
    this.dagUser = dagUser;
    this.users = amACLManager.users;
    this.groups = amACLManager.groups;
    this.aclsEnabled = amACLManager.aclsEnabled;
    if (!aclsEnabled) {
      return;
    }
    if (dagConf != null) {
      ACLConfigurationParser parser = new ACLConfigurationParser(dagConf, true);
      if (parser.getAllowedUsers() != null) {
        this.users.putAll(parser.getAllowedUsers());
      }
      if (parser.getAllowedGroups() != null) {
        this.groups.putAll(parser.getAllowedGroups());
      }
    }
  }

  @VisibleForTesting
  boolean checkAccess(String user, ACLType aclType) {
    if (!aclsEnabled) {
      return true;
    }
    if (amUser.equals(user)) {
      return true;
    }
    if (EnumSet.of(ACLType.DAG_MODIFY_ACL, ACLType.DAG_VIEW_ACL).contains(aclType)) {
      if (dagUser != null && dagUser.equals(user)) {
        return true;
      }
    }
    if (users != null && !users.isEmpty()) {
      Set<String> set = users.get(aclType);
      if (set != null) {
        if (set.contains(WILDCARD_ACL_VALUE)) {
          return true;
        }
        if (set.contains(user)) {
          return true;
        }
      }
    }
    if (groups != null && !groups.isEmpty()) {
      try {
        Set<String> set = groups.get(aclType);
        if (set != null) {
          Set<String> userGrps = userGroupMapping.getGroups(user);
          for (String userGrp : userGrps) {
            if (set.contains(userGrp)) {
              return true;
            }
          }
        }
      } catch (IOException e) {
        LOG.warn("Failed to retrieve groups for user"
            + ", user=" + user, e);
      }
    }
    return false;
  }

  public boolean checkAMViewAccess(String user) {
    return checkAccess(user, ACLType.AM_VIEW_ACL);
  }

  public boolean checkAMModifyAccess(String user) {
    return checkAccess(user, ACLType.AM_MODIFY_ACL);
  }

  public boolean checkDAGViewAccess(String user) {
    return checkAccess(user, ACLType.AM_VIEW_ACL)
        || checkAccess(user, ACLType.DAG_VIEW_ACL);
  }

  public boolean checkDAGModifyAccess(String user) {
    return checkAccess(user, ACLType.AM_MODIFY_ACL)
        || checkAccess(user, ACLType.DAG_MODIFY_ACL);
  }

}


