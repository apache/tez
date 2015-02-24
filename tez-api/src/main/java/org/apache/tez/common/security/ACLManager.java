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
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.tez.dag.api.TezConfiguration;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class to manage ACLs for the Tez AM and DAGs and provides functionality to check whether
 * a user is authorized to take certain actions.
 */
@Private
public class ACLManager {

  private static final Log LOG = LogFactory.getLog(ACLManager.class);
  public static final String WILDCARD_ACL_VALUE = "*";

  private final String dagUser;
  private final String amUser;
  private final Map<ACLType, Set<String>> users;
  private final Map<ACLType, Set<String>> groups;
  private final boolean aclsEnabled;

  public ACLManager(String amUser) {
    this(amUser, new Configuration(false));
  }

  public ACLManager(String amUser, Configuration conf) {
    this.amUser = amUser;
    this.dagUser = null;
    this.users = new HashMap<ACLType, Set<String>>();
    this.groups = new HashMap<ACLType, Set<String>>();
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
  }

  public ACLManager(ACLManager amACLManager, String dagUser, Configuration dagConf) {
    this.amUser = amACLManager.amUser;
    this.dagUser = dagUser;
    this.users = amACLManager.users;
    this.groups = amACLManager.groups;
    this.aclsEnabled = amACLManager.aclsEnabled;
    if (!aclsEnabled) {
      return;
    }
    ACLConfigurationParser parser = new ACLConfigurationParser(dagConf, true);
    if (parser.getAllowedUsers() != null) {
      this.users.putAll(parser.getAllowedUsers());
    }
    if (parser.getAllowedGroups() != null) {
      this.groups.putAll(parser.getAllowedGroups());
    }
  }

  public boolean isAclsEnabled() {
    return aclsEnabled;
  }

  @VisibleForTesting
  boolean checkAccess(UserGroupInformation ugi, ACLType aclType) {

    if (!aclsEnabled) {
      return true;
    }

    String user = ugi.getShortUserName();
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

    Collection<String> userGroups = Arrays.asList(ugi.getGroupNames());
    if (userGroups != null && !userGroups.isEmpty()
        && groups != null && !groups.isEmpty()) {
      Set<String> set = groups.get(aclType);
      if (set != null) {
        for (String userGrp : userGroups) {
          if (set.contains(userGrp)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public boolean checkAMViewAccess(UserGroupInformation ugi) {
    return checkAccess(ugi, ACLType.AM_VIEW_ACL);
  }

  public boolean checkAMModifyAccess(UserGroupInformation ugi) {
    return checkAccess(ugi, ACLType.AM_MODIFY_ACL);
  }

  public boolean checkDAGViewAccess(UserGroupInformation ugi) {
    return checkAccess(ugi, ACLType.AM_VIEW_ACL)
        || checkAccess(ugi, ACLType.DAG_VIEW_ACL);
  }

  public boolean checkDAGModifyAccess(UserGroupInformation ugi) {
    return checkAccess(ugi, ACLType.AM_MODIFY_ACL)
        || checkAccess(ugi, ACLType.DAG_MODIFY_ACL);
  }

  public Map<ApplicationAccessType, String> toYARNACls() {
    Map<ApplicationAccessType, String> acls = new HashMap<ApplicationAccessType, String>(2);
    if (!this.aclsEnabled) {
      acls.put(ApplicationAccessType.VIEW_APP, "*");
      acls.put(ApplicationAccessType.MODIFY_APP, "*");
      return acls;
    }
    acls.put(ApplicationAccessType.VIEW_APP, amUser);
    acls.put(ApplicationAccessType.MODIFY_APP, amUser);
    boolean viewAclsWildCard = false;
    boolean modifyAclsWildCard = false;
    if (users != null && !users.isEmpty()) {
      for (Entry<ACLType, Set<String>> entry : users.entrySet()) {
        if (entry.getKey().equals(ACLType.AM_VIEW_ACL)) {
          if (entry.getValue().contains(WILDCARD_ACL_VALUE)) {
            acls.put(ApplicationAccessType.VIEW_APP, "*");
            viewAclsWildCard = true;
            continue;
          } else if (!entry.getValue().isEmpty()) {
            String aclsStr = acls.get(ApplicationAccessType.VIEW_APP);
            String commaSepList = toCommaSeparatedString(entry.getValue());
            if (!commaSepList.isEmpty()) {
              aclsStr += "," + commaSepList;
            }
            acls.put(ApplicationAccessType.VIEW_APP, aclsStr);
          }
        } else if (entry.getKey().equals(ACLType.AM_MODIFY_ACL)) {
          if (entry.getValue().contains(WILDCARD_ACL_VALUE)) {
            acls.put(ApplicationAccessType.MODIFY_APP, "*");
            modifyAclsWildCard = true;
            continue;
          } else if (!entry.getValue().isEmpty()) {
            String aclsStr = acls.get(ApplicationAccessType.MODIFY_APP);
            String commaSepList = toCommaSeparatedString(entry.getValue());
            if (!commaSepList.isEmpty()) {
              aclsStr += "," + commaSepList;
            }
            acls.put(ApplicationAccessType.MODIFY_APP, aclsStr);
          }
        }
      }
    }
    if (groups != null && !groups.isEmpty()) {
      for (Entry<ACLType, Set<String>> entry : groups.entrySet()) {
        if (entry.getKey().equals(ACLType.AM_VIEW_ACL)
          && !viewAclsWildCard && !entry.getValue().isEmpty()) {
          // Append groups only if wild card not set
          String aclsStr = acls.containsKey(ApplicationAccessType.VIEW_APP) ?
              acls.get(ApplicationAccessType.VIEW_APP) : "";
          aclsStr += " " + toCommaSeparatedString(entry.getValue());
          acls.put(ApplicationAccessType.VIEW_APP, aclsStr);
        } else if (entry.getKey().equals(ACLType.AM_MODIFY_ACL)
            && !modifyAclsWildCard && !entry.getValue().isEmpty()) {
          // Append groups only if wild card not set
          String aclsStr = acls.containsKey(ApplicationAccessType.MODIFY_APP) ?
              acls.get(ApplicationAccessType.MODIFY_APP) : "";
          aclsStr += " " + toCommaSeparatedString(entry.getValue());
          acls.put(ApplicationAccessType.MODIFY_APP, aclsStr);
        }
      }
    }
    return acls;
  }

  public static String toCommaSeparatedString(Collection<String> collection) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String s : collection) {
      if (!first) {
        sb.append(",");
      } else {
        first = false;
      }
      sb.append(s);
    }
    return sb.toString();
  }
}


