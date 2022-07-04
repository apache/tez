/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.common.security;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;

import com.google.common.collect.ImmutableMap;

/**
 * Access controls for the DAG
 */
@Public
public class DAGAccessControls {

  private final Set<String> usersWithViewACLs;
  private final Set<String> usersWithModifyACLs;
  private final Set<String> groupsWithViewACLs;
  private final Set<String> groupsWithModifyACLs;

  public DAGAccessControls() {
    this.usersWithViewACLs = new HashSet<String>();
    this.usersWithModifyACLs = new HashSet<String>();
    this.groupsWithViewACLs = new HashSet<String>();
    this.groupsWithModifyACLs = new HashSet<String>();
  }

  /**
   * Helper API to support YARN and MapReduce format for specifying users and groups.
   * Format supports a comma-separated list of users and groups with the users and groups separated
   * by whitespace. e.g. "user1,user2 group1,group2"
   * If the value specified is "*", all users are allowed to do the operation.
   * @param viewACLsStr
   * @param modifyACLsStr
   */
  public DAGAccessControls(String viewACLsStr, String modifyACLsStr) {
    final Configuration conf = new Configuration(false);
    conf.set(TezConstants.TEZ_DAG_VIEW_ACLS, (viewACLsStr != null ? viewACLsStr : ""));
    conf.set(TezConstants.TEZ_DAG_MODIFY_ACLS, (modifyACLsStr != null ? modifyACLsStr : ""));
    ACLConfigurationParser parser = new ACLConfigurationParser(conf, true);

    this.usersWithViewACLs = new HashSet<String>();
    this.usersWithModifyACLs = new HashSet<String>();
    this.groupsWithViewACLs = new HashSet<String>();
    this.groupsWithModifyACLs = new HashSet<String>();

    Map<ACLType, Set<String>> allowedUsers = parser.getAllowedUsers();
    Map<ACLType, Set<String>> allowedGroups = parser.getAllowedGroups();

    if (allowedUsers.containsKey(ACLType.DAG_VIEW_ACL)) {
      this.usersWithViewACLs.addAll(allowedUsers.get(ACLType.DAG_VIEW_ACL));
    }
    if (allowedUsers.containsKey(ACLType.DAG_MODIFY_ACL)) {
      this.usersWithModifyACLs.addAll(allowedUsers.get(ACLType.DAG_MODIFY_ACL));
    }
    if (allowedGroups.containsKey(ACLType.DAG_VIEW_ACL)) {
      this.groupsWithViewACLs.addAll(allowedGroups.get(ACLType.DAG_VIEW_ACL));
    }
    if (allowedGroups.containsKey(ACLType.DAG_MODIFY_ACL)) {
      this.groupsWithModifyACLs.addAll(allowedGroups.get(ACLType.DAG_MODIFY_ACL));
    }
  }

  /**
   * Sets the list of users with view permissions on the DAG. If all users are allowed,
   * pass in a single entry "*"
   * @param users Set of users with view permissions
   * @return this object for further chained method calls
   */
  public synchronized DAGAccessControls setUsersWithViewACLs(Collection<String> users) {
    this.usersWithViewACLs.clear();
    this.usersWithViewACLs.addAll(users);
    return this;
  }

  /**
   * Sets the list of users with modify permissions on the DAG. If all users are allowed,
   * pass in a single entry "*"
   * @param users Set of users with modify permissions
   * @return this object for further chained method calls
   */
  public synchronized DAGAccessControls setUsersWithModifyACLs(Collection<String> users) {
    this.usersWithModifyACLs.clear();
    this.usersWithModifyACLs.addAll(users);
    return this;
  }

  /**
   * Sets the list of groups with view permissions on the DAG.
   * @param groups Set of groups with view permissions
   * @return this object for further chained method calls
   */
  public synchronized DAGAccessControls setGroupsWithViewACLs(Collection<String> groups) {
    this.groupsWithViewACLs.clear();
    this.groupsWithViewACLs.addAll(groups);
    return this;
  }

  /**
   * Sets the list of groups with modify permissions on the DAG.
   * @param groups Set of groups with modify permissions
   * @return this object for further chained method calls
   */
  public synchronized DAGAccessControls setGroupsWithModifyACLs(Collection<String> groups) {
    this.groupsWithModifyACLs.clear();
    this.groupsWithModifyACLs.addAll(groups);
    return this;
  }

  @Private
  public synchronized Set<String> getUsersWithViewACLs() {
    return Collections.unmodifiableSet(usersWithViewACLs);
  }

  @Private
  public synchronized Set<String> getUsersWithModifyACLs() {
    return Collections.unmodifiableSet(usersWithModifyACLs);
  }

  @Private
  public synchronized Set<String> getGroupsWithViewACLs() {
    return Collections.unmodifiableSet(groupsWithViewACLs);
  }

  @Private
  public synchronized Set<String> getGroupsWithModifyACLs() {
    return Collections.unmodifiableSet(groupsWithModifyACLs);
  }

  /**
   * Merge the dag acls with the AM acls in the configuration object. The config object will contain
   * the updated acls.
   * @param conf The AM config.
   */
  @Private
  public synchronized void mergeIntoAmAcls(Configuration conf) {
    ACLConfigurationParser parser = new ACLConfigurationParser(conf, false);
    parser.addAllowedGroups(ImmutableMap.of(
        ACLType.AM_VIEW_ACL, groupsWithViewACLs, ACLType.AM_MODIFY_ACL, groupsWithModifyACLs));
    parser.addAllowedUsers(ImmutableMap.of(
        ACLType.AM_VIEW_ACL, usersWithViewACLs, ACLType.AM_MODIFY_ACL, usersWithModifyACLs));

    Set<String> viewUsers = parser.getAllowedUsers().get(ACLType.AM_VIEW_ACL);
    Set<String> viewGroups = parser.getAllowedGroups().get(ACLType.AM_VIEW_ACL);
    if (viewUsers.contains(ACLManager.WILDCARD_ACL_VALUE)) {
      conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, ACLManager.WILDCARD_ACL_VALUE);
    } else {
      String userList = ACLManager.toCommaSeparatedString(viewUsers);
      String groupList = ACLManager.toCommaSeparatedString(viewGroups);
      conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, userList + " " + groupList);
    }

    Set<String> modifyUsers = parser.getAllowedUsers().get(ACLType.AM_MODIFY_ACL);
    Set<String> modifyGroups = parser.getAllowedGroups().get(ACLType.AM_MODIFY_ACL);
    if (modifyUsers.contains(ACLManager.WILDCARD_ACL_VALUE)) {
      conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, ACLManager.WILDCARD_ACL_VALUE);
    } else {
      String userList = ACLManager.toCommaSeparatedString(modifyUsers);
      String groupList = ACLManager.toCommaSeparatedString(modifyGroups);
      conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, userList + " " + groupList);
    }
  }
}
