/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.common.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.ACLInfo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestACLManager {

  private static final String[] noGroups = new String[0];

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testCurrentUserACLChecks() {
    UserGroupInformation currentUser = UserGroupInformation.createUserForTesting("currentUser", noGroups);
    UserGroupInformation dagUser = UserGroupInformation.createUserForTesting("dagUser", noGroups);
    UserGroupInformation user1 = UserGroupInformation.createUserForTesting("user1", noGroups);

    ACLManager aclManager = new ACLManager(currentUser.getShortUserName());

    UserGroupInformation user = user1;

    assertFalse(aclManager.checkAccess(user, ACLType.AM_VIEW_ACL));
    assertFalse(aclManager.checkAccess(user, ACLType.AM_MODIFY_ACL));

    user = currentUser;
    assertTrue(aclManager.checkAccess(user, ACLType.AM_VIEW_ACL));
    assertTrue(aclManager.checkAccess(user, ACLType.AM_MODIFY_ACL));

    aclManager = new ACLManager(currentUser.getShortUserName(), new Configuration(false));

    user = user1;
    assertFalse(aclManager.checkAccess(user, ACLType.AM_VIEW_ACL));
    assertFalse(aclManager.checkAccess(user, ACLType.AM_MODIFY_ACL));

    user = currentUser;
    assertTrue(aclManager.checkAccess(user, ACLType.AM_VIEW_ACL));
    assertTrue(aclManager.checkAccess(user, ACLType.AM_MODIFY_ACL));

    ACLManager dagAclManager = new ACLManager(aclManager, dagUser.getShortUserName(),
        ACLInfo.getDefaultInstance());
    user = dagUser;
    assertFalse(dagAclManager.checkAccess(user, ACLType.AM_VIEW_ACL));
    assertFalse(dagAclManager.checkAccess(user, ACLType.AM_MODIFY_ACL));
    assertTrue(dagAclManager.checkAccess(user, ACLType.DAG_VIEW_ACL));
    assertTrue(dagAclManager.checkAccess(user, ACLType.DAG_MODIFY_ACL));
    user = user1;
    assertFalse(dagAclManager.checkAccess(user, ACLType.DAG_VIEW_ACL));
    assertFalse(dagAclManager.checkAccess(user, ACLType.DAG_MODIFY_ACL));
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testOtherUserACLChecks() throws IOException {
    String[] groups1 = new String[] {"grp1", "grp2"};
    String[] groups2 = new String[] {"grp3", "grp4"};
    String[] groups3 = new String[] {"grp5", "grp6"};

    UserGroupInformation currentUser = UserGroupInformation.createUserForTesting("currentUser", noGroups);
    UserGroupInformation user1 = UserGroupInformation.createUserForTesting("user1", groups1); // belongs to grp1 and grp2
    UserGroupInformation user2 = UserGroupInformation.createUserForTesting("user2", groups2); // belongs to grp3 and grp4
    UserGroupInformation user3 = UserGroupInformation.createUserForTesting("user3", noGroups);
    UserGroupInformation user4 = UserGroupInformation.createUserForTesting("user4", noGroups);
    UserGroupInformation user5 = UserGroupInformation.createUserForTesting("user5", groups3); // belongs to grp5 and grp6
    UserGroupInformation user6 = UserGroupInformation.createUserForTesting("user6", noGroups);

    Configuration conf = new Configuration(false);
    // View ACLs: user1, user4, grp3, grp4.
    String viewACLs = user1.getShortUserName() + "," + user4.getShortUserName()
        + "   " + "grp3,grp4  ";
    // Modify ACLs: user3, grp6, grp7
    String modifyACLs = user3.getShortUserName() + "  " + "grp6,grp7";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, modifyACLs);

    ACLManager aclManager = new ACLManager(currentUser.getShortUserName(), conf);

    assertTrue(aclManager.checkAccess(currentUser, ACLType.AM_VIEW_ACL));
    assertTrue(aclManager.checkAccess(user1, ACLType.AM_VIEW_ACL));
    assertTrue(aclManager.checkAccess(user2, ACLType.AM_VIEW_ACL));
    assertFalse(aclManager.checkAccess(user3, ACLType.AM_VIEW_ACL));
    assertTrue(aclManager.checkAccess(user4, ACLType.AM_VIEW_ACL));
    assertFalse(aclManager.checkAccess(user5,  ACLType.AM_VIEW_ACL));
    assertFalse(aclManager.checkAccess(user6, ACLType.AM_VIEW_ACL));

    assertTrue(aclManager.checkAccess(currentUser, ACLType.AM_MODIFY_ACL));
    assertFalse(aclManager.checkAccess(user1, ACLType.AM_MODIFY_ACL));
    assertFalse(aclManager.checkAccess(user2, ACLType.AM_MODIFY_ACL));
    assertTrue(aclManager.checkAccess(user3, ACLType.AM_MODIFY_ACL));
    assertFalse(aclManager.checkAccess(user4, ACLType.AM_MODIFY_ACL));
    assertTrue(aclManager.checkAccess(user5, ACLType.AM_MODIFY_ACL));
    assertFalse(aclManager.checkAccess(user6, ACLType.AM_MODIFY_ACL));
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testNoGroupsACLChecks() throws IOException {
    String[] groups1 = new String[] {"grp1", "grp2"};
    String[] groups2 = new String[] {"grp3", "grp4"};
    String[] groups3 = new String[] {"grp5", "grp6"};

    UserGroupInformation currentUser = UserGroupInformation.createUserForTesting("currentUser", noGroups);
    UserGroupInformation user1 = UserGroupInformation.createUserForTesting("user1", groups1); // belongs to grp1 and grp2
    UserGroupInformation user2 = UserGroupInformation.createUserForTesting("user2", groups2); // belongs to grp3 and grp4
    UserGroupInformation user3 = UserGroupInformation.createUserForTesting("user3", noGroups);
    UserGroupInformation user4 = UserGroupInformation.createUserForTesting("user4", noGroups);
    UserGroupInformation user5 = UserGroupInformation.createUserForTesting("user5", groups3); // belongs to grp5 and grp6
    UserGroupInformation user6 = UserGroupInformation.createUserForTesting("user6", noGroups);

    Configuration conf = new Configuration(false);
    // View ACLs: user1, user4
    String viewACLs = user1.getShortUserName() + "," + user4.getShortUserName() + " ";
    // Modify ACLs: user3
    String modifyACLs = user3.getShortUserName() + " ";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, modifyACLs);

    ACLManager aclManager = new ACLManager(currentUser.getShortUserName(), conf);
    assertTrue(aclManager.checkAccess(currentUser, ACLType.AM_VIEW_ACL));
    assertTrue(aclManager.checkAccess(user1, ACLType.AM_VIEW_ACL));
    assertFalse(aclManager.checkAccess(user2, ACLType.AM_VIEW_ACL));
    assertFalse(aclManager.checkAccess(user3, ACLType.AM_VIEW_ACL));
    assertTrue(aclManager.checkAccess(user4, ACLType.AM_VIEW_ACL));
    assertFalse(aclManager.checkAccess(user5, ACLType.AM_VIEW_ACL));
    assertFalse(aclManager.checkAccess(user6, ACLType.AM_VIEW_ACL));

    assertTrue(aclManager.checkAccess(currentUser, ACLType.AM_MODIFY_ACL));
    assertFalse(aclManager.checkAccess(user1, ACLType.AM_MODIFY_ACL));
    assertFalse(aclManager.checkAccess(user2, ACLType.AM_MODIFY_ACL));
    assertTrue(aclManager.checkAccess(user3, ACLType.AM_MODIFY_ACL));
    assertFalse(aclManager.checkAccess(user4, ACLType.AM_MODIFY_ACL));
    assertFalse(aclManager.checkAccess(user5, ACLType.AM_MODIFY_ACL));
    assertFalse(aclManager.checkAccess(user6, ACLType.AM_MODIFY_ACL));
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void checkAMACLs() throws IOException {
    String[] groups1 = new String[] {"grp1", "grp2"};
    String[] groups2 = new String[] {"grp3", "grp4"};
    String[] groups3 = new String[] {"grp5", "grp6"};
    String[] admingroup1 = new String[] {"admgrp1"};

    UserGroupInformation currentUser = UserGroupInformation.createUserForTesting("currentUser", noGroups);
    UserGroupInformation user1 = UserGroupInformation.createUserForTesting("user1", groups1); // belongs to grp1 and grp2
    UserGroupInformation user2 = UserGroupInformation.createUserForTesting("user2", groups2); // belongs to grp3 and grp4
    UserGroupInformation user3 = UserGroupInformation.createUserForTesting("user3", noGroups);
    UserGroupInformation user4 = UserGroupInformation.createUserForTesting("user4", noGroups);
    UserGroupInformation user5 = UserGroupInformation.createUserForTesting("user5", groups3); // belongs to grp5 and grp6
    UserGroupInformation user6 = UserGroupInformation.createUserForTesting("user6", noGroups);
    UserGroupInformation admuser1 = UserGroupInformation.createUserForTesting("admuser1", admingroup1);
    UserGroupInformation admuser2 = UserGroupInformation.createUserForTesting("admuser2", noGroups);

    Configuration conf = new Configuration(false);
    // View ACLs: user1, user4, grp3, grp4.
    String viewACLs = "user1,user4,,   grp3,grp4  ";
    // Modify ACLs: user3, grp6, grp7
    String modifyACLs = "user3   grp6,grp7";
    // YARN Admin ACLs: admuser1, admgrp1
    String yarnAdminACLs = "admuser2,   admgrp1  ";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, modifyACLs);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, yarnAdminACLs);

    ACLManager aclManager = new ACLManager(currentUser.getShortUserName(), conf);

    assertTrue(aclManager.checkAMViewAccess(currentUser));
    assertTrue(aclManager.checkAMViewAccess(user1));
    assertTrue(aclManager.checkAMViewAccess(user2));
    assertFalse(aclManager.checkAMViewAccess(user3));
    assertTrue(aclManager.checkAMViewAccess(user4));
    assertFalse(aclManager.checkAMViewAccess(user5));
    assertFalse(aclManager.checkAMViewAccess(user6));
    assertTrue(aclManager.checkAMViewAccess(admuser1));
    assertTrue(aclManager.checkAMViewAccess(admuser2));

    assertTrue(aclManager.checkAMModifyAccess(currentUser));
    assertFalse(aclManager.checkAMModifyAccess(user1));
    assertFalse(aclManager.checkAMModifyAccess(user2));
    assertTrue(aclManager.checkAMModifyAccess(user3));
    assertFalse(aclManager.checkAMModifyAccess(user4));
    assertTrue(aclManager.checkAMModifyAccess(user5));
    assertFalse(aclManager.checkAMModifyAccess(user6));
    assertTrue(aclManager.checkAMModifyAccess(admuser1));
    assertTrue(aclManager.checkAMModifyAccess(admuser2));

    assertTrue(aclManager.checkDAGViewAccess(currentUser));
    assertTrue(aclManager.checkDAGViewAccess(user1));
    assertTrue(aclManager.checkDAGViewAccess(user2));
    assertFalse(aclManager.checkDAGViewAccess(user3));
    assertTrue(aclManager.checkDAGViewAccess(user4));
    assertFalse(aclManager.checkDAGViewAccess(user5));
    assertFalse(aclManager.checkDAGViewAccess(user6));
    assertTrue(aclManager.checkDAGViewAccess(admuser1));
    assertTrue(aclManager.checkDAGViewAccess(admuser2));

    assertTrue(aclManager.checkDAGModifyAccess(currentUser));
    assertFalse(aclManager.checkDAGModifyAccess(user1));
    assertFalse(aclManager.checkDAGModifyAccess(user2));
    assertTrue(aclManager.checkDAGModifyAccess(user3));
    assertFalse(aclManager.checkDAGModifyAccess(user4));
    assertTrue(aclManager.checkDAGModifyAccess(user5));
    assertFalse(aclManager.checkDAGModifyAccess(user6));
    assertTrue(aclManager.checkDAGModifyAccess(admuser1));
    assertTrue(aclManager.checkDAGModifyAccess(admuser2));
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void checkDAGACLs() throws IOException {
    String[] groups1 = new String[] {"grp1", "grp2"};
    String[] groups2 = new String[] {"grp3", "grp4"};
    String[] groups3 = new String[] {"grp5", "grp6"};
    String[] admingroup1 = new String[] {"admgrp1"};

    UserGroupInformation currentUser = UserGroupInformation.createUserForTesting("currentUser", noGroups);
    UserGroupInformation user1 = UserGroupInformation.createUserForTesting("user1", groups1); // belongs to grp1 and grp2
    UserGroupInformation user2 = UserGroupInformation.createUserForTesting("user2", groups2); // belongs to grp3 and grp4
    UserGroupInformation user3 = UserGroupInformation.createUserForTesting("user3", noGroups);
    UserGroupInformation user4 = UserGroupInformation.createUserForTesting("user4", noGroups);
    UserGroupInformation user5 = UserGroupInformation.createUserForTesting("user5", groups3); // belongs to grp5 and grp6
    UserGroupInformation user6 = UserGroupInformation.createUserForTesting("user6", noGroups);
    UserGroupInformation admuser1 = UserGroupInformation.createUserForTesting("admuser1", admingroup1);
    UserGroupInformation admuser2 = UserGroupInformation.createUserForTesting("admuser2", noGroups);

    Configuration conf = new Configuration(false);
    // View ACLs: user1, user4, grp3, grp4.
    String viewACLs = "user1,user4,,   grp3,grp4  ";
    // Modify ACLs: user3, grp6, grp7
    String modifyACLs = "user3   grp6,grp7";
    // YARN Admin ACLs: admuser1, admgrp1
    String yarnAdminACLs = "admuser2,   admgrp1  ";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, modifyACLs);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, yarnAdminACLs);

    ACLInfo.Builder builder = ACLInfo.newBuilder();
    // DAG View ACLs: user6, grp5
    builder.addUsersWithViewAccess("user6");
    builder.addGroupsWithViewAccess("grp5");

    // DAG Modify ACLs: user6,user5
    builder.addUsersWithModifyAccess("user6");
    builder.addUsersWithModifyAccess("user7");

    UserGroupInformation dagUser = UserGroupInformation.createUserForTesting("dagUser", noGroups);

    ACLManager amAclManager = new ACLManager(currentUser.getShortUserName(), conf);
    ACLManager aclManager = new ACLManager(amAclManager, dagUser.getShortUserName(),
        builder.build());

    assertTrue(aclManager.checkAMViewAccess(currentUser));
    assertFalse(aclManager.checkAMViewAccess(dagUser));
    assertTrue(aclManager.checkAMViewAccess(user1));
    assertTrue(aclManager.checkAMViewAccess(user2));
    assertFalse(aclManager.checkAMViewAccess(user3));
    assertTrue(aclManager.checkAMViewAccess(user4));
    assertFalse(aclManager.checkAMViewAccess(user5));
    assertFalse(aclManager.checkAMViewAccess(user6));
    assertTrue(aclManager.checkAMViewAccess(admuser1));
    assertTrue(aclManager.checkAMViewAccess(admuser2));

    assertTrue(aclManager.checkAMModifyAccess(currentUser));
    assertFalse(aclManager.checkAMModifyAccess(dagUser));
    assertFalse(aclManager.checkAMModifyAccess(user1));
    assertFalse(aclManager.checkAMModifyAccess(user2));
    assertTrue(aclManager.checkAMModifyAccess(user3));
    assertFalse(aclManager.checkAMModifyAccess(user4));
    assertTrue(aclManager.checkAMModifyAccess(user5));
    assertFalse(aclManager.checkAMModifyAccess(user6));
    assertTrue(aclManager.checkAMModifyAccess(admuser1));
    assertTrue(aclManager.checkAMModifyAccess(admuser2));

    assertTrue(aclManager.checkDAGViewAccess(currentUser));
    assertTrue(aclManager.checkDAGViewAccess(dagUser));
    assertTrue(aclManager.checkDAGViewAccess(user1));
    assertTrue(aclManager.checkDAGViewAccess(user2));
    assertFalse(aclManager.checkDAGViewAccess(user3));
    assertTrue(aclManager.checkDAGViewAccess(user4));
    assertTrue(aclManager.checkDAGViewAccess(user5));
    assertTrue(aclManager.checkDAGViewAccess(user6));
    assertTrue(aclManager.checkDAGViewAccess(admuser1));
    assertTrue(aclManager.checkDAGViewAccess(admuser2));

    assertTrue(aclManager.checkDAGModifyAccess(currentUser));
    assertTrue(aclManager.checkDAGModifyAccess(dagUser));
    assertFalse(aclManager.checkDAGModifyAccess(user1));
    assertFalse(aclManager.checkDAGModifyAccess(user2));
    assertTrue(aclManager.checkDAGModifyAccess(user3));
    assertFalse(aclManager.checkDAGModifyAccess(user4));
    assertTrue(aclManager.checkDAGModifyAccess(user5));
    assertTrue(aclManager.checkDAGModifyAccess(user6));
    assertTrue(aclManager.checkDAGModifyAccess(admuser1));
    assertTrue(aclManager.checkDAGModifyAccess(admuser2));

  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testWildCardCheck() {
    Configuration conf = new Configuration(false);
    String viewACLs = "   *  ";
    String modifyACLs = "   * ";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, modifyACLs);

    UserGroupInformation a1 = UserGroupInformation.createUserForTesting("a1", noGroups);
    UserGroupInformation u1 = UserGroupInformation.createUserForTesting("u1", noGroups);

    ACLManager aclManager = new ACLManager(a1.getShortUserName(), conf);
    assertTrue(aclManager.checkAMViewAccess(a1));
    assertTrue(aclManager.checkAMViewAccess(u1));
    assertTrue(aclManager.checkAMModifyAccess(a1));
    assertTrue(aclManager.checkAMModifyAccess(u1));
    assertTrue(aclManager.checkDAGViewAccess(a1));
    assertTrue(aclManager.checkDAGViewAccess(u1));
    assertTrue(aclManager.checkDAGModifyAccess(a1));
    assertTrue(aclManager.checkDAGModifyAccess(u1));
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testAdminWildCardCheck() {
    Configuration conf = new Configuration(false);
    String yarnAdminACLs = " *  ";
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, yarnAdminACLs);

    UserGroupInformation a1 = UserGroupInformation.createUserForTesting("a1", noGroups);
    UserGroupInformation u1 = UserGroupInformation.createUserForTesting("u1", noGroups);

    ACLManager aclManager = new ACLManager(a1.getShortUserName(), conf);
    assertTrue(aclManager.checkAMViewAccess(a1));
    assertTrue(aclManager.checkAMViewAccess(u1));
    assertTrue(aclManager.checkAMModifyAccess(a1));
    assertTrue(aclManager.checkAMModifyAccess(u1));
    assertTrue(aclManager.checkDAGViewAccess(a1));
    assertTrue(aclManager.checkDAGViewAccess(u1));
    assertTrue(aclManager.checkDAGModifyAccess(a1));
    assertTrue(aclManager.checkDAGModifyAccess(u1));
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testACLsDisabled() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TezConfiguration.TEZ_AM_ACLS_ENABLED, false);
    String viewACLs = "a2,u2  ";
    String modifyACLs = "a2,u2 ";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, modifyACLs);

    UserGroupInformation a1 = UserGroupInformation.createUserForTesting("a1", noGroups);
    UserGroupInformation u1 = UserGroupInformation.createUserForTesting("u1", noGroups);

    ACLManager aclManager = new ACLManager(a1.getShortUserName(), conf);
    assertTrue(aclManager.checkAMViewAccess(a1));
    assertTrue(aclManager.checkAMViewAccess(u1));
    assertTrue(aclManager.checkAMModifyAccess(a1));
    assertTrue(aclManager.checkAMModifyAccess(u1));
    assertTrue(aclManager.checkDAGViewAccess(a1));
    assertTrue(aclManager.checkDAGViewAccess(u1));
    assertTrue(aclManager.checkDAGModifyAccess(a1));
    assertTrue(aclManager.checkDAGModifyAccess(u1));

    ACLManager dagAclManager = new ACLManager(aclManager, "dagUser", null);
    assertTrue(dagAclManager.checkAMViewAccess(a1));
    assertTrue(dagAclManager.checkAMViewAccess(u1));
    assertTrue(dagAclManager.checkAMModifyAccess(a1));
    assertTrue(dagAclManager.checkAMModifyAccess(u1));
    assertTrue(dagAclManager.checkDAGViewAccess(a1));
    assertTrue(dagAclManager.checkDAGViewAccess(u1));
    assertTrue(dagAclManager.checkDAGModifyAccess(a1));
    assertTrue(dagAclManager.checkDAGModifyAccess(u1));
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testConvertToYARNACLs() {
    String currentUser = "c1";
    Configuration conf = new Configuration(false);
    String viewACLs = "user1,user4,,   grp3,grp4  ";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, "   * ");
    ACLManager aclManager = new ACLManager(currentUser, conf);

    Map<ApplicationAccessType, String> yarnAcls = aclManager.toYARNACls();
    assertTrue(yarnAcls.containsKey(ApplicationAccessType.VIEW_APP));
    assertEquals("c1,user1,user4 grp3,grp4",
        yarnAcls.get(ApplicationAccessType.VIEW_APP));
    assertTrue(yarnAcls.containsKey(ApplicationAccessType.MODIFY_APP));
    assertEquals("*",
        yarnAcls.get(ApplicationAccessType.MODIFY_APP));

    viewACLs = "   grp3,grp4  ";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    ACLManager aclManager1 = new ACLManager(currentUser, conf);
    yarnAcls = aclManager1.toYARNACls();
    assertEquals("c1 grp3,grp4",
        yarnAcls.get(ApplicationAccessType.VIEW_APP));

  }

}
