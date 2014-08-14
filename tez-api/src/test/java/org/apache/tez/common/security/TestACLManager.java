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

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestACLManager {

  @Test
  public void testCurrentUserACLChecks() {
    Groups groups = null;
    String currentUser = "currentUser";
    ACLManager aclManager = new ACLManager(groups, currentUser);

    String user = "user1";
    Assert.assertFalse(aclManager.checkAccess(user, ACLType.AM_VIEW_ACL));
    Assert.assertFalse(aclManager.checkAccess(user, ACLType.AM_MODIFY_ACL));

    user = currentUser;
    Assert.assertTrue(aclManager.checkAccess(user, ACLType.AM_VIEW_ACL));
    Assert.assertTrue(aclManager.checkAccess(user, ACLType.AM_MODIFY_ACL));

    aclManager = new ACLManager(groups, currentUser, new Configuration(false));

    user = "user1";
    Assert.assertFalse(aclManager.checkAccess(user, ACLType.AM_VIEW_ACL));
    Assert.assertFalse(aclManager.checkAccess(user, ACLType.AM_MODIFY_ACL));

    user = currentUser;
    Assert.assertTrue(aclManager.checkAccess(user, ACLType.AM_VIEW_ACL));
    Assert.assertTrue(aclManager.checkAccess(user, ACLType.AM_MODIFY_ACL));

    String dagUser = "dagUser";
    ACLManager dagAclManager = new ACLManager(aclManager, dagUser, new Configuration(false));
    user = dagUser;
    Assert.assertFalse(dagAclManager.checkAccess(user, ACLType.AM_VIEW_ACL));
    Assert.assertFalse(dagAclManager.checkAccess(user, ACLType.AM_MODIFY_ACL));
    Assert.assertTrue(dagAclManager.checkAccess(user, ACLType.DAG_VIEW_ACL));
    Assert.assertTrue(dagAclManager.checkAccess(user, ACLType.DAG_MODIFY_ACL));
    user = "user1";
    Assert.assertFalse(dagAclManager.checkAccess(user, ACLType.DAG_VIEW_ACL));
    Assert.assertFalse(dagAclManager.checkAccess(user, ACLType.DAG_MODIFY_ACL));
  }

  @Test
  public void testOtherUserACLChecks() throws IOException {
    Groups groups = mock(Groups.class);
    Set<String> groups1 = Sets.newHashSet("grp1", "grp2");
    Set<String> groups2 = Sets.newHashSet("grp3", "grp4");
    Set<String> groups3 = Sets.newHashSet("grp5", "grp6");

    String currentUser = "currentUser";
    String user1 = "user1"; // belongs to grp1 and grp2
    String user2 = "user2"; // belongs to grp3 and grp4
    String user3 = "user3";
    String user4 = "user4";
    String user5 = "user5"; // belongs to grp5 and grp6
    String user6 = "user6";

    doReturn(groups1).when(groups).getGroups(eq(user1));
    doReturn(groups2).when(groups).getGroups(eq(user2));
    doReturn(Sets.newHashSet()).when(groups).getGroups(user3);
    doReturn(Sets.newHashSet()).when(groups).getGroups(user4);
    doReturn(groups3).when(groups).getGroups(eq(user5));
    doReturn(Sets.newHashSet()).when(groups).getGroups(user6);

    Configuration conf = new Configuration(false);
    // View ACLs: user1, user4, grp3, grp4.
    String viewACLs = user1 + "," + user4
        + "   " + "grp3,grp4  ";
    // Modify ACLs: user3, grp6, grp7
    String modifyACLs = user3 + "  " + "grp6,grp7";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, modifyACLs);

    ACLManager aclManager = new ACLManager(groups, currentUser, conf);

    Assert.assertTrue(aclManager.checkAccess(currentUser, ACLType.AM_VIEW_ACL));
    Assert.assertTrue(aclManager.checkAccess(user1, ACLType.AM_VIEW_ACL));
    Assert.assertTrue(aclManager.checkAccess(user2, ACLType.AM_VIEW_ACL));
    Assert.assertFalse(aclManager.checkAccess(user3, ACLType.AM_VIEW_ACL));
    Assert.assertTrue(aclManager.checkAccess(user4, ACLType.AM_VIEW_ACL));
    Assert.assertFalse(aclManager.checkAccess(user5, ACLType.AM_VIEW_ACL));
    Assert.assertFalse(aclManager.checkAccess(user6, ACLType.AM_VIEW_ACL));

    Assert.assertTrue(aclManager.checkAccess(currentUser, ACLType.AM_MODIFY_ACL));
    Assert.assertFalse(aclManager.checkAccess(user1, ACLType.AM_MODIFY_ACL));
    Assert.assertFalse(aclManager.checkAccess(user2, ACLType.AM_MODIFY_ACL));
    Assert.assertTrue(aclManager.checkAccess(user3, ACLType.AM_MODIFY_ACL));
    Assert.assertFalse(aclManager.checkAccess(user4, ACLType.AM_MODIFY_ACL));
    Assert.assertTrue(aclManager.checkAccess(user5, ACLType.AM_MODIFY_ACL));
    Assert.assertFalse(aclManager.checkAccess(user6, ACLType.AM_MODIFY_ACL));
  }

  @Test
  public void testNoGroupsACLChecks() throws IOException {
    Groups groups = mock(Groups.class);
    Set<String> groups1 = Sets.newHashSet("grp1", "grp2");
    Set<String> groups2 = Sets.newHashSet("grp3", "grp4");
    Set<String> groups3 = Sets.newHashSet("grp5", "grp6");

    String currentUser = "currentUser";
    String user1 = "user1"; // belongs to grp1 and grp2
    String user2 = "user2"; // belongs to grp3 and grp4
    String user3 = "user3";
    String user4 = "user4";
    String user5 = "user5"; // belongs to grp5 and grp6
    String user6 = "user6";

    doReturn(groups1).when(groups).getGroups(eq(user1));
    doReturn(groups2).when(groups).getGroups(eq(user2));
    doReturn(Sets.newHashSet()).when(groups).getGroups(user3);
    doReturn(Sets.newHashSet()).when(groups).getGroups(user4);
    doReturn(groups3).when(groups).getGroups(eq(user5));
    doReturn(Sets.newHashSet()).when(groups).getGroups(user6);

    Configuration conf = new Configuration(false);
    // View ACLs: user1, user4
    String viewACLs = user1 + "," + user4 + " ";
    // Modify ACLs: user3
    String modifyACLs = "user3  ";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, modifyACLs);

    ACLManager aclManager = new ACLManager(groups, currentUser, conf);
    Assert.assertTrue(aclManager.checkAccess(currentUser, ACLType.AM_VIEW_ACL));
    Assert.assertTrue(aclManager.checkAccess(user1, ACLType.AM_VIEW_ACL));
    Assert.assertFalse(aclManager.checkAccess(user2, ACLType.AM_VIEW_ACL));
    Assert.assertFalse(aclManager.checkAccess(user3, ACLType.AM_VIEW_ACL));
    Assert.assertTrue(aclManager.checkAccess(user4, ACLType.AM_VIEW_ACL));
    Assert.assertFalse(aclManager.checkAccess(user5, ACLType.AM_VIEW_ACL));
    Assert.assertFalse(aclManager.checkAccess(user6, ACLType.AM_VIEW_ACL));

    Assert.assertTrue(aclManager.checkAccess(currentUser, ACLType.AM_MODIFY_ACL));
    Assert.assertFalse(aclManager.checkAccess(user1, ACLType.AM_MODIFY_ACL));
    Assert.assertFalse(aclManager.checkAccess(user2, ACLType.AM_MODIFY_ACL));
    Assert.assertTrue(aclManager.checkAccess(user3, ACLType.AM_MODIFY_ACL));
    Assert.assertFalse(aclManager.checkAccess(user4, ACLType.AM_MODIFY_ACL));
    Assert.assertFalse(aclManager.checkAccess(user5, ACLType.AM_MODIFY_ACL));
    Assert.assertFalse(aclManager.checkAccess(user6, ACLType.AM_MODIFY_ACL));
  }

  @Test
  public void checkAMACLs() throws IOException {
    Groups groups = mock(Groups.class);
    Set<String> groups1 = Sets.newHashSet("grp1", "grp2");
    Set<String> groups2 = Sets.newHashSet("grp3", "grp4");
    Set<String> groups3 = Sets.newHashSet("grp5", "grp6");

    String currentUser = "currentUser";
    String user1 = "user1"; // belongs to grp1 and grp2
    String user2 = "user2"; // belongs to grp3 and grp4
    String user3 = "user3";
    String user4 = "user4";
    String user5 = "user5"; // belongs to grp5 and grp6
    String user6 = "user6";

    doReturn(groups1).when(groups).getGroups(eq(user1));
    doReturn(groups2).when(groups).getGroups(eq(user2));
    doReturn(Sets.newHashSet()).when(groups).getGroups(user3);
    doReturn(Sets.newHashSet()).when(groups).getGroups(user4);
    doReturn(groups3).when(groups).getGroups(eq(user5));
    doReturn(Sets.newHashSet()).when(groups).getGroups(user6);

    Configuration conf = new Configuration(false);
    // View ACLs: user1, user4, grp3, grp4.
    String viewACLs = "user1,user4,,   grp3,grp4  ";
    // Modify ACLs: user3, grp6, grp7
    String modifyACLs = "user3   grp6,grp7";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, modifyACLs);

    ACLManager aclManager = new ACLManager(groups, currentUser, conf);

    Assert.assertTrue(aclManager.checkAMViewAccess(currentUser));
    Assert.assertTrue(aclManager.checkAMViewAccess(user1));
    Assert.assertTrue(aclManager.checkAMViewAccess(user2));
    Assert.assertFalse(aclManager.checkAMViewAccess(user3));
    Assert.assertTrue(aclManager.checkAMViewAccess(user4));
    Assert.assertFalse(aclManager.checkAMViewAccess(user5));
    Assert.assertFalse(aclManager.checkAMViewAccess(user6));

    Assert.assertTrue(aclManager.checkAMModifyAccess(currentUser));
    Assert.assertFalse(aclManager.checkAMModifyAccess(user1));
    Assert.assertFalse(aclManager.checkAMModifyAccess(user2));
    Assert.assertTrue(aclManager.checkAMModifyAccess(user3));
    Assert.assertFalse(aclManager.checkAMModifyAccess(user4));
    Assert.assertTrue(aclManager.checkAMModifyAccess(user5));
    Assert.assertFalse(aclManager.checkAMModifyAccess(user6));

    Assert.assertTrue(aclManager.checkDAGViewAccess(currentUser));
    Assert.assertTrue(aclManager.checkDAGViewAccess(user1));
    Assert.assertTrue(aclManager.checkDAGViewAccess(user2));
    Assert.assertFalse(aclManager.checkDAGViewAccess(user3));
    Assert.assertTrue(aclManager.checkDAGViewAccess(user4));
    Assert.assertFalse(aclManager.checkDAGViewAccess(user5));
    Assert.assertFalse(aclManager.checkDAGViewAccess(user6));

    Assert.assertTrue(aclManager.checkDAGModifyAccess(currentUser));
    Assert.assertFalse(aclManager.checkDAGModifyAccess(user1));
    Assert.assertFalse(aclManager.checkDAGModifyAccess(user2));
    Assert.assertTrue(aclManager.checkDAGModifyAccess(user3));
    Assert.assertFalse(aclManager.checkDAGModifyAccess(user4));
    Assert.assertTrue(aclManager.checkDAGModifyAccess(user5));
    Assert.assertFalse(aclManager.checkDAGModifyAccess(user6));

  }

  @Test
  public void checkDAGACLs() throws IOException {
    Groups groups = mock(Groups.class);
    Set<String> groups1 = Sets.newHashSet("grp1", "grp2");
    Set<String> groups2 = Sets.newHashSet("grp3", "grp4");
    Set<String> groups3 = Sets.newHashSet("grp5", "grp6");

    String currentUser = "currentUser";
    String user1 = "user1"; // belongs to grp1 and grp2
    String user2 = "user2"; // belongs to grp3 and grp4
    String user3 = "user3";
    String user4 = "user4";
    String user5 = "user5"; // belongs to grp5 and grp6
    String user6 = "user6";

    doReturn(groups1).when(groups).getGroups(eq(user1));
    doReturn(groups2).when(groups).getGroups(eq(user2));
    doReturn(Sets.newHashSet()).when(groups).getGroups(user3);
    doReturn(Sets.newHashSet()).when(groups).getGroups(user4);
    doReturn(groups3).when(groups).getGroups(eq(user5));
    doReturn(Sets.newHashSet()).when(groups).getGroups(user6);

    Configuration conf = new Configuration(false);
    // View ACLs: user1, user4, grp3, grp4.
    String viewACLs = "user1,user4,,   grp3,grp4  ";
    // Modify ACLs: user3, grp6, grp7
    String modifyACLs = "user3   grp6,grp7";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, modifyACLs);

    // DAG View ACLs: user1, user4, grp3, grp4.
    String dagViewACLs = "user6,   grp5  ";
    // DAG Modify ACLs: user3, grp6, grp7
    String dagModifyACLs = "user6,user5 ";
    conf.set(TezConfiguration.TEZ_DAG_VIEW_ACLS, dagViewACLs);
    conf.set(TezConfiguration.TEZ_DAG_MODIFY_ACLS, dagModifyACLs);

    String dagUser = "dagUser";

    ACLManager amAclManager = new ACLManager(groups, currentUser, conf);
    ACLManager aclManager = new ACLManager(amAclManager, dagUser, conf);

    Assert.assertTrue(aclManager.checkAMViewAccess(currentUser));
    Assert.assertFalse(aclManager.checkAMViewAccess(dagUser));
    Assert.assertTrue(aclManager.checkAMViewAccess(user1));
    Assert.assertTrue(aclManager.checkAMViewAccess(user2));
    Assert.assertFalse(aclManager.checkAMViewAccess(user3));
    Assert.assertTrue(aclManager.checkAMViewAccess(user4));
    Assert.assertFalse(aclManager.checkAMViewAccess(user5));
    Assert.assertFalse(aclManager.checkAMViewAccess(user6));

    Assert.assertTrue(aclManager.checkAMModifyAccess(currentUser));
    Assert.assertFalse(aclManager.checkAMModifyAccess(dagUser));
    Assert.assertFalse(aclManager.checkAMModifyAccess(user1));
    Assert.assertFalse(aclManager.checkAMModifyAccess(user2));
    Assert.assertTrue(aclManager.checkAMModifyAccess(user3));
    Assert.assertFalse(aclManager.checkAMModifyAccess(user4));
    Assert.assertTrue(aclManager.checkAMModifyAccess(user5));
    Assert.assertFalse(aclManager.checkAMModifyAccess(user6));

    Assert.assertTrue(aclManager.checkDAGViewAccess(currentUser));
    Assert.assertTrue(aclManager.checkDAGViewAccess(dagUser));
    Assert.assertTrue(aclManager.checkDAGViewAccess(user1));
    Assert.assertTrue(aclManager.checkDAGViewAccess(user2));
    Assert.assertFalse(aclManager.checkDAGViewAccess(user3));
    Assert.assertTrue(aclManager.checkDAGViewAccess(user4));
    Assert.assertTrue(aclManager.checkDAGViewAccess(user5));
    Assert.assertTrue(aclManager.checkDAGViewAccess(user6));

    Assert.assertTrue(aclManager.checkDAGModifyAccess(currentUser));
    Assert.assertTrue(aclManager.checkDAGModifyAccess(dagUser));
    Assert.assertFalse(aclManager.checkDAGModifyAccess(user1));
    Assert.assertFalse(aclManager.checkDAGModifyAccess(user2));
    Assert.assertTrue(aclManager.checkDAGModifyAccess(user3));
    Assert.assertFalse(aclManager.checkDAGModifyAccess(user4));
    Assert.assertTrue(aclManager.checkDAGModifyAccess(user5));
    Assert.assertTrue(aclManager.checkDAGModifyAccess(user6));

  }

  @Test
  public void testWildCardCheck() {
    Groups groups = mock(Groups.class);
    Configuration conf = new Configuration(false);
    String viewACLs = "   *  ";
    String modifyACLs = "   * ";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, modifyACLs);

    ACLManager aclManager = new ACLManager(groups, "a1", conf);
    Assert.assertTrue(aclManager.checkAMViewAccess("a1"));
    Assert.assertTrue(aclManager.checkAMViewAccess("u1"));
    Assert.assertTrue(aclManager.checkAMModifyAccess("a1"));
    Assert.assertTrue(aclManager.checkAMModifyAccess("u1"));
    Assert.assertTrue(aclManager.checkDAGViewAccess("a1"));
    Assert.assertTrue(aclManager.checkDAGViewAccess("u1"));
    Assert.assertTrue(aclManager.checkDAGModifyAccess("a1"));
    Assert.assertTrue(aclManager.checkDAGModifyAccess("u1"));
  }

  @Test
  public void testACLsDisabled() {
    Groups groups = mock(Groups.class);
    Configuration conf = new Configuration(false);
    conf.setBoolean(TezConfiguration.TEZ_AM_ACLS_ENABLED, false);
    String viewACLs = "a2,u2  ";
    String modifyACLs = "a2,u2 ";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, modifyACLs);
    ACLManager aclManager = new ACLManager(groups, "a1", conf);
    Assert.assertTrue(aclManager.checkAMViewAccess("a1"));
    Assert.assertTrue(aclManager.checkAMViewAccess("u1"));
    Assert.assertTrue(aclManager.checkAMModifyAccess("a1"));
    Assert.assertTrue(aclManager.checkAMModifyAccess("u1"));
    Assert.assertTrue(aclManager.checkDAGViewAccess("a1"));
    Assert.assertTrue(aclManager.checkDAGViewAccess("u1"));
    Assert.assertTrue(aclManager.checkDAGModifyAccess("a1"));
    Assert.assertTrue(aclManager.checkDAGModifyAccess("u1"));

    ACLManager dagAclManager = new ACLManager(aclManager, "dagUser", null);
    Assert.assertTrue(dagAclManager.checkAMViewAccess("a1"));
    Assert.assertTrue(dagAclManager.checkAMViewAccess("u1"));
    Assert.assertTrue(dagAclManager.checkAMModifyAccess("a1"));
    Assert.assertTrue(dagAclManager.checkAMModifyAccess("u1"));
    Assert.assertTrue(dagAclManager.checkDAGViewAccess("a1"));
    Assert.assertTrue(dagAclManager.checkDAGViewAccess("u1"));
    Assert.assertTrue(dagAclManager.checkDAGModifyAccess("a1"));
    Assert.assertTrue(dagAclManager.checkDAGModifyAccess("u1"));
  }

  @Test
  public void testConvertToYARNACLs() {
    Groups groups = mock(Groups.class);
    String currentUser = "c1";
    Configuration conf = new Configuration(false);
    String viewACLs = "user1,user4,,   grp3,grp4  ";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, "   * ");
    ACLManager aclManager = new ACLManager(groups, currentUser, conf);

    Map<ApplicationAccessType, String> yarnAcls = aclManager.toYARNACls();
    Assert.assertTrue(yarnAcls.containsKey(ApplicationAccessType.VIEW_APP));
    Assert.assertEquals("c1,user1,user4 grp3,grp4",
        yarnAcls.get(ApplicationAccessType.VIEW_APP));
    Assert.assertTrue(yarnAcls.containsKey(ApplicationAccessType.MODIFY_APP));
    Assert.assertEquals("*",
        yarnAcls.get(ApplicationAccessType.MODIFY_APP));

    viewACLs = "   grp3,grp4  ";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    ACLManager aclManager1 = new ACLManager(groups, currentUser, conf);
    yarnAcls = aclManager1.toYARNACls();
    Assert.assertEquals("c1 grp3,grp4",
        yarnAcls.get(ApplicationAccessType.VIEW_APP));

  }

}
