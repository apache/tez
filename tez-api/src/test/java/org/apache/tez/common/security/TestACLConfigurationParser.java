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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.junit.Assert;
import org.junit.Test;

public class TestACLConfigurationParser {

  @Test(timeout = 5000)
  public void testACLConfigParser() {

    Configuration conf = new Configuration(false);
    String adminACLs = "admin1,admin4,       admgrp3,admgrp4,admgrp5  ";
    String viewACLs = "user1,user4,       grp3,grp4,grp5  ";
    String modifyACLs = "user3 ";
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, adminACLs);
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);

    ACLConfigurationParser parser = new ACLConfigurationParser(conf);
    Assert.assertTrue(parser.getAllowedUsers().containsKey(ACLType.AM_VIEW_ACL));
    Assert.assertFalse(parser.getAllowedUsers().containsKey(ACLType.AM_MODIFY_ACL));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.AM_VIEW_ACL).contains("user1"));
    Assert.assertFalse(parser.getAllowedUsers().get(ACLType.AM_VIEW_ACL).contains("user3"));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.AM_VIEW_ACL).contains("user4"));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.YARN_ADMIN_ACL).contains("admin1"));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.YARN_ADMIN_ACL).contains("admin4"));
    Assert.assertFalse(parser.getAllowedGroups().isEmpty());
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.AM_VIEW_ACL).contains("grp3"));
    Assert.assertFalse(parser.getAllowedGroups().get(ACLType.AM_VIEW_ACL).contains("grp6"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.AM_VIEW_ACL).contains("grp4"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.AM_VIEW_ACL).contains("grp5"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.YARN_ADMIN_ACL).contains("admgrp3"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.YARN_ADMIN_ACL).contains("admgrp4"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.YARN_ADMIN_ACL).contains("admgrp5"));

    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, modifyACLs);
    parser = new ACLConfigurationParser(conf);
    Assert.assertTrue(parser.getAllowedUsers().containsKey(ACLType.AM_VIEW_ACL));
    Assert.assertTrue(parser.getAllowedUsers().containsKey(ACLType.AM_MODIFY_ACL));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.AM_VIEW_ACL).contains("user1"));
    Assert.assertFalse(parser.getAllowedUsers().get(ACLType.AM_VIEW_ACL).contains("user3"));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.AM_VIEW_ACL).contains("user4"));
    Assert.assertFalse(parser.getAllowedUsers().get(ACLType.AM_MODIFY_ACL).contains("user1"));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.AM_MODIFY_ACL).contains("user3"));
    Assert.assertFalse(parser.getAllowedGroups().isEmpty());
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.AM_VIEW_ACL).contains("grp3"));
    Assert.assertFalse(parser.getAllowedGroups().get(ACLType.AM_VIEW_ACL).contains("grp6"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.AM_VIEW_ACL).contains("grp4"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.AM_VIEW_ACL).contains("grp5"));
    Assert.assertFalse(parser.getAllowedGroups().isEmpty());
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.YARN_ADMIN_ACL).contains("admgrp3"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.YARN_ADMIN_ACL).contains("admgrp4"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.YARN_ADMIN_ACL).contains("admgrp5"));
  }

  @Test(timeout = 5000)
  public void testGroupsOnly() {
    Configuration conf = new Configuration(false);
    String adminACLs = "admin1,admin4,       admgrp3,admgrp4,admgrp5  ";
    String viewACLs = "     grp3,grp4,grp5";
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewACLs);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, adminACLs);

    ACLConfigurationParser parser = new ACLConfigurationParser(conf);
    Assert.assertFalse(parser.getAllowedUsers().isEmpty());
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.YARN_ADMIN_ACL).contains("admin1"));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.YARN_ADMIN_ACL).contains("admin4"));
    Assert.assertFalse(parser.getAllowedGroups().isEmpty());
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.AM_VIEW_ACL).contains("grp3"));
    Assert.assertFalse(parser.getAllowedGroups().get(ACLType.AM_VIEW_ACL).contains("grp6"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.AM_VIEW_ACL).contains("grp4"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.AM_VIEW_ACL).contains("grp5"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.YARN_ADMIN_ACL).contains("admgrp3"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.YARN_ADMIN_ACL).contains("admgrp4"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.YARN_ADMIN_ACL).contains("admgrp5"));
  }

  @Test(timeout = 5000)
  public void testDAGACLConfigParser() {

    Configuration conf = new Configuration(false);
    String adminACLs = "admin1,admin4,       admgrp3,admgrp4,admgrp5  ";
    String viewACLs = "user1,user4 grp3,grp4,grp5";
    String modifyACLs = "user3 grp4";
    conf.set(TezConstants.TEZ_DAG_VIEW_ACLS, viewACLs);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, adminACLs);

    ACLConfigurationParser parser = new ACLConfigurationParser(conf, true);
    Assert.assertTrue(parser.getAllowedUsers().containsKey(ACLType.DAG_VIEW_ACL));
    Assert.assertFalse(parser.getAllowedUsers().containsKey(ACLType.DAG_MODIFY_ACL));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.DAG_VIEW_ACL).contains("user1"));
    Assert.assertFalse(parser.getAllowedUsers().get(ACLType.DAG_VIEW_ACL).contains("user3"));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.DAG_VIEW_ACL).contains("user4"));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.YARN_ADMIN_ACL).contains("admin1"));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.YARN_ADMIN_ACL).contains("admin4"));
    Assert.assertFalse(parser.getAllowedGroups().isEmpty());
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.DAG_VIEW_ACL).contains("grp3"));
    Assert.assertFalse(parser.getAllowedGroups().get(ACLType.DAG_VIEW_ACL).contains("grp6"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.DAG_VIEW_ACL).contains("grp4"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.DAG_VIEW_ACL).contains("grp5"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.YARN_ADMIN_ACL).contains("admgrp3"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.YARN_ADMIN_ACL).contains("admgrp4"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.YARN_ADMIN_ACL).contains("admgrp5"));

    conf.set(TezConstants.TEZ_DAG_MODIFY_ACLS, modifyACLs);
    parser = new ACLConfigurationParser(conf, true);
    Assert.assertTrue(parser.getAllowedUsers().containsKey(ACLType.DAG_VIEW_ACL));
    Assert.assertTrue(parser.getAllowedUsers().containsKey(ACLType.DAG_MODIFY_ACL));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.DAG_VIEW_ACL).contains("user1"));
    Assert.assertFalse(parser.getAllowedUsers().get(ACLType.DAG_VIEW_ACL).contains("user3"));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.DAG_VIEW_ACL).contains("user4"));
    Assert.assertFalse(parser.getAllowedUsers().get(ACLType.DAG_MODIFY_ACL).contains("user1"));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.DAG_MODIFY_ACL).contains("user3"));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.YARN_ADMIN_ACL).contains("admin1"));
    Assert.assertTrue(parser.getAllowedUsers().get(ACLType.YARN_ADMIN_ACL).contains("admin4"));
    Assert.assertFalse(parser.getAllowedGroups().isEmpty());
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.DAG_VIEW_ACL).contains("grp3"));
    Assert.assertFalse(parser.getAllowedGroups().get(ACLType.DAG_VIEW_ACL).contains("grp6"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.DAG_VIEW_ACL).contains("grp4"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.DAG_VIEW_ACL).contains("grp5"));
    Assert.assertNotNull(parser.getAllowedGroups().get(ACLType.DAG_MODIFY_ACL));
    Assert.assertFalse(parser.getAllowedGroups().get(ACLType.DAG_MODIFY_ACL).contains("grp6"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.DAG_MODIFY_ACL).contains("grp4"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.YARN_ADMIN_ACL).contains("admgrp3"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.YARN_ADMIN_ACL).contains("admgrp4"));
    Assert.assertTrue(parser.getAllowedGroups().get(ACLType.YARN_ADMIN_ACL).contains("admgrp5"));
  }
}
