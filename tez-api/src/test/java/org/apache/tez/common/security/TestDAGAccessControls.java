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

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class TestDAGAccessControls {

  @Test
  public void testBasicSerializeToConf()  {
    DAGAccessControls dagAccessControls = new DAGAccessControls();
    dagAccessControls.setUsersWithViewACLs(Arrays.asList("u1"))
        .setUsersWithModifyACLs(Arrays.asList("u2"))
        .setGroupsWithViewACLs(Arrays.asList("g1"))
        .setGroupsWithModifyACLs(Arrays.asList("g2"));

    Configuration conf = new Configuration(false);
    dagAccessControls.serializeToConfiguration(conf);
    Assert.assertNotNull(conf.get(TezConfiguration.TEZ_DAG_VIEW_ACLS));
    Assert.assertNotNull(conf.get(TezConfiguration.TEZ_DAG_MODIFY_ACLS));

    Assert.assertEquals("u1 g1", conf.get(TezConfiguration.TEZ_DAG_VIEW_ACLS));
    Assert.assertEquals("u2 g2", conf.get(TezConfiguration.TEZ_DAG_MODIFY_ACLS));
  }

  @Test
  public void testWildCardSerializeToConf()  {
    DAGAccessControls dagAccessControls = new DAGAccessControls();
    dagAccessControls.setUsersWithViewACLs(Arrays.asList("*"))
        .setUsersWithModifyACLs(Arrays.asList("*"))
        .setGroupsWithViewACLs(Arrays.asList("g1"))
        .setGroupsWithModifyACLs(Arrays.asList("g2"));

    Configuration conf = new Configuration(false);
    dagAccessControls.serializeToConfiguration(conf);
    Assert.assertNotNull(conf.get(TezConfiguration.TEZ_DAG_VIEW_ACLS));
    Assert.assertNotNull(conf.get(TezConfiguration.TEZ_DAG_MODIFY_ACLS));

    Assert.assertEquals("*", conf.get(TezConfiguration.TEZ_DAG_VIEW_ACLS));
    Assert.assertEquals("*", conf.get(TezConfiguration.TEZ_DAG_MODIFY_ACLS));
  }

  @Test
  public void testGroupsOnlySerializeToConf()  {
    DAGAccessControls dagAccessControls = new DAGAccessControls();
    dagAccessControls.setGroupsWithViewACLs(Arrays.asList("g1"))
        .setGroupsWithModifyACLs(Arrays.asList("g2"));

    Configuration conf = new Configuration(false);
    dagAccessControls.serializeToConfiguration(conf);
    Assert.assertNotNull(conf.get(TezConfiguration.TEZ_DAG_VIEW_ACLS));
    Assert.assertNotNull(conf.get(TezConfiguration.TEZ_DAG_MODIFY_ACLS));

    Assert.assertEquals(" g1", conf.get(TezConfiguration.TEZ_DAG_VIEW_ACLS));
    Assert.assertEquals(" g2", conf.get(TezConfiguration.TEZ_DAG_MODIFY_ACLS));
  }

  @Test
  public void testEmptySerializeToConf()  {
    DAGAccessControls dagAccessControls = new DAGAccessControls();

    Configuration conf = new Configuration(false);
    dagAccessControls.serializeToConfiguration(conf);
    Assert.assertNotNull(conf.get(TezConfiguration.TEZ_DAG_VIEW_ACLS));
    Assert.assertNotNull(conf.get(TezConfiguration.TEZ_DAG_MODIFY_ACLS));

    Assert.assertEquals(" ", conf.get(TezConfiguration.TEZ_DAG_VIEW_ACLS));
    Assert.assertEquals(" ", conf.get(TezConfiguration.TEZ_DAG_MODIFY_ACLS));
  }

}
