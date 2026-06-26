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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;

import com.google.common.collect.Sets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestDAGAccessControls {

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testStringBasedConstructor() {
    DAGAccessControls dagAccessControls = new DAGAccessControls("u1 g1", "u2 g2");

    assertEquals(1, dagAccessControls.getUsersWithViewACLs().size());
    assertEquals(1, dagAccessControls.getUsersWithModifyACLs().size());
    assertEquals(1, dagAccessControls.getGroupsWithViewACLs().size());
    assertEquals(1, dagAccessControls.getGroupsWithModifyACLs().size());

    assertTrue(dagAccessControls.getUsersWithViewACLs().contains("u1"));
    assertTrue(dagAccessControls.getUsersWithModifyACLs().contains("u2"));
    assertTrue(dagAccessControls.getGroupsWithViewACLs().contains("g1"));
    assertTrue(dagAccessControls.getGroupsWithModifyACLs().contains("g2"));
  }


  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testMergeIntoAmAcls() {
    DAGAccessControls dagAccessControls = new DAGAccessControls("u1 g1", "u2 g2");
    Configuration conf = new Configuration(false);

    // default conf should have ACLs copied over.
    dagAccessControls.mergeIntoAmAcls(conf);
    assertACLS("u1 g1", conf.get(TezConfiguration.TEZ_AM_VIEW_ACLS));
    assertACLS("u2 g2", conf.get(TezConfiguration.TEZ_AM_MODIFY_ACLS));

    // both have unique users merged should have all
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, "u1 g1");
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, "u2 g2");
    dagAccessControls.mergeIntoAmAcls(conf);
    assertACLS("u1 g1", conf.get(TezConfiguration.TEZ_AM_VIEW_ACLS));
    assertACLS("u2 g2", conf.get(TezConfiguration.TEZ_AM_MODIFY_ACLS));

    // both have unique users merged should have all
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, "u3 g3");
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, "u4 g4");
    dagAccessControls.mergeIntoAmAcls(conf);
    assertACLS("u3,u1 g3,g1", conf.get(TezConfiguration.TEZ_AM_VIEW_ACLS));
    assertACLS("u4,u2 g4,g2", conf.get(TezConfiguration.TEZ_AM_MODIFY_ACLS));

    // one of the user is *, merged is always *
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, "*,u3 g3");
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, "*,u4 g4");
    dagAccessControls.mergeIntoAmAcls(conf);
    assertACLS("*", conf.get(TezConfiguration.TEZ_AM_VIEW_ACLS));
    assertACLS("*", conf.get(TezConfiguration.TEZ_AM_MODIFY_ACLS));

    // only * in the config, merged is *
    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, "*");
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, "*");
    dagAccessControls.mergeIntoAmAcls(conf);
    assertACLS("*", conf.get(TezConfiguration.TEZ_AM_VIEW_ACLS));
    assertACLS("*", conf.get(TezConfiguration.TEZ_AM_MODIFY_ACLS));

    // DAG access with *, all operation yeild *
    dagAccessControls = new DAGAccessControls("*", "*");

    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, "u3 g3");
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, "u4 g4");
    dagAccessControls.mergeIntoAmAcls(conf);
    assertACLS("*", conf.get(TezConfiguration.TEZ_AM_VIEW_ACLS));
    assertACLS("*", conf.get(TezConfiguration.TEZ_AM_MODIFY_ACLS));

    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, "*,u3 g3");
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, "*,u4 g4");
    dagAccessControls.mergeIntoAmAcls(conf);
    assertACLS("*", conf.get(TezConfiguration.TEZ_AM_VIEW_ACLS));
    assertACLS("*", conf.get(TezConfiguration.TEZ_AM_MODIFY_ACLS));

    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, "*");
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, "*");
    dagAccessControls.mergeIntoAmAcls(conf);
    assertACLS("*", conf.get(TezConfiguration.TEZ_AM_VIEW_ACLS));
    assertACLS("*", conf.get(TezConfiguration.TEZ_AM_MODIFY_ACLS));

    // DAG access is empty, conf should be same.
    dagAccessControls = new DAGAccessControls("", "");

    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, "u3 g3");
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, "u4 g4");
    dagAccessControls.mergeIntoAmAcls(conf);
    assertACLS("u3 g3", conf.get(TezConfiguration.TEZ_AM_VIEW_ACLS));
    assertACLS("u4 g4", conf.get(TezConfiguration.TEZ_AM_MODIFY_ACLS));

    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, "*,u3 g3");
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, "*,u4 g4");
    dagAccessControls.mergeIntoAmAcls(conf);
    assertACLS("*", conf.get(TezConfiguration.TEZ_AM_VIEW_ACLS));
    assertACLS("*", conf.get(TezConfiguration.TEZ_AM_MODIFY_ACLS));

    conf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, "*");
    conf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, "*");
    dagAccessControls.mergeIntoAmAcls(conf);
    assertACLS("*", conf.get(TezConfiguration.TEZ_AM_VIEW_ACLS));
    assertACLS("*", conf.get(TezConfiguration.TEZ_AM_MODIFY_ACLS));
  }

  public void assertACLS(String expected, String obtained) {
    if (expected.equals(obtained)) {
      return;
    }

    String [] parts1 = expected.split(" ");
    String [] parts2 = obtained.split(" ");

    assertEquals(parts1.length, parts2.length);

    assertEquals(
        Sets.newHashSet(parts1[0].split(",")), Sets.newHashSet(parts2[0].split(",")));

    if (parts1.length < 2) {
      return;
    }
    assertEquals(
        Sets.newHashSet(parts1[1].split(",")), Sets.newHashSet(parts2[1].split(",")));
  }
}
