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

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.junit.Assert;
import org.junit.Test;

public class TestTezYARNUtils {

  @Test(timeout = 5000)
  public void testAuxClasspath() {
    Configuration conf = new Configuration(false);
    conf.set(TezConfiguration.TEZ_CLUSTER_ADDITIONAL_CLASSPATH_PREFIX, "foobar");
    String classpath = TezYARNUtils.getFrameworkClasspath(conf, true);
    Assert.assertTrue(classpath.contains("foobar"));
    Assert.assertTrue(classpath.indexOf("foobar") <
        classpath.indexOf(TezConstants.TEZ_TAR_LR_NAME));
    Assert.assertTrue(classpath.indexOf("foobar") <
        classpath.indexOf(Environment.PWD.$()));
  }

  @Test(timeout = 5000)
  public void testBasicArchiveClasspath() {
    Configuration conf = new Configuration(false);
    String classpath = TezYARNUtils.getFrameworkClasspath(conf, true);
    Assert.assertTrue(classpath.contains(Environment.PWD.$()));
    Assert.assertTrue(classpath.contains(Environment.PWD.$() + File.separator + "*"));
    Assert.assertTrue(classpath.contains(TezConstants.TEZ_TAR_LR_NAME + File.separator + "*"));
    Assert.assertTrue(classpath.contains(TezConstants.TEZ_TAR_LR_NAME + File.separator
        + "lib" + File.separator + "*"));
    Assert.assertTrue(classpath.contains(Environment.HADOOP_CONF_DIR.$()));
    Assert.assertTrue(classpath.indexOf(Environment.PWD.$()) <
        classpath.indexOf(TezConstants.TEZ_TAR_LR_NAME));
    Assert.assertTrue(classpath.indexOf(TezConstants.TEZ_TAR_LR_NAME) <
        classpath.indexOf(Environment.HADOOP_CONF_DIR.$()));
  }

}
