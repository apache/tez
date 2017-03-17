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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.tez.common.TezYARNUtils.appendToEnvFromInputString;
import static org.junit.Assert.assertEquals;

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

  @Test(timeout = 20000)
  public void testUserClasspathFirstFalse() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TezConfiguration.TEZ_USER_CLASSPATH_FIRST, false);
    conf.set(TezConfiguration.TEZ_CLUSTER_ADDITIONAL_CLASSPATH_PREFIX, "foobar");
    String classpath = TezYARNUtils.getFrameworkClasspath(conf, true);
    Assert.assertTrue(classpath.contains("foobar"));
    Assert.assertTrue(classpath.indexOf("foobar") >
        classpath.indexOf(TezConstants.TEZ_TAR_LR_NAME));
    Assert.assertTrue(classpath.indexOf("foobar") >
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
    Assert.assertTrue(!classpath.contains(Environment.HADOOP_CONF_DIR.$()));
    Assert.assertTrue(classpath.indexOf(Environment.PWD.$()) <
        classpath.indexOf(TezConstants.TEZ_TAR_LR_NAME));
  }

  @Test(timeout = 5000)
  public void testNoHadoopConfInClasspath() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TezConfiguration.TEZ_CLASSPATH_ADD_HADOOP_CONF, true);
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

  @Test(timeout = 5000)
  public void testSetupDefaultEnvironment() {
    Configuration conf = new Configuration(false);
    conf.set(TezConfiguration.TEZ_AM_LAUNCH_ENV, "LD_LIBRARY_PATH=USER_PATH,USER_KEY=USER_VALUE");
    conf.set(TezConfiguration.TEZ_AM_LAUNCH_CLUSTER_DEFAULT_ENV, "LD_LIBRARY_PATH=DEFAULT_PATH,DEFAULT_KEY=DEFAULT_VALUE");

    Map<String, String> environment = new TreeMap<String, String>();
    TezYARNUtils.setupDefaultEnv(environment, conf,
        TezConfiguration.TEZ_AM_LAUNCH_ENV,
        TezConfiguration.TEZ_AM_LAUNCH_ENV_DEFAULT,
        TezConfiguration.TEZ_AM_LAUNCH_CLUSTER_DEFAULT_ENV,
        TezConfiguration.TEZ_AM_LAUNCH_CLUSTER_DEFAULT_ENV_DEFAULT, false);

    String value1 = environment.get("USER_KEY");
    Assert.assertEquals("User env should merge with default env", "USER_VALUE", value1);
    String value2 = environment.get("DEFAULT_KEY");
    Assert.assertEquals("User env should merge with default env", "DEFAULT_VALUE", value2);
    String value3 = environment.get("LD_LIBRARY_PATH");
    Assert.assertEquals("User env should append default env",
        Environment.PWD.$() + File.pathSeparator + "USER_PATH" + File.pathSeparator + "DEFAULT_PATH", value3);
    }

  @Test(timeout = 5000)
  public void testTezLibUrisClasspath() {
    Configuration conf = new Configuration(false);
    conf.set(TezConfiguration.TEZ_LIB_URIS_CLASSPATH, "foobar");
    String classpath = TezYARNUtils.getFrameworkClasspath(conf, true);
    Assert.assertTrue(classpath.contains("foobar"));
    Assert.assertTrue(classpath.contains(Environment.PWD.$()));
    Assert.assertTrue(classpath.indexOf("foobar") >
        classpath.indexOf(Environment.PWD.$()));
  }

  @Test
  public void testSetEnvFromInputString() {
    Map<String, String> environment = new HashMap<String, String>();
    environment.put("JAVA_HOME", "/path/jdk");
    String goodEnv = "a1=1,b_2=2,_c=3,d=4,e=,f_win=%JAVA_HOME%"
      + ",g_nix=$JAVA_HOME";
    appendToEnvFromInputString(environment, goodEnv, File.pathSeparator);
    assertEquals("1", environment.get("a1"));
    assertEquals("2", environment.get("b_2"));
    assertEquals("3", environment.get("_c"));
    assertEquals("4", environment.get("d"));
    assertEquals("", environment.get("e"));
    if (Shell.WINDOWS) {
      assertEquals("$JAVA_HOME", environment.get("g_nix"));
      assertEquals("/path/jdk", environment.get("f_win"));
    } else {
      assertEquals("/path/jdk", environment.get("g_nix"));
      assertEquals("%JAVA_HOME%", environment.get("f_win"));
    }
    String badEnv = "0=,1,,2=a=b,3=a=,4==,5==a,==,c-3=3,=";
    environment.clear();
    appendToEnvFromInputString(environment, badEnv, File.pathSeparator);
    assertEquals(environment.size(), 0);

    // Test "=" in the value part
    environment.clear();
    appendToEnvFromInputString(environment, "b1,e1==,e2=a1=a2,b2",
      File.pathSeparator);
    assertEquals("=", environment.get("e1"));
    assertEquals("a1=a2", environment.get("e2"));
  }
}
