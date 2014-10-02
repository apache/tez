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

package org.apache.tez.mapreduce.hadoop;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.TezConstants;
import org.junit.Assert;
import org.junit.Test;

public class TestMRHelpers {

  private Configuration createConfForJavaOptsTest() {
    Configuration conf = new Configuration(false);
    conf.set(MRJobConfig.MAPRED_MAP_ADMIN_JAVA_OPTS, "fooMapAdminOpts");
    conf.set(MRJobConfig.MAP_JAVA_OPTS, "fooMapJavaOpts");
    conf.set(MRJobConfig.MAP_LOG_LEVEL, "FATAL");
    conf.set(MRJobConfig.MAPRED_REDUCE_ADMIN_JAVA_OPTS, "fooReduceAdminOpts");
    conf.set(MRJobConfig.REDUCE_JAVA_OPTS, "fooReduceJavaOpts");
    conf.set(MRJobConfig.REDUCE_LOG_LEVEL, "TRACE");
    return conf;
  }

  @Test
  public void testMapJavaOptions() {
    Configuration conf = createConfForJavaOptsTest();
    String opts = MRHelpers.getJavaOptsForMRMapper(conf);

    Assert.assertTrue(opts.contains("fooMapAdminOpts"));
    Assert.assertTrue(opts.contains(" fooMapJavaOpts "));
    Assert.assertFalse(opts.contains("fooReduceAdminOpts "));
    Assert.assertFalse(opts.contains(" fooReduceJavaOpts "));
    Assert.assertTrue(opts.indexOf("fooMapAdminOpts")
        < opts.indexOf("fooMapJavaOpts"));
    Assert.assertTrue(opts.contains(" -D"
        + TezConstants.TEZ_ROOT_LOGGER_NAME + "=FATAL"));
    Assert.assertFalse(opts.contains(" -D"
        + TezConstants.TEZ_ROOT_LOGGER_NAME + "=TRACE"));
  }

  @Test
  public void testReduceJavaOptions() {
    Configuration conf = createConfForJavaOptsTest();
    String opts = MRHelpers.getJavaOptsForMRReducer(conf);

    Assert.assertFalse(opts.contains("fooMapAdminOpts"));
    Assert.assertFalse(opts.contains(" fooMapJavaOpts "));
    Assert.assertTrue(opts.contains("fooReduceAdminOpts"));
    Assert.assertTrue(opts.contains(" fooReduceJavaOpts "));
    Assert.assertTrue(opts.indexOf("fooReduceAdminOpts")
        < opts.indexOf("fooReduceJavaOpts"));
    Assert.assertFalse(opts.contains(" -D"
        + TezConstants.TEZ_ROOT_LOGGER_NAME + "=FATAL"));
    Assert.assertTrue(opts.contains(" -D"
        + TezConstants.TEZ_ROOT_LOGGER_NAME + "=TRACE"));
  }

  @Test
  public void testContainerResourceConstruction() {
    JobConf conf = new JobConf(new Configuration());
    Resource mapResource = MRHelpers.getResourceForMRMapper(conf);
    Resource reduceResource = MRHelpers.getResourceForMRReducer(conf);

    Assert.assertEquals(MRJobConfig.DEFAULT_MAP_CPU_VCORES,
        mapResource.getVirtualCores());
    Assert.assertEquals(MRJobConfig.DEFAULT_MAP_MEMORY_MB,
        mapResource.getMemory());
    Assert.assertEquals(MRJobConfig.DEFAULT_REDUCE_CPU_VCORES,
        reduceResource.getVirtualCores());
    Assert.assertEquals(MRJobConfig.DEFAULT_REDUCE_MEMORY_MB,
        reduceResource.getMemory());

    conf.setInt(MRJobConfig.MAP_CPU_VCORES, 2);
    conf.setInt(MRJobConfig.MAP_MEMORY_MB, 123);
    conf.setInt(MRJobConfig.REDUCE_CPU_VCORES, 20);
    conf.setInt(MRJobConfig.REDUCE_MEMORY_MB, 1234);

    mapResource = MRHelpers.getResourceForMRMapper(conf);
    reduceResource = MRHelpers.getResourceForMRReducer(conf);

    Assert.assertEquals(2, mapResource.getVirtualCores());
    Assert.assertEquals(123, mapResource.getMemory());
    Assert.assertEquals(20, reduceResource.getVirtualCores());
    Assert.assertEquals(1234, reduceResource.getMemory());
  }

  private Configuration setupConfigForMREnvTest() {
    JobConf conf = new JobConf(new Configuration());
    conf.set(MRJobConfig.MAP_ENV, "foo=map1,bar=map2");
    conf.set(MRJobConfig.REDUCE_ENV, "foo=red1,bar=red2");
    conf.set(MRJobConfig.MAP_LOG_LEVEL, "TRACE");
    conf.set(MRJobConfig.REDUCE_LOG_LEVEL, "FATAL");
    final String mapredAdminUserEnv = Shell.WINDOWS ?
        "PATH=%PATH%" + File.pathSeparator + "%TEZ_ADMIN_ENV%\\bin":
        "LD_LIBRARY_PATH=$TEZ_ADMIN_ENV_TEST/lib/native";

    conf.set(MRJobConfig.MAPRED_ADMIN_USER_ENV, mapredAdminUserEnv);
    return conf;
  }

  private void testCommonEnvSettingsForMRTasks(Map<String, String> env) {
    Assert.assertTrue(env.containsKey("foo"));
    Assert.assertTrue(env.containsKey("bar"));
    Assert.assertTrue(env.containsKey(Environment.LD_LIBRARY_PATH.name()));
    Assert.assertTrue(env.containsKey(Environment.SHELL.name()));
    Assert.assertTrue(env.containsKey("HADOOP_ROOT_LOGGER"));

    /* On non-windows platform ensure that LD_LIBRARY_PATH is being set and PWD is present.
     * on windows platform LD_LIBRARY_PATH is not applicable. check the PATH is being appended
     * by the user setting (ex user may set HADOOP_HOME\\bin.
     */
    if (!Shell.WINDOWS) {
      Assert.assertEquals("$PWD:$TEZ_ADMIN_ENV_TEST/lib/native",
          env.get(Environment.LD_LIBRARY_PATH.name()));
    } else {
      Assert.assertTrue(env.get(Environment.PATH.name()).contains(";%TEZ_ADMIN_ENV%\\bin"));
    }

//    TEZ-273 will reinstate this or similar. 
//    for (String val : YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH) {
//      Assert.assertTrue(env.get(Environment.CLASSPATH.name()).contains(val));
//    }
//    Assert.assertTrue(0 ==
//        env.get(Environment.CLASSPATH.name()).indexOf(Environment.PWD.$()));
  }

  @Test
  public void testMREnvSetupForMap() {
    Configuration conf = setupConfigForMREnvTest();
    Map<String, String> env = new HashMap<String, String>();
    MRHelpers.updateEnvBasedOnMRTaskEnv(conf, env, true);
    testCommonEnvSettingsForMRTasks(env);
    Assert.assertEquals("map1", env.get("foo"));
    Assert.assertEquals("map2", env.get("bar"));
  }

  @Test
  public void testMREnvSetupForReduce() {
    Configuration conf = setupConfigForMREnvTest();
    Map<String, String> env = new HashMap<String, String>();
    MRHelpers.updateEnvBasedOnMRTaskEnv(conf, env, false);
    testCommonEnvSettingsForMRTasks(env);
    Assert.assertEquals("red1", env.get("foo"));
    Assert.assertEquals("red2", env.get("bar"));
  }

  @Test
  public void testMRAMJavaOpts() {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_ADMIN_COMMAND_OPTS, " -Dadminfoobar   ");
    conf.set(MRJobConfig.MR_AM_COMMAND_OPTS, "  -Duserfoo  ");
    String opts = MRHelpers.getJavaOptsForMRAM(conf);
    Assert.assertEquals("-Dadminfoobar -Duserfoo", opts);
  }

  @Test
  public void testMRAMEnvironmentSetup() {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_ADMIN_USER_ENV, "foo=bar,admin1=foo1");
    conf.set(MRJobConfig.MR_AM_ENV, "foo=bar2,user=foo2");
    Map<String, String> env =
        new HashMap<String, String>();
    MRHelpers.updateEnvBasedOnMRAMEnv(conf, env);
    Assert.assertEquals("foo1", env.get("admin1"));
    Assert.assertEquals("foo2", env.get("user"));
    Assert.assertEquals(("bar" + File.pathSeparator + "bar2"), env.get("foo"));
  }
}
