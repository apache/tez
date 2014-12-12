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

package org.apache.tez.dag.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.Test;

public class TestTaskSpecificLaunchCmdOption {
  static Configuration conf = new Configuration();

  private TaskSpecificLaunchCmdOption getOptions(Configuration conf, String tasks) {
    return getOptions(conf, tasks, "dummyOpts");
  }

  private TaskSpecificLaunchCmdOption getOptions(Configuration conf, String tasks, String launchOpts) {
    conf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS_LIST, tasks);
    conf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS, launchOpts);
    return new TaskSpecificLaunchCmdOption(conf);
  }


  @Test(timeout = 5000)
  public void testTaskSpecificJavaOptions() {
    Random rnd = new Random();
    Configuration conf = new Configuration();
    TaskSpecificLaunchCmdOption option = getOptions(conf, "");
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 0));

    option = getOptions(conf, "v[0,1,2]");
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 0));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 1));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 2));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v1",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, "v[,5]", "dummyOpts");
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 5));

    option = getOptions(conf, "v 1[1,5]", "dummyOpts");
    assertTrue(option.addTaskSpecificLaunchCmdOption("v 1", 1));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v 1", 5));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v",
        rnd.nextInt(Integer.MAX_VALUE)));
    assertFalse(option.addTaskSpecificLaunchCmdOption("1",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, "v 1[1,5], 5 [50,60], m  1[10, 11],", "dummyOpts");
    assertTrue(option.addTaskSpecificLaunchCmdOption("v 1", 1));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v 1", 5));
    assertTrue(option.addTaskSpecificLaunchCmdOption("m  1", 10));
    assertTrue(option.addTaskSpecificLaunchCmdOption("m  1", 11));
    assertTrue(option.addTaskSpecificLaunchCmdOption("5", 50));
    assertTrue(option.addTaskSpecificLaunchCmdOption("5", 60));
    assertFalse(option.addTaskSpecificLaunchCmdOption("5", 600));
    assertFalse(option.addTaskSpecificLaunchCmdOption("m  1", 1));
    assertFalse(option.addTaskSpecificLaunchCmdOption("1",
        rnd.nextInt(Integer.MAX_VALUE)));
    assertFalse(option.addTaskSpecificLaunchCmdOption("",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, "v 1[1,5], 5 [50,60],  @#425[10, 11]", "dummyOpts");
    assertFalse(option.addTaskSpecificLaunchCmdOption("v 1", 1));
    assertFalse(option.addTaskSpecificLaunchCmdOption("@#425", 10));

    option = getOptions(conf, "v[0,1,2];v2[5,6:8]");
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 0));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 1));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 2));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v2", 5));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v2", 6));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v2", 7));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v2", 8));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v5",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, "v[1:3,5]");
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 1));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 2));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 3));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 5));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v5",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, "v[3:1,5]");
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 1));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 2));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 3));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 5));

    option = getOptions(conf, "v[-1]");
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 1));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", -1));

    // Profile all tasks in a vertex. ANY task in the vertex
    option = getOptions(conf, "v[]");
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 0));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, "v[,, ,]");
    assertTrue(option.addTaskSpecificLaunchCmdOption("v",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, "v[    ]");
    assertTrue(option.addTaskSpecificLaunchCmdOption("v",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, "v[:,,]");
    assertFalse(option.addTaskSpecificLaunchCmdOption("v",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, " v[3:1,4]");
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 4));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 1));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 3));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, " v[1:3,4, 5]");
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 4));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 2));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 4));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 1));
    assertTrue(option.addTaskSpecificLaunchCmdOption("v", 5));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, " v[:,,:, 5]");
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 0));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 5));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 1));

    option = getOptions(conf, " v[ : ,,]");
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 0));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v",
        rnd.nextInt(Integer.MAX_VALUE)));

    //-ve tests
    option = getOptions(conf, "v12#fs[0,1,2]");
    assertFalse(option.addTaskSpecificLaunchCmdOption("v12#fs", 0));
    assertFalse(option.addTaskSpecificLaunchCmdOption("fs", 0));
    assertFalse(option.addTaskSpecificLaunchCmdOption("#", 0));

    option = getOptions(conf, "v[-3:1,5]");
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 0));

    option = getOptions(conf, " ^&*%[0,1,2]");
    assertFalse(option.addTaskSpecificLaunchCmdOption("^&*%", 0));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 0));

    option = getOptions(conf, "^&*%[0,1,2]");
    assertFalse(option.addTaskSpecificLaunchCmdOption("^&*%", 0));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 0));
    assertFalse(option.addTaskSpecificLaunchCmdOption("^&*%",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, "v[-1]");
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 1));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", -1));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, " [:, 4:]");
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 1));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 4));
    assertFalse(option.addTaskSpecificLaunchCmdOption("", 0));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, "v[:,,:]");
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 1));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 4));
    assertFalse(option.addTaskSpecificLaunchCmdOption("", 0));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, "v[:5,1]");
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 0));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 1));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 2));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 3));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 4));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 5));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 6));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, "v[1:,5]");
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 1));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 5));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, "v[:1,5]");
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 0));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 1));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 5));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", -1));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, " v[1:,4, 5],    [5,4]");
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 4));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 1));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 5));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 3));
    assertFalse(option.addTaskSpecificLaunchCmdOption(" ", 4));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v",
        rnd.nextInt(Integer.MAX_VALUE)));

    option = getOptions(conf, " v[-3:1,4]");
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 4));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 1));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", 3));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v", -3));
    assertFalse(option.addTaskSpecificLaunchCmdOption("v",
        rnd.nextInt(Integer.MAX_VALUE)));
  }

  @Test(timeout = 5000)
  public void testConfigOptions() {
    Configuration conf = new Configuration();
    TaskSpecificLaunchCmdOption taskSpecificLaunchCmdOption = getOptions(conf, "", "");
    String optionStr = taskSpecificLaunchCmdOption.getTaskSpecificOption("", "", 0);
    assertTrue(optionStr.trim().equals(""));

    taskSpecificLaunchCmdOption = getOptions(conf, "", "dir=__VERTEX_NAME__");
    optionStr = taskSpecificLaunchCmdOption.getTaskSpecificOption("", "Map 1", 0);
    assertTrue(optionStr.equals("dir=Map1"));

    taskSpecificLaunchCmdOption = getOptions(conf, "", "dir=__TASK_INDEX__");
    optionStr = taskSpecificLaunchCmdOption.getTaskSpecificOption("", "Map 1", 0);
    assertTrue(optionStr.equals("dir=0"));

    taskSpecificLaunchCmdOption = getOptions(conf, "v[1,3,4]", "dir=/tmp/__VERTEX_NAME__/__TASK_INDEX__");
    optionStr = taskSpecificLaunchCmdOption.getTaskSpecificOption("", "v", 1);
    assertTrue(optionStr.equals("dir=/tmp/v/1"));

    optionStr = taskSpecificLaunchCmdOption.getTaskSpecificOption("", "v", 3);
    assertTrue(optionStr.equals("dir=/tmp/v/3"));

    optionStr = taskSpecificLaunchCmdOption.getTaskSpecificOption("", "v", 4);
    assertTrue(optionStr.equals("dir=/tmp/v/4"));
  }


  @Test(timeout = 5000)
  public void testTaskSpecificLogOptions() {
    Configuration conf = new Configuration(false);
    conf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS_LIST, "v1[0,2,5]");
    TaskSpecificLaunchCmdOption options;

    conf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LOG_LEVEL, "DEBUG;org.apache.tez=INFO");
    options = new TaskSpecificLaunchCmdOption(conf);
    assertTrue(options.hasModifiedLogProperties());
    assertFalse(options.hasModifiedTaskLaunchOpts());
    assertEquals(2, options.getTaskSpecificLogParams().length);

    conf.unset(TezConfiguration.TEZ_TASK_SPECIFIC_LOG_LEVEL);
    options = new TaskSpecificLaunchCmdOption(conf);
    assertFalse(options.hasModifiedLogProperties());
    assertFalse(options.hasModifiedTaskLaunchOpts());

    conf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LOG_LEVEL, "DEBUG");
    options = new TaskSpecificLaunchCmdOption(conf);
    assertTrue(options.hasModifiedLogProperties());
    assertFalse(options.hasModifiedTaskLaunchOpts());
    assertEquals(1, options.getTaskSpecificLogParams().length);
  }

  @Test (timeout=5000)
  public void testTaskSpecificLogOptionsWithCommandOptions() {
    Configuration conf = new Configuration(false);
    conf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS_LIST, "v1[0,2,5]");
    TaskSpecificLaunchCmdOption options;

    conf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LOG_LEVEL, "DEBUG;org.apache.tez=INFO");
    conf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS, "-Xmx128m");
    options = new TaskSpecificLaunchCmdOption(conf);
    assertTrue(options.hasModifiedLogProperties());
    assertTrue(options.hasModifiedTaskLaunchOpts());
    String optionStr = options.getTaskSpecificOption("", "v", 0);
    assertTrue(optionStr.equals("-Xmx128m"));
  }
}
