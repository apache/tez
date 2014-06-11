package org.apache.tez.dag.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.Test;

public class TestJavaProfilerOptions {
  static Configuration conf = new Configuration();

  private JavaProfilerOptions getOptions(Configuration conf, String tasks) {
    return getOptions(conf, tasks, "dummyOpts");
  }

  private JavaProfilerOptions getOptions(Configuration conf, String tasks, String jvmOpts) {
    conf.set(TezConfiguration.TEZ_PROFILE_TASK_LIST, tasks);
    conf.set(TezConfiguration.TEZ_PROFILE_JVM_OPTS, jvmOpts);
    return new JavaProfilerOptions(conf);
  }

  @Test
  public void testTasksToBeProfiled() {
    Random rnd = new Random();
    Configuration conf = new Configuration();
    JavaProfilerOptions profilerOptions = getOptions(conf, "");
    assertFalse(profilerOptions.shouldProfileJVM("v", 0));

    profilerOptions = getOptions(conf, "v[0,1,2]");
    assertTrue(profilerOptions.shouldProfileJVM("v", 0));
    assertTrue(profilerOptions.shouldProfileJVM("v", 1));
    assertTrue(profilerOptions.shouldProfileJVM("v", 2));
    assertFalse(profilerOptions.shouldProfileJVM("v1", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, "v[,5]", "dummyOpts");
    assertTrue(profilerOptions.shouldProfileJVM("v", 5));

    profilerOptions = getOptions(conf, "v 1[1,5]", "dummyOpts");
    assertTrue(profilerOptions.shouldProfileJVM("v 1", 1));
    assertTrue(profilerOptions.shouldProfileJVM("v 1", 5));
    assertFalse(profilerOptions.shouldProfileJVM("v", rnd.nextInt(Integer.MAX_VALUE)));
    assertFalse(profilerOptions.shouldProfileJVM("1", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, "v 1[1,5], 5 [50,60], m  1[10, 11],", "dummyOpts");
    assertTrue(profilerOptions.shouldProfileJVM("v 1", 1));
    assertTrue(profilerOptions.shouldProfileJVM("v 1", 5));
    assertTrue(profilerOptions.shouldProfileJVM("m  1", 10));
    assertTrue(profilerOptions.shouldProfileJVM("m  1", 11));
    assertTrue(profilerOptions.shouldProfileJVM("5", 50));
    assertTrue(profilerOptions.shouldProfileJVM("5", 60));
    assertFalse(profilerOptions.shouldProfileJVM("5", 600));
    assertFalse(profilerOptions.shouldProfileJVM("m  1", 1));
    assertFalse(profilerOptions.shouldProfileJVM("1", rnd.nextInt(Integer.MAX_VALUE)));
    assertFalse(profilerOptions.shouldProfileJVM("", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, "v 1[1,5], 5 [50,60],  @#425[10, 11]", "dummyOpts");
    assertFalse(profilerOptions.shouldProfileJVM("v 1", 1));
    assertFalse(profilerOptions.shouldProfileJVM("@#425", 10));

    profilerOptions = getOptions(conf, "v[0,1,2];v2[5,6:8]");
    assertTrue(profilerOptions.shouldProfileJVM("v", 0));
    assertTrue(profilerOptions.shouldProfileJVM("v", 1));
    assertTrue(profilerOptions.shouldProfileJVM("v", 2));
    assertTrue(profilerOptions.shouldProfileJVM("v2", 5));
    assertTrue(profilerOptions.shouldProfileJVM("v2", 6));
    assertTrue(profilerOptions.shouldProfileJVM("v2", 7));
    assertTrue(profilerOptions.shouldProfileJVM("v2", 8));
    assertFalse(profilerOptions.shouldProfileJVM("v5", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, "v[1:3,5]");
    assertTrue(profilerOptions.shouldProfileJVM("v", 1));
    assertTrue(profilerOptions.shouldProfileJVM("v", 2));
    assertTrue(profilerOptions.shouldProfileJVM("v", 3));
    assertTrue(profilerOptions.shouldProfileJVM("v", 5));
    assertFalse(profilerOptions.shouldProfileJVM("v5", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, "v[3:1,5]");
    assertTrue(profilerOptions.shouldProfileJVM("v", 1));
    assertTrue(profilerOptions.shouldProfileJVM("v", 2));
    assertTrue(profilerOptions.shouldProfileJVM("v", 3));
    assertTrue(profilerOptions.shouldProfileJVM("v", 5));

    profilerOptions = getOptions(conf, "v[-1]");
    assertFalse(profilerOptions.shouldProfileJVM("v", 1));
    assertFalse(profilerOptions.shouldProfileJVM("v", -1));

    // Profile all tasks in a vertex. ANY task in the vertex
    profilerOptions = getOptions(conf, "v[]");
    assertTrue(profilerOptions.shouldProfileJVM("v", 0));
    assertTrue(profilerOptions.shouldProfileJVM("v", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, "v[,, ,]");
    assertTrue(profilerOptions.shouldProfileJVM("v", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, "v[    ]");
    assertTrue(profilerOptions.shouldProfileJVM("v", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, "v[:,,]");
    assertFalse(profilerOptions.shouldProfileJVM("v", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, " v[3:1,4]");
    assertTrue(profilerOptions.shouldProfileJVM("v", 4));
    assertTrue(profilerOptions.shouldProfileJVM("v", 1));
    assertTrue(profilerOptions.shouldProfileJVM("v", 3));
    assertFalse(profilerOptions.shouldProfileJVM("v", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, " v[1:3,4, 5]");
    assertTrue(profilerOptions.shouldProfileJVM("v", 4));
    assertTrue(profilerOptions.shouldProfileJVM("v", 2));
    assertTrue(profilerOptions.shouldProfileJVM("v", 4));
    assertTrue(profilerOptions.shouldProfileJVM("v", 1));
    assertTrue(profilerOptions.shouldProfileJVM("v", 5));
    assertFalse(profilerOptions.shouldProfileJVM("v", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, " v[:,,:, 5]");
    assertFalse(profilerOptions.shouldProfileJVM("v", 0));
    assertFalse(profilerOptions.shouldProfileJVM("v", 5));
    assertFalse(profilerOptions.shouldProfileJVM("v", 1));

    profilerOptions = getOptions(conf, " v[ : ,,]");
    assertFalse(profilerOptions.shouldProfileJVM("v", 0));
    assertFalse(profilerOptions.shouldProfileJVM("v", rnd.nextInt(Integer.MAX_VALUE)));

    //-ve tests
    profilerOptions = getOptions(conf, "v12#fs[0,1,2]");
    assertFalse(profilerOptions.shouldProfileJVM("v12#fs", 0));
    assertFalse(profilerOptions.shouldProfileJVM("fs", 0));
    assertFalse(profilerOptions.shouldProfileJVM("#", 0));

    profilerOptions = getOptions(conf, "v[-3:1,5]");
    assertFalse(profilerOptions.shouldProfileJVM("v", 0));

    profilerOptions = getOptions(conf, " ^&*%[0,1,2]");
    assertFalse(profilerOptions.shouldProfileJVM("^&*%", 0));
    assertFalse(profilerOptions.shouldProfileJVM("v", 0));

    profilerOptions = getOptions(conf, "^&*%[0,1,2]");
    assertFalse(profilerOptions.shouldProfileJVM("^&*%", 0));
    assertFalse(profilerOptions.shouldProfileJVM("v", 0));
    assertFalse(profilerOptions.shouldProfileJVM("^&*%", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, "v[-1]");
    assertFalse(profilerOptions.shouldProfileJVM("v", 1));
    assertFalse(profilerOptions.shouldProfileJVM("v", -1));
    assertFalse(profilerOptions.shouldProfileJVM("v", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, " [:, 4:]");
    assertFalse(profilerOptions.shouldProfileJVM("v", 1));
    assertFalse(profilerOptions.shouldProfileJVM("v", 4));
    assertFalse(profilerOptions.shouldProfileJVM("", 0));
    assertFalse(profilerOptions.shouldProfileJVM("v", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, "v[:,,:]");
    assertFalse(profilerOptions.shouldProfileJVM("v", 1));
    assertFalse(profilerOptions.shouldProfileJVM("v", 4));
    assertFalse(profilerOptions.shouldProfileJVM("", 0));
    assertFalse(profilerOptions.shouldProfileJVM("v", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, "v[:5,1]");
    assertFalse(profilerOptions.shouldProfileJVM("v", 0));
    assertFalse(profilerOptions.shouldProfileJVM("v", 1));
    assertFalse(profilerOptions.shouldProfileJVM("v", 2));
    assertFalse(profilerOptions.shouldProfileJVM("v", 3));
    assertFalse(profilerOptions.shouldProfileJVM("v", 4));
    assertFalse(profilerOptions.shouldProfileJVM("v", 5));
    assertFalse(profilerOptions.shouldProfileJVM("v", 6));
    assertFalse(profilerOptions.shouldProfileJVM("v", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, "v[1:,5]");
    assertFalse(profilerOptions.shouldProfileJVM("v", 1));
    assertFalse(profilerOptions.shouldProfileJVM("v", 5));
    assertFalse(profilerOptions.shouldProfileJVM("v", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, "v[:1,5]");
    assertFalse(profilerOptions.shouldProfileJVM("v", 0));
    assertFalse(profilerOptions.shouldProfileJVM("v", 1));
    assertFalse(profilerOptions.shouldProfileJVM("v", 5));
    assertFalse(profilerOptions.shouldProfileJVM("v", -1));
    assertFalse(profilerOptions.shouldProfileJVM("v", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, " v[1:,4, 5],    [5,4]");
    assertFalse(profilerOptions.shouldProfileJVM("v", 4));
    assertFalse(profilerOptions.shouldProfileJVM("v", 1));
    assertFalse(profilerOptions.shouldProfileJVM("v", 5));
    assertFalse(profilerOptions.shouldProfileJVM("v", 3));
    assertFalse(profilerOptions.shouldProfileJVM(" ", 4));
    assertFalse(profilerOptions.shouldProfileJVM("v", rnd.nextInt(Integer.MAX_VALUE)));

    profilerOptions = getOptions(conf, " v[-3:1,4]");
    assertFalse(profilerOptions.shouldProfileJVM("v", 4));
    assertFalse(profilerOptions.shouldProfileJVM("v", 1));
    assertFalse(profilerOptions.shouldProfileJVM("v", 3));
    assertFalse(profilerOptions.shouldProfileJVM("v", -3));
    assertFalse(profilerOptions.shouldProfileJVM("v", rnd.nextInt(Integer.MAX_VALUE)));
  }

  @Test
  public void testProfilingJVMOpts() {
    Configuration conf = new Configuration();
    JavaProfilerOptions profilerOptions = getOptions(conf, "", "");
    String jvmOpts = profilerOptions.getProfilerOptions("", "", 0);
    assertTrue(jvmOpts.trim().equals(""));

    profilerOptions = getOptions(conf, "", "dir=__VERTEX_NAME__");
    jvmOpts = profilerOptions.getProfilerOptions("", "Map 1", 0);
    assertTrue(jvmOpts.equals("dir=Map1"));

    profilerOptions = getOptions(conf, "", "dir=__TASK_INDEX__");
    jvmOpts = profilerOptions.getProfilerOptions("", "Map 1", 0);
    assertTrue(jvmOpts.equals("dir=0"));

    profilerOptions = getOptions(conf, "v[1,3,4]", "dir=/tmp/__VERTEX_NAME__/__TASK_INDEX__");
    jvmOpts = profilerOptions.getProfilerOptions("", "v", 1);
    assertTrue(jvmOpts.equals("dir=/tmp/v/1"));

    jvmOpts = profilerOptions.getProfilerOptions("", "v", 3);
    assertTrue(jvmOpts.equals("dir=/tmp/v/3"));

    jvmOpts = profilerOptions.getProfilerOptions("", "v", 4);
    assertTrue(jvmOpts.equals("dir=/tmp/v/4"));
  }
}
