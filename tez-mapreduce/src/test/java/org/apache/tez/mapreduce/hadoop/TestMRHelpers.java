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

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMRHelpers {

  protected static MiniDFSCluster dfsCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem remoteFs;
  private static Path testFilePath;
  private static Path oldSplitsDir;
  private static Path newSplitsDir;

  private static String TEST_ROOT_DIR = "target"
      + Path.SEPARATOR + TestMRHelpers.class.getName() + "-tmpDir";

  @BeforeClass
  public static void setup() throws IOException {
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }

    Configuration testConf = new YarnConfiguration(
        dfsCluster.getFileSystem().getConf());

    File testConfFile = new File(TEST_ROOT_DIR, "test.xml");
    try {
      testConfFile.createNewFile();
      testConf.writeXml(new FileOutputStream(testConfFile));
      testConfFile.deleteOnExit();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    remoteFs.mkdirs(new Path("/tmp/input/"));
    remoteFs.mkdirs(new Path("/tmp/splitsDirNew/"));
    remoteFs.mkdirs(new Path("/tmp/splitsDirOld/"));
    testFilePath = remoteFs.makeQualified(new Path("/tmp/input/test.xml"));
    remoteFs.copyFromLocalFile(new Path(testConfFile.getAbsolutePath()),
        testFilePath);
    FileStatus fsStatus = remoteFs.getFileStatus(testFilePath);
    Assert.assertTrue(fsStatus.getLen() > 0);

    oldSplitsDir = remoteFs.makeQualified(new Path("/tmp/splitsDirOld/"));
    newSplitsDir = remoteFs.makeQualified(new Path("/tmp/splitsDirNew/"));
  }

  private void verifyLocationHints(Path inputSplitsDir,
      List<TaskLocationHint> actual) throws Exception {
    JobID jobId = new JobID("dummy", 1);
    TaskSplitMetaInfo[] splitsInfo =
        SplitMetaInfoReader.readSplitMetaInfo(jobId , remoteFs,
            conf, inputSplitsDir);
    int splitsCount = splitsInfo.length;
    List<TaskLocationHint> locationHints =
        new ArrayList<TaskLocationHint>(splitsCount);
    for (int i = 0; i < splitsCount; ++i) {
      locationHints.add(
          new TaskLocationHint(new HashSet<String>(
              Arrays.asList(splitsInfo[i].getLocations())), null));
    }

    Assert.assertEquals(locationHints, actual);
  }

  private InputSplitInfo generateNewSplits(Path inputSplitsDir)
      throws Exception {
    JobConf jobConf = new JobConf();
    jobConf.setUseNewMapper(true);
    jobConf.setClass(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, TextInputFormat.class,
        InputFormat.class);
    jobConf.set(TextInputFormat.INPUT_DIR, testFilePath.toString());

    return MRHelpers.generateInputSplits(jobConf, inputSplitsDir);
  }

  @Test
  public void testNewSplitsGen() throws Exception {
    InputSplitInfo info = generateNewSplits(newSplitsDir);

    Assert.assertEquals(new Path(newSplitsDir,
        MRHelpers.JOB_SPLIT_RESOURCE_NAME),
        info.getSplitsFile());
    Assert.assertEquals(new Path(newSplitsDir,
        MRHelpers.JOB_SPLIT_METAINFO_RESOURCE_NAME),
        info.getSplitsMetaInfoFile());

    RemoteIterator<LocatedFileStatus> files =
        remoteFs.listFiles(newSplitsDir, false);

    boolean foundSplitsFile = false;
    boolean foundMetaFile = false;
    int totalFilesFound = 0;

    while (files.hasNext()) {
      LocatedFileStatus status = files.next();
      String fName = status.getPath().getName();
      totalFilesFound++;
      if (fName.equals(MRHelpers.JOB_SPLIT_RESOURCE_NAME)) {
        foundSplitsFile = true;
      } else if (fName.equals(MRHelpers.JOB_SPLIT_METAINFO_RESOURCE_NAME)) {
        foundMetaFile = true;
      } else {
        Assert.fail("Found invalid file in splits dir, filename=" + fName);
      }
      Assert.assertTrue(status.getLen() > 0);
    }

    Assert.assertEquals(2, totalFilesFound);
    Assert.assertTrue(foundSplitsFile);
    Assert.assertTrue(foundMetaFile);

    verifyLocationHints(newSplitsDir, info.getTaskLocationHints());
  }

  private InputSplitInfo generateOldSplits(Path inputSplitsDir)
      throws Exception {
    JobConf jobConf = new JobConf();
    jobConf.setUseNewMapper(false);
    jobConf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
    jobConf.set(TextInputFormat.INPUT_DIR, testFilePath.toString());

    return MRHelpers.generateInputSplits(jobConf, inputSplitsDir);
  }

  @Test
  public void testOldSplitsGen() throws Exception {
    InputSplitInfo info = generateOldSplits(oldSplitsDir);
    Assert.assertEquals(new Path(oldSplitsDir,
        MRHelpers.JOB_SPLIT_RESOURCE_NAME),
        info.getSplitsFile());
    Assert.assertEquals(new Path(oldSplitsDir,
        MRHelpers.JOB_SPLIT_METAINFO_RESOURCE_NAME),
        info.getSplitsMetaInfoFile());

    RemoteIterator<LocatedFileStatus> files =
        remoteFs.listFiles(oldSplitsDir, false);

    boolean foundSplitsFile = false;
    boolean foundMetaFile = false;
    int totalFilesFound = 0;

    while (files.hasNext()) {
      LocatedFileStatus status = files.next();
      String fName = status.getPath().getName();
      totalFilesFound++;
      if (fName.equals(MRHelpers.JOB_SPLIT_RESOURCE_NAME)) {
        foundSplitsFile = true;
      } else if (fName.equals(MRHelpers.JOB_SPLIT_METAINFO_RESOURCE_NAME)) {
        foundMetaFile = true;
      } else {
        Assert.fail("Found invalid file in splits dir, filename=" + fName);
      }
      Assert.assertTrue(status.getLen() > 0);
    }

    Assert.assertEquals(2, totalFilesFound);
    Assert.assertTrue(foundSplitsFile);
    Assert.assertTrue(foundMetaFile);

    verifyLocationHints(oldSplitsDir, info.getTaskLocationHints());
  }

  @Test
  public void testInputSplitLocalResourceCreation() throws Exception {
    InputSplitInfo inputSplitInfo = generateOldSplits(oldSplitsDir);
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();
    localResources.put("job.split", null);

    try {
      MRHelpers.updateLocalResourcesForInputSplits(remoteFs,
          inputSplitInfo, localResources);
      fail("Expected failure for job.split override in local resources map");
    } catch (RuntimeException e) {
      // Expected
    }

    localResources.clear();
    MRHelpers.updateLocalResourcesForInputSplits(remoteFs,
        inputSplitInfo, localResources);

    Assert.assertEquals(2, localResources.size());
    Assert.assertTrue(localResources.containsKey(
        MRHelpers.JOB_SPLIT_RESOURCE_NAME));
    Assert.assertTrue(localResources.containsKey(
        MRHelpers.JOB_SPLIT_METAINFO_RESOURCE_NAME));
  }

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
    String opts = MRHelpers.getMapJavaOpts(conf);

    Assert.assertTrue(opts.contains("fooMapAdminOpts"));
    Assert.assertTrue(opts.contains(" fooMapJavaOpts "));
    Assert.assertFalse(opts.contains("fooReduceAdminOpts "));
    Assert.assertFalse(opts.contains(" fooReduceJavaOpts "));
    Assert.assertTrue(opts.indexOf("fooMapAdminOpts")
        < opts.indexOf("fooMapJavaOpts"));
    Assert.assertTrue(opts.contains(" -D"
        + TezConfiguration.TEZ_ROOT_LOGGER_NAME + "=FATAL"));
    Assert.assertFalse(opts.contains(" -D"
        + TezConfiguration.TEZ_ROOT_LOGGER_NAME + "=TRACE"));
  }

  @Test
  public void testReduceJavaOptions() {
    Configuration conf = createConfForJavaOptsTest();
    String opts = MRHelpers.getReduceJavaOpts(conf);

    Assert.assertFalse(opts.contains("fooMapAdminOpts"));
    Assert.assertFalse(opts.contains(" fooMapJavaOpts "));
    Assert.assertTrue(opts.contains("fooReduceAdminOpts"));
    Assert.assertTrue(opts.contains(" fooReduceJavaOpts "));
    Assert.assertTrue(opts.indexOf("fooReduceAdminOpts")
        < opts.indexOf("fooReduceJavaOpts"));
    Assert.assertFalse(opts.contains(" -D"
        + TezConfiguration.TEZ_ROOT_LOGGER_NAME + "=FATAL"));
    Assert.assertTrue(opts.contains(" -D"
        + TezConfiguration.TEZ_ROOT_LOGGER_NAME + "=TRACE"));
  }

  @Test
  public void testContainerResourceConstruction() {
    JobConf conf = new JobConf(new Configuration());
    Resource mapResource = MRHelpers.getMapResource(conf);
    Resource reduceResource = MRHelpers.getReduceResource(conf);

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

    mapResource = MRHelpers.getMapResource(conf);
    reduceResource = MRHelpers.getReduceResource(conf);

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
    conf.set(MRJobConfig.MAPRED_ADMIN_USER_ENV, "LD_LIBRARY_PATH=$TEZ_ADMIN_ENV_TEST/lib/native");
    return conf;
  }

  private void testCommonEnvSettingsForMRTasks(Map<String, String> env) {
    Assert.assertTrue(env.containsKey("foo"));
    Assert.assertTrue(env.containsKey("bar"));
    Assert.assertTrue(env.containsKey(Environment.LD_LIBRARY_PATH.name()));
    Assert.assertTrue(env.containsKey(Environment.SHELL.name()));
    Assert.assertTrue(env.containsKey("HADOOP_ROOT_LOGGER"));
    Assert.assertEquals("$PWD:$TEZ_ADMIN_ENV_TEST/lib/native",
        env.get(Environment.LD_LIBRARY_PATH.name()));

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
    MRHelpers.updateEnvironmentForMRTasks(conf, env, true);
    testCommonEnvSettingsForMRTasks(env);
    Assert.assertEquals("map1", env.get("foo"));
    Assert.assertEquals("map2", env.get("bar"));
  }

  @Test
  public void testMREnvSetupForReduce() {
    Configuration conf = setupConfigForMREnvTest();
    Map<String, String> env = new HashMap<String, String>();
    MRHelpers.updateEnvironmentForMRTasks(conf, env, false);
    testCommonEnvSettingsForMRTasks(env);
    Assert.assertEquals("red1", env.get("foo"));
    Assert.assertEquals("red2", env.get("bar"));
  }

  @Test
  public void testGetBaseMRConf() {
    Configuration conf = MRHelpers.getBaseMRConfiguration();
    Assert.assertNotNull(conf);
    conf = MRHelpers.getBaseMRConfiguration(new YarnConfiguration());
    Assert.assertNotNull(conf);
  }

  @Test
  public void testMRAMJavaOpts() {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_ADMIN_COMMAND_OPTS, " -Dadminfoobar   ");
    conf.set(MRJobConfig.MR_AM_COMMAND_OPTS, "  -Duserfoo  ");
    String opts = MRHelpers.getMRAMJavaOpts(conf);
    Assert.assertEquals("-Dadminfoobar -Duserfoo", opts);
  }

  public void testMRAMEnvironmentSetup() {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_ADMIN_USER_ENV, "foo=bar,admin1=foo1");
    conf.set(MRJobConfig.MR_AM_ENV, "foo=bar2,user=foo2");
    Map<String, String> env =
        new HashMap<String, String>();
    MRHelpers.updateEnvironmentForMRAM(conf, env);
    Assert.assertEquals("foo1", env.get("admin1"));
    Assert.assertEquals("foo2", env.get("user"));
    Assert.assertEquals("bar2", env.get("foo"));
  }
}
