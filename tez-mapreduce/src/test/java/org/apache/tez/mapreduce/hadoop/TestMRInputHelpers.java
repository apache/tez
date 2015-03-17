/*
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.TaskLocationHint;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMRInputHelpers {

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


    FSDataOutputStream dataOutputStream = null;
    try {
      dataOutputStream = remoteFs.create(new Path("/tmp/input/test.xml"), true);
      testConf.writeXml(dataOutputStream);
      dataOutputStream.hsync();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    } finally {
      if (dataOutputStream != null) {
        dataOutputStream.close();
      }
    }

    remoteFs.mkdirs(new Path("/tmp/input/"));
    remoteFs.mkdirs(new Path("/tmp/splitsDirNew/"));
    remoteFs.mkdirs(new Path("/tmp/splitsDirOld/"));
    testFilePath = remoteFs.makeQualified(new Path("/tmp/input/test.xml"));
    FileStatus fsStatus = remoteFs.getFileStatus(testFilePath);
    Assert.assertTrue(fsStatus.getLen() > 0);

    oldSplitsDir = remoteFs.makeQualified(new Path("/tmp/splitsDirOld/"));
    newSplitsDir = remoteFs.makeQualified(new Path("/tmp/splitsDirNew/"));
  }


  @Test(timeout = 5000)
  public void testNewSplitsGen() throws Exception {

    DataSourceDescriptor dataSource = generateDataSourceDescriptorMapReduce(newSplitsDir);

    Assert.assertTrue(dataSource.getAdditionalLocalFiles()
        .containsKey(MRInputHelpers.JOB_SPLIT_RESOURCE_NAME));
    Assert.assertTrue(dataSource.getAdditionalLocalFiles()
        .containsKey(MRInputHelpers.JOB_SPLIT_METAINFO_RESOURCE_NAME));

    RemoteIterator<LocatedFileStatus> files =
        remoteFs.listFiles(newSplitsDir, false);

    boolean foundSplitsFile = false;
    boolean foundMetaFile = false;
    int totalFilesFound = 0;

    while (files.hasNext()) {
      LocatedFileStatus status = files.next();
      String fName = status.getPath().getName();
      totalFilesFound++;
      if (fName.equals(MRInputHelpers.JOB_SPLIT_RESOURCE_NAME)) {
        foundSplitsFile = true;
      } else if (fName.equals(MRInputHelpers.JOB_SPLIT_METAINFO_RESOURCE_NAME)) {
        foundMetaFile = true;
      } else {
        Assert.fail("Found invalid file in splits dir, filename=" + fName);
      }
      Assert.assertTrue(status.getLen() > 0);
    }

    Assert.assertEquals(2, totalFilesFound);
    Assert.assertTrue(foundSplitsFile);
    Assert.assertTrue(foundMetaFile);

    verifyLocationHints(newSplitsDir, dataSource.getLocationHint().getTaskLocationHints());
  }

  @Test(timeout = 5000)
  public void testOldSplitsGen() throws Exception {
    DataSourceDescriptor dataSource = generateDataSourceDescriptorMapRed(oldSplitsDir);
    Assert.assertTrue(
        dataSource.getAdditionalLocalFiles().containsKey(MRInputHelpers.JOB_SPLIT_RESOURCE_NAME));
    Assert.assertTrue(dataSource.getAdditionalLocalFiles()
        .containsKey(MRInputHelpers.JOB_SPLIT_METAINFO_RESOURCE_NAME));

    RemoteIterator<LocatedFileStatus> files =
        remoteFs.listFiles(oldSplitsDir, false);

    boolean foundSplitsFile = false;
    boolean foundMetaFile = false;
    int totalFilesFound = 0;

    while (files.hasNext()) {
      LocatedFileStatus status = files.next();
      String fName = status.getPath().getName();
      totalFilesFound++;
      if (fName.equals(MRInputHelpers.JOB_SPLIT_RESOURCE_NAME)) {
        foundSplitsFile = true;
      } else if (fName.equals(MRInputHelpers.JOB_SPLIT_METAINFO_RESOURCE_NAME)) {
        foundMetaFile = true;
      } else {
        Assert.fail("Found invalid file in splits dir, filename=" + fName);
      }
      Assert.assertTrue(status.getLen() > 0);
    }

    Assert.assertEquals(2, totalFilesFound);
    Assert.assertTrue(foundSplitsFile);
    Assert.assertTrue(foundMetaFile);

    verifyLocationHints(oldSplitsDir, dataSource.getLocationHint().getTaskLocationHints());
  }

  @Test(timeout = 5000)
  public void testInputSplitLocalResourceCreation() throws Exception {
    DataSourceDescriptor dataSource = generateDataSourceDescriptorMapRed(oldSplitsDir);

    Map<String, LocalResource> localResources = dataSource.getAdditionalLocalFiles();

    Assert.assertEquals(2, localResources.size());
    Assert.assertTrue(localResources.containsKey(
        MRInputHelpers.JOB_SPLIT_RESOURCE_NAME));
    Assert.assertTrue(localResources.containsKey(
        MRInputHelpers.JOB_SPLIT_METAINFO_RESOURCE_NAME));
  }

  private void verifyLocationHints(Path inputSplitsDir,
                                   List<TaskLocationHint> actual) throws Exception {
    JobID jobId = new JobID("dummy", 1);
    JobSplit.TaskSplitMetaInfo[] splitsInfo =
        SplitMetaInfoReader.readSplitMetaInfo(jobId, remoteFs,
            conf, inputSplitsDir);
    int splitsCount = splitsInfo.length;
    List<TaskLocationHint> locationHints =
        new ArrayList<TaskLocationHint>(splitsCount);
    for (int i = 0; i < splitsCount; ++i) {
      locationHints.add(
          TaskLocationHint.createTaskLocationHint(new HashSet<String>(
              Arrays.asList(splitsInfo[i].getLocations())), null)
      );
    }

    Assert.assertEquals(locationHints, actual);
  }

  private DataSourceDescriptor generateDataSourceDescriptorMapReduce(Path inputSplitsDir)
      throws Exception {
    JobConf jobConf = new JobConf(dfsCluster.getFileSystem().getConf());
    jobConf.setUseNewMapper(true);
    jobConf.setClass(org.apache.hadoop.mapreduce.MRJobConfig.INPUT_FORMAT_CLASS_ATTR, TextInputFormat.class,
        InputFormat.class);
    jobConf.set(TextInputFormat.INPUT_DIR, testFilePath.toString());

    return MRInputHelpers.configureMRInputWithLegacySplitGeneration(jobConf, inputSplitsDir, true);
  }


  private DataSourceDescriptor generateDataSourceDescriptorMapRed(Path inputSplitsDir)
      throws Exception {
    JobConf jobConf = new JobConf(dfsCluster.getFileSystem().getConf());
    jobConf.setUseNewMapper(false);
    jobConf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
    jobConf.set(TextInputFormat.INPUT_DIR, testFilePath.toString());

    return MRInputHelpers.configureMRInputWithLegacySplitGeneration(jobConf, inputSplitsDir, true);
  }

  @Test(timeout = 5000)
  public void testInputSplitLocalResourceCreationWithDifferentFS() throws Exception {
    FileSystem localFs = FileSystem.getLocal(conf);
    Path LOCAL_TEST_ROOT_DIR = new Path("target"
        + Path.SEPARATOR + TestMRHelpers.class.getName() + "-localtmpDir");

    try {
      localFs.mkdirs(LOCAL_TEST_ROOT_DIR);

      Path splitsDir = localFs.resolvePath(LOCAL_TEST_ROOT_DIR);

      DataSourceDescriptor dataSource = generateDataSourceDescriptorMapRed(splitsDir);

      Map<String, LocalResource> localResources = dataSource.getAdditionalLocalFiles();

      Assert.assertEquals(2, localResources.size());
      Assert.assertTrue(localResources.containsKey(
          MRInputHelpers.JOB_SPLIT_RESOURCE_NAME));
      Assert.assertTrue(localResources.containsKey(
          MRInputHelpers.JOB_SPLIT_METAINFO_RESOURCE_NAME));

      for (LocalResource lr : localResources.values()) {
        Assert.assertFalse(lr.getResource().getScheme().contains(remoteFs.getScheme()));
      }
    } finally {
      localFs.delete(LOCAL_TEST_ROOT_DIR, true);
    }
  }

}
