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
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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
      TaskLocationHint[] actual) throws Exception {
    JobID jobId = new JobID("dummy", 1);
    TaskSplitMetaInfo[] splitsInfo =
        SplitMetaInfoReader.readSplitMetaInfo(jobId , remoteFs,
            conf, inputSplitsDir);
    int splitsCount = splitsInfo.length;
    TaskLocationHint[] locationHints =
        new TaskLocationHint[splitsCount];
    for (int i = 0; i < splitsCount; ++i) {
      TaskLocationHint locationHint =
          new TaskLocationHint(splitsInfo[i].getLocations(), null);
      locationHints[i] = locationHint;
    }

    Assert.assertArrayEquals(locationHints, actual);
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

    Assert.assertEquals(new Path(newSplitsDir, "job.split"),
        info.getSplitsFile());
    Assert.assertEquals(new Path(newSplitsDir, "job.splitmetainfo"),
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
      if (fName.equals("job.split")) {
        foundSplitsFile = true;
      } else if (fName.equals("job.splitmetainfo")) {
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
    Assert.assertEquals(new Path(oldSplitsDir, "job.split"),
        info.getSplitsFile());
    Assert.assertEquals(new Path(oldSplitsDir, "job.splitmetainfo"),
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
      if (fName.equals("job.split")) {
        foundSplitsFile = true;
      } else if (fName.equals("job.splitmetainfo")) {
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

}
