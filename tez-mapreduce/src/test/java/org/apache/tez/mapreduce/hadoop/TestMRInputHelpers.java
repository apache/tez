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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
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
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestMRInputHelpers {

  protected static MiniDFSCluster dfsCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem remoteFs;
  private static LocalFileSystem localFs;
  private static Path testFilePath;
  private static Path oldSplitsDir;
  private static Path newSplitsDir;

  private static Path testRootDir;
  private static Path localTestRootDir;

  @BeforeClass
  public static void setup() throws IOException {
    testRootDir = new Path(Files.createTempDirectory(TestMRHelpers.class.getName()).toString());
    localTestRootDir = new Path(Files.createTempDirectory(TestMRHelpers.class.getName() + "-local").toString());

    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testRootDir.toString());
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
          .format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }

    Configuration testConf = new Configuration(
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

    localFs = FileSystem.getLocal(conf);
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

  @Test
  public void testInputEventSerializedPayload() throws IOException {
    MRSplitProto proto = MRSplitProto.newBuilder().setSplitBytes(ByteString.copyFrom("splits".getBytes())).build();

    InputDataInformationEvent initEvent =
        InputDataInformationEvent.createWithSerializedPayload(0, proto.toByteString().asReadOnlyByteBuffer());
    MRSplitProto protoFromEvent = MRInputHelpers.getProto(initEvent, new JobConf(conf));

    Assert.assertEquals(proto, protoFromEvent);
  }

  @Test
  public void testInputEventSerializedPath() throws IOException {
    MRSplitProto proto = MRSplitProto.newBuilder().setSplitBytes(ByteString.copyFrom("splits".getBytes())).build();

    Path splitsDir = localFs.resolvePath(localTestRootDir);

    Path serializedPath = new Path(splitsDir + Path.SEPARATOR + "splitpayload");

    try (FSDataOutputStream out = localFs.create(serializedPath)) {
      proto.writeTo(out);
    }

    // event file is present on fs
    Assert.assertTrue("Event file should be present on fs", localFs.exists(serializedPath));

    InputDataInformationEvent initEvent =
        InputDataInformationEvent.createWithSerializedPath(0, serializedPath.toUri().toString());
    MRSplitProto protoFromEvent = MRInputHelpers.getProto(initEvent, new JobConf(conf));

    Assert.assertEquals(proto, protoFromEvent);

    // event file is deleted after read
    Assert.assertFalse("Event file should be deleted after read", localFs.exists(serializedPath));
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
    Path splitsDir = localFs.resolvePath(localTestRootDir);

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
  }

  @Before
  public void before() throws IOException {
    localFs.mkdirs(localTestRootDir);
  }

  @After
  public void after() throws IOException {
    localFs.delete(localTestRootDir, true);
  }
}
