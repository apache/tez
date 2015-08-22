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

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FileChunk;
import org.apache.hadoop.io.IntWritable;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestMergeManager {


  private static final Log LOG = LogFactory.getLog(TestMergeManager.class);

  private static Configuration defaultConf = new TezConfiguration();
  private static FileSystem localFs = null;
  private static Path workDir = null;

  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
      workDir =
          new Path(new Path(System.getProperty("test.build.data", "/tmp")),
              TestMergeManager.class.getSimpleName());
      workDir = localFs.makeQualified(workDir);
      localFs.mkdirs(workDir);
      LOG.info("Using workDir: " + workDir);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  @Before
  @After
  public void cleanup() throws IOException {
    localFs.delete(workDir, true);
  }

  @Test(timeout = 10000)
  public void testLocalDiskMergeMultipleTasks() throws IOException {

    Configuration conf = new TezConfiguration(defaultConf);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, false);
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, IntWritable.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, IntWritable.class.getName());

    Path localDir = new Path(workDir, "local");
    Path srcDir = new Path(workDir, "srcData");
    localFs.mkdirs(localDir);
    localFs.mkdirs(srcDir);

    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, localDir.toString());

    FileSystem localFs = FileSystem.getLocal(conf);
    LocalDirAllocator localDirAllocator =
        new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);
    InputContext t0inputContext = createMockInputContext(UUID.randomUUID().toString());
    InputContext t1inputContext = createMockInputContext(UUID.randomUUID().toString());

    ExceptionReporter t0exceptionReporter = mock(ExceptionReporter.class);
    ExceptionReporter t1exceptionReporter = mock(ExceptionReporter.class);

    MergeManager t0mergeManagerReal =
        new MergeManager(conf, localFs, localDirAllocator, t0inputContext, null, null, null, null,
            t0exceptionReporter, 2000000, null, false, -1);
    MergeManager t0mergeManager = spy(t0mergeManagerReal);

    MergeManager t1mergeManagerReal =
        new MergeManager(conf, localFs, localDirAllocator, t1inputContext, null, null, null, null,
            t1exceptionReporter, 2000000, null, false, -1);
    MergeManager t1mergeManager = spy(t1mergeManagerReal);

    // Partition 0 Keys 0-2, Partition 1 Keys 3-5
    SrcFileInfo src1Info =
        createFile(conf, localFs, new Path(srcDir, InputAttemptIdentifier.PATH_PREFIX + "src1.out"),
            2, 3, 0);
    // Partition 0 Keys 6-8, Partition 1 Keys 9-11
    SrcFileInfo src2Info =
        createFile(conf, localFs, new Path(srcDir, InputAttemptIdentifier.PATH_PREFIX + "src2.out"),
            2, 3, 6);


    // Simulating Task 0 fetches partition 0. (targetIndex = 0,1)

    // Simulating Task 1 fetches partition 1. (targetIndex = 0,1)

    InputAttemptIdentifier t0Identifier0 =
        new InputAttemptIdentifier(0, 0, src1Info.path.getName());
    InputAttemptIdentifier t0Identifier1 =
        new InputAttemptIdentifier(1, 0, src2Info.path.getName());

    InputAttemptIdentifier t1Identifier0 =
        new InputAttemptIdentifier(0, 0, src1Info.path.getName());
    InputAttemptIdentifier t1Identifier1 =
        new InputAttemptIdentifier(1, 0, src2Info.path.getName());


    MapOutput t0MapOutput0 =
        getMapOutputForDirectDiskFetch(t0Identifier0, src1Info.path, src1Info.indexedRecords[0],
            t0mergeManager);
    MapOutput t0MapOutput1 =
        getMapOutputForDirectDiskFetch(t0Identifier1, src2Info.path, src2Info.indexedRecords[0],
            t0mergeManager);

    MapOutput t1MapOutput0 =
        getMapOutputForDirectDiskFetch(t1Identifier0, src1Info.path, src1Info.indexedRecords[1],
            t1mergeManager);
    MapOutput t1MapOutput1 =
        getMapOutputForDirectDiskFetch(t1Identifier1, src2Info.path, src2Info.indexedRecords[1],
            t1mergeManager);


    t0MapOutput0.commit();
    t0MapOutput1.commit();
    verify(t0mergeManager).closeOnDiskFile(t0MapOutput0.getOutputPath());
    verify(t0mergeManager).closeOnDiskFile(t0MapOutput1.getOutputPath());
    // Run the OnDiskMerge via MergeManager
    // Simulate the thread invocation - remove files, and invoke merge
    List<FileChunk> t0MergeFiles = new LinkedList<FileChunk>();
    t0MergeFiles.addAll(t0mergeManager.onDiskMapOutputs);
    t0mergeManager.onDiskMapOutputs.clear();
    t0mergeManager.onDiskMerger.merge(t0MergeFiles);
    Assert.assertEquals(1, t0mergeManager.onDiskMapOutputs.size());


    t1MapOutput0.commit();
    t1MapOutput1.commit();
    verify(t1mergeManager).closeOnDiskFile(t1MapOutput0.getOutputPath());
    verify(t1mergeManager).closeOnDiskFile(t1MapOutput1.getOutputPath());
    // Run the OnDiskMerge via MergeManager
    // Simulate the thread invocation - remove files, and invoke merge
    List<FileChunk> t1MergeFiles = new LinkedList<FileChunk>();
    t1MergeFiles.addAll(t1mergeManager.onDiskMapOutputs);
    t1mergeManager.onDiskMapOutputs.clear();
    t1mergeManager.onDiskMerger.merge(t1MergeFiles);
    Assert.assertEquals(1, t1mergeManager.onDiskMapOutputs.size());

    Assert.assertNotEquals(t0mergeManager.onDiskMapOutputs.iterator().next().getPath(),
        t1mergeManager.onDiskMapOutputs.iterator().next().getPath());

    Assert.assertTrue(t0mergeManager.onDiskMapOutputs.iterator().next().getPath().toString()
        .contains(t0inputContext.getUniqueIdentifier()));
    Assert.assertTrue(t1mergeManager.onDiskMapOutputs.iterator().next().getPath().toString()
        .contains(t1inputContext.getUniqueIdentifier()));

  }

  @Test(timeout = 10000)
  public void testOnDiskMergerFilenames() throws IOException, InterruptedException {
    Configuration conf = new TezConfiguration(defaultConf);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, false);
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, IntWritable.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, IntWritable.class.getName());

    Path localDir = new Path(workDir, "local");
    Path srcDir = new Path(workDir, "srcData");
    localFs.mkdirs(localDir);
    localFs.mkdirs(srcDir);

    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, localDir.toString());

    FileSystem localFs = FileSystem.getLocal(conf);

    LocalDirAllocator localDirAllocator =
        new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);
    InputContext inputContext = createMockInputContext(UUID.randomUUID().toString());

    ExceptionReporter exceptionReporter = mock(ExceptionReporter.class);

    MergeManager mergeManagerReal =
        new MergeManager(conf, localFs, localDirAllocator, inputContext, null, null, null, null,
            exceptionReporter, 1 * 1024l * 1024l, null, false, -1);
    MergeManager mergeManager = spy(mergeManagerReal);

    // Partition 0 Keys 0-2, Partition 1 Keys 3-5
    SrcFileInfo file1Info =
        createFile(conf, localFs, new Path(srcDir, InputAttemptIdentifier.PATH_PREFIX + "src1.out"),
            2, 3, 6);

    SrcFileInfo file2Info =
        createFile(conf, localFs, new Path(srcDir, InputAttemptIdentifier.PATH_PREFIX + "src2.out"),
            2, 3, 0);

    InputAttemptIdentifier iIdentifier1 =
        new InputAttemptIdentifier(0, 0, file1Info.path.getName());
    InputAttemptIdentifier iIdentifier2 =
        new InputAttemptIdentifier(1, 0, file2Info.path.getName());

    MapOutput mapOutput1 =
        getMapOutputForDirectDiskFetch(iIdentifier1, file1Info.path, file1Info.indexedRecords[0],
            mergeManager);
    MapOutput mapOutput2 =
        getMapOutputForDirectDiskFetch(iIdentifier2, file2Info.path, file2Info.indexedRecords[0],
            mergeManager);

    mapOutput1.commit();
    mapOutput2.commit();
    verify(mergeManager).closeOnDiskFile(mapOutput1.getOutputPath());
    verify(mergeManager).closeOnDiskFile(mapOutput2.getOutputPath());

    List<FileChunk> mergeFiles = new LinkedList<FileChunk>();
    mergeFiles.addAll(mergeManager.onDiskMapOutputs);
    mergeManager.onDiskMapOutputs.clear();

    mergeManager.onDiskMerger.merge(mergeFiles);
    Assert.assertEquals(1, mergeManager.onDiskMapOutputs.size());

    FileChunk fcMerged1 = mergeManager.onDiskMapOutputs.iterator().next();
    Path m1Path = fcMerged1.getPath();
    assertTrue(m1Path.toString().endsWith("merged0"));

    // Add another file. Make sure the filename is different, and does not get clobbered.

    SrcFileInfo file3Info =
        createFile(conf, localFs, new Path(srcDir, InputAttemptIdentifier.PATH_PREFIX + "src3.out"),
            2, 22, 5);
    InputAttemptIdentifier iIdentifier3 =
        new InputAttemptIdentifier(2, 0, file1Info.path.getName());
    MapOutput mapOutput3 =
        getMapOutputForDirectDiskFetch(iIdentifier3, file3Info.path, file3Info.indexedRecords[0],
            mergeManager);
    mapOutput3.commit();
    verify(mergeManager).closeOnDiskFile(mapOutput3.getOutputPath());

    mergeFiles = new LinkedList<FileChunk>();
    mergeFiles.addAll(mergeManager.onDiskMapOutputs);
    mergeManager.onDiskMapOutputs.clear();

    mergeManager.onDiskMerger.merge(mergeFiles);
    Assert.assertEquals(1, mergeManager.onDiskMapOutputs.size());

    FileChunk fcMerged2 = mergeManager.onDiskMapOutputs.iterator().next();
    Path m2Path = fcMerged2.getPath();
    assertTrue(m2Path.toString().endsWith("merged1"));
    assertNotEquals(m1Path, m2Path);

    // Add another file. This time add it to the head of the list.
    SrcFileInfo file4Info =
        createFile(conf, localFs, new Path(srcDir, InputAttemptIdentifier.PATH_PREFIX + "src4.out"),
            2, 45, 35);
    InputAttemptIdentifier iIdentifier4 =
        new InputAttemptIdentifier(3, 0, file4Info.path.getName());
    MapOutput mapOutput4 =
        getMapOutputForDirectDiskFetch(iIdentifier4, file4Info.path, file4Info.indexedRecords[0],
            mergeManager);
    mapOutput4.commit();
    verify(mergeManager).closeOnDiskFile(mapOutput4.getOutputPath());

    // Add in reverse order this time.
    List<FileChunk> tmpList = new LinkedList<FileChunk>();
    mergeFiles = new LinkedList<FileChunk>();
    assertEquals(2, mergeManager.onDiskMapOutputs.size());
    tmpList.addAll(mergeManager.onDiskMapOutputs);
    mergeFiles.add(tmpList.get(1));
    mergeFiles.add(tmpList.get(0));

    mergeManager.onDiskMapOutputs.clear();

    mergeManager.onDiskMerger.merge(mergeFiles);
    Assert.assertEquals(1, mergeManager.onDiskMapOutputs.size());

    FileChunk fcMerged3 = mergeManager.onDiskMapOutputs.iterator().next();
    Path m3Path = fcMerged3.getPath();

    assertTrue(m3Path.toString().endsWith("merged2"));
    assertNotEquals(m2Path, m3Path);

    // Ensure the lengths are the same - since the source file names are the same. No append happening.
    assertEquals(m1Path.toString().length(), m2Path.toString().length());
    assertEquals(m2Path.toString().length(), m3Path.toString().length());

    // Ensure the filenames are used correctly - based on the first file given to the merger.
    String m1Prefix = m1Path.toString().substring(0, m1Path.toString().indexOf("."));
    String m2Prefix = m2Path.toString().substring(0, m2Path.toString().indexOf("."));
    String m3Prefix = m3Path.toString().substring(0, m3Path.toString().indexOf("."));

    assertEquals(m1Prefix, m2Prefix);
    assertNotEquals(m1Prefix, m3Prefix);
    assertNotEquals(m2Prefix, m3Prefix);

  }

  private InputContext createMockInputContext(String uniqueId) {
    InputContext inputContext = mock(InputContext.class);
    doReturn(new TezCounters()).when(inputContext).getCounters();
    doReturn(200 * 1024 * 1024l).when(inputContext).getTotalMemoryAvailableToTask();
    doReturn("srcVertexName").when(inputContext).getSourceVertexName();
    doReturn(uniqueId).when(inputContext).getUniqueIdentifier();
    return inputContext;
  }

  private SrcFileInfo createFile(Configuration conf, FileSystem fs, Path path, int numPartitions,
                                 int numKeysPerPartition, int startKey) throws IOException {
    FSDataOutputStream outStream = fs.create(path);
    int currentKey = startKey;
    SrcFileInfo srcFileInfo = new SrcFileInfo();
    srcFileInfo.indexedRecords = new TezIndexRecord[numPartitions];
    srcFileInfo.path = path;
    for (int i = 0; i < numPartitions; i++) {
      long pos = outStream.getPos();
      IFile.Writer writer =
          new IFile.Writer(conf, outStream, IntWritable.class, IntWritable.class, null, null, null);
      for (int j = 0; j < numKeysPerPartition; j++) {
        writer.append(new IntWritable(currentKey), new IntWritable(currentKey));
        currentKey++;
      }
      writer.close();
      srcFileInfo.indexedRecords[i] =
          new TezIndexRecord(pos, writer.getRawLength(), writer.getCompressedLength());
    }
    outStream.close();
    return srcFileInfo;
  }

  private class SrcFileInfo {
    private Path path;
    private TezIndexRecord[] indexedRecords;
  }

  // Copied from FetcherOrderedGrouped
  private static MapOutput getMapOutputForDirectDiskFetch(InputAttemptIdentifier srcAttemptId,
                                                          Path filename, TezIndexRecord indexRecord,
                                                          MergeManager merger)
      throws IOException {
    return MapOutput.createLocalDiskMapOutput(srcAttemptId, merger, filename,
        indexRecord.getStartOffset(), indexRecord.getPartLength(), true);
  }
}
