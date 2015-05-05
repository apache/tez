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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestMergeManager {


  private static final Logger LOG = LoggerFactory.getLogger(TestMergeManager.class);

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
  public void testConfigs() throws IOException {
    long maxTaskMem = 8192 * 1024 * 1024l;

    //Test Shuffle fetch buffer and post merge buffer percentage
    Configuration conf = new TezConfiguration(defaultConf);
    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 0.8f);
    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT, 0.5f);
    Assert.assertTrue(MergeManager.getInitialMemoryRequirement(conf, maxTaskMem) == 6871947776l);

    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 0.5f);
    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT, 0.5f);
    Assert.assertTrue(MergeManager.getInitialMemoryRequirement(conf, maxTaskMem) > Integer.MAX_VALUE);

    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 0.4f);
    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT, 0.9f);
    Assert.assertTrue(MergeManager.getInitialMemoryRequirement(conf, maxTaskMem) > Integer.MAX_VALUE);

    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 0.1f);
    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT, 0.1f);
    Assert.assertTrue(MergeManager.getInitialMemoryRequirement(conf, maxTaskMem) < Integer.MAX_VALUE);

    try {
      conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 2.4f);
      MergeManager.getInitialMemoryRequirement(conf, maxTaskMem);
      Assert.fail("Should have thrown wrong buffer percent configuration exception");
    } catch(IllegalArgumentException ie) {
    }

    try {
      conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, -2.4f);
      MergeManager.getInitialMemoryRequirement(conf, maxTaskMem);
      Assert.fail("Should have thrown wrong buffer percent configuration exception");
    } catch(IllegalArgumentException ie) {
    }

    try {
      conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT, 1.4f);
      MergeManager.getInitialMemoryRequirement(conf, maxTaskMem);
      Assert.fail("Should have thrown wrong post merge buffer percent configuration exception");
    } catch(IllegalArgumentException ie) {
    }

    try {
      conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT, -1.4f);
      MergeManager.getInitialMemoryRequirement(conf, maxTaskMem);
      Assert.fail("Should have thrown wrong post merge buffer percent configuration exception");
    } catch(IllegalArgumentException ie) {
    }

    try {
      conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 1.4f);
      MergeManager.getInitialMemoryRequirement(conf, maxTaskMem);
      Assert.fail("Should have thrown wrong shuffle fetch buffer percent configuration exception");
    } catch(IllegalArgumentException ie) {
    }

    try {
      conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, -1.4f);
      MergeManager.getInitialMemoryRequirement(conf, maxTaskMem);
      Assert.fail("Should have thrown wrong shuffle fetch buffer percent configuration exception");
    } catch(IllegalArgumentException ie) {
    }

    //test post merge mem limit
    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 0.4f);
    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT, 0.8f);
    FileSystem localFs = FileSystem.getLocal(conf);
    LocalDirAllocator localDirAllocator =
        new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);
    InputContext t0inputContext = createMockInputContext(UUID.randomUUID().toString(), maxTaskMem);
    ExceptionReporter t0exceptionReporter = mock(ExceptionReporter.class);
    long initialMemoryAvailable = (long) (maxTaskMem * 0.8);
    MergeManager mergeManager =
        new MergeManager(conf, localFs, localDirAllocator, t0inputContext, null, null, null, null,
            t0exceptionReporter, initialMemoryAvailable, null, false, -1);
    Assert.assertTrue(mergeManager.postMergeMemLimit > Integer.MAX_VALUE);

    initialMemoryAvailable = 200 * 1024 * 1024l; //initial mem < memlimit
    mergeManager =
        new MergeManager(conf, localFs, localDirAllocator, t0inputContext, null, null, null, null,
            t0exceptionReporter, initialMemoryAvailable, null, false, -1);
    Assert.assertTrue(mergeManager.postMergeMemLimit == initialMemoryAvailable);
  }

  class InterruptingThread implements Runnable {

    MergeManager.OnDiskMerger mergeThread;

    public InterruptingThread(MergeManager.OnDiskMerger mergeThread) {
      this.mergeThread = mergeThread;
    }

    @Override public void run() {
        while(this.mergeThread.tmpDir == null) {
          //this is tight loop
        }

        this.mergeThread.interrupt();
    }
  }

  @Test(timeout = 10000)
  public void testLocalDiskMergeMultipleTasks() throws IOException, InterruptedException {
    testLocalDiskMergeMultipleTasks(false);
    testLocalDiskMergeMultipleTasks(true);
  }


  void testLocalDiskMergeMultipleTasks(boolean interruptInMiddle)
      throws IOException, InterruptedException {
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
    t0mergeManager.configureAndStart();

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

    if (!interruptInMiddle) {
      t0mergeManager.onDiskMerger.merge(t0MergeFiles);
      Assert.assertEquals(1, t0mergeManager.onDiskMapOutputs.size());
    } else {

      //Start Interrupting thread
      Thread interruptingThread = new Thread(new InterruptingThread(t0mergeManager.onDiskMerger));
      interruptingThread.start();
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      //Will be interrupted in the middle by interruptingThread.
      t0mergeManager.onDiskMerger.startMerge(Sets.newHashSet(t0MergeFiles));
      t0mergeManager.onDiskMerger.waitForMerge();
      Assert.assertNotEquals(1, t0mergeManager.onDiskMapOutputs.size());
    }

    if (!interruptInMiddle) {
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
  }

  private InputContext createMockInputContext(String uniqueId) {
    return createMockInputContext(uniqueId, 200 * 1024 * 1024l);
  }

  private InputContext createMockInputContext(String uniqueId, long mem) {
    InputContext inputContext = mock(InputContext.class);
    doReturn(new TezCounters()).when(inputContext).getCounters();
    doReturn(mem).when(inputContext).getTotalMemoryAvailableToTask();
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
