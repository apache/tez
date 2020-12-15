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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Sets;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.serializer.WritableSerialization;
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
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
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

  @Test(timeout = 10000)
  public void testReservationAccounting() throws IOException {
    Configuration conf = new TezConfiguration(defaultConf);
    FileSystem localFs = FileSystem.getLocal(conf);
    InputContext inputContext = createMockInputContext(UUID.randomUUID().toString());
    MergeManager mergeManager =
        new MergeManager(conf, localFs, null, inputContext, null, null, null, null,
        mock(ExceptionReporter.class), 2000000, null, false, -1);
    mergeManager.configureAndStart();
    assertEquals(0, mergeManager.getUsedMemory());
    assertEquals(0, mergeManager.getCommitMemory());
    MapOutput mapOutput = mergeManager.reserve(null, 1, 1, 0);
    assertEquals(1, mergeManager.getUsedMemory());
    assertEquals(0, mergeManager.getCommitMemory());
    mapOutput.abort();
    assertEquals(0, mergeManager.getUsedMemory());
    assertEquals(0, mergeManager.getCommitMemory());
    mapOutput = mergeManager.reserve(null, 2, 2, 0);
    mergeManager.closeInMemoryFile(mapOutput);
    assertEquals(2, mergeManager.getUsedMemory());
    assertEquals(2, mergeManager.getCommitMemory());
    mergeManager.releaseCommittedMemory(2);
    assertEquals(0, mergeManager.getUsedMemory());
    assertEquals(0, mergeManager.getCommitMemory());
  }

  @Test(timeout=20000)
  public void testIntermediateMemoryMergeAccounting() throws Exception {
    Configuration conf = new TezConfiguration(defaultConf);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, false);
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, IntWritable.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, IntWritable.class.getName());
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM, true);
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS, 2);

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

    MergeManager mergeManager =
        new MergeManager(conf, localFs, localDirAllocator, inputContext, null, null, null, null,
            exceptionReporter, 2000000, null, false, -1);
    mergeManager.configureAndStart();

    assertEquals(0, mergeManager.getUsedMemory());
    assertEquals(0, mergeManager.getCommitMemory());

    byte[] data1 = generateData(conf, 10, null);
    byte[] data2 = generateData(conf, 20, null);
    MapOutput firstMapOutput = mergeManager.reserve(null, data1.length, data1.length, 0);
    MapOutput secondMapOutput = mergeManager.reserve(null, data2.length, data2.length, 0);
    assertEquals(MapOutput.Type.MEMORY, firstMapOutput.getType());
    assertEquals(MapOutput.Type.MEMORY, secondMapOutput.getType());
    assertEquals(0, mergeManager.getCommitMemory());
    assertEquals(data1.length + data2.length, mergeManager.getUsedMemory());

    System.arraycopy(data1, 0, firstMapOutput.getMemory(), 0, data1.length);
    System.arraycopy(data2, 0, secondMapOutput.getMemory(), 0, data2.length);

    secondMapOutput.commit();
    assertEquals(data2.length, mergeManager.getCommitMemory());
    assertEquals(data1.length + data2.length, mergeManager.getUsedMemory());
    firstMapOutput.commit();

    mergeManager.waitForMemToMemMerge();
    assertEquals(data1.length + data2.length, mergeManager.getCommitMemory());
    assertEquals(data1.length + data2.length, mergeManager.getUsedMemory());
  }

  @Test
  public void testDiskMergeWithCodec() throws Throwable {
    Configuration conf = new TezConfiguration(defaultConf);
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, IntWritable.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, IntWritable.class.getName());
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR, 3);

    Path localDir = new Path(workDir, "local");
    localFs.mkdirs(localDir);

    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, localDir.toString());

    LocalDirAllocator localDirAllocator =
            new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);
    InputContext inputContext = createMockInputContext(UUID.randomUUID().toString());

    // Create a mock compressor. We will check if it is used.
    CompressionCodec dummyCodec = spy(new DummyCompressionCodec());

    MergeManager mergeManager =
            new MergeManager(conf, localFs, localDirAllocator, inputContext, null, null, null, null,
                    mock(ExceptionReporter.class), 2000, dummyCodec, false, -1);
    mergeManager.configureAndStart();

    assertEquals(0, mergeManager.getUsedMemory());
    assertEquals(0, mergeManager.getCommitMemory());

    InputAttemptIdentifier inputAttemptIdentifier1 = new InputAttemptIdentifier(0, 0);
    InputAttemptIdentifier inputAttemptIdentifier2 = new InputAttemptIdentifier(1, 0);
    InputAttemptIdentifier inputAttemptIdentifier3 = new InputAttemptIdentifier(2, 0);
    InputAttemptIdentifier inputAttemptIdentifier4 = new InputAttemptIdentifier(3, 0);
    byte[] data1 = generateDataBySizeAndGetBytes(conf, 500, inputAttemptIdentifier1);
    byte[] data2 = generateDataBySizeAndGetBytes(conf, 500, inputAttemptIdentifier2);
    byte[] data3 = generateDataBySizeAndGetBytes(conf, 500, inputAttemptIdentifier3);
    byte[] data4 = generateDataBySizeAndGetBytes(conf, 500, inputAttemptIdentifier3);

    MapOutput mo1 = mergeManager.reserve(inputAttemptIdentifier1, data1.length, data1.length, 0);
    MapOutput mo2 = mergeManager.reserve(inputAttemptIdentifier2, data2.length, data2.length, 0);
    MapOutput mo3 = mergeManager.reserve(inputAttemptIdentifier3, data3.length, data3.length, 0);
    MapOutput mo4 = mergeManager.reserve(inputAttemptIdentifier4, data4.length, data4.length, 0);

    mo1.getDisk().write(data1);
    mo1.getDisk().flush();
    mo2.getDisk().write(data2);
    mo2.getDisk().flush();
    mo3.getDisk().write(data3);
    mo3.getDisk().flush();
    mo4.getDisk().write(data4);
    mo4.getDisk().flush();

    mo1.commit();
    mo2.commit();
    mo3.commit();
    mo4.commit();

    mergeManager.close(true);
    verify(dummyCodec, atLeastOnce()).createOutputStream(any(), any());
  }

  @Test(timeout = 60000l)
  public void testIntermediateMemoryMerge() throws Throwable {
    Configuration conf = new TezConfiguration(defaultConf);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, false);
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, IntWritable.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, IntWritable.class.getName());
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM, true);
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS, 3);

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

    MergeManager mergeManager =
        new MergeManager(conf, localFs, localDirAllocator, inputContext, null, null, null, null,
            exceptionReporter, 2000000, null, false, -1);
    mergeManager.configureAndStart();

    assertEquals(0, mergeManager.getUsedMemory());
    assertEquals(0, mergeManager.getCommitMemory());

    /**
     * Test #1
     * - Have 4 segments where all of them can fit into memory.
     * - After 3 segment commits, it would trigger mem-to-mem merge.
     * - All of them can be merged in memory.
     */
    InputAttemptIdentifier inputAttemptIdentifier1 = new InputAttemptIdentifier(0,0);
    InputAttemptIdentifier inputAttemptIdentifier2 = new InputAttemptIdentifier(1,0);
    InputAttemptIdentifier inputAttemptIdentifier3 = new InputAttemptIdentifier(2,0);
    InputAttemptIdentifier inputAttemptIdentifier4 = new InputAttemptIdentifier(3,0);
    byte[] data1 = generateDataBySize(conf, 10, inputAttemptIdentifier1);
    byte[] data2 = generateDataBySize(conf, 20, inputAttemptIdentifier2);
    byte[] data3 = generateDataBySize(conf, 200, inputAttemptIdentifier3);
    byte[] data4 = generateDataBySize(conf, 20000, inputAttemptIdentifier4);

    MapOutput mo1 = mergeManager.reserve(inputAttemptIdentifier1, data1.length, data1.length, 0);
    MapOutput mo2 = mergeManager.reserve(inputAttemptIdentifier1, data2.length, data2.length, 0);
    MapOutput mo3 = mergeManager.reserve(inputAttemptIdentifier1, data3.length, data3.length, 0);
    MapOutput mo4 = mergeManager.reserve(inputAttemptIdentifier1, data4.length, data4.length, 0);

    assertEquals(MapOutput.Type.MEMORY, mo1.getType());
    assertEquals(MapOutput.Type.MEMORY, mo2.getType());
    assertEquals(MapOutput.Type.MEMORY, mo3.getType());
    assertEquals(MapOutput.Type.MEMORY, mo4.getType());
    assertEquals(0, mergeManager.getCommitMemory());

    //size should be ~20230.
    assertEquals(data1.length + data2.length + data3.length + data4.length,
        mergeManager.getUsedMemory());


    System.arraycopy(data1, 0, mo1.getMemory(), 0, data1.length);
    System.arraycopy(data2, 0, mo2.getMemory(), 0, data2.length);
    System.arraycopy(data3, 0, mo3.getMemory(), 0, data3.length);
    System.arraycopy(data4, 0, mo4.getMemory(), 0, data4.length);

    //Committing 3 segments should trigger mem-to-mem merge
    mo1.commit();
    mo2.commit();
    mo3.commit();
    mo4.commit();

    //Wait for mem-to-mem to complete
    mergeManager.waitForMemToMemMerge();

    assertEquals(1, mergeManager.inMemoryMergedMapOutputs.size());
    assertEquals(1, mergeManager.inMemoryMapOutputs.size());

    mergeManager.close(true);


    /**
     * Test #2
     * - Have 4 segments where all of them can fit into memory, but one of
     * them would be big enough that it can not be fit in memory during
     * mem-to-mem merging.
     *
     * - After 3 segment commits, it would trigger mem-to-mem merge.
     * - Smaller segments which can be fit in additional memory allocated gets
     * merged.
     */
    mergeManager =
        new MergeManager(conf, localFs, localDirAllocator, inputContext, null, null, null, null,
            exceptionReporter, 2000000, null, false, -1);
    mergeManager.configureAndStart();

    //Single shuffle limit is 25% of 2000000
    data1 = generateDataBySize(conf, 10, inputAttemptIdentifier1);
    data2 = generateDataBySize(conf, 400000, inputAttemptIdentifier2);
    data3 = generateDataBySize(conf, 400000, inputAttemptIdentifier3);
    data4 = generateDataBySize(conf, 400000, inputAttemptIdentifier4);

    mo1 = mergeManager.reserve(inputAttemptIdentifier1, data1.length, data1.length, 0);
    mo2 = mergeManager.reserve(inputAttemptIdentifier2, data2.length, data2.length, 0);
    mo3 = mergeManager.reserve(inputAttemptIdentifier3, data3.length, data3.length, 0);
    mo4 = mergeManager.reserve(inputAttemptIdentifier4, data4.length, data4.length, 0);

    assertEquals(MapOutput.Type.MEMORY, mo1.getType());
    assertEquals(MapOutput.Type.MEMORY, mo2.getType());
    assertEquals(MapOutput.Type.MEMORY, mo3.getType());
    assertEquals(MapOutput.Type.MEMORY, mo4.getType());
    assertEquals(0, mergeManager.getCommitMemory());

    assertEquals(data1.length + data2.length + data3.length + data4.length,
        mergeManager.getUsedMemory());

    System.arraycopy(data1, 0, mo1.getMemory(), 0, data1.length);
    System.arraycopy(data2, 0, mo2.getMemory(), 0, data2.length);
    System.arraycopy(data3, 0, mo3.getMemory(), 0, data3.length);
    System.arraycopy(data4, 0, mo4.getMemory(), 0, data4.length);

    //Committing 3 segments should trigger mem-to-mem merge
    mo1.commit();
    mo2.commit();
    mo3.commit();
    mo4.commit();

    //Wait for mem-to-mem to complete
    mergeManager.waitForMemToMemMerge();

    /**
     * Already all segments are in memory which is around 120000. It
     * would not be able to allocate more than 800000 for mem-to-mem. So it
     * would pick up only 2 small segments which can be accomodated within
     * 800000.
     */
    assertEquals(1, mergeManager.inMemoryMergedMapOutputs.size());
    assertEquals(2, mergeManager.inMemoryMapOutputs.size());

    mergeManager.close(true);

    /**
     * Test #3
     * - Set number of segments for merging to 4.
     * - Have 4 in-memory segments of size 400000 each
     * - Committing 4 segments would trigger mem-to-mem
     * - But none of them can be merged as there is no enough head room for
     * merging in memory.
     */
    mergeManager =
        new MergeManager(conf, localFs, localDirAllocator, inputContext, null, null, null, null,
            exceptionReporter, 2000000, null, false, -1);
    mergeManager.configureAndStart();

    //Single shuffle limit is 25% of 2000000
    data1 = generateDataBySize(conf, 400000, inputAttemptIdentifier1);
    data2 = generateDataBySize(conf, 400000, inputAttemptIdentifier2);
    data3 = generateDataBySize(conf, 400000, inputAttemptIdentifier3);
    data4 = generateDataBySize(conf, 400000, inputAttemptIdentifier4);

    mo1 = mergeManager.reserve(inputAttemptIdentifier1, data1.length, data1.length, 0);
    mo2 = mergeManager.reserve(inputAttemptIdentifier2, data2.length, data2.length, 0);
    mo3 = mergeManager.reserve(inputAttemptIdentifier3, data3.length, data3.length, 0);
    mo4 = mergeManager.reserve(inputAttemptIdentifier4, data4.length, data4.length, 0);

    assertEquals(MapOutput.Type.MEMORY, mo1.getType());
    assertEquals(MapOutput.Type.MEMORY, mo2.getType());
    assertEquals(MapOutput.Type.MEMORY, mo3.getType());
    assertEquals(MapOutput.Type.MEMORY, mo4.getType());
    assertEquals(0, mergeManager.getCommitMemory());

    assertEquals(data1.length + data2.length + data3.length + data4.length,
        mergeManager.getUsedMemory());

    System.arraycopy(data1, 0, mo1.getMemory(), 0, data1.length);
    System.arraycopy(data2, 0, mo2.getMemory(), 0, data2.length);
    System.arraycopy(data3, 0, mo3.getMemory(), 0, data3.length);
    System.arraycopy(data4, 0, mo4.getMemory(), 0, data4.length);

    //Committing 3 segments should trigger mem-to-mem merge
    mo1.commit();
    mo2.commit();
    mo3.commit();
    mo4.commit();

    //Wait for mem-to-mem to complete
    mergeManager.waitForMemToMemMerge();

    // None of them can be merged as new mem needed for mem-to-mem can't
    // accomodate any segements
    assertEquals(0, mergeManager.inMemoryMergedMapOutputs.size());
    assertEquals(4, mergeManager.inMemoryMapOutputs.size());

    mergeManager.close(true);

    /**
     * Test #4
     * - Set number of segments for merging to 4.
     * - Have 4 in-memory segments of size {490000,490000,490000,230000}
     * - Committing 4 segments would trigger mem-to-mem
     * - But only 300000 can fit into memory. This should not be
     * merged as there is no point in merging single segment. It should be
     * added back to the inMemorySegments
     */
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS, 4);
    mergeManager =
        new MergeManager(conf, localFs, localDirAllocator, inputContext, null, null, null, null,
            exceptionReporter, 2000000, null, false, -1);
    mergeManager.configureAndStart();

    //Single shuffle limit is 25% of 2000000
    data1 = generateDataBySize(conf, 490000, inputAttemptIdentifier1);
    data2 = generateDataBySize(conf, 490000, inputAttemptIdentifier2);
    data3 = generateDataBySize(conf, 490000, inputAttemptIdentifier3);
    data4 = generateDataBySize(conf, 230000, inputAttemptIdentifier4);

    mo1 = mergeManager.reserve(inputAttemptIdentifier1, data1.length, data1.length, 0);
    mo2 = mergeManager.reserve(inputAttemptIdentifier2, data2.length, data2.length, 0);
    mo3 = mergeManager.reserve(inputAttemptIdentifier3, data3.length, data3.length, 0);
    mo4 = mergeManager.reserve(inputAttemptIdentifier4, data4.length, data4.length, 0);

    assertTrue(mergeManager.getUsedMemory() >= (490000 + 490000 + 490000 + 23000));

    assertEquals(MapOutput.Type.MEMORY, mo1.getType());
    assertEquals(MapOutput.Type.MEMORY, mo2.getType());
    assertEquals(MapOutput.Type.MEMORY, mo3.getType());
    assertEquals(MapOutput.Type.MEMORY, mo4.getType());
    assertEquals(0, mergeManager.getCommitMemory());

    assertEquals(data1.length + data2.length + data3.length + data4.length,
        mergeManager.getUsedMemory());

    System.arraycopy(data1, 0, mo1.getMemory(), 0, data1.length);
    System.arraycopy(data2, 0, mo2.getMemory(), 0, data2.length);
    System.arraycopy(data3, 0, mo3.getMemory(), 0, data3.length);
    System.arraycopy(data4, 0, mo4.getMemory(), 0, data4.length);

    //Committing 4 segments should trigger mem-to-mem merge
    mo1.commit();
    mo2.commit();
    mo3.commit();
    mo4.commit();

    //4 segments were there originally in inMemoryMapOutput.
    int numberOfMapOutputs = 4;

    //Wait for mem-to-mem to complete. Since only 1 segment (230000) can fit
    //into memory, it should return early
    mergeManager.waitForMemToMemMerge();

    //Check if inMemorySegment has got the MapOutput back for merging later
    assertEquals(numberOfMapOutputs, mergeManager.inMemoryMapOutputs.size());

    mergeManager.close(true);

    /**
     * Test #5
     * - Same to #4, but calling mergeManager.close(false) and confirm that final merge doesn't occur.
     */
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS, 4);
    mergeManager =
        new MergeManager(conf, localFs, localDirAllocator, inputContext, null, null, null, null,
            exceptionReporter, 2000000, null, false, -1);
    mergeManager.configureAndStart();

    //Single shuffle limit is 25% of 2000000
    data1 = generateDataBySize(conf, 490000, inputAttemptIdentifier1);
    data2 = generateDataBySize(conf, 490000, inputAttemptIdentifier2);
    data3 = generateDataBySize(conf, 490000, inputAttemptIdentifier3);
    data4 = generateDataBySize(conf, 230000, inputAttemptIdentifier4);

    mo1 = mergeManager.reserve(inputAttemptIdentifier1, data1.length, data1.length, 0);
    mo2 = mergeManager.reserve(inputAttemptIdentifier2, data2.length, data2.length, 0);
    mo3 = mergeManager.reserve(inputAttemptIdentifier3, data3.length, data3.length, 0);
    mo4 = mergeManager.reserve(inputAttemptIdentifier4, data4.length, data4.length, 0);

    assertTrue(mergeManager.getUsedMemory() >= (490000 + 490000 + 490000 + 23000));

    assertEquals(MapOutput.Type.MEMORY, mo1.getType());
    assertEquals(MapOutput.Type.MEMORY, mo2.getType());
    assertEquals(MapOutput.Type.MEMORY, mo3.getType());
    assertEquals(MapOutput.Type.MEMORY, mo4.getType());
    assertEquals(0, mergeManager.getCommitMemory());

    assertEquals(data1.length + data2.length + data3.length + data4.length,
        mergeManager.getUsedMemory());

    System.arraycopy(data1, 0, mo1.getMemory(), 0, data1.length);
    System.arraycopy(data2, 0, mo2.getMemory(), 0, data2.length);
    System.arraycopy(data3, 0, mo3.getMemory(), 0, data3.length);
    System.arraycopy(data4, 0, mo4.getMemory(), 0, data4.length);

    //Committing 4 segments should trigger mem-to-mem merge
    mo1.commit();
    mo2.commit();
    mo3.commit();
    mo4.commit();

    //4 segments were there originally in inMemoryMapOutput.
    numberOfMapOutputs = 4;

    //Wait for mem-to-mem to complete. Since only 1 segment (230000) can fit
    //into memory, it should return early
    mergeManager.waitForMemToMemMerge();

    //Check if inMemorySegment has got the MapOutput back for merging later
    assertEquals(numberOfMapOutputs, mergeManager.inMemoryMapOutputs.size());

    Assert.assertNull(mergeManager.close(false));
    Assert.assertFalse(mergeManager.isMergeComplete());
  }

  private byte[] generateDataBySize(Configuration conf, int rawLen, InputAttemptIdentifier inputAttemptIdentifier) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FSDataOutputStream fsdos = new FSDataOutputStream(baos, null);
    IFile.Writer writer = new IFile.Writer(new WritableSerialization(), new WritableSerialization(),
        fsdos, IntWritable.class, IntWritable.class, null, null, null);
    int i = 0;
    while(true) {
      writer.append(new IntWritable(i), new IntWritable(i));
      i++;
      if (writer.getRawLength() > rawLen) {
        break;
      }
    }
    writer.close();
    int compressedLength = (int)writer.getCompressedLength();
    int rawLength = (int)writer.getRawLength();
    byte[] data = new byte[rawLength];
    ShuffleUtils.shuffleToMemory(data, new ByteArrayInputStream(baos.toByteArray()),
        rawLength, compressedLength, null, false, 0, LOG, inputAttemptIdentifier);
    return data;
  }

  private byte[] generateDataBySizeAndGetBytes(Configuration conf, int rawLen,
                                               InputAttemptIdentifier inputAttemptIdentifier) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FSDataOutputStream fsdos = new FSDataOutputStream(baos, null);
    IFile.Writer writer = new IFile.Writer(new WritableSerialization(), new WritableSerialization(),
        fsdos, IntWritable.class, IntWritable.class, null, null, null);
    int i = 0;
    while(true) {
      writer.append(new IntWritable(i), new IntWritable(i));
      i++;
      if (writer.getRawLength() > rawLen) {
        break;
      }
    }
    writer.close();
    int compressedLength = (int)writer.getCompressedLength();
    int rawLength = (int)writer.getRawLength();
    byte[] data = new byte[rawLength];
    ShuffleUtils.shuffleToMemory(data, new ByteArrayInputStream(baos.toByteArray()),
            rawLength, compressedLength, null, false, 0, LOG, inputAttemptIdentifier);
    return baos.toByteArray();
  }

  private byte[] generateData(Configuration conf, int numEntries,
                              InputAttemptIdentifier inputAttemptIdentifier) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FSDataOutputStream fsdos = new FSDataOutputStream(baos, null);
    IFile.Writer writer = new IFile.Writer(new WritableSerialization(), new WritableSerialization(),
        fsdos, IntWritable.class, IntWritable.class, null, null, null);
    for (int i = 0; i < numEntries; ++i) {
      writer.append(new IntWritable(i), new IntWritable(i));
    }
    writer.close();
    int compressedLength = (int)writer.getCompressedLength();
    int rawLength = (int)writer.getRawLength();
    byte[] data = new byte[rawLength];
    ShuffleUtils.shuffleToMemory(data, new ByteArrayInputStream(baos.toByteArray()),
        rawLength, compressedLength, null, false, 0, LOG, inputAttemptIdentifier);
    return data;
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
    List<FileChunk> tmpList = new LinkedList<>();
    mergeFiles = new LinkedList<>();
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
    String m1Prefix = m1Path.toString().substring(0, m1Path.toString().lastIndexOf('.'));
    String m2Prefix = m2Path.toString().substring(0, m2Path.toString().lastIndexOf('.'));
    String m3Prefix = m3Path.toString().substring(0, m3Path.toString().lastIndexOf('.'));

    assertEquals(m1Prefix, m2Prefix);
    assertNotEquals(m1Prefix, m3Prefix);
    assertNotEquals(m2Prefix, m3Prefix);
    
    verify(inputContext, atLeastOnce()).notifyProgress();

  }


  void testLocalDiskMergeMultipleTasks(final boolean interruptInMiddle)
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
            t0exceptionReporter, 2000000, null, false, -1) {
          // override for interruptInMiddle testing
          @Override
          public synchronized void closeOnDiskFile(FileChunk file) {
            if (interruptInMiddle) {
              try {
                Thread.sleep(2000);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }
            }
            super.closeOnDiskFile(file);
          }
        };
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
          new IFile.Writer(new WritableSerialization(), new WritableSerialization(), outStream,
              IntWritable.class, IntWritable.class, null, null, null);
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
