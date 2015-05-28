package org.apache.tez.runtime.library.common.sort.impl;

import com.google.common.collect.Maps;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVOutputConfig.SorterImpl;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class TestPipelinedSorter {
  private static Configuration conf = new Configuration();
  private static FileSystem localFs = null;
  private static Path workDir = null;
  private OutputContext outputContext;

  private int numOutputs;
  private long initialAvailableMem;

  //TODO: Need to make it nested structure so that multiple partition cases can be validated
  private static TreeMap<String, String> sortedDataMap = Maps.newTreeMap();

  static {
    conf.set("fs.defaultFS", "file:///");
    try {
      localFs = FileSystem.getLocal(conf);
      workDir = new Path(
          new Path(System.getProperty("test.build.data", "/tmp")),
          TestPipelinedSorter.class.getName())
          .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
    localFs.delete(workDir, true);
  }

  @Before
  public void setup() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(10000, 1);
    TezCounters counters = new TezCounters();
    String uniqueId = UUID.randomUUID().toString();
    this.outputContext = createMockOutputContext(counters, appId, uniqueId);

    //To enable PipelinedSorter
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS, SorterImpl.PIPELINED.name());

    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, HashPartitioner.class.getName());

    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, true);

    //Setup localdirs
    String localDirs = workDir.toString();
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, localDirs);
  }

  @After
  public void reset() throws IOException {
    cleanup();
    localFs.mkdirs(workDir);
    conf = new Configuration();
  }

  @Test
  public void basicTest() throws IOException {
    //TODO: need to support multiple partition testing later

    //# partition, # of keys, size per key, InitialMem, blockSize
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 5);
    basicTest(1, 100000, 100, (10 * 1024l * 1024l), 3 << 20);

  }

  @Test
  public void testWithEmptyData() throws IOException {
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 5);
    //# partition, # of keys, size per key, InitialMem, blockSize
    basicTest(1, 0, 0, (10 * 1024l * 1024l), 3 << 20);
  }

  @Test
  public void basicTestWithSmallBlockSize() throws IOException {
    try {
      //3 MB key & 3 MB value, whereas block size is just 3 MB
      basicTest(1, 5, (3 << 20), (10 * 1024l * 1024l), 3 << 20);
      fail();
    } catch (IOException ioe) {
      Assert.assertTrue(
          ioe.getMessage().contains("Record too large for in-memory buffer."
              + " Exceeded buffer overflow limit"));
    }
  }

  @Test
  public void testWithLargeKeyValue() throws IOException {
    //15 MB key & 15 MB value, 48 MB sort buffer.  block size is 48MB (or 1 block)
    //meta would be 16 MB
    basicTest(1, 5, (15 << 20), (48 * 1024l * 1024l), 48 << 20);
  }

  @Test
  public void testWithCustomComparator() throws IOException {
    //Test with custom comparator
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS, CustomComparator.class.getName());
    basicTest(1, 100000, 100, (10 * 1024l * 1024l), 3 << 20);
  }

  @Test
  public void testWithPipelinedShuffle() throws IOException {
    this.numOutputs = 1;
    this.initialAvailableMem = 5 *1024 * 1024;
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 5);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem, 1 << 20);

    //Write 100 keys each of size 10
    writeData(sorter, 10000, 100);

    //final merge is disabled. Final output file would not be populated in this case.
    assertTrue(sorter.finalOutputFile == null);
    verify(outputContext, times(1)).sendEvents(anyListOf(Event.class));
  }

  public void basicTest(int partitions, int numKeys, int keySize,
      long initialAvailableMem, int blockSize) throws IOException {
    this.numOutputs = partitions; // single output
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem, blockSize);

    writeData(sorter, numKeys, keySize);

    verifyCounters(sorter, outputContext);
    Path outputFile = sorter.finalOutputFile;
    FileSystem fs = outputFile.getFileSystem(conf);

    IFile.Reader reader = new IFile.Reader(fs, outputFile, null, null, null, false, -1, 4096);
    //Verify dataset
    verifyData(reader);
    reader.close();
  }

  private void verifyCounters(PipelinedSorter sorter, OutputContext context) {
    TezCounter numShuffleChunks = context.getCounters().findCounter(TaskCounter.SHUFFLE_CHUNK_COUNT);
    TezCounter additionalSpills =
        context.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILL_COUNT);
    TezCounter additionalSpillBytesWritten =
        context.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_WRITTEN);
    TezCounter additionalSpillBytesRead =
        context.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_READ);

    if (sorter.isFinalMergeEnabled()) {
      assertTrue(additionalSpills.getValue() == (sorter.getNumSpills() - 1));
      //Number of files served by shuffle-handler
      assertTrue(1 == numShuffleChunks.getValue());
      if (sorter.getNumSpills() > 1) {
        assertTrue(additionalSpillBytesRead.getValue() > 0);
        assertTrue(additionalSpillBytesWritten.getValue() > 0);
      }
    } else {
      assertTrue(0 == additionalSpills.getValue());
      //Number of files served by shuffle-handler
      assertTrue(sorter.getNumSpills() == numShuffleChunks.getValue());
      assertTrue(additionalSpillBytesRead.getValue() == 0);
      assertTrue(additionalSpillBytesWritten.getValue() == 0);
    }

    TezCounter finalOutputBytes =
        context.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_PHYSICAL);
    assertTrue(finalOutputBytes.getValue() > 0);

    TezCounter outputBytesWithOverheadCounter = context.getCounters().findCounter
        (TaskCounter.OUTPUT_BYTES_WITH_OVERHEAD);
    assertTrue(outputBytesWithOverheadCounter.getValue() > 0);
  }


  @Test
  //Its not possible to allocate > 2 GB in test environment.  Carry out basic checks here.
  public void memTest() throws IOException {
    //Verify if > 2 GB can be set via config
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 3076);
    long size = ExternalSorter.getInitialMemoryRequirement(conf, 4096 * 1024 * 1024l);
    Assert.assertTrue(size == (3076l << 20));

    //Verify number of block buffers allocated
    this.initialAvailableMem = 10 * 1024 * 1024;
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem, 1 << 20);
    Assert.assertTrue(sorter.bufferList.size() == 10);

    sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem, 3 << 20);
    Assert.assertTrue(sorter.bufferList.size() == 4);

    sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem, 10 << 20);
    Assert.assertTrue(sorter.bufferList.size() == 1);

    //Verify block sizes
    int blockSize = PipelinedSorter.computeBlockSize(0, (10 * 1024 * 1024));
    //initialAvailableMem is < 2 GB. So consider it as the blockSize
    Assert.assertTrue(blockSize == (10 * 1024 * 1024));

    blockSize = PipelinedSorter.computeBlockSize(0, (10 * 1024 * 1024 * 1024l));
    //initialAvailableMem is > 2 GB. Restrict block size to Integer.MAX_VALUE;
    Assert.assertTrue(blockSize == Integer.MAX_VALUE);

    blockSize = PipelinedSorter.computeBlockSize((1*1024*1024*1024), (10 * 1024 * 1024));
    //sort buffer is 10 MB. But block size requested is 1 GB. Restrict block size to 10 MB.
    Assert.assertTrue(blockSize == (10 * 1024 * 1024));

    try {
      blockSize = PipelinedSorter.computeBlockSize(-1, (10 * 1024 * 1024 * 1024l));
      //block size can't set to -1
      fail();
    } catch(IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("should be between 1 and Integer.MAX_VALUE"));
    }
  }

  private void writeData(ExternalSorter sorter, int numKeys, int keyLen) throws IOException {
    sortedDataMap.clear();
    for (int i = 0; i < numKeys; i++) {
      Text key = new Text(RandomStringUtils.randomAlphanumeric(keyLen));
      Text value = new Text(RandomStringUtils.randomAlphanumeric(keyLen));
      sorter.write(key, value);
      sortedDataMap.put(key.toString(), value.toString()); //for verifying data later
    }
    sorter.flush();
    sorter.close();
  }

  private void verifyData(IFile.Reader reader)
      throws IOException {
    Text readKey = new Text();
    Text readValue = new Text();
    DataInputBuffer keyIn = new DataInputBuffer();
    DataInputBuffer valIn = new DataInputBuffer();
    SerializationFactory serializationFactory = new SerializationFactory(conf);
    Deserializer<Text> keyDeserializer = serializationFactory.getDeserializer(Text.class);
    Deserializer<Text> valDeserializer = serializationFactory.getDeserializer(Text.class);
    keyDeserializer.open(keyIn);
    valDeserializer.open(valIn);

    int numRecordsRead = 0;

    for (Map.Entry<String, String> entry : sortedDataMap.entrySet()) {
      String key = entry.getKey();
      String val = entry.getValue();
      if (reader.nextRawKey(keyIn)) {
        reader.nextRawValue(valIn);
        readKey = keyDeserializer.deserialize(readKey);
        readValue = valDeserializer.deserialize(readValue);
        Assert.assertTrue(key.equalsIgnoreCase(readKey.toString()));
        Assert.assertTrue(val.equalsIgnoreCase(readValue.toString()));
        numRecordsRead++;
      }
    }
    Assert.assertTrue(numRecordsRead == sortedDataMap.size());
  }

  private static OutputContext createMockOutputContext(TezCounters counters, ApplicationId appId,
      String uniqueId) throws IOException {
    OutputContext outputContext = mock(OutputContext.class);

    ExecutionContext execContext = new ExecutionContextImpl("localhost");

    DataOutputBuffer serviceProviderMetaData = new DataOutputBuffer();
    serviceProviderMetaData.writeInt(80);
    doReturn(ByteBuffer.wrap(serviceProviderMetaData.getData())).when(outputContext)
        .getServiceProviderMetaData
            (ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID);

    doReturn(execContext).when(outputContext).getExecutionContext();
    doReturn(counters).when(outputContext).getCounters();
    doReturn(appId).when(outputContext).getApplicationId();
    doReturn(1).when(outputContext).getDAGAttemptNumber();
    doReturn("dagName").when(outputContext).getDAGName();
    doReturn("destinationVertexName").when(outputContext).getDestinationVertexName();
    doReturn(1).when(outputContext).getOutputIndex();
    doReturn(1).when(outputContext).getTaskAttemptNumber();
    doReturn(1).when(outputContext).getTaskIndex();
    doReturn(1).when(outputContext).getTaskVertexIndex();
    doReturn("vertexName").when(outputContext).getTaskVertexName();
    doReturn(uniqueId).when(outputContext).getUniqueIdentifier();
    Path outDirBase = new Path(workDir, "outDir_" + uniqueId);
    String[] outDirs = new String[] { outDirBase.toString() };
    doReturn(outDirs).when(outputContext).getWorkDirs();
    return outputContext;
  }

  /**
   * E.g Hive uses TezBytesComparator which internally makes use of WritableComparator's comparison.
   * Any length mismatches are handled there.
   *
   * However, custom comparators can handle this differently and might throw
   * IndexOutOfBoundsException in case of invalid lengths.
   *
   * This comparator (similar to comparator in BinInterSedes of pig) would thrown exception when
   * wrong lengths are mentioned.
   */
  public static class CustomComparator extends WritableComparator {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      //wrapping is done so that it would throw exceptions on wrong lengths
      ByteBuffer bb1 = ByteBuffer.wrap(b1, s1, l1);
      ByteBuffer bb2 = ByteBuffer.wrap(b2, s2, l2);

      return bb1.compareTo(bb2);
    }

  }
}
