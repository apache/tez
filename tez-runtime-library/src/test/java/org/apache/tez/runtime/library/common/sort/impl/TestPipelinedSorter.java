package org.apache.tez.runtime.library.common.sort.impl;

import com.google.common.collect.Maps;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

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
  private static final Configuration conf = new Configuration();
  private static FileSystem localFs = null;
  private static Path workDir = null;

  private int numOutputs;
  private long initialAvailableMem;
  private OutputContext outputContext;

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

  @Before
  public void setup() {
    ApplicationId appId = ApplicationId.newInstance(10000, 1);
    TezCounters counters = new TezCounters();
    String uniqueId = UUID.randomUUID().toString();
    this.outputContext = createMockOutputContext(counters, appId, uniqueId);

    //To enable PipelinedSorter, set 2 threads
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SORT_THREADS, 2);
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS,
        HashPartitioner.class.getName());

    //Setup localdirs
    String localDirs = workDir.toString();
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, localDirs);
  }

  @After
  public void cleanup() throws IOException {
    localFs.delete(workDir, true);
    sortedDataMap.clear();
  }

  @Test
  public void basicTest() throws IOException {
    //TODO: need to support multiple partition testing later

    //# partition, # of keys, size per key, InitialMem, blockSize
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 5);
    basicTest(1, 100000, 100, (10 * 1024l * 1024l), 3 << 20);
  }

  public void basicTest(int partitions, int numKeys, int keySize,
      long initialAvailableMem, int blockSize) throws IOException {
    this.numOutputs = partitions; // single output
    PipelinedSorter sorter = new PipelinedSorter(this.outputContext, conf, numOutputs,
        initialAvailableMem, blockSize);

    //Write 100 keys each of size 10
    writeData(sorter, numKeys, keySize);

    Path outputFile = sorter.finalOutputFile;
    FileSystem fs = outputFile.getFileSystem(conf);

    IFile.Reader reader = new IFile.Reader(fs, outputFile, null, null, null, false, -1, 4096);
    //Verify dataset
    verifyData(reader);
    reader.close();
  }

  @Test
  //Its not possible to allocate > 2 GB in test environment.  Carry out basic checks here.
  public void memTest() throws IOException {
    //Verify if > 2 GB can be set via config
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 3076);
    long size = ExternalSorter.getInitialMemoryRequirement(conf, 4096 * 1024 * 1024l);
    Assert.assertTrue(size == (3076l << 20));

    //Verify BLOCK_SIZEs
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

  private OutputContext createMockOutputContext(TezCounters counters, ApplicationId appId,
      String uniqueId) {
    OutputContext outputContext = mock(OutputContext.class);
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
}
