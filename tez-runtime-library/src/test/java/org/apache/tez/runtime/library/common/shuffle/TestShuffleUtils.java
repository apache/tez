package org.apache.tez.runtime.library.common.shuffle;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils.FetchStatsLogger;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
public class TestShuffleUtils {

  private OutputContext outputContext;
  private Configuration conf;
  private FileSystem localFs;
  private Path workingDir;

  private InputContext createTezInputContext() {
    ApplicationId applicationId = ApplicationId.newInstance(1, 1);
    InputContext inputContext = mock(InputContext.class);
    doReturn(applicationId).when(inputContext).getApplicationId();
    doReturn("sourceVertex").when(inputContext).getSourceVertexName();
    when(inputContext.getCounters()).thenReturn(new TezCounters());
    return inputContext;
  }

  private OutputContext createTezOutputContext() throws IOException {
    ApplicationId applicationId = ApplicationId.newInstance(1, 1);
    OutputContext outputContext = mock(OutputContext.class);

    ExecutionContextImpl executionContext = mock(ExecutionContextImpl.class);
    doReturn("localhost").when(executionContext).getHostName();
    doReturn(executionContext).when(outputContext).getExecutionContext();

    DataOutputBuffer serviceProviderMetaData = new DataOutputBuffer();
    serviceProviderMetaData.writeInt(80);
    doReturn(ByteBuffer.wrap(serviceProviderMetaData.getData())).when(outputContext)
        .getServiceProviderMetaData
            (conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
                TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT));


    doReturn(1).when(outputContext).getTaskVertexIndex();
    doReturn(1).when(outputContext).getOutputIndex();
    doReturn(0).when(outputContext).getDAGAttemptNumber();
    doReturn("destVertex").when(outputContext).getDestinationVertexName();

    when(outputContext.getCounters()).thenReturn(new TezCounters());
    return outputContext;
  }


  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    outputContext = createTezOutputContext();
    conf.set("fs.defaultFS", "file:///");
    localFs = FileSystem.getLocal(conf);

    workingDir = new Path(
        new Path(System.getProperty("test.build.data", "/tmp")),
        TestShuffleUtils.class.getName())
        .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
    String localDirs = workingDir.toString();
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS,
        HashPartitioner.class.getName());
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, localDirs);
  }

  private Path createIndexFile(int numPartitions, boolean allEmptyPartitions) throws IOException {
    Path path = new Path(workingDir, "file.index.out");
    TezSpillRecord spillRecord = new TezSpillRecord(numPartitions);
    long startOffset = 0;
    long partLen = 200; //compressed
    for(int i=0;i<numPartitions;i++) {
      long rawLen = ThreadLocalRandom.current().nextLong(100, 200);
      if (i % 2  == 0 || allEmptyPartitions) {
        rawLen = 0; //indicates empty partition, see TEZ-3605
      }
      TezIndexRecord indexRecord = new TezIndexRecord(startOffset, rawLen, partLen);
      startOffset += partLen;
      spillRecord.putIndex(indexRecord, i);
    }
    spillRecord.writeToFile(path, conf);
    return path;
  }

  @Test
  public void testGenerateOnSpillEvent() throws Exception {
    List<Event> events = Lists.newLinkedList();
    Path indexFile = createIndexFile(10, false);

    boolean finalMergeEnabled = false;
    boolean isLastEvent = false;
    int spillId = 0;
    int physicalOutputs = 10;
    String pathComponent = "/attempt_x_y_0/file.out";
    String auxiliaryService = conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
        TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);

    ShuffleUtils.generateEventOnSpill(events, finalMergeEnabled, isLastEvent,
        outputContext, spillId, new TezSpillRecord(indexFile, conf),
            physicalOutputs, true, pathComponent, null, false, auxiliaryService, TezCommonUtils.newBestCompressionDeflater());

    Assert.assertTrue(events.size() == 1);
    Assert.assertTrue(events.get(0) instanceof CompositeDataMovementEvent);

    CompositeDataMovementEvent cdme = (CompositeDataMovementEvent) events.get(0);
    Assert.assertTrue(cdme.getCount() == physicalOutputs);
    Assert.assertTrue(cdme.getSourceIndexStart() == 0);

    ByteBuffer payload = cdme.getUserPayload();
    ShuffleUserPayloads.DataMovementEventPayloadProto dmeProto =
        ShuffleUserPayloads.DataMovementEventPayloadProto.parseFrom(ByteString.copyFrom(payload));

    Assert.assertTrue(dmeProto.getSpillId() == 0);
    Assert.assertTrue(dmeProto.hasLastEvent() && !dmeProto.getLastEvent());

    byte[] emptyPartitions = TezCommonUtils.decompressByteStringToByteArray(dmeProto.getEmptyPartitions());
    BitSet emptyPartitionsBitSet = TezUtilsInternal.fromByteArray(emptyPartitions);
    Assert.assertTrue("emptyPartitionBitSet cardinality (expecting 5) = " + emptyPartitionsBitSet
        .cardinality(), emptyPartitionsBitSet.cardinality() == 5);

    events.clear();

  }

  @Test
  public void testGenerateOnSpillEvent_With_FinalMerge() throws Exception {
    List<Event> events = Lists.newLinkedList();
    Path indexFile = createIndexFile(10, false);

    boolean finalMergeEnabled = true;
    boolean isLastEvent = true;
    int spillId = 0;
    int physicalOutputs = 10;
    String pathComponent = "/attempt_x_y_0/file.out";
    String auxiliaryService = conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
        TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);

    //normal code path where we do final merge all the time
    ShuffleUtils.generateEventOnSpill(events, finalMergeEnabled, isLastEvent,
        outputContext, spillId, new TezSpillRecord(indexFile, conf),
            physicalOutputs, true, pathComponent, null, false, auxiliaryService, TezCommonUtils.newBestCompressionDeflater());

    Assert.assertTrue(events.size() == 2); //one for VM
    Assert.assertTrue(events.get(0) instanceof VertexManagerEvent);
    Assert.assertTrue(events.get(1) instanceof CompositeDataMovementEvent);

    CompositeDataMovementEvent cdme = (CompositeDataMovementEvent) events.get(1);
    Assert.assertTrue(cdme.getCount() == physicalOutputs);
    Assert.assertTrue(cdme.getSourceIndexStart() == 0);

    ShuffleUserPayloads.DataMovementEventPayloadProto dmeProto =
        ShuffleUserPayloads.DataMovementEventPayloadProto.parseFrom(ByteString.copyFrom( cdme.getUserPayload()));

    //With final merge, spill details should not be present
    Assert.assertFalse(dmeProto.hasSpillId());
    Assert.assertFalse(dmeProto.hasLastEvent() || dmeProto.getLastEvent());

    byte[]  emptyPartitions = TezCommonUtils.decompressByteStringToByteArray(dmeProto
        .getEmptyPartitions());
    BitSet  emptyPartitionsBitSet = TezUtilsInternal.fromByteArray(emptyPartitions);
    Assert.assertTrue("emptyPartitionBitSet cardinality (expecting 5) = " + emptyPartitionsBitSet
        .cardinality(), emptyPartitionsBitSet.cardinality() == 5);

  }

  @Test
  public void testGenerateOnSpillEvent_With_All_EmptyPartitions() throws Exception {
    List<Event> events = Lists.newLinkedList();

    //Create an index file with all empty partitions
    Path indexFile = createIndexFile(10, true);

    boolean finalMergeDisabled = false;
    boolean isLastEvent = true;
    int spillId = 0;
    int physicalOutputs = 10;
    String pathComponent = "/attempt_x_y_0/file.out";
    String auxiliaryService = conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
        TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);

    //normal code path where we do final merge all the time
    ShuffleUtils.generateEventOnSpill(events, finalMergeDisabled, isLastEvent,
        outputContext, spillId, new TezSpillRecord(indexFile, conf),
            physicalOutputs, true, pathComponent, null, false, auxiliaryService, TezCommonUtils.newBestCompressionDeflater());

    Assert.assertTrue(events.size() == 2); //one for VM
    Assert.assertTrue(events.get(0) instanceof VertexManagerEvent);
    Assert.assertTrue(events.get(1) instanceof CompositeDataMovementEvent);

    CompositeDataMovementEvent cdme = (CompositeDataMovementEvent) events.get(1);
    Assert.assertTrue(cdme.getCount() == physicalOutputs);
    Assert.assertTrue(cdme.getSourceIndexStart() == 0);

    ShuffleUserPayloads.DataMovementEventPayloadProto dmeProto =
        ShuffleUserPayloads.DataMovementEventPayloadProto.parseFrom(ByteString.copyFrom( cdme.getUserPayload()));

    //spill details should be present
    Assert.assertTrue(dmeProto.getSpillId() == 0);
    Assert.assertTrue(dmeProto.hasLastEvent() && dmeProto.getLastEvent());

    Assert.assertTrue(dmeProto.getPathComponent().equals(""));

    byte[]  emptyPartitions = TezCommonUtils.decompressByteStringToByteArray(dmeProto
        .getEmptyPartitions());
    BitSet  emptyPartitionsBitSet = TezUtilsInternal.fromByteArray(emptyPartitions);
    Assert.assertTrue("emptyPartitionBitSet cardinality (expecting 10) = " + emptyPartitionsBitSet
        .cardinality(), emptyPartitionsBitSet.cardinality() == 10);

  }

  @Test
  public void testInternalErrorTranslation() throws Exception {
    String codecErrorMsg = "codec failure";
    CompressionInputStream mockCodecStream = mock(CompressionInputStream.class);
    when(mockCodecStream.read(any(byte[].class), anyInt(), anyInt()))
        .thenThrow(new InternalError(codecErrorMsg));
    Decompressor mockDecoder = mock(Decompressor.class);
    CompressionCodec mockCodec = mock(CompressionCodec.class);
    when(mockCodec.createDecompressor()).thenReturn(mockDecoder);
    when(mockCodec.createInputStream(any(InputStream.class), any(Decompressor.class)))
        .thenReturn(mockCodecStream);
    byte[] header = new byte[] { (byte) 'T', (byte) 'I', (byte) 'F', (byte) 1};
    try {
      ShuffleUtils.shuffleToMemory(new byte[1024], new ByteArrayInputStream(header),
          1024, 128, mockCodec, false, 0, mock(Logger.class), null);
      Assert.fail("shuffle was supposed to throw!");
    } catch (IOException e) {
      Assert.assertTrue(e.getCause() instanceof InternalError);
      Assert.assertTrue(e.getMessage().contains(codecErrorMsg));
    }
  }

  @Test
  public void testExceptionTranslation() throws Exception {
    String codecErrorMsg = "codec failure";
    CompressionInputStream mockCodecStream = mock(CompressionInputStream.class);
    when(mockCodecStream.read(any(byte[].class), anyInt(), anyInt()))
        .thenThrow(new IllegalArgumentException(codecErrorMsg));
    Decompressor mockDecoder = mock(Decompressor.class);
    CompressionCodec mockCodec = mock(CompressionCodec.class);
    when(mockCodec.createDecompressor()).thenReturn(mockDecoder);
    when(mockCodec.createInputStream(any(InputStream.class), any(Decompressor.class)))
        .thenReturn(mockCodecStream);
    byte[] header = new byte[] { (byte) 'T', (byte) 'I', (byte) 'F', (byte) 1};
    try {
      ShuffleUtils.shuffleToMemory(new byte[1024], new ByteArrayInputStream(header),
          1024, 128, mockCodec, false, 0, mock(Logger.class), null);
      Assert.fail("shuffle was supposed to throw!");
    } catch (IOException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
      Assert.assertTrue(e.getMessage().contains(codecErrorMsg));
    }
    CompressionInputStream mockCodecStream1 = mock(CompressionInputStream.class);
    when(mockCodecStream1.read(any(byte[].class), anyInt(), anyInt()))
        .thenThrow(new SocketTimeoutException(codecErrorMsg));
    CompressionCodec mockCodec1 = mock(CompressionCodec.class);
    when(mockCodec1.createDecompressor()).thenReturn(mockDecoder);
    when(mockCodec1.createInputStream(any(InputStream.class), any(Decompressor.class)))
        .thenReturn(mockCodecStream1);
    try {
      ShuffleUtils.shuffleToMemory(new byte[1024], new ByteArrayInputStream(header),
          1024, 128, mockCodec1, false, 0, mock(Logger.class), null);
      Assert.fail("shuffle was supposed to throw!");
    } catch (IOException e) {
      Assert.assertTrue(e instanceof SocketTimeoutException);
      Assert.assertTrue(e.getMessage().contains(codecErrorMsg));
    }
    CompressionInputStream mockCodecStream2 = mock(CompressionInputStream.class);
    when(mockCodecStream2.read(any(byte[].class), anyInt(), anyInt()))
        .thenThrow(new InternalError(codecErrorMsg));
    CompressionCodec mockCodec2 = mock(CompressionCodec.class);
    when(mockCodec2.createDecompressor()).thenReturn(mockDecoder);
    when(mockCodec2.createInputStream(any(InputStream.class), any(Decompressor.class)))
        .thenReturn(mockCodecStream2);
    try {
      ShuffleUtils.shuffleToMemory(new byte[1024], new ByteArrayInputStream(header),
          1024, 128, mockCodec2, false, 0, mock(Logger.class), null);
      Assert.fail("shuffle was supposed to throw!");
    } catch (IOException e) {
      Assert.assertTrue(e.getCause() instanceof InternalError);
      Assert.assertTrue(e.getMessage().contains(codecErrorMsg));
    }
  }

  @Test
  public void testShuffleToDiskChecksum() throws Exception {
    // verify sending a stream of zeroes without checksum validation
    // does not trigger an exception
    byte[] bogusData = new byte[1000];
    Arrays.fill(bogusData, (byte) 0);
    ByteArrayInputStream in = new ByteArrayInputStream(bogusData);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ShuffleUtils.shuffleToDisk(baos, "somehost", in,
        bogusData.length, 2000, mock(Logger.class), null, false, 0, false);
    Assert.assertArrayEquals(bogusData, baos.toByteArray());

    // verify sending same stream of zeroes with validation generates an exception
    in.reset();
    try {
      ShuffleUtils.shuffleToDisk(mock(OutputStream.class), "somehost", in,
          bogusData.length, 2000, mock(Logger.class), null, false, 0, true);
      Assert.fail("shuffle was supposed to throw!");
    } catch (IOException e) {
    }
  }

  @Test
  public void testFetchStatsLogger() throws Exception {
    Logger activeLogger = mock(Logger.class);
    Logger aggregateLogger = mock(Logger.class);
    FetchStatsLogger logger = new FetchStatsLogger(activeLogger, aggregateLogger);

    InputAttemptIdentifier ident = new InputAttemptIdentifier(1, 1);
    when(activeLogger.isInfoEnabled()).thenReturn(false);
    for (int i = 0; i < 1000; i++) {
      logger.logIndividualFetchComplete(10, 100, 1000, "testType", ident);
    }
    verify(activeLogger, times(0)).info(anyString());
    verify(aggregateLogger, times(1)).info(anyString(), Matchers.<Object[]>anyVararg());

    when(activeLogger.isInfoEnabled()).thenReturn(true);
    for (int i = 0; i < 1000; i++) {
      logger.logIndividualFetchComplete(10, 100, 1000, "testType", ident);
    }
    verify(activeLogger, times(1000)).info(anyString());
    verify(aggregateLogger, times(1)).info(anyString(), Matchers.<Object[]>anyVararg());
  }
}
