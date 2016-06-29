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

package org.apache.tez.mapreduce.input;

import java.nio.ByteBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestMultiMRInput {

  private static final Logger LOG = LoggerFactory.getLogger(TestMultiMRInput.class);

  private static final JobConf defaultConf = new JobConf();
  private static final String testTmpDir;
  private static final Path TEST_ROOT_DIR;
  private static FileSystem localFs;

  static {
    defaultConf.set("fs.defaultFS", "file:///");
    try {
      localFs = FileSystem.getLocal(defaultConf);
      testTmpDir = System.getProperty("test.build.data", "target");
      TEST_ROOT_DIR = new Path(testTmpDir, TestMultiMRInput.class.getSimpleName() + "-tmpDir");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setup() throws IOException {
    LOG.info("Setup. Using test dir: " + TEST_ROOT_DIR);
    localFs.delete(TEST_ROOT_DIR, true);
    localFs.mkdirs(TEST_ROOT_DIR);
  }

  @Test(timeout = 5000)
  public void test0PhysicalInputs() throws Exception {

    Path workDir = new Path(TEST_ROOT_DIR, "testSingleSplit");
    JobConf jobConf = new JobConf(defaultConf);
    jobConf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class);
    FileInputFormat.setInputPaths(jobConf, workDir);

    InputContext inputContext = createTezInputContext(jobConf);

    MultiMRInput mMrInput = new MultiMRInput(inputContext, 0);

    mMrInput.initialize();

    mMrInput.start();

    assertEquals(0, mMrInput.getKeyValueReaders().size());

    List<Event> events = new LinkedList<>();
    try {
      mMrInput.handleEvents(events);
      fail("HandleEvents should cause an input with 0 physical inputs to fail");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalStateException);
    }
  }

  @Test(timeout = 5000)
  public void testSingleSplit() throws Exception {

    Path workDir = new Path(TEST_ROOT_DIR, "testSingleSplit");
    JobConf jobConf = new JobConf(defaultConf);
    jobConf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class);
    FileInputFormat.setInputPaths(jobConf, workDir);

    InputContext inputContext = createTezInputContext(jobConf);

    MultiMRInput input = new MultiMRInput(inputContext, 1);
    input.initialize();

    AtomicLong inputLength = new AtomicLong();
    LinkedHashMap<LongWritable, Text> data = createSplits(1, workDir, jobConf, inputLength);

    SequenceFileInputFormat<LongWritable, Text> format =
        new SequenceFileInputFormat<LongWritable, Text>();
    InputSplit[] splits = format.getSplits(jobConf, 1);
    assertEquals(1, splits.length);

    MRSplitProto splitProto = MRInputHelpers.createSplitProto(splits[0]);
    InputDataInformationEvent event =
        InputDataInformationEvent.createWithSerializedPayload(0,
            splitProto.toByteString().asReadOnlyByteBuffer());

    List<Event> eventList = new ArrayList<Event>();
    eventList.add(event);
    input.handleEvents(eventList);

    assertReaders(input, data, 1, inputLength.get());
  }

  @Test
  public void testNewFormatSplits() throws Exception {
    Path workDir = new Path(TEST_ROOT_DIR, "testNewFormatSplits");
    Job job = Job.getInstance(defaultConf);
    job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, workDir);
    Configuration conf = job.getConfiguration();
    conf.setBoolean("mapred.mapper.new-api", true);

    // Create sequence file.
    AtomicLong inputLength = new AtomicLong();
    LinkedHashMap<LongWritable, Text> data = createSplits(1, workDir, conf, inputLength);

    // Get split information.
    org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat<LongWritable, Text> format =
        new org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat<>();
    List<org.apache.hadoop.mapreduce.InputSplit> splits = format.getSplits(job);
    assertEquals(1, splits.size());

    // Create the event.
    MRSplitProto splitProto =
        MRInputHelpers.createSplitProto(splits.get(0), new SerializationFactory(conf));
    InputDataInformationEvent event = InputDataInformationEvent.createWithSerializedPayload(0,
        splitProto.toByteString().asReadOnlyByteBuffer());

    // Create input context.
    InputContext inputContext = createTezInputContext(conf);

    // Create the MR input object and process the event
    MultiMRInput input = new MultiMRInput(inputContext, 1);
    input.initialize();
    input.handleEvents(Collections.<Event>singletonList(event));

    assertReaders(input, data, 1, inputLength.get());
  }

  @Test(timeout = 5000)
  public void testMultipleSplits() throws Exception {

    Path workDir = new Path(TEST_ROOT_DIR, "testMultipleSplits");
    JobConf jobConf = new JobConf(defaultConf);
    jobConf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class);
    FileInputFormat.setInputPaths(jobConf, workDir);

    InputContext inputContext = createTezInputContext(jobConf);

    MultiMRInput input = new MultiMRInput(inputContext, 2);
    input.initialize();

    AtomicLong inputLength = new AtomicLong();
    LinkedHashMap<LongWritable, Text> data = createSplits(2, workDir, jobConf, inputLength);

    SequenceFileInputFormat<LongWritable, Text> format =
        new SequenceFileInputFormat<LongWritable, Text>();
    InputSplit[] splits = format.getSplits(jobConf, 2);
    assertEquals(2, splits.length);

    MRSplitProto splitProto1 = MRInputHelpers.createSplitProto(splits[0]);
    InputDataInformationEvent event1 =
        InputDataInformationEvent.createWithSerializedPayload(0,
            splitProto1.toByteString().asReadOnlyByteBuffer());

    MRSplitProto splitProto2 = MRInputHelpers.createSplitProto(splits[1]);
    InputDataInformationEvent event2 =
        InputDataInformationEvent.createWithSerializedPayload(0,
            splitProto2.toByteString().asReadOnlyByteBuffer());

    List<Event> eventList = new ArrayList<Event>();
    eventList.add(event1);
    eventList.add(event2);
    input.handleEvents(eventList);

    assertReaders(input, data, 2, inputLength.get());
  }

  private void assertReaders(MultiMRInput input, LinkedHashMap<LongWritable, Text> data,
      int expectedReaderCounts, long inputBytes) throws Exception {
    int readerCount = 0;
    int recordCount = 0;
    for (KeyValueReader reader : input.getKeyValueReaders()) {
      readerCount++;
      while (reader.next()) {
        verify(input.getContext(), times(++recordCount + readerCount - 1)).notifyProgress();
        if (data.size() == 0) {
          fail("Found more records than expected");
        }
        Object key = reader.getCurrentKey();
        Object val = reader.getCurrentValue();
        assertEquals(val, data.remove(key));
      }

      try {
        reader.next(); //should throw exception
        fail();
      } catch(IOException e) {
        assertTrue(e.getMessage().contains("For usage, please refer to"));
      }
    }
    long counterValue = input.getContext().getCounters()
        .findCounter(TaskCounter.INPUT_SPLIT_LENGTH_BYTES).getValue();
    assertEquals(inputBytes, counterValue);
    assertEquals(expectedReaderCounts, readerCount);
  }

  @Test(timeout = 5000)
  public void testExtraEvents() throws Exception {
    Path workDir = new Path(TEST_ROOT_DIR, "testExtraEvents");
    JobConf jobConf = new JobConf(defaultConf);
    jobConf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class);
    FileInputFormat.setInputPaths(jobConf, workDir);

    InputContext inputContext = createTezInputContext(jobConf);

    MultiMRInput input = new MultiMRInput(inputContext, 1);
    input.initialize();

    createSplits(1, workDir, jobConf, new AtomicLong());

    SequenceFileInputFormat<LongWritable, Text> format =
        new SequenceFileInputFormat<LongWritable, Text>();
    InputSplit[] splits = format.getSplits(jobConf, 1);
    assertEquals(1, splits.length);

    MRSplitProto splitProto = MRInputHelpers.createSplitProto(splits[0]);
    InputDataInformationEvent event1 =
        InputDataInformationEvent.createWithSerializedPayload(0,
            splitProto.toByteString().asReadOnlyByteBuffer());
    InputDataInformationEvent event2 =
        InputDataInformationEvent.createWithSerializedPayload(1,
            splitProto.toByteString().asReadOnlyByteBuffer());

    List<Event> eventList = new ArrayList<Event>();
    eventList.add(event1);
    eventList.add(event2);
    try {
      input.handleEvents(eventList);
      fail("Expecting Exception due to too many events");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(
          "Unexpected event. All physical sources already initialized"));
    }
  }

  private LinkedHashMap<LongWritable, Text> createSplits(int splitCount, Path workDir,
      Configuration conf, AtomicLong totalSize) throws Exception {
    LinkedHashMap<LongWritable, Text> data = new LinkedHashMap<LongWritable, Text>();
    for (int i = 0; i < splitCount; ++i) {
      int start = i * 10;
      int end = start + 10;
      data.putAll(createInputData(localFs, workDir, conf, "file" + i, start, end, totalSize));
    }
    return data;
  }

  private InputContext createTezInputContext(Configuration conf) throws Exception {
    MRInputUserPayloadProto.Builder builder = MRInputUserPayloadProto.newBuilder();
    builder.setGroupingEnabled(false);
    builder.setConfigurationBytes(TezUtils.createByteStringFromConf(conf));
    byte[] payload = builder.build().toByteArray();

    ApplicationId applicationId = ApplicationId.newInstance(10000, 1);
    TezCounters counters = new TezCounters();

    InputContext inputContext = mock(InputContext.class);
    doReturn(applicationId).when(inputContext).getApplicationId();
    doReturn(counters).when(inputContext).getCounters();
    doReturn(1).when(inputContext).getDAGAttemptNumber();
    doReturn("dagName").when(inputContext).getDAGName();
    doReturn(1).when(inputContext).getInputIndex();
    doReturn("srcVertexName").when(inputContext).getSourceVertexName();
    doReturn(1).when(inputContext).getTaskAttemptNumber();
    doReturn(1).when(inputContext).getTaskIndex();
    doReturn(1).when(inputContext).getTaskVertexIndex();
    doReturn(UUID.randomUUID().toString()).when(inputContext).getUniqueIdentifier();
    doReturn("taskVertexName").when(inputContext).getTaskVertexName();
    doReturn(UserPayload.create(ByteBuffer.wrap(payload))).when(inputContext).getUserPayload();
    return inputContext;
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    localFs.delete(TEST_ROOT_DIR, true);
  }

  public static LinkedHashMap<LongWritable, Text> createInputData(FileSystem fs, Path workDir,
      Configuration job, String filename, long startKey, long numKeys, AtomicLong fileLength)
          throws IOException {
    LinkedHashMap<LongWritable, Text> data = new LinkedHashMap<LongWritable, Text>();
    Path file = new Path(workDir, filename);
    LOG.info("Generating data at path: " + file);
    // create a file with length entries
    @SuppressWarnings("deprecation")
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, job, file, LongWritable.class,
        Text.class);
    try {
      Random r = new Random(System.currentTimeMillis());
      LongWritable key = new LongWritable();
      Text value = new Text();
      for (long i = startKey; i < numKeys; i++) {
        key.set(i);
        value.set(Integer.toString(r.nextInt(10000)));
        data.put(new LongWritable(key.get()), new Text(value.toString()));
        writer.append(key, value);
        LOG.info("<k, v> : <" + key.get() + ", " + value + ">");
      }
      fileLength.addAndGet(writer.getLength());
    } finally {
      writer.close();
    }
    return data;
  }
}
