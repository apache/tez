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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.junit.Before;
import org.junit.Test;

public class TestMultiMRInput {

  private static final Log LOG = LogFactory.getLog(TestMultiMRInput.class);

  private static final JobConf defaultConf = new JobConf();
  private static final String testTmpDir;
  private static final Path TEST_ROOT_DIR;
  private static FileSystem localFs;

  static {
    defaultConf.set("fs.defaultFS", "file:///");
    try {
      localFs = FileSystem.getLocal(defaultConf);
      testTmpDir = System.getProperty("test.build.data", "/tmp");
      TEST_ROOT_DIR = new Path(testTmpDir, TestMultiMRInput.class.getSimpleName());
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
  public void testSingleSplit() throws Exception {

    Path workDir = new Path(TEST_ROOT_DIR, "testSingleSplit");
    JobConf jobConf = new JobConf(defaultConf);
    jobConf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class);
    FileInputFormat.setInputPaths(jobConf, workDir);

    MRInputUserPayloadProto.Builder builder = MRInputUserPayloadProto.newBuilder();
    builder.setInputFormatName(SequenceFileInputFormat.class.getName());
    builder.setConfigurationBytes(TezUtils.createByteStringFromConf(jobConf));
    byte[] payload = builder.build().toByteArray();

    TezInputContext inputContext = createTezInputContext(payload);

    MultiMRInput input = new MultiMRInput();
    input.setNumPhysicalInputs(1);
    input.initialize(inputContext);
    List<Event> eventList = new ArrayList<Event>();

    String file1 = "file1";
    LinkedHashMap<LongWritable, Text> data1 = createInputData(localFs, workDir, jobConf, file1, 0,
        10);
    SequenceFileInputFormat<LongWritable, Text> format =
        new SequenceFileInputFormat<LongWritable, Text>();
    InputSplit[] splits = format.getSplits(jobConf, 1);
    assertEquals(1, splits.length);

    MRSplitProto splitProto = MRHelpers.createSplitProto(splits[0]);
    RootInputDataInformationEvent event = new RootInputDataInformationEvent(0,
        splitProto.toByteArray());

    eventList.clear();
    eventList.add(event);
    input.handleEvents(eventList);

    int readerCount = 0;
    for (KeyValueReader reader : input.getKeyValueReaders()) {
      readerCount++;
      while (reader.next()) {
        if (data1.size() == 0) {
          fail("Found more records than expected");
        }
        Object key = reader.getCurrentKey();
        Object val = reader.getCurrentValue();
        assertEquals(val, data1.remove(key));
      }
    }
    assertEquals(1, readerCount);
  }

  @Test(timeout = 5000)
  public void testMultipleSplits() throws Exception {

    Path workDir = new Path(TEST_ROOT_DIR, "testMultipleSplits");
    JobConf jobConf = new JobConf(defaultConf);
    jobConf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class);
    FileInputFormat.setInputPaths(jobConf, workDir);

    MRInputUserPayloadProto.Builder builder = MRInputUserPayloadProto.newBuilder();
    builder.setInputFormatName(SequenceFileInputFormat.class.getName());
    builder.setConfigurationBytes(TezUtils.createByteStringFromConf(jobConf));
    byte[] payload = builder.build().toByteArray();

    TezInputContext inputContext = createTezInputContext(payload);

    MultiMRInput input = new MultiMRInput();
    input.setNumPhysicalInputs(2);
    input.initialize(inputContext);
    List<Event> eventList = new ArrayList<Event>();

    LinkedHashMap<LongWritable, Text> data = new LinkedHashMap<LongWritable, Text>();

    String file1 = "file1";
    LinkedHashMap<LongWritable, Text> data1 = createInputData(localFs, workDir, jobConf, file1, 0,
        10);

    String file2 = "file2";
    LinkedHashMap<LongWritable, Text> data2 = createInputData(localFs, workDir, jobConf, file2, 10,
        20);

    data.putAll(data1);
    data.putAll(data2);

    SequenceFileInputFormat<LongWritable, Text> format =
        new SequenceFileInputFormat<LongWritable, Text>();
    InputSplit[] splits = format.getSplits(jobConf, 2);
    assertEquals(2, splits.length);

    MRSplitProto splitProto1 = MRHelpers.createSplitProto(splits[0]);
    RootInputDataInformationEvent event1 = new RootInputDataInformationEvent(0,
        splitProto1.toByteArray());

    MRSplitProto splitProto2 = MRHelpers.createSplitProto(splits[1]);
    RootInputDataInformationEvent event2 = new RootInputDataInformationEvent(0,
        splitProto2.toByteArray());

    eventList.clear();
    eventList.add(event1);
    eventList.add(event2);
    input.handleEvents(eventList);

    int readerCount = 0;
    for (KeyValueReader reader : input.getKeyValueReaders()) {
      readerCount++;
      while (reader.next()) {
        if (data.size() == 0) {
          fail("Found more records than expected");
        }
        Object key = reader.getCurrentKey();
        Object val = reader.getCurrentValue();
        assertEquals(val, data.remove(key));
      }
    }
    assertEquals(2, readerCount);
  }

  @Test(timeout = 5000)
  public void testExtraEvents() throws Exception {
    Path workDir = new Path(TEST_ROOT_DIR, "testExtraEvents");
    JobConf jobConf = new JobConf(defaultConf);
    jobConf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class);
    FileInputFormat.setInputPaths(jobConf, workDir);

    MRInputUserPayloadProto.Builder builder = MRInputUserPayloadProto.newBuilder();
    builder.setInputFormatName(SequenceFileInputFormat.class.getName());
    builder.setConfigurationBytes(TezUtils.createByteStringFromConf(jobConf));
    byte[] payload = builder.build().toByteArray();

    TezInputContext inputContext = createTezInputContext(payload);

    MultiMRInput input = new MultiMRInput();
    input.setNumPhysicalInputs(1);
    input.initialize(inputContext);
    List<Event> eventList = new ArrayList<Event>();

    String file1 = "file1";
    createInputData(localFs, workDir, jobConf, file1, 0, 10);
    SequenceFileInputFormat<LongWritable, Text> format =
        new SequenceFileInputFormat<LongWritable, Text>();
    InputSplit[] splits = format.getSplits(jobConf, 1);
    assertEquals(1, splits.length);

    MRSplitProto splitProto = MRHelpers.createSplitProto(splits[0]);
    RootInputDataInformationEvent event1 = new RootInputDataInformationEvent(0,
        splitProto.toByteArray());
    RootInputDataInformationEvent event2 = new RootInputDataInformationEvent(1,
        splitProto.toByteArray());

    eventList.clear();
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

  private TezInputContext createTezInputContext(byte[] payload) {
    ApplicationId applicationId = ApplicationId.newInstance(10000, 1);
    TezCounters counters = new TezCounters();

    TezInputContext inputContext = mock(TezInputContext.class);
    doReturn(applicationId).when(inputContext).getApplicationId();
    doReturn(counters).when(inputContext).getCounters();
    doReturn(1).when(inputContext).getDAGAttemptNumber();
    doReturn("dagName").when(inputContext).getDAGName();
    doReturn(1).when(inputContext).getInputIndex();
    doReturn("srcVertexName").when(inputContext).getSourceVertexName();
    doReturn(1).when(inputContext).getTaskAttemptNumber();
    doReturn(1).when(inputContext).getTaskIndex();
    doReturn(1).when(inputContext).getTaskVertexIndex();
    doReturn("taskVertexName").when(inputContext).getTaskVertexName();
    doReturn(payload).when(inputContext).getUserPayload();
    return inputContext;
  }

  public static LinkedHashMap<LongWritable, Text> createInputData(FileSystem fs, Path workDir,
                                                                  JobConf job, String filename,
                                                                  long startKey,
                                                                  long numKeys) throws IOException {
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
    } finally {
      writer.close();
    }
    return data;
  }
}
