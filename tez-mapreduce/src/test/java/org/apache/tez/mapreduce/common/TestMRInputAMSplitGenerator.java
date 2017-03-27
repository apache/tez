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

package org.apache.tez.mapreduce.common;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.split.TezGroupedSplit;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.UserPayload;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.mapreduce.TezTestUtils;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.lib.MRInputUtils;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestMRInputAMSplitGenerator {

  private static String SPLITS_LENGTHS = "splits.length";

  @Test(timeout = 5000)
  public void testGroupSplitsDisabledSortSplitsEnabled()
      throws Exception {
    testGroupSplitsAndSortSplits(false, true);
  }

  @Test(timeout = 5000)
  public void testGroupSplitsDisabledSortSplitsDisabled()
      throws Exception {
    testGroupSplitsAndSortSplits(false, false);
  }

  @Test(timeout = 5000)
  public void testGroupSplitsEnabledSortSplitsEnabled()
      throws Exception {
    testGroupSplitsAndSortSplits(true, true);
  }

  @Test(timeout = 5000)
  public void testGroupSplitsEnabledSortSplitsDisabled()
          throws Exception {
    testGroupSplitsAndSortSplits(true, false);
  }

  private void testGroupSplitsAndSortSplits(boolean groupSplitsEnabled,
      boolean sortSplitsEnabled) throws Exception {
    Configuration conf = new Configuration();
    String[] splitLengths = new String[50];
    for (int i = 0; i < splitLengths.length; i++) {
      splitLengths[i] = Integer.toString(1000 * (i + 1));
    }
    conf.setStrings(SPLITS_LENGTHS, splitLengths);
    DataSourceDescriptor dataSource = MRInput.createConfigBuilder(
        conf, InputFormatForTest.class).
        groupSplits(groupSplitsEnabled).sortSplits(sortSplitsEnabled).build();
    UserPayload userPayload = dataSource.getInputDescriptor().getUserPayload();

    InputInitializerContext context =
        new TezTestUtils.TezRootInputInitializerContextForTest(userPayload);
    MRInputAMSplitGenerator splitGenerator =
        new MRInputAMSplitGenerator(context);

    List<Event> events = splitGenerator.initialize();

    assertTrue(events.get(0) instanceof InputConfigureVertexTasksEvent);
    boolean shuffled = false;
    InputSplit previousIs = null;
    int numRawInputSplits = 0;
    for (int i = 1; i < events.size(); i++) {
      assertTrue(events.get(i) instanceof InputDataInformationEvent);
      InputDataInformationEvent diEvent = (InputDataInformationEvent) (events.get(i));
      assertNull(diEvent.getDeserializedUserPayload());
      assertNotNull(diEvent.getUserPayload());
      MRSplitProto eventProto = MRSplitProto.parseFrom(ByteString.copyFrom(
          diEvent.getUserPayload()));
      InputSplit is = MRInputUtils.getNewSplitDetailsFromEvent(
          eventProto, new Configuration());
      if (groupSplitsEnabled) {
        numRawInputSplits += ((TezGroupedSplit)is).getGroupedSplits().size();
        for (InputSplit inputSplit : ((TezGroupedSplit)is).getGroupedSplits()) {
          assertTrue(inputSplit instanceof InputSplitForTest);
        }
        assertTrue(((TezGroupedSplit)is).getGroupedSplits().get(0)
            instanceof InputSplitForTest);
      } else {
        numRawInputSplits++;
        assertTrue(is instanceof InputSplitForTest);
      }
      // The splits in the list returned from InputFormat has ascending
      // size in order.
      // If sortSplitsEnabled is true, MRInputAMSplitGenerator will sort the
      // splits in descending order.
      // If sortSplitsEnabled is false, MRInputAMSplitGenerator will shuffle
      // the splits.
      if (previousIs != null) {
        if (sortSplitsEnabled) {
          assertTrue(is.getLength() <= previousIs.getLength());
        } else {
          shuffled |= (is.getLength() > previousIs.getLength());
        }
      }
      previousIs = is;
    }
    assertEquals(splitLengths.length, numRawInputSplits);
    if (!sortSplitsEnabled) {
      assertTrue(shuffled);
    }
  }

  private static class InputFormatForTest
      extends InputFormat<IntWritable, IntWritable> {

    @Override
    public RecordReader<IntWritable, IntWritable> createRecordReader(
        org.apache.hadoop.mapreduce.InputSplit split,
        TaskAttemptContext context) throws IOException,
        InterruptedException {
      return new RecordReader<IntWritable, IntWritable>() {

        private boolean done = false;

        @Override
        public void close() throws IOException {
        }

        @Override
        public IntWritable getCurrentKey() throws IOException,
                InterruptedException {
          return new IntWritable(0);
        }

        @Override
        public IntWritable getCurrentValue() throws IOException,
                InterruptedException {
          return new IntWritable(0);
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
          return done ? 0 : 1;
        }

        @Override
        public void initialize(org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
          if (!done) {
            done = true;
            return true;
          }
          return false;
        }
      };
    }

    @Override
    public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(
        JobContext context) throws IOException, InterruptedException {
      List<org.apache.hadoop.mapreduce.InputSplit> list = new ArrayList<org.apache.hadoop.mapreduce.InputSplit>();
      int[] lengths = context.getConfiguration().getInts(SPLITS_LENGTHS);
      for (int i = 0; i < lengths.length; i++) {
        list.add(new InputSplitForTest(i + 1, lengths[i]));
      }
      return list;
    }
  }

  @Private
  public static class InputSplitForTest extends InputSplit
      implements Writable {

    private int identifier;
    private int length;

    @SuppressWarnings("unused")
    public InputSplitForTest() {
      // For writable
    }

    public int getIdentifier() {
      return this.identifier;
    }
    public InputSplitForTest(int identifier, int length) {
      this.identifier = identifier;
      this.length = length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(identifier);
      out.writeInt(length);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      identifier = in.readInt();
      length = in.readInt();
    }

    @Override
    public long getLength() throws IOException {
      return length;
    }

    @Override
    public String[] getLocations() throws IOException {
      return new String[] {"localhost"};
    }
  }
}
