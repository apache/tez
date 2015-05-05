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

package org.apache.tez.mapreduce.lib;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestKVReadersWithMR {

  private JobConf conf;
  private TezCounters counters;
  private TezCounter inputRecordCounter;

  @Before
  public void setup() {
    conf = new JobConf();
    counters = new TezCounters();
    inputRecordCounter = counters.findCounter(TaskCounter.INPUT_RECORDS_PROCESSED);
  }

  @Test(timeout = 10000)
  public void testMRReaderMapred() throws IOException {
    //empty
    testWithSpecificNumberOfKV(0);

    testWithSpecificNumberOfKV(10);

    //empty
    testWithSpecificNumberOfKV_MapReduce(0);

    testWithSpecificNumberOfKV_MapReduce(10);
  }

  public void testWithSpecificNumberOfKV(int kvPairs) throws IOException {
    MRReaderMapred reader = new MRReaderMapred(conf, counters, inputRecordCounter);

    reader.recordReader = new DummyRecordReader(kvPairs);
    int records = 0;
    while (reader.next()) {
      records++;
    }
    assertTrue(kvPairs == records);

    //reading again should fail
    try {
      boolean hasNext = reader.next();
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("For usage, please refer to"));
    }

  }

  public void testWithSpecificNumberOfKV_MapReduce(int kvPairs) throws IOException {
    MRReaderMapReduce reader = new MRReaderMapReduce(conf, counters, inputRecordCounter, -1, 1,
        10, 20, 30);

    reader.recordReader = new DummyRecordReaderMapReduce(kvPairs);
    int records = 0;
    while (reader.next()) {
      records++;
    }
    assertTrue(kvPairs == records);

    //reading again should fail
    try {
      boolean hasNext = reader.next();
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("For usage, please refer to"));
    }
  }

  static class DummyRecordReader implements RecordReader {
    int records;

    public DummyRecordReader(int records) {
      this.records = records;
    }

    @Override
    public boolean next(Object o, Object o2) throws IOException {
      return (records-- > 0);
    }

    @Override
    public Object createKey() {
      return null;
    }

    @Override
    public Object createValue() {
      return null;
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException {
      return 0;
    }
  }

  static class DummyRecordReaderMapReduce extends org.apache.hadoop.mapreduce.RecordReader {
    int records;

    public DummyRecordReaderMapReduce(int records) {
      this.records = records;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
        throws IOException, InterruptedException {

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return (records-- > 0);
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
      return null;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
      return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public void close() throws IOException {
    }
  }

}
