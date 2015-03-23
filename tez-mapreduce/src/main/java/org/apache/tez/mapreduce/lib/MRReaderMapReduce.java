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

package org.apache.tez.mapreduce.lib;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.mapreduce.hadoop.mapreduce.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;

import com.google.common.base.Preconditions;

public class MRReaderMapReduce extends MRReader {

  private static final Logger LOG = LoggerFactory.getLogger(MRReader.class);

  private final TezCounter inputRecordCounter;

  private final TaskAttemptContext taskAttemptContext;
  @SuppressWarnings("rawtypes")
  private final InputFormat inputFormat;
  @SuppressWarnings("rawtypes")
  private RecordReader recordReader;
  private InputSplit inputSplit;

  private boolean setupComplete = false;

  public MRReaderMapReduce(JobConf jobConf, TezCounters tezCounters, TezCounter inputRecordCounter,
      long clusterId, int vertexIndex, int appId, int taskIndex, int taskAttemptNumber)
      throws IOException {
    this(jobConf, null, tezCounters, inputRecordCounter, clusterId, vertexIndex, appId, taskIndex,
        taskAttemptNumber);
  }

  public MRReaderMapReduce(JobConf jobConf, InputSplit inputSplit, TezCounters tezCounters,
      TezCounter inputRecordCounter, long clusterId, int vertexIndex, int appId, int taskIndex,
      int taskAttemptNumber) throws IOException {
    this.inputRecordCounter = inputRecordCounter;
    this.taskAttemptContext = new TaskAttemptContextImpl(jobConf, tezCounters, clusterId,
        vertexIndex, appId, taskIndex, taskAttemptNumber, true, null);

    Class<? extends org.apache.hadoop.mapreduce.InputFormat<?, ?>> inputFormatClazz;

    try {
      inputFormatClazz = taskAttemptContext.getInputFormatClass();
    } catch (ClassNotFoundException e) {
      throw new IOException("Unable to instantiate InputFormat class", e);
    }

    inputFormat = ReflectionUtils.newInstance(inputFormatClazz, jobConf);

    if (inputSplit != null) {
      this.inputSplit = inputSplit;
      setupNewRecordReader();
    }
  }

  @Override
  public void setSplit(Object inputSplit) throws IOException {
    this.inputSplit = (InputSplit) inputSplit;
    setupNewRecordReader();
  }

  public boolean isSetup() {
    return setupComplete;
  }

  public float getProgress() throws IOException, InterruptedException {
    return setupComplete ? recordReader.getProgress() : 0.0f;
  }

  public void close() throws IOException {
    if (setupComplete) {
      recordReader.close();
    }
  }

  @Override
  public Object getSplit() {
    return inputSplit;
  }

  @Override
  public Object getRecordReader() {
    return recordReader;
  }

  @Override
  public boolean next() throws IOException {
    boolean hasNext;
    try {
      hasNext = recordReader.nextKeyValue();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while checking for next key-value", e);
    }
    if (hasNext) {
      inputRecordCounter.increment(1);
    }
    return hasNext;
  }

  @Override
  public Object getCurrentKey() throws IOException {
    try {
      return recordReader.getCurrentKey();
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while fetching next key", e);
    }
  }

  @Override
  public Object getCurrentValue() throws IOException {
    try {
      return recordReader.getCurrentValue();
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while fetching next value", e);
    }
  }

  private void setupNewRecordReader() throws IOException {
    Preconditions.checkNotNull(inputSplit, "Input split hasn't yet been setup");
    try {
      recordReader = inputFormat.createRecordReader(inputSplit, taskAttemptContext);
      recordReader.initialize(inputSplit, taskAttemptContext);
      setupComplete = true;
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while creating record reader", e);
    }
  }
}
