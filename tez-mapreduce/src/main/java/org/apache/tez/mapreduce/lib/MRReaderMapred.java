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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.mapreduce.hadoop.mapred.MRReporter;
import org.apache.tez.mapreduce.input.MRInput;

import com.google.common.base.Preconditions;

public class MRReaderMapred extends MRReader {

  private static final Logger LOG = LoggerFactory.getLogger(MRReaderMapred.class);

  Object key;
  Object value;

  private final JobConf jobConf;
  private final TezCounters tezCounters;
  private final TezCounter inputRecordCounter;

  @SuppressWarnings("rawtypes")
  private final InputFormat inputFormat;
  protected InputSplit inputSplit;
  @SuppressWarnings("rawtypes")
  protected RecordReader recordReader;
  private Configuration incrementalConf;

  private boolean setupComplete = false;

  public MRReaderMapred(JobConf jobConf, TezCounters tezCounters, TezCounter inputRecordCounter)
      throws IOException {
    this(jobConf, null, tezCounters, inputRecordCounter);
  }

  public MRReaderMapred(JobConf jobConf, InputSplit inputSplit, TezCounters tezCounters,
      TezCounter inputRecordCounter) throws IOException {
    this.jobConf = jobConf;
    this.tezCounters = tezCounters;
    this.inputRecordCounter = inputRecordCounter;
    inputFormat = this.jobConf.getInputFormat();

    if (inputSplit != null) {
      this.inputSplit = inputSplit;
      setupOldRecordReader();
    }
  }

  @Override
  public void setSplit(Object inputSplit) throws IOException {
    this.inputSplit = (InputSplit) inputSplit;
    setupOldRecordReader();
  }

  @Override
  public boolean isSetup() {
    return setupComplete;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return setupComplete ? recordReader.getProgress() : 0.0f;
  }

  @Override
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

  @SuppressWarnings("unchecked")
  @Override
  public boolean next() throws IOException {
    boolean hasNext = recordReader.next(key, value);
    if (hasNext) {
      inputRecordCounter.increment(1);
    } else {
      hasCompletedProcessing();
      completedProcessing = true;
    }
    return hasNext;
  }

  @Override
  public Object getCurrentKey() throws IOException {
    return key;
  }

  @Override
  public Object getCurrentValue() throws IOException {
    return value;
  }

  /**
   * {@link MRInput} sets some additional parameters like split location when using the new API.
   * This methods returns the list of additional updates, and should be used by Processors using the
   * old MapReduce API with {@link MRInput}.
   * 
   * @return the additional fields set by {@link MRInput}
   */
  public Configuration getConfigUpdates() {
    if (incrementalConf != null) {
      return new Configuration(incrementalConf);
    }
    return null;
  }

  private void setupOldRecordReader() throws IOException {
    Preconditions.checkNotNull(inputSplit, "Input split hasn't yet been setup");
    recordReader = inputFormat.getRecordReader(inputSplit, this.jobConf, new MRReporter(
        tezCounters, inputSplit));
    setIncrementalConfigParams(inputSplit);
    key = recordReader.createKey();
    value = recordReader.createValue();
    setupComplete = true;
  }

  private void setIncrementalConfigParams(InputSplit inputSplit) {
    if (inputSplit instanceof FileSplit) {
      FileSplit fileSplit = (FileSplit) inputSplit;
      this.incrementalConf = new Configuration(false);

      this.incrementalConf.set(JobContext.MAP_INPUT_FILE, fileSplit.getPath().toString());
      this.incrementalConf.setLong(JobContext.MAP_INPUT_START, fileSplit.getStart());
      this.incrementalConf.setLong(JobContext.MAP_INPUT_PATH, fileSplit.getLength());
    }
    LOG.info("Processing split: " + inputSplit);
  }
}
