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

package org.apache.hadoop.mapreduce.split;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;

import org.apache.tez.common.Preconditions;

/**
 * An InputFormat that provides a generic grouping around
 * the splits of a real InputFormat
 */
@Public
@Evolving
public class TezGroupedSplitsInputFormat<K, V> extends InputFormat<K, V>
  implements Configurable{
  
  private static final Logger LOG = LoggerFactory.getLogger(TezGroupedSplitsInputFormat.class);

  InputFormat<K, V> wrappedInputFormat;
  int desiredNumSplits = 0;
  Configuration conf;
  SplitSizeEstimator estimator;
  SplitLocationProvider locationProvider;
  
  public TezGroupedSplitsInputFormat() {
    
  }
  
  public void setInputFormat(InputFormat<K, V> wrappedInputFormat) {
    this.wrappedInputFormat = wrappedInputFormat;
    if (LOG.isDebugEnabled()) {
      LOG.debug("wrappedInputFormat: " + wrappedInputFormat.getClass().getName());
    }
  }
  
  public void setDesiredNumberOfSplits(int num) {
    Preconditions.checkArgument(num >= 0);
    this.desiredNumSplits = num;
    if (LOG.isDebugEnabled()) {
      LOG.debug("desiredNumSplits: " + desiredNumSplits);
    }
  }

  public void setSplitSizeEstimator(SplitSizeEstimator estimator) {
    Preconditions.checkArgument(estimator != null);
    this.estimator = estimator;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Split size estimator : " + estimator);
    }
  }

  public void setSplitLocationProvider(SplitLocationProvider locationProvider) {
    Preconditions.checkArgument(locationProvider != null);
    this.locationProvider = locationProvider;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Split location provider : " + locationProvider);
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    List<InputSplit> originalSplits = wrappedInputFormat.getSplits(context);
    TezMapReduceSplitsGrouper grouper = new TezMapReduceSplitsGrouper();
    String wrappedInputFormatName = wrappedInputFormat.getClass().getName();
    return grouper
        .getGroupedSplits(conf, originalSplits, desiredNumSplits, wrappedInputFormatName, estimator,
            locationProvider);
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    TezGroupedSplit groupedSplit = (TezGroupedSplit) split;
    try {
      initInputFormatFromSplit(groupedSplit);
    } catch (TezException e) {
      throw new IOException(e);
    }
    return new TezGroupedSplitsRecordReader(groupedSplit, context);
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  void initInputFormatFromSplit(TezGroupedSplit split) throws TezException {
    if (wrappedInputFormat == null) {
      Class<? extends InputFormat> clazz = (Class<? extends InputFormat>) 
          getClassFromName(split.wrappedInputFormatName);
      try {
        wrappedInputFormat = org.apache.hadoop.util.ReflectionUtils.newInstance(clazz, conf);
      } catch (Exception e) {
        throw new TezException(e);
      }
    }
  }
  
  static Class<?> getClassFromName(String name) throws TezException {
    return ReflectionUtils.getClazz(name);
  }
  
  public class TezGroupedSplitsRecordReader  extends RecordReader<K, V> {

    TezGroupedSplit groupedSplit;
    TaskAttemptContext context;
    int idx = 0;
    long progress;
    RecordReader<K, V> curReader;
    
    public TezGroupedSplitsRecordReader(TezGroupedSplit split,
        TaskAttemptContext context) throws IOException {
      this.groupedSplit = split;
      this.context = context;
    }
    
    public void initialize(InputSplit split,
        TaskAttemptContext context) throws IOException, InterruptedException {
      if (this.groupedSplit != split) {
        throw new TezUncheckedException("Splits dont match");
      }
      if (this.context != context) {
        throw new TezUncheckedException("Contexts dont match");
      }
      initNextRecordReader();
    }
    
    public boolean nextKeyValue() throws IOException, InterruptedException {
      while ((curReader == null) || !curReader.nextKeyValue()) {
        // false return finishes. true return loops back for nextKeyValue()
        if (!initNextRecordReader()) {
          return false;
        }
      }
      return true;
    }

    public K getCurrentKey() throws IOException, InterruptedException {
      return curReader.getCurrentKey();
    }
    
    public V getCurrentValue() throws IOException, InterruptedException {
      return curReader.getCurrentValue();
    }
    
    public void close() throws IOException {
      if (curReader != null) {
        curReader.close();
        curReader = null;
      }
    }
    
    protected boolean initNextRecordReader() throws IOException {
      if (curReader != null) {
        curReader.close();
        curReader = null;
        if (idx > 0) {
          try {
            progress += groupedSplit.wrappedSplits.get(idx-1).getLength();
          } catch (InterruptedException e) {
            throw new TezUncheckedException(e);
          }
        }
      }

      // if all chunks have been processed, nothing more to do.
      if (idx == groupedSplit.wrappedSplits.size()) {
        return false;
      }

      // get a record reader for the idx-th chunk
      try {
        curReader = wrappedInputFormat.createRecordReader(
            groupedSplit.wrappedSplits.get(idx), context);

        curReader.initialize(groupedSplit.wrappedSplits.get(idx), context);
      } catch (Exception e) {
        throw new RuntimeException (e);
      }
      idx++;
      return true;
    }
    
    /**
     * return progress based on the amount of data processed so far.
     */
    public float getProgress() throws IOException, InterruptedException {
      long subprogress = 0;    // bytes processed in current split
      if (null != curReader) {
        // idx is always one past the current subsplit's true index.
        subprogress = (long) (curReader.getProgress() * groupedSplit.wrappedSplits
            .get(idx - 1).getLength());
      }
      return Math.min(1.0f,  (progress + subprogress)/(float)(groupedSplit.getLength()));
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

}
