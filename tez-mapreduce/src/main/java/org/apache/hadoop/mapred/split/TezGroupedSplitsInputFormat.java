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

package org.apache.hadoop.mapred.split;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.tez.mapreduce.grouper.TezSplitGrouper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.TezException;

import org.apache.tez.common.Preconditions;

/**
 * An InputFormat that provides a generic grouping around the splits
 * of a real InputFormat
 */
@Public
@Evolving
public class TezGroupedSplitsInputFormat<K, V> 
  implements InputFormat<K, V>, Configurable{
  
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

  public void setSplitSizeEstimator(SplitSizeEstimator estimator) {
    this.estimator = Objects.requireNonNull(estimator);
    LOG.debug("Split size estimator : {}", estimator);
  }

  public void setSplitLocationProvider(SplitLocationProvider locationProvider) {
    this.locationProvider = Objects.requireNonNull(locationProvider);
    LOG.debug("Split size location provider: {}", locationProvider);
  }
  
  public void setDesiredNumberOfSplits(int num) {
    Preconditions.checkArgument(num >= 0);
    this.desiredNumSplits = num;
    LOG.debug("desiredNumSplits: {}", desiredNumSplits);
  }
  
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    InputSplit[] originalSplits = wrappedInputFormat.getSplits(job, numSplits);
    TezMapredSplitsGrouper grouper = new TezMapredSplitsGrouper();
    String wrappedInputFormatName = wrappedInputFormat.getClass().getName();
    return grouper
        .getGroupedSplits(conf, originalSplits, desiredNumSplits, wrappedInputFormatName, estimator,
            locationProvider);
  }
  
  @Override
  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    TezGroupedSplit groupedSplit = (TezGroupedSplit) split;
    try {
      initInputFormatFromSplit(groupedSplit);
    } catch (TezException e) {
      throw new IOException(e);
    }
    return new TezGroupedSplitsRecordReader(groupedSplit, job, reporter);
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
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

  public class TezGroupedSplitsRecordReader implements RecordReader<K, V> {

    TezGroupedSplit groupedSplit;
    JobConf job;
    Reporter reporter;
    int idx = 0;
    long progress;
    RecordReader<K, V> curReader;
    final AtomicInteger initIndex;
    final int numReaders;
    final ExecutorService initReaderExecService;
    final Queue<Future<RecordReader<K,V>>> initedReaders;

    public TezGroupedSplitsRecordReader(TezGroupedSplit split, JobConf job,
        Reporter reporter) throws IOException {
      this.groupedSplit = split;
      this.job = job;
      this.reporter = reporter;
      this.initIndex = new AtomicInteger(0);
      int numThreads = conf.getInt(TezSplitGrouper.TEZ_GROUPING_SPLIT_INIT_THREADS,
          TezSplitGrouper.TEZ_GROUPING_SPLIT_INIT_THREADS_DEFAULT);
      this.numReaders = conf.getInt(TezSplitGrouper.TEZ_GROUPING_SPLIT_INIT_NUM_RECORDREADERS,
          TezSplitGrouper.TEZ_GROUPING_SPLIT_INIT_NUM_RECORDREADERS_DEFAULT);
      this.initReaderExecService = Executors.newFixedThreadPool(numThreads,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setPriority(Thread.MAX_PRIORITY)
              .setNameFormat("TEZ-Split-Init-Thread-%d")
              .build());
      this.initedReaders = new LinkedList<>();
      preInitReaders();
      initNextRecordReader();
    }

    private void preInitReaders() {
      if (initReaderExecService == null) {
        LOG.info("Init record reader threadpool is not initialized");
        return;
      }
      for (int i = 0; i < numReaders; i++) {
        initedReaders.offer(this.initReaderExecService.submit(() -> {
          try {
            int index = initIndex.getAndIncrement();
            if (index >= groupedSplit.wrappedSplits.size()) {
              return null;
            }
            InputSplit s = groupedSplit.wrappedSplits.get(index);
            RecordReader<K, V> reader = wrappedInputFormat.getRecordReader(s, job, reporter);
            LOG.debug("Init Thread processed reader number {} initialization", index);
            return reader;
          } catch (Exception e) {
            if (e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
            cancelsFutures();
            throw new RuntimeException(e);
          }
        }));
      }
    }
    
    @Override
    public boolean next(K key, V value) throws IOException {

      while ((curReader == null) || !curReader.next(key, value)) {
        if (!initNextRecordReader()) {
          return false;
        }
      }
      return true;
    }

    @Override
    public K createKey() {
      return curReader.createKey();
    }
    
    @Override
    public V createValue() {
      return curReader.createValue();
    }
    
    @Override
    public float getProgress() throws IOException {
      return Math.min(1.0f,  getPos()/(float)(groupedSplit.getLength()));
    }
    
    @Override
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
          progress += groupedSplit.wrappedSplits.get(idx-1).getLength();
        }
      }

      // if all chunks have been processed, nothing more to do.
      if (idx == groupedSplit.wrappedSplits.size()) {
        LOG.info("Shutting down the init record reader threadpool");
        initReaderExecService.shutdownNow();
        return false;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Init record reader for index " + idx + " of " + 
                  groupedSplit.wrappedSplits.size());
      }

      // get a record reader for the idx-th chunk
      try {
        curReader = initedReaders.poll().get();
        preInitReaders();
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        cancelsFutures();
        throw new RuntimeException(e);
      }
      idx++;
      return true;
    }

    private void cancelsFutures() {
      for (Future<RecordReader<K,V>> f : initedReaders) {
        f.cancel(true);
      }
    }

    @Override
    public long getPos() throws IOException {
      long subprogress = 0;    // bytes processed in current split
      if (null != curReader) {
        // idx is always one past the current subsplit's true index.
        subprogress = curReader.getPos();
      }
      return (progress + subprogress);
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
