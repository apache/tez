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

package org.apache.tez.mapreduce.combine;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.mapreduce.hadoop.MRConfig;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.mapred.MRCounters;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.ValuesIterator;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;

/**
 * Implements a Map Reduce compatible combiner
 */
@Public
@SuppressWarnings({"rawtypes", "unchecked"})
public class MRCombiner implements Combiner {

  private static Log LOG = LogFactory.getLog(MRCombiner.class);
  
  private final Configuration conf;
  private final Class<?> keyClass;
  private final Class<?> valClass;
  private final RawComparator<?> comparator;
  private final boolean useNewApi;
  
  private final TezCounter combineInputKeyCounter;
  private final TezCounter combineInputValueCounter;
  
  private final MRTaskReporter reporter;
  private final TaskAttemptID mrTaskAttemptID;

  public MRCombiner(TaskContext taskContext) throws IOException {
    this.conf = TezUtils.createConfFromUserPayload(taskContext.getUserPayload());

    assert(taskContext instanceof InputContext || taskContext instanceof OutputContext);
    if (taskContext instanceof OutputContext) {
      this.keyClass = ConfigUtils.getIntermediateOutputKeyClass(conf);
      this.valClass = ConfigUtils.getIntermediateOutputValueClass(conf);
      this.comparator = ConfigUtils.getIntermediateOutputKeyComparator(conf);
      this.reporter = new MRTaskReporter((OutputContext)taskContext);
    } else {
      this.keyClass = ConfigUtils.getIntermediateInputKeyClass(conf);
      this.valClass = ConfigUtils.getIntermediateInputValueClass(conf);
      this.comparator = ConfigUtils.getIntermediateInputKeyComparator(conf);
      this.reporter = new MRTaskReporter((InputContext)taskContext);
    }

    this.useNewApi = ConfigUtils.useNewApi(conf);
    
    combineInputKeyCounter = taskContext.getCounters().findCounter(TaskCounter.COMBINE_INPUT_RECORDS);
    combineInputValueCounter = taskContext.getCounters().findCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);
    
    boolean isMap = conf.getBoolean(MRConfig.IS_MAP_PROCESSOR,false);
    this.mrTaskAttemptID = new TaskAttemptID(
        new TaskID(String.valueOf(taskContext.getApplicationId()
            .getClusterTimestamp()), taskContext.getApplicationId().getId(),
            isMap ? TaskType.MAP : TaskType.REDUCE,
            taskContext.getTaskIndex()), taskContext.getTaskAttemptNumber());
    
    LOG.info("Using combineKeyClass: " + keyClass + ", combineValueClass: " + valClass + ", combineComparator: " +comparator + ", useNewApi: " + useNewApi);
  }

  @Override
  public void combine(TezRawKeyValueIterator rawIter, Writer writer)
      throws InterruptedException, IOException {
    if (useNewApi) {
      runNewCombiner(rawIter, writer);
    } else {
      runOldCombiner(rawIter, writer);
    }
    
  }

  ///////////////// Methods for old API //////////////////////
  
  private void runOldCombiner(final TezRawKeyValueIterator rawIter, final Writer writer) throws IOException {
    Class<? extends Reducer> reducerClazz = (Class<? extends Reducer>) conf.getClass("mapred.combiner.class", null, Reducer.class);
    
    Reducer combiner = ReflectionUtils.newInstance(reducerClazz, conf);
    
    OutputCollector collector = new OutputCollector() {
      @Override
      public void collect(Object key, Object value) throws IOException {
        writer.append(key, value);
      }
    };
    
    CombinerValuesIterator values = new CombinerValuesIterator(rawIter, keyClass, valClass, comparator);
    
    while (values.moveToNext()) {
      combiner.reduce(values.getKey(), values.getValues().iterator(), collector, reporter);
    }
  }
  
  private final class CombinerValuesIterator<KEY,VALUE> extends ValuesIterator<KEY, VALUE> {
    public CombinerValuesIterator(TezRawKeyValueIterator rawIter,
        Class<KEY> keyClass, Class<VALUE> valClass,
        RawComparator<KEY> comparator) throws IOException {
      super(rawIter, comparator, keyClass, valClass, conf,
          combineInputKeyCounter, combineInputValueCounter);
    }
  }
  
  ///////////////// End of methods for old API //////////////////////
  
  ///////////////// Methods for new API //////////////////////
  
  private void runNewCombiner(final TezRawKeyValueIterator rawIter, final Writer writer) throws InterruptedException, IOException {
    
    RecordWriter recordWriter = new RecordWriter() {

      @Override
      public void write(Object key, Object value) throws IOException,
          InterruptedException {
        writer.append(key, value);
      }

      @Override
      public void close(TaskAttemptContext context) throws IOException,
          InterruptedException {
        // Will be closed by whoever invokes the combiner.
      }
    };
    
    Class<? extends org.apache.hadoop.mapreduce.Reducer> reducerClazz = (Class<? extends org.apache.hadoop.mapreduce.Reducer>) conf
        .getClass(MRJobConfig.COMBINE_CLASS_ATTR, null,
            org.apache.hadoop.mapreduce.Reducer.class);
    org.apache.hadoop.mapreduce.Reducer reducer = ReflectionUtils.newInstance(reducerClazz, conf);
    
    org.apache.hadoop.mapreduce.Reducer.Context reducerContext =
        createReduceContext(
            conf,
            mrTaskAttemptID,
            rawIter,
            new MRCounters.MRCounter(combineInputKeyCounter),
            new MRCounters.MRCounter(combineInputValueCounter),
            recordWriter,
            reporter,
            (RawComparator)comparator,
            keyClass,
            valClass);
    
    reducer.run(reducerContext);
    recordWriter.close(reducerContext);
  }

  private static <KEYIN, VALUEIN, KEYOUT, VALUEOUT> org.apache.hadoop.mapreduce.Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context createReduceContext(
      Configuration conf,
      TaskAttemptID mrTaskAttemptID,
      final TezRawKeyValueIterator rawIter,
      Counter combineInputKeyCounter,
      Counter combineInputValueCounter,
      RecordWriter<KEYOUT, VALUEOUT> recordWriter,
      MRTaskReporter reporter,
      RawComparator<KEYIN> comparator,
      Class<KEYIN> keyClass,
      Class<VALUEIN> valClass) throws InterruptedException, IOException {

    RawKeyValueIterator r = new RawKeyValueIterator() {

      @Override
      public boolean next() throws IOException {
        return rawIter.next();
      }

      @Override
      public DataInputBuffer getValue() throws IOException {
        return rawIter.getValue();
      }

      @Override
      public Progress getProgress() {
        return rawIter.getProgress();
      }

      @Override
      public DataInputBuffer getKey() throws IOException {
        return rawIter.getKey();
      }

      @Override
      public void close() throws IOException {
        rawIter.close();
      }
    };

    ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> rContext = new ReduceContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(
        conf, mrTaskAttemptID, r, combineInputKeyCounter,
        combineInputValueCounter, recordWriter, null, reporter, comparator,
        keyClass, valClass);

    org.apache.hadoop.mapreduce.Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context reducerContext = new WrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>()
        .getReducerContext(rContext);
    return reducerContext;
  }

  
 
}
