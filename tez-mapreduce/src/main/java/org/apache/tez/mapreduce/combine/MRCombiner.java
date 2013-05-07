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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.engine.api.Input;
import org.apache.tez.engine.api.Master;
import org.apache.tez.engine.api.Output;
import org.apache.tez.engine.api.Processor;
import org.apache.tez.engine.common.ConfigUtils;
import org.apache.tez.engine.common.combine.CombineInput;
import org.apache.tez.engine.common.combine.CombineOutput;
import org.apache.tez.engine.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.engine.records.TezTaskAttemptID;
import org.apache.tez.mapreduce.hadoop.IDConverter;
import org.apache.tez.mapreduce.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.tez.mapreduce.processor.MRTask;
import org.apache.tez.mapreduce.processor.MRTaskReporter;

public class MRCombiner implements Processor {

  private static Log LOG = LogFactory.getLog(MRCombiner.class);

  JobConf jobConf;
  boolean useNewApi;

  private final MRTask task;

  private Counter combinerInputKeyCounter;
  private Counter combinerInputValueCounter;
  private Progress combinePhase;

  public MRCombiner(MRTask task) {
    this.task = task;
  }

  @Override
  public void initialize(Configuration conf, Master master) throws IOException,
      InterruptedException {
    if (conf instanceof JobConf) {
      jobConf = (JobConf)conf;
    } else {
      jobConf = new JobConf(conf);
    }
    useNewApi = jobConf.getUseNewMapper();
  }

  @Override
  public void process(Input[] in, Output[] out) throws IOException,
      InterruptedException {
    LOG.info("DEBUG: Running MRCombiner"
        + ", usingNewAPI=" + useNewApi);

    CombineInput input = (CombineInput)in[0];
    CombineOutput output = (CombineOutput)out[0];

    combinePhase  = task.getProgress().addPhase("combine");

    Class<?> keyClass = ConfigUtils.getIntermediateOutputKeyClass(jobConf);
    Class<?> valueClass = ConfigUtils.getIntermediateOutputValueClass(jobConf);
    LOG.info("Using combineKeyClass: " + keyClass);
    LOG.info("Using combineValueClass: " + valueClass);
    RawComparator<?> comparator =
        ConfigUtils.getIntermediateOutputKeyComparator(jobConf);
    LOG.info("Using combineComparator: " + comparator);

    combinerInputKeyCounter =
        task.getMRReporter().getCounter(TaskCounter.COMBINE_INPUT_RECORDS);
    combinerInputValueCounter =
        task.getMRReporter().getCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);

    if (useNewApi) {
      try {
        runNewCombiner(this.jobConf,
            task.getUmbilical(),
            task.getMRReporter(),
            input, comparator, keyClass, valueClass, output);
      } catch (ClassNotFoundException cnfe) {
        throw new IOException(cnfe);
      }
    } else {
      runOldCombiner(this.jobConf,
          task.getUmbilical(),
          task.getMRReporter(),
          input,
          comparator, keyClass, valueClass,
          output);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void runOldCombiner(JobConf job,
        TezTaskUmbilicalProtocol umbilical,
        final MRTaskReporter reporter,
        CombineInput input,
        RawComparator comparator,
        Class keyClass,
        Class valueClass,
        final Output output) throws IOException, InterruptedException {

    Reducer combiner =
        ReflectionUtils.newInstance(job.getCombinerClass(), job);

    // make output collector

    OutputCollector collector =
        new OutputCollector() {
      public void collect(Object key, Object value)
          throws IOException {
        try {
          output.write(key, value);
        } catch (InterruptedException ie) {
          throw new IOException(ie);
        }
      }
    };

    // apply combiner function
    CombinerValuesIterator values =
        new CombinerValuesIterator(input,
            comparator, keyClass, valueClass, job, reporter,
            combinerInputValueCounter, combinePhase);

    values.informReduceProgress();
    while (values.more()) {
      combinerInputKeyCounter.increment(1);
      combiner.reduce(values.getKey(), values, collector, reporter);
      values.nextKey();
      values.informReduceProgress();
    }
  }

  private static final class CombinerValuesIterator<KEY,VALUE>
  extends org.apache.tez.engine.common.task.impl.ValuesIterator<KEY,VALUE> {
    private Counter combineInputValueCounter;
    private Progress combinePhase;

    public CombinerValuesIterator (CombineInput in,
        RawComparator<KEY> comparator,
        Class<KEY> keyClass,
        Class<VALUE> valClass,
        Configuration conf, Progressable reporter,
        Counter combineInputValueCounter,
        Progress combinePhase)
            throws IOException {
      super(in.getIterator(), comparator, keyClass, valClass, conf, reporter);
      this.combineInputValueCounter = combineInputValueCounter;
      this.combinePhase = combinePhase;
    }

    @Override
    public VALUE next() {
      combineInputValueCounter.increment(1);
      return moveToNext();
    }

    protected VALUE moveToNext() {
      return super.next();
    }

    public void informReduceProgress() {
      combinePhase.set(super.in.getProgress().getProgress()); // update progress
      reporter.progress();
    }
  }


  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void runNewCombiner(JobConf job,
      final TezTaskUmbilicalProtocol umbilical,
      final MRTaskReporter reporter,
      CombineInput input,
      RawComparator comparator,
      Class keyClass,
      Class valueClass,
      final Output out
      ) throws IOException,InterruptedException,
      ClassNotFoundException {
    // wrap value iterator to report progress.
    final TezRawKeyValueIterator rawIter = input.getIterator();
    TezRawKeyValueIterator rIter = new TezRawKeyValueIterator() {
      public void close() throws IOException {
        rawIter.close();
      }
      public DataInputBuffer getKey() throws IOException {
        return rawIter.getKey();
      }
      public Progress getProgress() {
        return rawIter.getProgress();
      }
      public DataInputBuffer getValue() throws IOException {
        return rawIter.getValue();
      }
      public boolean next() throws IOException {
        boolean ret = rawIter.next();
        // FIXME progress updates for combiner
        // reporter.setProgress(rawIter.getProgress().getProgress());
        return ret;
      }
    };

    // make a task context so we can get the classes
    org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
        new TaskAttemptContextImpl(job, task.getTaskAttemptId(), reporter);

    // make a reducer
    org.apache.hadoop.mapreduce.Reducer reducer =
        (org.apache.hadoop.mapreduce.Reducer)
        ReflectionUtils.newInstance(taskContext.getReducerClass(), job);

    org.apache.hadoop.mapreduce.RecordWriter trackedRW =
        new org.apache.hadoop.mapreduce.RecordWriter() {

          @Override
          public void write(Object key, Object value) throws IOException,
              InterruptedException {
            out.write(key, value);
          }

          @Override
          public void close(TaskAttemptContext context) throws IOException,
              InterruptedException {
            // Should not close this here as the sorter will close the
            // combine output
          }
        };

    org.apache.hadoop.mapreduce.Reducer.Context reducerContext =
        createReduceContext(
            reducer, job, task.getTaskAttemptId(),
            rIter, combinerInputKeyCounter,
            combinerInputValueCounter,
            trackedRW,
            null,
            reporter, comparator, keyClass,
            valueClass);
    reducer.run(reducerContext);
    trackedRW.close(reducerContext);
  }

  @Override
  public void close() throws IOException, InterruptedException {
  }

  protected static <INKEY,INVALUE,OUTKEY,OUTVALUE>
  org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>.Context
  createReduceContext(org.apache.hadoop.mapreduce.Reducer
                        <INKEY,INVALUE,OUTKEY,OUTVALUE> reducer,
                      Configuration job,
                      TezTaskAttemptID taskId,
                      final TezRawKeyValueIterator rIter,
                      org.apache.hadoop.mapreduce.Counter inputKeyCounter,
                      org.apache.hadoop.mapreduce.Counter inputValueCounter,
                      org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE> output,
                      org.apache.hadoop.mapreduce.OutputCommitter committer,
                      org.apache.hadoop.mapreduce.StatusReporter reporter,
                      RawComparator<INKEY> comparator,
                      Class<INKEY> keyClass, Class<INVALUE> valueClass
  ) throws IOException, InterruptedException {
    RawKeyValueIterator r =
        new RawKeyValueIterator() {

          @Override
          public boolean next() throws IOException {
            return rIter.next();
          }

          @Override
          public DataInputBuffer getValue() throws IOException {
            return rIter.getValue();
          }

          @Override
          public Progress getProgress() {
            return rIter.getProgress();
          }

          @Override
          public DataInputBuffer getKey() throws IOException {
            return rIter.getKey();
          }

          @Override
          public void close() throws IOException {
            rIter.close();
          }
        };
    org.apache.hadoop.mapreduce.ReduceContext<INKEY, INVALUE, OUTKEY, OUTVALUE>
    reduceContext =
      new ReduceContextImpl<INKEY, INVALUE, OUTKEY, OUTVALUE>(
          job,
          IDConverter.toMRTaskAttemptId(taskId),
          r,
          inputKeyCounter,
          inputValueCounter,
          output,
          committer,
          reporter,
          comparator,
          keyClass,
          valueClass);
    LOG.info("DEBUG: Using combineKeyClass: "
          + keyClass + ", combineValueClass: " + valueClass);

    org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>.Context
        reducerContext = new
          WrappedReducer<INKEY, INVALUE, OUTKEY, OUTVALUE>().getReducerContext(
              reduceContext);

    return reducerContext;
  }

}
