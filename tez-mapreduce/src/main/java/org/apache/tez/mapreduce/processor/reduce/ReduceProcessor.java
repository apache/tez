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
package org.apache.tez.mapreduce.processor.reduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.engine.api.Event;
import org.apache.tez.engine.api.KVReader;
import org.apache.tez.engine.api.KVWriter;
import org.apache.tez.engine.api.LogicalIOProcessor;
import org.apache.tez.engine.api.LogicalInput;
import org.apache.tez.engine.api.LogicalOutput;
import org.apache.tez.engine.api.TezProcessorContext;
import org.apache.tez.engine.common.ConfigUtils;
import org.apache.tez.engine.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.engine.lib.output.OnFileSortedOutput;
import org.apache.tez.mapreduce.input.ShuffledMergedInputLegacy;
import org.apache.tez.mapreduce.output.SimpleOutput;
import org.apache.tez.mapreduce.processor.MRTask;
import org.apache.tez.mapreduce.processor.MRTaskReporter;


@SuppressWarnings({ "unchecked", "rawtypes" })
public class ReduceProcessor
extends MRTask
implements LogicalIOProcessor {

  private static final Log LOG = LogFactory.getLog(ReduceProcessor.class);

  private Counter reduceInputKeyCounter;
  private Counter reduceInputValueCounter;

  public ReduceProcessor() {
    super(false);
  }

  @Override
  public void initialize(TezProcessorContext processorContext)
      throws IOException {
    try {
      super.initialize(processorContext);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }


  @Override
  public void handleEvents(List<Event> processorEvents) {
    // TODO Auto-generated method stub

  }

  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void run(Map<String, LogicalInput> inputs,
      Map<String, LogicalOutput> outputs) throws Exception {

    LOG.info("Running reduce: " + processorContext.getUniqueIdentifier());

    initTask();

    if (outputs.size() <= 0 || outputs.size() > 1) {
      throw new IOException("Invalid number of outputs"
          + ", outputCount=" + outputs.size());
    }

    if (inputs.size() <= 0 || inputs.size() > 1) {
      throw new IOException("Invalid number of inputs"
          + ", inputCount=" + inputs.size());
    }

    LogicalInput in = inputs.values().iterator().next();
    LogicalOutput out = outputs.values().iterator().next();

    this.statusUpdate();

    Class keyClass = ConfigUtils.getIntermediateInputKeyClass(jobConf);
    Class valueClass = ConfigUtils.getIntermediateInputValueClass(jobConf);
    LOG.info("Using keyClass: " + keyClass);
    LOG.info("Using valueClass: " + valueClass);
    RawComparator comparator =
        ConfigUtils.getInputKeySecondaryGroupingComparator(jobConf);
    LOG.info("Using comparator: " + comparator);

    reduceInputKeyCounter =
        mrReporter.getCounter(TaskCounter.REDUCE_INPUT_GROUPS);
    reduceInputValueCounter =
        mrReporter.getCounter(TaskCounter.REDUCE_INPUT_RECORDS);

    // Sanity check
    if (!(in instanceof ShuffledMergedInputLegacy)) {
      throw new IOException("Illegal input to reduce: " + in.getClass());
    }
    ShuffledMergedInputLegacy shuffleInput = (ShuffledMergedInputLegacy)in;
    KVReader kvReader = shuffleInput.getReader();

    KVWriter kvWriter = null;
    if((out instanceof SimpleOutput)) {
      kvWriter = ((SimpleOutput) out).getWriter();
    } else if ((out instanceof OnFileSortedOutput)) {
      kvWriter = ((OnFileSortedOutput) out).getWriter();
    } else {
      throw new IOException("Illegal input to reduce: " + in.getClass());
    }

    if (useNewApi) {
      try {
        runNewReducer(
            jobConf,
            mrReporter,
            shuffleInput, comparator,  keyClass, valueClass,
            kvWriter);
      } catch (ClassNotFoundException cnfe) {
        throw new IOException(cnfe);
      }
    } else {
      runOldReducer(
          jobConf, mrReporter,
          kvReader, comparator, keyClass, valueClass, kvWriter);
    }

    done(out);
  }

  void runOldReducer(JobConf job,
      final MRTaskReporter reporter,
      KVReader input,
      RawComparator comparator,
      Class keyClass,
      Class valueClass,
      final KVWriter output) throws IOException, InterruptedException {

    Reducer reducer =
        ReflectionUtils.newInstance(job.getReducerClass(), job);

    // make output collector

    OutputCollector collector =
        new OutputCollector() {
      public void collect(Object key, Object value)
          throws IOException {
        output.write(key, value);
      }
    };

    // apply reduce function
    try {
      ReduceValuesIterator values =
          new ReduceValuesIterator(
              input, reporter, reduceInputValueCounter);

      values.informReduceProgress();
      while (values.more()) {
        reduceInputKeyCounter.increment(1);
        reducer.reduce(values.getKey(), values, collector, reporter);
        values.informReduceProgress();
      }

      //Clean up: repeated in catch block below
      reducer.close();
      //End of clean up.
    } catch (IOException ioe) {
      try {
        reducer.close();
      } catch (IOException ignored) {
      }

      throw ioe;
    }
  }

  private static class ReduceValuesIterator<KEY,VALUE>
  implements Iterator<VALUE> {
    private Counter reduceInputValueCounter;
    private KVReader in;
    private Progressable reporter;
    private Object currentKey;
    private Iterator<Object> currentValues;

    public ReduceValuesIterator (KVReader in,
        Progressable reporter,
        Counter reduceInputValueCounter)
            throws IOException {
      this.reduceInputValueCounter = reduceInputValueCounter;
      this.in = in;
      this.reporter = reporter;
    }

    public boolean more() throws IOException {
      boolean more = in.next();
      if(more) {
        currentKey = in.getCurrentKV().getKey();
        currentValues = in.getCurrentKV().getValues().iterator();
      } else {
        currentKey = null;
        currentValues = null;
      }
      return more;
    }

    public KEY getKey() throws IOException {
      return (KEY) currentKey;
    }

    public void informReduceProgress() {
      reporter.progress();
    }

    @Override
    public boolean hasNext() {
      return currentValues.hasNext();
    }

    @Override
    public VALUE next() {
      reduceInputValueCounter.increment(1);
      return (VALUE) currentValues.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  void runNewReducer(JobConf job,
      final MRTaskReporter reporter,
      ShuffledMergedInputLegacy input,
      RawComparator comparator,
      Class keyClass,
      Class valueClass,
      final KVWriter out
      ) throws IOException,InterruptedException,
      ClassNotFoundException {

    // make a task context so we can get the classes
    org.apache.hadoop.mapreduce.TaskAttemptContext taskContext = getTaskAttemptContext();

    // make a reducer
    org.apache.hadoop.mapreduce.Reducer reducer =
        (org.apache.hadoop.mapreduce.Reducer)
        ReflectionUtils.newInstance(taskContext.getReducerClass(), job);

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
        reporter.setProgress(rawIter.getProgress().getProgress());
        return ret;
      }
    };

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
      }
    };

    org.apache.hadoop.mapreduce.Reducer.Context reducerContext =
        createReduceContext(
            reducer, job, taskAttemptId,
            rIter, reduceInputKeyCounter,
            reduceInputValueCounter,
            trackedRW,
            committer,
            reporter, comparator, keyClass,
            valueClass);



    reducer.run(reducerContext);
    trackedRW.close(reducerContext);
  }

  @Override
  public void localizeConfiguration(JobConf jobConf)
      throws IOException, InterruptedException {
    super.localizeConfiguration(jobConf);
    jobConf.setBoolean(JobContext.TASK_ISMAP, false);
  }

  @Override
  public TezCounter getOutputRecordsCounter() {
    return processorContext.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS);
  }

  @Override
  public TezCounter getInputRecordsCounter() {
    return processorContext.getCounters().findCounter(TaskCounter.REDUCE_INPUT_GROUPS);
  }

}
