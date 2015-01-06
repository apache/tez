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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
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
import org.apache.tez.dag.api.TezException;
import org.apache.tez.mapreduce.output.MROutputLegacy;
import org.apache.tez.mapreduce.processor.MRTask;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.runtime.library.input.OrderedGroupedInputLegacy;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;

@Private
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ReduceProcessor extends MRTask {

  private static final Log LOG = LogFactory.getLog(ReduceProcessor.class);

  private Counter reduceInputKeyCounter;
  private Counter reduceInputValueCounter;

  public ReduceProcessor(ProcessorContext processorContext) {
    super(processorContext, false);
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

    if (outputs.size() <= 0 || outputs.size() > 1) {
      throw new IOException("Invalid number of outputs"
          + ", outputCount=" + outputs.size());
    }

    if (inputs.size() <= 0 || inputs.size() > 1) {
      throw new IOException("Invalid number of inputs"
          + ", inputCount=" + inputs.size());
    }

    LogicalInput in = inputs.values().iterator().next();
    in.start();

    List<Input> pendingInputs = new LinkedList<Input>();
    pendingInputs.add(in);
    processorContext.waitForAllInputsReady(pendingInputs);
    LOG.info("Input is ready for consumption. Starting Output");

    LogicalOutput out = outputs.values().iterator().next();
    out.start();

    initTask(out);

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
    if (!(in instanceof OrderedGroupedInputLegacy)) {
      throw new IOException("Illegal input to reduce: " + in.getClass());
    }
    OrderedGroupedInputLegacy shuffleInput = (OrderedGroupedInputLegacy)in;
    KeyValuesReader kvReader = shuffleInput.getReader();

    KeyValueWriter kvWriter = null;
    if((out instanceof MROutputLegacy)) {
      kvWriter = ((MROutputLegacy) out).getWriter();
    } else if ((out instanceof OrderedPartitionedKVOutput)) {
      kvWriter = ((OrderedPartitionedKVOutput) out).getWriter();
    } else {
      throw new IOException("Illegal output to reduce: " + in.getClass());
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

    done();
  }

  void runOldReducer(JobConf job,
      final MRTaskReporter reporter,
      KeyValuesReader input,
      RawComparator comparator,
      Class keyClass,
      Class valueClass,
      final KeyValueWriter output) throws IOException, InterruptedException {

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

      // Set progress to 1.0f if there was no exception,
      reporter.setProgress(1.0f);
      
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
    private KeyValuesReader in;
    private Progressable reporter;
    private Object currentKey;
    private Iterator<Object> currentValues;

    public ReduceValuesIterator (KeyValuesReader in,
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
        currentKey = in.getCurrentKey();
        currentValues = in.getCurrentValues().iterator();
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
      OrderedGroupedInputLegacy input,
      RawComparator comparator,
      Class keyClass,
      Class valueClass,
      final KeyValueWriter out
      ) throws IOException, InterruptedException, ClassNotFoundException, TezException {

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

      @Override
      public boolean isSameKey() throws IOException {
        return rawIter.isSameKey();
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

    // Set progress to 1.0f if there was no exception,
    reporter.setProgress(1.0f);

    trackedRW.close(reducerContext);
  }

  @Override
  public void localizeConfiguration(JobConf jobConf)
      throws IOException, InterruptedException {
    super.localizeConfiguration(jobConf);
    jobConf.setBoolean(JobContext.TASK_ISMAP, false);
  }

}
