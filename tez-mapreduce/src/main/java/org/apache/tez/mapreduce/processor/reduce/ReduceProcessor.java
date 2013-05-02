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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezTaskStatus;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.engine.api.Input;
import org.apache.tez.engine.api.Master;
import org.apache.tez.engine.api.Output;
import org.apache.tez.engine.api.Processor;
import org.apache.tez.engine.common.ConfigUtils;
import org.apache.tez.engine.common.sort.SortingOutput;
import org.apache.tez.engine.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.engine.lib.input.ShuffledMergedInput;
import org.apache.tez.mapreduce.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.tez.mapreduce.input.SimpleInput;
import org.apache.tez.mapreduce.output.SimpleOutput;
import org.apache.tez.mapreduce.processor.MRTask;
import org.apache.tez.mapreduce.processor.MRTaskReporter;

import com.google.common.base.Preconditions;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class ReduceProcessor
extends MRTask
implements Processor {

  private static final Log LOG = LogFactory.getLog(ReduceProcessor.class);
  
  private Progress sortPhase;
  private Progress reducePhase;

  private Counter reduceInputKeyCounter;
  private Counter reduceInputValueCounter;
  private int numMapTasks;

  public ReduceProcessor(TezEngineTaskContext context) {
    super(context);
    TezEngineTaskContext tezEngineContext = (TezEngineTaskContext) context;
    Preconditions.checkNotNull(tezEngineContext.getInputSpecList(),
        "InputSpecList should not be null");
    Preconditions.checkArgument(
        tezEngineContext.getInputSpecList().size() == 1,
        "Expected exactly one input, found : "
            + tezEngineContext.getInputSpecList().size());
    this.numMapTasks = tezEngineContext.getInputSpecList().get(0)
        .getNumInputs();
  }
  
  @Override
  public void initialize(Configuration conf, Master master) throws IOException,
      InterruptedException {
    super.initialize(conf, master);
  }

  @Override
  public void process(Input[] ins, Output[] outs)
      throws IOException, InterruptedException {
    MRTaskReporter reporter = new MRTaskReporter(getTaskReporter());
    boolean useNewApi = jobConf.getUseNewMapper();
    initTask(jobConf, taskAttemptId.getTaskID().getVertexID().getDAGId(),
        reporter, useNewApi);

    if (ins.length != 1
        || outs.length != 1) {
      throw new IOException("Cannot handle multiple inputs or outputs"
          + ", inputCount=" + ins.length
          + ", outputCount=" + outs.length);
    }
    Input in = ins[0];
    Output out = outs[0];

    if (in instanceof SimpleInput) {
      ((SimpleInput)in).setTask(this);
    } else if (in instanceof ShuffledMergedInput) {
      ((ShuffledMergedInput)in).setTask(this);
    }
    
    if (out instanceof SimpleOutput) {
      ((SimpleOutput)out).setTask(this);
    } else if (out instanceof SortingOutput) {
      ((SortingOutput)out).setTask(this);
    }

    in.initialize(jobConf, getTaskReporter());
    out.initialize(jobConf, getTaskReporter());

    sortPhase  = getProgress().addPhase("sort");
    reducePhase = getProgress().addPhase("reduce");
    sortPhase.complete();                         // sort is complete
    setPhase(TezTaskStatus.Phase.REDUCE); 

    this.statusUpdate();
    
    Class keyClass = ConfigUtils.getIntermediateInputKeyClass(jobConf);
    Class valueClass = ConfigUtils.getIntermediateInputValueClass(jobConf);
    LOG.info("Using keyClass: " + keyClass);
    LOG.info("Using valueClass: " + valueClass);
    RawComparator comparator = 
        ConfigUtils.getInputKeySecondaryGroupingComparator(jobConf);
    LOG.info("Using comparator: " + comparator);

    reduceInputKeyCounter = 
        reporter.getCounter(TaskCounter.REDUCE_INPUT_GROUPS);
    reduceInputValueCounter = 
        reporter.getCounter(TaskCounter.REDUCE_INPUT_RECORDS);
        
    // Sanity check
    if (!(in instanceof ShuffledMergedInput)) {
      throw new IOException("Illegal input to reduce: " + in.getClass());
    }
    ShuffledMergedInput shuffleInput = (ShuffledMergedInput)in;

    if (useNewApi) {
      try {
        runNewReducer(
            jobConf, 
            (TezTaskUmbilicalProtocol)getUmbilical(), reporter, 
            shuffleInput, comparator,  keyClass, valueClass, 
            out);
      } catch (ClassNotFoundException cnfe) {
        throw new IOException(cnfe);
      }
    } else {
      runOldReducer(
          jobConf, (TezTaskUmbilicalProtocol)getUmbilical(), reporter, 
          shuffleInput, comparator, keyClass, valueClass, out);
    }
    
    done(out.getOutputContext(), reporter);
  }

  public void close() throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    
  }

  void runOldReducer(JobConf job,
      TezTaskUmbilicalProtocol umbilical,
      final MRTaskReporter reporter,
      ShuffledMergedInput input,
      RawComparator comparator,
      Class keyClass,
      Class valueClass,
      final Output output) throws IOException, InterruptedException {
    
    Reducer reducer = 
        ReflectionUtils.newInstance(job.getReducerClass(), job);
    
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

    // apply reduce function
    try {
      ReduceValuesIterator values = 
          new ReduceValuesIterator(
              input, 
              job.getOutputValueGroupingComparator(), keyClass, valueClass, 
              job, reporter, reduceInputValueCounter, reducePhase);
      
      values.informReduceProgress();
      while (values.more()) {
        reduceInputKeyCounter.increment(1);
        reducer.reduce(values.getKey(), values, collector, reporter);
        values.nextKey();
        values.informReduceProgress();
      }

      //Clean up: repeated in catch block below
      reducer.close();
      output.close();
      //End of clean up.
    } catch (IOException ioe) {
      try {
        reducer.close();
      } catch (IOException ignored) {}

      try {
        output.close();
      } catch (IOException ignored) {}

      throw ioe;
    }
  }
  
  private static class ReduceValuesIterator<KEY,VALUE> 
  extends org.apache.tez.engine.common.task.impl.ValuesIterator<KEY,VALUE> {
    private Counter reduceInputValueCounter;
    private Progress reducePhase;

    public ReduceValuesIterator (ShuffledMergedInput in,
        RawComparator<KEY> comparator, 
        Class<KEY> keyClass,
        Class<VALUE> valClass,
        Configuration conf, Progressable reporter,
        Counter reduceInputValueCounter,
        Progress reducePhase)
            throws IOException {
      super(in.getIterator(), comparator, keyClass, valClass, conf, reporter);
      this.reduceInputValueCounter = reduceInputValueCounter;
      this.reducePhase = reducePhase;
    }

    @Override
    public VALUE next() {
      reduceInputValueCounter.increment(1);
      return moveToNext();
    }

    protected VALUE moveToNext() {
      return super.next();
    }

    public void informReduceProgress() {
      reducePhase.set(super.in.getProgress().getProgress()); // update progress
      reporter.progress();
    }
  }

  void runNewReducer(JobConf job,
      final TezTaskUmbilicalProtocol umbilical,
      final MRTaskReporter reporter,
      ShuffledMergedInput input,
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
        reporter.setProgress(rawIter.getProgress().getProgress());
        return ret;
      }
    };
    
    // make a task context so we can get the classes
    org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
        new TaskAttemptContextImpl(job, taskAttemptId, reporter);
    
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
            out.close();
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
    jobConf.setInt(TezJobConfig.TEZ_ENGINE_TASK_INDEGREE, numMapTasks); 
  }

  @Override
  public TezCounter getOutputRecordsCounter() {
    return reporter.getCounter(TaskCounter.REDUCE_OUTPUT_RECORDS);
  }

  @Override
  public TezCounter getInputRecordsCounter() {
    return reporter.getCounter(TaskCounter.REDUCE_INPUT_GROUPS);
  }
}
