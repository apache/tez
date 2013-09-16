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
package org.apache.tez.mapreduce.newprocessor.map;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.engine.lib.output.OnFileSortedOutput;
import org.apache.tez.engine.newapi.Event;
import org.apache.tez.engine.newapi.KVReader;
import org.apache.tez.engine.newapi.KVWriter;
import org.apache.tez.engine.newapi.LogicalIOProcessor;
import org.apache.tez.engine.newapi.LogicalInput;
import org.apache.tez.engine.newapi.LogicalOutput;
import org.apache.tez.engine.newapi.Output;
import org.apache.tez.engine.newapi.TezProcessorContext;
import org.apache.tez.mapreduce.hadoop.newmapreduce.MapContextImpl;
import org.apache.tez.mapreduce.newinput.SimpleInput;
import org.apache.tez.mapreduce.newinput.SimpleInputLegacy;
import org.apache.tez.mapreduce.newoutput.SimpleOutput;
import org.apache.tez.mapreduce.newprocessor.MRTask;
import org.apache.tez.mapreduce.newprocessor.MRTaskReporter;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class MapProcessor extends MRTask implements LogicalIOProcessor {

  private static final Log LOG = LogFactory.getLog(MapProcessor.class);

  public MapProcessor(){
    super(true);
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

    LOG.info("Running map: " + processorContext.getUniqueIdentifier());
    
    initTask();

    if (inputs.size() != 1
        || outputs.size() != 1) {
      throw new IOException("Cannot handle multiple inputs or outputs"
          + ", inputCount=" + inputs.size()
          + ", outputCount=" + outputs.size());
    }
    LogicalInput in = inputs.values().iterator().next();
    Output out = outputs.values().iterator().next();
    
    // Sanity check
    if (!(in instanceof SimpleInputLegacy)) {
      throw new IOException(new TezException(
          "Only Simple Input supported. Input: " + in.getClass()));
    }
    SimpleInputLegacy input = (SimpleInputLegacy)in;
    
    boolean doingShuffle = true;
    KVWriter kvWriter = null;
    if (!(out instanceof OnFileSortedOutput)) {
      doingShuffle = false;
      kvWriter = ((SimpleOutput)out).getWriter();
    } else {
      kvWriter = ((OnFileSortedOutput)out).getWriter();
    }
    
    if (out instanceof SimpleOutput) {
      initCommitter(jobConf, useNewApi, false);
    } else { // Assuming no other output needs commit.
      initCommitter(jobConf, useNewApi, true);
    }

    if (useNewApi) {
      runNewMapper(jobConf, mrReporter, input, kvWriter, doingShuffle);
    } else {
      runOldMapper(jobConf, mrReporter, input, kvWriter, doingShuffle);
    }

    done();    
  }

  void runOldMapper(
      final JobConf job,
      final MRTaskReporter reporter,
      final SimpleInputLegacy input,
      final KVWriter output,
      final boolean doingShuffle
      ) throws IOException, InterruptedException {
    
    // Initialize input in-line since it sets parameters which may be used by the processor.
    // Done only for SimpleInput.
    // TODO use new method in SimpleInput to get required info
    //input.initialize(job, master);
    
    RecordReader in = new OldRecordReader(input);
        
    OutputCollector collector = new OldOutputCollector(output);

    MapRunnable runner =
        (MapRunnable)ReflectionUtils.newInstance(job.getMapRunnerClass(), job);

    runner.run(in, collector, (Reporter)reporter);
    // start the sort phase only if there are reducers
    this.statusUpdate();
  }

  private void runNewMapper(final JobConf job,
      MRTaskReporter reporter,
      final SimpleInputLegacy in,
      KVWriter out,
      final boolean doingShuffle
      ) throws IOException, InterruptedException {

    // Initialize input in-line since it sets parameters which may be used by the processor.
    // Done only for SimpleInput.
    // TODO use new method in SimpleInput to get required info
    //in.initialize(job, master);
    
    // make a task context so we can get the classes
    org.apache.hadoop.mapreduce.TaskAttemptContext taskContext = 
        getTaskAttemptContext();

    // make a mapper
    org.apache.hadoop.mapreduce.Mapper mapper;
    try {
      mapper = (org.apache.hadoop.mapreduce.Mapper)
          ReflectionUtils.newInstance(taskContext.getMapperClass(), job);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    }

    org.apache.hadoop.mapreduce.RecordReader input =
        new NewRecordReader(in);

    org.apache.hadoop.mapreduce.RecordWriter output = 
        new NewOutputCollector(out);

    org.apache.hadoop.mapreduce.InputSplit split = in.getNewInputSplit();
    
    org.apache.hadoop.mapreduce.MapContext 
    mapContext = 
    new MapContextImpl(
        job, taskAttemptId, 
        input, output, 
        getCommitter(), 
        processorContext, split);

    org.apache.hadoop.mapreduce.Mapper.Context mapperContext = 
        new WrappedMapper().getMapContext(mapContext);

    input.initialize(split, mapperContext);
    mapper.run(mapperContext);
    this.statusUpdate();
    input.close();
    output.close(mapperContext);
  }

  private static class NewRecordReader extends
      org.apache.hadoop.mapreduce.RecordReader {
    private final SimpleInput in;
    private KVReader reader;

    private NewRecordReader(SimpleInput in) throws IOException {
      this.in = in;
      this.reader = in.getReader();
    }

    @Override
    public void initialize(org.apache.hadoop.mapreduce.InputSplit split,
        TaskAttemptContext context) throws IOException,
        InterruptedException {
      //in.initializeNewRecordReader(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException,
        InterruptedException {
      return reader.next();
    }

    @Override
    public Object getCurrentKey() throws IOException,
        InterruptedException {
      return reader.getCurrentKV().getKey();
    }

    @Override
    public Object getCurrentValue() throws IOException,
        InterruptedException {
      return reader.getCurrentKV().getValues().iterator().next();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return in.getProgress();
    }

    @Override
    public void close() throws IOException {
    }
  }

  private static class OldRecordReader implements RecordReader {
    private final SimpleInputLegacy simpleInput;

    private OldRecordReader(SimpleInputLegacy simpleInput) {
      this.simpleInput = simpleInput;
    }

    @Override
    public boolean next(Object key, Object value) throws IOException {
      // TODO broken
//      simpleInput.setKey(key);
//      simpleInput.setValue(value);
//      try {
//        return simpleInput.hasNext();
//      } catch (InterruptedException ie) {
//        throw new IOException(ie);
//      }
      return simpleInput.getOldRecordReader().next(key, value);
    }

    @Override
    public Object createKey() {
      return simpleInput.getOldRecordReader().createKey();
    }

    @Override
    public Object createValue() {
      return simpleInput.getOldRecordReader().createValue();
    }

    @Override
    public long getPos() throws IOException {
      return simpleInput.getOldRecordReader().getPos();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
      try {
        return simpleInput.getProgress();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
  }

  private static class OldOutputCollector 
  implements OutputCollector {
    private final KVWriter output;
    
    OldOutputCollector(KVWriter output) {
      this.output = output;
    }

    public void collect(Object key, Object value) throws IOException {
        output.write(key, value);
    }
  }

  private class NewOutputCollector
    extends org.apache.hadoop.mapreduce.RecordWriter {
    private final KVWriter out;

    NewOutputCollector(KVWriter out) throws IOException {
      this.out = out;
    }

    @Override
    public void write(Object key, Object value) throws IOException, InterruptedException {
      out.write(key, value);
    }

    @Override
    public void close(TaskAttemptContext context
                      ) throws IOException, InterruptedException {
    }
  }

  @Override
  public void localizeConfiguration(JobConf jobConf) 
      throws IOException, InterruptedException {
    super.localizeConfiguration(jobConf);
    jobConf.setBoolean(JobContext.TASK_ISMAP, true);
  }
  
  @Override
  public TezCounter getOutputRecordsCounter() {
    return processorContext.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS);
  }

  @Override
  public TezCounter getInputRecordsCounter() {
    return processorContext.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS);
  }

}
