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

package org.apache.tez.mapreduce.processor;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.Collections;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.tez.common.InputSpec;
import org.apache.tez.common.OutputSpec;
import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.engine.api.Input;
import org.apache.tez.engine.api.Output;
import org.apache.tez.engine.api.Processor;
import org.apache.tez.engine.api.Task;
import org.apache.tez.engine.lib.output.LocalOnFileSorterOutput;
import org.apache.tez.engine.runtime.RuntimeUtils;
import org.apache.tez.engine.task.RuntimeTask;
import org.apache.tez.mapreduce.TezTestUtils;
import org.apache.tez.mapreduce.hadoop.TezTaskUmbilicalProtocol;
import org.apache.tez.mapreduce.input.SimpleInput;
import org.apache.tez.mapreduce.processor.map.MapProcessor;

public class MapUtils {

  private static final Log LOG = LogFactory.getLog(MapUtils.class);
  
  private static InputSplit 
  createInputSplit(FileSystem fs, Path workDir, JobConf job, Path file) 
      throws IOException {
    FileInputFormat.setInputPaths(job, workDir);
  
    
    // create a file with length entries
    SequenceFile.Writer writer = 
        SequenceFile.createWriter(fs, job, file, 
            LongWritable.class, Text.class);
    try {
      Random r = new Random(System.currentTimeMillis());
      LongWritable key = new LongWritable();
      Text value = new Text();
      for (int i = 10; i > 0; i--) {
        key.set(r.nextInt(1000));
        value.set(Integer.toString(i));
        writer.append(key, value);
        LOG.info("<k, v> : <" + key.get() + ", " + value + ">");
      }
    } finally {
      writer.close();
    }
    
    SequenceFileInputFormat<LongWritable, Text> format = 
        new SequenceFileInputFormat<LongWritable, Text>();
    InputSplit[] splits = format.getSplits(job, 1);
    System.err.println("#split = " + splits.length + " ; " +
        "#locs = " + splits[0].getLocations().length + "; " +
        "loc = " + splits[0].getLocations()[0] + "; " + 
        "off = " + splits[0].getLength() + "; " +
        "file = " + ((FileSplit)splits[0]).getPath());
    return splits[0];
  }

  public static Task runMapProcessor(FileSystem fs, Path workDir,
      JobConf jobConf,
      int mapId, Path mapInput,
      TezTaskUmbilicalProtocol umbilical,
      Class<?> outputClazz) throws Exception {
    jobConf.setInputFormat(SequenceFileInputFormat.class);
    InputSplit split = createInputSplit(fs, workDir, jobConf, mapInput);
    TezEngineTaskContext taskContext = 
        new TezEngineTaskContext(
        TezTestUtils.getMockTaskAttemptId(0, 0, mapId, 0), "tez",
        "tez", "TODO_vertexName", MapProcessor.class.getName(),
        Collections.singletonList(new InputSpec("srcVertex", 0,
            SimpleInput.class.getName())),
        Collections.singletonList(new OutputSpec("targetVertex", 0,
            outputClazz.getName())));

    Task t = RuntimeUtils.createRuntimeTask(taskContext);
    t.initialize(jobConf, umbilical);
    SimpleInput[] real = ((SimpleInput[])t.getInputs());
    SimpleInput[] inputs = spy(real);
    doReturn(split).when(inputs[0]).getOldSplitDetails(any(TaskSplitIndex.class));
    t.getProcessor().process(inputs, t.getOutputs());
    return t;
  }

}
