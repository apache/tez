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

package org.apache.tez.engine.common.localshuffle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.RunningTaskContext;
import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezTaskReporter;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.engine.common.ConfigUtils;
import org.apache.tez.engine.common.sort.impl.TezMerger;
import org.apache.tez.engine.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.engine.common.task.local.output.TezLocalTaskOutputFiles;
import org.apache.tez.engine.common.task.local.output.TezTaskOutput;

@SuppressWarnings({"rawtypes"})
public class LocalShuffle {

  private final TezEngineTaskContext taskContext;
  private final RunningTaskContext runningTaskContext;
  private final Configuration conf;
  private final int tasksInDegree;

  private final Class keyClass;
  private final Class valClass;
  private final RawComparator comparator;

  private final FileSystem rfs;
  private final int sortFactor;
  
  private final TezCounter spilledRecordsCounter;
  private final CompressionCodec codec;
  private final TezTaskOutput mapOutputFile;

  public LocalShuffle(TezEngineTaskContext taskContext, 
      RunningTaskContext runningTaskContext, 
      Configuration conf,
      TezTaskReporter reporter
      ) throws IOException {
    this.taskContext = taskContext;
    this.runningTaskContext = runningTaskContext;
    this.conf = conf;
    this.keyClass = ConfigUtils.getIntermediateInputKeyClass(conf);
    this.valClass = ConfigUtils.getIntermediateInputValueClass(conf);
    this.comparator = ConfigUtils.getIntermediateInputKeyComparator(conf);

    this.sortFactor =
        conf.getInt(
            TezJobConfig.TEZ_ENGINE_IO_SORT_FACTOR, 
            TezJobConfig.DEFAULT_TEZ_ENGINE_IO_SORT_FACTOR);
    
    this.rfs = FileSystem.getLocal(conf).getRaw();

    this.spilledRecordsCounter = 
        reporter.getCounter(TaskCounter.SPILLED_RECORDS);
    
    // compression
    if (ConfigUtils.isIntermediateInputCompressed(conf)) {
      Class<? extends CompressionCodec> codecClass =
          ConfigUtils.getIntermediateInputCompressorClass(conf, DefaultCodec.class);
      this.codec = ReflectionUtils.newInstance(codecClass, conf);
    } else {
      this.codec = null;
    }

    this.tasksInDegree = taskContext.getInputSpecList().get(0).getNumInputs();

    // Always local
    this.mapOutputFile = new TezLocalTaskOutputFiles();
    this.mapOutputFile.setConf(conf);

  }
  
  public TezRawKeyValueIterator run() throws IOException {
    // Copy is complete, obviously! 
    this.runningTaskContext.getProgress().addPhase("copy").complete();

    // Merge
    return TezMerger.merge(conf, rfs, 
        keyClass, valClass,
        codec, 
        getMapFiles(),
        false, 
        sortFactor,
        new Path(taskContext.getTaskAttemptId().toString()), 
        comparator,
        runningTaskContext.getTaskReporter(), spilledRecordsCounter, null, null);
  }
  
  private Path[] getMapFiles() 
  throws IOException {
    List<Path> fileList = new ArrayList<Path>();
      // for local jobs
      for(int i = 0; i < tasksInDegree; ++i) {
        fileList.add(mapOutputFile.getInputFile(i));
      }
      
    return fileList.toArray(new Path[0]);
  }
}
