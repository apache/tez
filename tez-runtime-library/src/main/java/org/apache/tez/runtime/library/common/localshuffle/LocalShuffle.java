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

package org.apache.tez.runtime.library.common.localshuffle;

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
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.runtime.library.common.task.local.output.TezLocalTaskOutputFiles;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;

@SuppressWarnings({"rawtypes"})
public class LocalShuffle {

  // TODO NEWTEZ This is broken.

  private final TezInputContext inputContext;
  private final Configuration conf;
  private final int numInputs;

  private final Class keyClass;
  private final Class valClass;
  private final RawComparator comparator;

  private final FileSystem rfs;
  private final int sortFactor;
  
  private final TezCounter spilledRecordsCounter;
  private final CompressionCodec codec;
  private final TezTaskOutput mapOutputFile;
  
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;
  private final int ifileBufferSize;

  public LocalShuffle(TezInputContext inputContext, Configuration conf, int numInputs) throws IOException {
    this.inputContext = inputContext;
    this.conf = conf;
    this.numInputs = numInputs;
    
    this.keyClass = ConfigUtils.getIntermediateInputKeyClass(conf);
    this.valClass = ConfigUtils.getIntermediateInputValueClass(conf);
    this.comparator = ConfigUtils.getIntermediateInputKeyComparator(conf);
    
    this.sortFactor =
        conf.getInt(
            TezJobConfig.TEZ_RUNTIME_IO_SORT_FACTOR, 
            TezJobConfig.DEFAULT_TEZ_RUNTIME_IO_SORT_FACTOR);
    
    this.rfs = FileSystem.getLocal(conf).getRaw();

    this.spilledRecordsCounter = inputContext.getCounters().findCounter(TaskCounter.SPILLED_RECORDS);
    
 // compression
    if (ConfigUtils.isIntermediateInputCompressed(conf)) {
      Class<? extends CompressionCodec> codecClass =
          ConfigUtils.getIntermediateInputCompressorClass(conf, DefaultCodec.class);
      this.codec = ReflectionUtils.newInstance(codecClass, conf);
    } else {
      this.codec = null;
    }
    this.ifileReadAhead = conf.getBoolean(
        TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD,
        TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT);
    if (this.ifileReadAhead) {
      this.ifileReadAheadLength = conf.getInt(
          TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
          TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT);
    } else {
      this.ifileReadAheadLength = 0;
    }
    this.ifileBufferSize = conf.getInt("io.file.buffer.size",
        TezJobConfig.TEZ_RUNTIME_IFILE_BUFFER_SIZE_DEFAULT);
    
    // Always local
    this.mapOutputFile = new TezLocalTaskOutputFiles(conf, inputContext.getUniqueIdentifier());
  }
 
  
  public TezRawKeyValueIterator run() throws IOException {
    // Copy is complete, obviously! 

    
    // Merge
    return TezMerger.merge(conf, rfs, 
        keyClass, valClass,
        codec, 
        ifileReadAhead, ifileReadAheadLength, ifileBufferSize,
        getMapFiles(),
        false, 
        sortFactor,
        new Path(inputContext.getUniqueIdentifier()), // TODO NEWTEZ This is likely broken 
        comparator,
        null, spilledRecordsCounter, null, null, null);
  }
  
  private Path[] getMapFiles() 
  throws IOException {
    List<Path> fileList = new ArrayList<Path>();
      // for local jobs
      for(int i = 0; i < numInputs; ++i) {
        //fileList.add(mapOutputFile.getInputFile(i));
      }
      
    return fileList.toArray(new Path[0]);
  }
}
