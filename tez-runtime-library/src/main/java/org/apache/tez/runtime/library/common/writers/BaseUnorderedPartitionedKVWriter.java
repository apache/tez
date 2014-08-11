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

package org.apache.tez.runtime.library.common.writers;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;

@SuppressWarnings("rawtypes")
public abstract class BaseUnorderedPartitionedKVWriter extends KeyValuesWriter {

  private static final Log LOG = LogFactory.getLog(BaseUnorderedPartitionedKVWriter.class);
  
  protected final OutputContext outputContext;
  protected final Configuration conf;
  protected final Partitioner partitioner;
  protected final Class keyClass;
  protected final Class valClass;
  protected final Serializer keySerializer;
  protected final Serializer valSerializer;
  protected final SerializationFactory serializationFactory;
  protected final int numPartitions;
  protected final CompressionCodec codec;
  protected final TezTaskOutput outputFileHandler;
  
  protected final boolean ifileReadAhead;
  protected final int ifileReadAheadLength;
  protected final int ifileBufferSize;

  /**
   * Represents the serialized size of the output records. Does not consider
   * overheads from the buffer meta-information, storage format, or compression
   * if it is enabled.
   */
  protected final TezCounter outputRecordBytesCounter;
  /**
   * Represents final number of records written (spills are not counted)
   */
  protected final TezCounter outputRecordsCounter;
  /**
   * Represents the size of the final output - with any overheads introduced by
   * meta-information.
   */
  protected final TezCounter outputBytesWithOverheadCounter;
  
  /**
   * Represents the final output size, with file format overheads and compression factored in.
   * Does not consider spills.
   */
  protected final TezCounter fileOutputBytesCounter;
  /**
   * Represents the additional records written to disk due to spills. Does not
   * count the final write to disk.
   */
  protected final TezCounter spilledRecordsCounter;
  /**
   * Represents additional bytes written to disk as a result of spills, excluding the final spill.
   */
  protected final TezCounter additionalSpillBytesWritternCounter;
  /**
   * Represents additional bytes read from disk to merge all the previous spills into a single file.
   */
  protected final TezCounter additionalSpillBytesReadCounter;
  /**
   * Represents the number of additional spills. The final spill is not counted towards this.
   */
  protected final TezCounter numAdditionalSpillsCounter;

  @SuppressWarnings("unchecked")
  public BaseUnorderedPartitionedKVWriter(OutputContext outputContext, Configuration conf, int numOutputs) {
    this.outputContext = outputContext;
    this.conf = conf;
    this.numPartitions = numOutputs;
    
    // k/v serialization
    keyClass = ConfigUtils.getIntermediateOutputKeyClass(this.conf);
    valClass = ConfigUtils.getIntermediateOutputValueClass(this.conf);
    serializationFactory = new SerializationFactory(this.conf);
    keySerializer = serializationFactory.getSerializer(keyClass);
    valSerializer = serializationFactory.getSerializer(valClass);
    
    outputRecordBytesCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES);
    outputRecordsCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_RECORDS);
    outputBytesWithOverheadCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_WITH_OVERHEAD);
    fileOutputBytesCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_PHYSICAL);
    spilledRecordsCounter = outputContext.getCounters().findCounter(TaskCounter.SPILLED_RECORDS);
    additionalSpillBytesWritternCounter = outputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_WRITTEN);
    additionalSpillBytesReadCounter = outputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_READ);
    numAdditionalSpillsCounter = outputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILL_COUNT);
    
    // compression
    if (ConfigUtils.shouldCompressIntermediateOutput(this.conf)) {
      Class<? extends CompressionCodec> codecClass =
          ConfigUtils.getIntermediateOutputCompressorClass(this.conf, DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, this.conf);
    } else {
      codec = null;
    }
    
    this.ifileReadAhead = this.conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT);
    if (this.ifileReadAhead) {
      this.ifileReadAheadLength = conf.getInt(
          TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
          TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT);
    } else {
      this.ifileReadAheadLength = 0;
    }
    this.ifileBufferSize = conf.getInt("io.file.buffer.size",
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_BUFFER_SIZE_DEFAULT);
    
    LOG.info("Instantiating Partitioner: [" + conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS) + "]");
    try {
      this.partitioner = TezRuntimeUtils.instantiatePartitioner(this.conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    outputFileHandler = TezRuntimeUtils.instantiateTaskOutputManager(conf, outputContext);
  }

  @Override
  public abstract void write(Object key, Object value) throws IOException;

  @Override
  public void write(Object key, Iterable<Object> values) throws IOException {
    //TODO: UnorderedPartitionedKVWriter should override this method later.
    Iterator<Object> it = values.iterator();
    while(it.hasNext()) {
      write(key, it.next());
    }
  }

  public abstract List<Event> close() throws IOException, InterruptedException;

}
