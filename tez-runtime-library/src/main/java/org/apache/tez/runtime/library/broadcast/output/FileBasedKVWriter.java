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

package org.apache.tez.runtime.library.broadcast.output;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;

import com.google.common.base.Preconditions;

@Private
public class FileBasedKVWriter extends KeyValuesWriter {

  private static final Logger LOG = LoggerFactory.getLogger(FileBasedKVWriter.class);
  
  public static final int INDEX_RECORD_LENGTH = 24;

  private final Configuration conf;
  private int numRecords = 0;

  @SuppressWarnings("rawtypes")
  private final Class keyClass;
  @SuppressWarnings("rawtypes")
  private final Class valClass;
  private final CompressionCodec codec;
  private final FileSystem rfs;
  private final IFile.Writer writer;

  private final Path outputPath;
  private Path indexPath;
  
  private final TezTaskOutput ouputFileManager;
  private boolean closed = false;

  // Number of output key-value pairs
  private final TezCounter outputRecordsCounter;
  // Number of bytes of actual output - uncompressed.
  private final TezCounter outputBytesCounter;
  // Size of the data with additional meta-data
  private final TezCounter outputBytesCounterWithOverhead;
  // Actual physical size of the data on disk.
  private final TezCounter outputMaterializedBytesCounter;
  
  
  // TODO NEWTEZ Define Counters
  // Number of records
  // Time waiting for a write to complete, if that's possible.
  // Size of key-value pairs written.

  public FileBasedKVWriter(OutputContext outputContext, Configuration conf) throws IOException {
    this.conf = conf;

    this.outputRecordsCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_RECORDS);
    this.outputBytesCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES);
    this.outputBytesCounterWithOverhead = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_WITH_OVERHEAD);
    this.outputMaterializedBytesCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_PHYSICAL);

    this.rfs = ((LocalFileSystem) FileSystem.getLocal(this.conf)).getRaw();

    // Setup serialization
    keyClass = ConfigUtils.getIntermediateOutputKeyClass(this.conf);
    valClass = ConfigUtils.getIntermediateOutputValueClass(this.conf);

    // Setup compression
    if (ConfigUtils.shouldCompressIntermediateOutput(this.conf)) {
      Class<? extends CompressionCodec> codecClass = ConfigUtils
          .getIntermediateOutputCompressorClass(this.conf, DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, this.conf);
    } else {
      codec = null;
    }

    this.ouputFileManager = TezRuntimeUtils.instantiateTaskOutputManager(conf,
        outputContext);
    LOG.info("Created KVWriter -> " + "compressionCodec: " + (codec == null ? "NoCompressionCodec"
        : codec.getClass().getName()));

    this.outputPath = ouputFileManager.getOutputFileForWrite();
    LOG.info("Writing data file: " + outputPath);

    // TODO NEWTEZ Consider making the buffer size configurable. Also consider
    // setting up an in-memory buffer which is occasionally flushed to disk so
    // that the output does not block.

    // TODO NEWTEZ maybe use appropriate counter
    this.writer = new IFile.Writer(conf, rfs, outputPath, keyClass, valClass,
        codec, null, outputBytesCounter);
  }

  /**
   * @return true if any output was generated. false otherwise
   * @throws IOException
   */
  public boolean close() throws IOException {
    this.closed = true;
    this.writer.close();
    long rawLen = writer.getRawLength();
    long compLen = writer.getCompressedLength();
    outputBytesCounterWithOverhead.increment(rawLen);
    outputMaterializedBytesCounter.increment(compLen);
    TezIndexRecord rec = new TezIndexRecord(0, rawLen, compLen);
    TezSpillRecord sr = new TezSpillRecord(1);
    sr.putIndex(rec, 0);

    this.indexPath = ouputFileManager
        .getOutputIndexFileForWrite(INDEX_RECORD_LENGTH);
    LOG.info("Writing index file: " + indexPath);
    sr.writeToFile(indexPath, conf);
    return numRecords > 0;
  }

  @Override
  public void write(Object key, Object value) throws IOException {
    this.writer.append(key, value);
    this.outputRecordsCounter.increment(1);
    numRecords++;
  }

  @Override
  public void write(Object key, Iterable<Object> values) throws IOException {
    writer.appendKeyValues(key, values.iterator());
    this.outputRecordsCounter.increment(1);
    numRecords++;
  }

  public long getRawLength() {
    Preconditions.checkState(closed, "Only available after the Writer has been closed");
    return this.writer.getRawLength();
  }
  
  public long getCompressedLength() {
    Preconditions.checkState(closed, "Only available after the Writer has been closed");
    return this.writer.getCompressedLength();
  }

  public byte[] getData() throws IOException {
    Preconditions.checkState(closed,
        "Only available after the Writer has been closed");
    FSDataInputStream inStream = null;
    byte[] buf = null;
    try {
      inStream = rfs.open(outputPath);
      buf = new byte[(int) getCompressedLength()];
      IOUtils.readFully(inStream, buf, 0, (int) getCompressedLength());
    } finally {
      if (inStream != null) {
        inStream.close();
      }
    }
    return buf;
  }

}
