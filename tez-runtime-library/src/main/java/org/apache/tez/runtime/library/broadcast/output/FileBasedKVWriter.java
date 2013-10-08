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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.runtime.api.TezOutputContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;

public class FileBasedKVWriter implements KeyValueWriter {

  private static final Log LOG = LogFactory.getLog(FileBasedKVWriter.class);
  
  public static final int INDEX_RECORD_LENGTH = 24;

  private final Configuration conf;
  private int numRecords = 0;

  @SuppressWarnings("rawtypes")
  private Class keyClass;
  @SuppressWarnings("rawtypes")
  private Class valClass;
  private CompressionCodec codec;
  private FileSystem rfs;
  private IFile.Writer writer;

  private TezTaskOutput ouputFileManager;

  // TODO NEWTEZ Define Counters
  // Number of records
  // Time waiting for a write to complete, if that's possible.
  // Size of key-value pairs written.

  public FileBasedKVWriter(TezOutputContext outputContext) throws IOException {
    this.conf = TezUtils.createConfFromUserPayload(outputContext
        .getUserPayload());
    this.conf.setStrings(TezJobConfig.LOCAL_DIRS,
        outputContext.getWorkDirs());

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

    initWriter();
  }

  /**
   * @return true if any output was generated. false otherwise
   * @throws IOException
   */
  public boolean close() throws IOException {
    this.writer.close();
    TezIndexRecord rec = new TezIndexRecord(0, writer.getRawLength(),
        writer.getCompressedLength());
    TezSpillRecord sr = new TezSpillRecord(1);
    sr.putIndex(rec, 0);

    Path indexFile = ouputFileManager
        .getOutputIndexFileForWrite(INDEX_RECORD_LENGTH);
    LOG.info("Writing index file: " + indexFile);
    sr.writeToFile(indexFile, conf);
    return numRecords > 0;
  }

  @Override
  public void write(Object key, Object value) throws IOException {
    this.writer.append(key, value);
    numRecords++;
  }

  public void initWriter() throws IOException {
    Path outputFile = ouputFileManager.getOutputFileForWrite();
    LOG.info("Writing data file: " + outputFile);

    // TODO NEWTEZ Consider making the buffer size configurable. Also consider
    // setting up an in-memory buffer which is occasionally flushed to disk so
    // that the output does not block.

    // TODO NEWTEZ maybe use appropriate counter
    this.writer = new IFile.Writer(conf, rfs, outputFile, keyClass, valClass,
        codec, null);
  }

}
