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

package org.apache.tez.mapreduce.output;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Output;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.IOInterruptedException;
import org.apache.tez.runtime.library.api.KeyValueWriterWithBasePath;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.tez.mapreduce.hadoop.mapred.MRReporter;

/**
 * {@link MultiMROutput} is an {@link Output} which allows key/values pairs
 * to be written by a processor to different output files.
 *
 * It is compatible with all standard Apache Hadoop MapReduce
 * OutputFormat implementations.
 *
 */
@Public
public class MultiMROutput extends MROutput {

  Map<String, org.apache.hadoop.mapreduce.RecordWriter<?, ?>>
      newRecordWriters;

  Map<String, org.apache.hadoop.mapred.RecordWriter<?, ?>>
      oldRecordWriters;

  public MultiMROutput(OutputContext outputContext, int numPhysicalOutputs) {
    super(outputContext, numPhysicalOutputs);
  }

  @Override
  public List<Event> initialize() throws IOException, InterruptedException {
    List<Event> events = super.initializeBase();
    if (useNewApi) {
      newRecordWriters = new HashMap<>();
    } else {
      oldRecordWriters = new HashMap<>();
    }
    return events;
  }

  /**
   * Create an
   * {@link org.apache.tez.mapreduce.output.MROutput.MROutputConfigBuilder}
   *
   * @param conf         Configuration for the {@link MROutput}
   * @param outputFormat FileInputFormat derived class
   * @param outputPath   Output path
   * @return {@link org.apache.tez.mapreduce.output.MROutput.MROutputConfigBuilder}
   */
  public static MROutputConfigBuilder createConfigBuilder(Configuration conf,
      Class<?> outputFormat, String outputPath, boolean useLazyOutputFormat) {
    return MROutput.createConfigBuilder(conf, outputFormat, outputPath, useLazyOutputFormat)
        .setOutputClassName(MultiMROutput.class.getName());
  }

  @Override
  public KeyValueWriterWithBasePath getWriter() throws IOException {
    return new KeyValueWriterWithBasePath() {

      @SuppressWarnings("unchecked")
      @Override
      public void write(Object key, Object value) throws IOException {
        throw new UnsupportedOperationException(
            "Write without basePath isn't supported.");
      }

      @SuppressWarnings("unchecked")
      @Override
      public void write(Object key, Object value, String basePath)
          throws IOException {
        if (basePath == null) {
          throw new UnsupportedOperationException(
              "Write without basePath isn't supported.");
        }
        if (basePath.length() > 0 && basePath.charAt(0) == '/' ) {
          // The base path can't be absolute path starting with "/".
          // Otherwise, it will cause the task temporary files being
          // written outside the output committer's task work path.
          throw new UnsupportedOperationException(
              "Write with absolute basePath isn't supported.");
        }
        if (useNewApi) {
          try {
            getNewRecordWriter(newApiTaskAttemptContext, basePath).write(
                key, value);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOInterruptedException(
                "Interrupted while writing next key-value",e);
          }
        } else {
          getOldRecordWriter(basePath).write(key, value);
        }
        outputRecordCounter.increment(1);
        getContext().notifyProgress();
      }
    };
  }

  /**
   * Call this in the processor before finishing to ensure outputs that
   * outputs have been flushed. Must be called before commit.
   * @throws IOException
   */
  @Override
  public void flush() throws IOException {
    if (flushed.getAndSet(true)) {
      return;
    }
    try {
      if (useNewApi) {
          for (RecordWriter writer : newRecordWriters.values()) {
            writer.close(newApiTaskAttemptContext);
          }
      } else {
        for (org.apache.hadoop.mapred.RecordWriter writer :
            oldRecordWriters.values()) {
          writer.close(null);
        }
      }
    } catch (InterruptedException e) {
        throw new IOException("Interrupted while closing record writer", e);
    }
  }

  @SuppressWarnings("unchecked")
  private synchronized RecordWriter getNewRecordWriter(
      TaskAttemptContext taskContext, String baseFileName)
      throws IOException, InterruptedException {

    // look for record-writer in the cache
    RecordWriter writer = newRecordWriters.get(baseFileName);

    // If not in cache, create a new one
    if (writer == null) {
      // get the record writer from context output format
      taskContext.getConfiguration().set(
          MRJobConfig.FILEOUTPUTFORMAT_BASE_OUTPUT_NAME, baseFileName);
      try {
        writer = ((OutputFormat) ReflectionUtils.newInstance(
            taskContext.getOutputFormatClass(), taskContext.getConfiguration()))
            .getRecordWriter(taskContext);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
      // add the record-writer to the cache
      newRecordWriters.put(baseFileName, writer);
    }
    return writer;
  }

  @SuppressWarnings("unchecked")
  private synchronized org.apache.hadoop.mapred.RecordWriter
      getOldRecordWriter(String baseFileName) throws IOException {

    // look for record-writer in the cache
    org.apache.hadoop.mapred.RecordWriter writer =
        oldRecordWriters.get(baseFileName);

    // If not in cache, create a new one
    if (writer == null) {
        FileSystem fs = FileSystem.get(jobConf);
        String finalName = getOutputName(baseFileName);
        writer = oldOutputFormat.getRecordWriter(fs, jobConf,
            finalName, new MRReporter(getContext().getCounters()));
      // add the record-writer to the cache
      oldRecordWriters.put(baseFileName, writer);
    }
    return writer;
  }
};
