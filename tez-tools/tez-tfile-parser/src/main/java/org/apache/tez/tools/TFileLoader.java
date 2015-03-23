/*
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

package org.apache.tez.tools;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Pattern;

/**
 * Simple pig loader function (mainly loader) which reads TFile and emits every line as a tuple of
 * (machine, key, line).  This would be beneficial if huge amount of logs need to be mined.
 */
public class TFileLoader extends FileInputLoadFunc implements LoadMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(TFileLoader.class);

  private TFileRecordReader recReader = null;

  private BufferedReader bufReader;
  private Text currentKey;
  private final TupleFactory tupleFactory = TupleFactory.getInstance();

  private final Pattern PATTERN = Pattern.compile(":");

  /**
   * We get one complete TFile per KV read.
   * Add a BufferedReader so that we can scan a line at a time.
   *
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  //TODO: tasks can sometime throw OOM when single TFile is way too large. Adjust mem accordinly.
  private void setupReader() throws IOException, InterruptedException {
    if (recReader.nextKeyValue() && bufReader == null) {
      currentKey = recReader.getCurrentKey();
      Text val = recReader.getCurrentValue();
      bufReader = new BufferedReader(new StringReader(val.toString()));
    }
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      String line = readLine();
      if (line != null) {
        //machine, key, line
        Tuple tuple = tupleFactory.newTuple(3);
        if (currentKey != null) {
          String[] data = PATTERN.split(currentKey.toString());
          if (data == null || data.length != 2) {
            LOG.warn("unable to parse " + currentKey.toString());
            return null;
          }
          tuple.set(0, data[0]);
          tuple.set(1, data[1]);
        } else {
          tuple.set(0, "");
          tuple.set(1, "");
        }
        tuple.set(2, line); //line
        return tuple;
      }
    } catch (IOException e) {
      return null;
    } catch (InterruptedException e) {
      return null;
    }
    return null;
  }

  private String readLine() throws IOException, InterruptedException {
    String line = null;
    if (bufReader == null) {
      setupReader();
    }
    line = bufReader.readLine();
    if (line == null) { //end of stream. Move to the next reader
      bufReader = null;
      setupReader();
      if (bufReader != null) {
        line = bufReader.readLine();
      }
    }
    return line;
  }

  public static class TFileInputFormat extends
      PigFileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException,
        InterruptedException {
      return new TFileRecordReader();
    }

  }

  @Override
  public InputFormat getInputFormat() {
    return new TFileInputFormat();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this);
  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) {
    recReader = (TFileRecordReader) reader;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    FileInputFormat.setInputPaths(job, location);
  }

  @Override
  public String[] getPartitionKeys(String location, Job job)
      throws IOException {
    return null;
  }

  @Override
  public ResourceSchema getSchema(String location, Job job)
      throws IOException {
    return Utils.getSchema(this, location, true, job);
  }

  @Override
  public ResourceStatistics getStatistics(String location, Job job)
      throws IOException {
    return null;
  }

  @Override
  public void setPartitionFilter(Expression plan) throws IOException {
    throw new UnsupportedOperationException("setPartitionFilter() not yet supported");
  }

}
