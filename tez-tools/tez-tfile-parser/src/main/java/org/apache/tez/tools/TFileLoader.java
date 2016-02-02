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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Simple pig loader function (mainly loader) which reads TFile and emits every line as a tuple of
 * (machine, key, line).  This would be beneficial if huge amount of logs need to be mined.
 */
public class TFileLoader extends FileInputLoadFunc implements LoadMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(TFileLoader.class);

  protected TFileRecordReader recReader = null;

  private Text currentKey;
  private final TupleFactory tupleFactory = TupleFactory.getInstance();

  private final Pattern PATTERN = Pattern.compile(":");

  @Override
  public Tuple getNext() throws IOException {
    try {
      if (!recReader.nextKeyValue()) {
        return null;
      }

      currentKey = recReader.getCurrentKey();
      String line = recReader.getCurrentValue().toString();
      if (LOG.isDebugEnabled()) {
        LOG.debug("currentKey: " + currentKey
                + ", line=" + line);
      }
      //Tuple would be of format: machine, key, line
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
      //set the line field
      tuple.set(2, line);
      return tuple;
    } catch (InterruptedException e) {
      return null;
    }
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
