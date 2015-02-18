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

package org.apache.tez.mapreduce.combine;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.junit.Test;
import org.mockito.Mockito;

public class TestMRCombiner {

  @Test
  public void testRunOldCombiner() throws IOException, InterruptedException {
    TezConfiguration conf = new TezConfiguration();
    setKeyAndValueClassTypes(conf);
    conf.setClass("mapred.combiner.class", OldReducer.class, Object.class);
    TaskContext taskContext = getTaskContext(conf);
    MRCombiner combiner = new MRCombiner(taskContext);
    Writer writer = Mockito.mock(Writer.class);
    combiner.combine(new TezRawKeyValueIteratorTest(), writer);
    // verify combiner output keys and values
    verifyKeyAndValues(writer);
  }

  @Test
  public void testRunNewCombiner() throws IOException, InterruptedException {
    TezConfiguration conf = new TezConfiguration();
    setKeyAndValueClassTypes(conf);
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setClass(MRJobConfig.COMBINE_CLASS_ATTR, NewReducer.class,
        Object.class);
    TaskContext taskContext = getTaskContext(conf);
    MRCombiner combiner = new MRCombiner(taskContext);
    Writer writer = Mockito.mock(Writer.class);
    combiner.combine(new TezRawKeyValueIteratorTest(), writer);
    // verify combiner output keys and values
    verifyKeyAndValues(writer);
  }

  private void setKeyAndValueClassTypes(TezConfiguration conf) {
    conf.setClass(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS,
        Text.class, Object.class);
    conf.setClass(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS,
        IntWritable.class, Object.class);
  }

  private TaskContext getTaskContext(TezConfiguration conf)
      throws IOException {
    UserPayload payload = TezUtils.createUserPayloadFromConf(conf);
    TaskContext taskContext = Mockito.mock(InputContext.class);
    Mockito.when(taskContext.getUserPayload()).thenReturn(payload);
    Mockito.when(taskContext.getCounters()).thenReturn(new TezCounters());
    Mockito.when(taskContext.getApplicationId()).thenReturn(
        ApplicationId.newInstance(123456, 1));
    return taskContext;
  }

  private void verifyKeyAndValues(Writer writer) throws IOException {
    Mockito.verify(writer, Mockito.atLeastOnce()).append(new Text("tez"),
        new IntWritable(3));
    Mockito.verify(writer, Mockito.atLeastOnce()).append(new Text("apache"),
        new IntWritable(1));
    Mockito.verify(writer, Mockito.atLeastOnce()).append(new Text("hadoop"),
        new IntWritable(2));
  }

  private static class TezRawKeyValueIteratorTest implements
      TezRawKeyValueIterator {

    private int i = -1;
    private String[] keys = { "tez", "tez", "tez", "apache", "hadoop", "hadoop" };

    @Override
    public boolean next() throws IOException {
      if (i++ < keys.length - 1) {
        return true;
      }
      return false;
    }

    @Override
    public DataInputBuffer getValue() throws IOException {
      DataInputBuffer value = new DataInputBuffer();
      IntWritable intValue = new IntWritable(1);
      DataOutputBuffer out = new DataOutputBuffer();
      intValue.write(out);
      value.reset(out.getData(), out.getLength());
      return value;
    }

    @Override
    public Progress getProgress() {
      return null;
    }

    @Override
    public boolean isSameKey() throws IOException {
      return false;
    }

    @Override
    public DataInputBuffer getKey() throws IOException {
      DataInputBuffer key = new DataInputBuffer();
      Text text = new Text(keys[i]);
      DataOutputBuffer out = new DataOutputBuffer();
      text.write(out);
      key.reset(out.getData(), out.getLength());
      return key;
    }

    @Override
    public void close() throws IOException {
    }
  }

  private static class OldReducer implements
      Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void configure(JobConf arg0) {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void reduce(Text key, Iterator<IntWritable> value,
        OutputCollector<Text, IntWritable> collector, Reporter reporter)
        throws IOException {
      int count = 0;
      while (value.hasNext()) {
        count += value.next().get();
      }
      collector.collect(new Text(key.toString()), new IntWritable(count));
    }
  }

  private static class NewReducer extends
      org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      int count = 0;
      for (IntWritable value : values) {
        count += value.get();
      }
      context.write(new Text(key.toString()), new IntWritable(count));
    }
  }
}
