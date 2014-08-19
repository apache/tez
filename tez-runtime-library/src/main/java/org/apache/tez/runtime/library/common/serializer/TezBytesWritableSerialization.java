package org.apache.tez.runtime.library.common.serializer;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * <pre>
 * When using BytesWritable, data is serialized in memory (4 bytes per key and 4 bytes per value)
 * and written to IFile where it gets serialized again (4 bytes per key and 4 bytes per value).
 * This adds an overhead of 8 bytes per key value pair written. This class reduces this overhead
 * by providing a fast serializer/deserializer to speed up inner loop of sort,
 * spill, merge.
 *
 * Usage e.g:
 *  OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
 *         .newBuilder(keyClass, valClass, MRPartitioner.class.getName(), partitionerConf)
 *         .setFromConfiguration(conf)
 *         .setKeySerializationClass(TezBytesWritableSerialization.class.getName(),
 *            TezBytesComparator.class.getName()).build())
 * </pre>
 */
@Public
@Unstable
public class TezBytesWritableSerialization extends Configured implements Serialization<Writable> {

  private static final Log LOG = LogFactory.getLog(TezBytesWritableSerialization.class.getName());

  @Override
  public boolean accept(Class<?> c) {
    return (BytesWritable.class.isAssignableFrom(c));
  }

  @Override
  public Serializer<Writable> getSerializer(Class<Writable> c) {
    return new TezBytesWritableSerializer();
  }

  @Override
  public Deserializer<Writable> getDeserializer(Class<Writable> c) {
    return new TezBytesWritableDeserializer(getConf(), c);
  }

  public static class TezBytesWritableDeserializer extends Configured
      implements Deserializer<Writable> {
    private Class<?> writableClass;
    private DataInputBuffer dataIn;

    public TezBytesWritableDeserializer(Configuration conf, Class<?> c) {
      setConf(conf);
      this.writableClass = c;
    }

    @Override
    public void open(InputStream in) {
      dataIn = (DataInputBuffer) in;
    }

    @Override
    public Writable deserialize(Writable w) throws IOException {
      BytesWritable writable = (BytesWritable) w;
      if (w == null) {
        writable = (BytesWritable) ReflectionUtils.newInstance(writableClass, getConf());
      }

      writable.set(dataIn.getData(), dataIn.getPosition(), dataIn.getLength() - dataIn
          .getPosition());
      return writable;
    }

    @Override
    public void close() throws IOException {
      dataIn.close();
    }

  }

  public static class TezBytesWritableSerializer extends Configured implements
      Serializer<Writable> {

    private OutputStream dataOut;

    @Override
    public void open(OutputStream out) {
      this.dataOut = out;
    }

    @Override
    public void serialize(Writable w) throws IOException {
      BytesWritable writable = (BytesWritable) w;
      dataOut.write(writable.getBytes(), 0, writable.getLength());
    }

    @Override
    public void close() throws IOException {
      dataOut.close();
    }
  }
}

