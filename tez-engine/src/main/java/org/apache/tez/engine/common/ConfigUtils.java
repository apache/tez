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

package org.apache.tez.engine.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ConfigUtils {
  public static  Class<? extends CompressionCodec> getMapOutputCompressorClass(
      Configuration conf, Class<DefaultCodec> class1) {
    // TODO Auto-generated method stub
    return null;
  }

  public static  boolean getCompressMapOutput(Configuration conf) {
    // TODO Auto-generated method stub
    return false;
  }

  public static <V> Class<V> getMapOutputValueClass(Configuration conf) {
    Class<V> retv = 
        (Class<V>) 
        conf.getClass("mapreduce.map.output.value.class", null, Object.class);
    if (retv == null) {
      retv = getOutputValueClass(conf);
    }
    return retv;
  }

  public static <V> Class<V> getOutputValueClass(Configuration conf) {
    return (Class<V>) conf.getClass(
        "mapreduce.job.output.value.class", Text.class, Object.class);
  }

  public static <K> Class<K> getMapOutputKeyClass(Configuration conf) {
    Class<K> retv = 
        (Class<K>) conf.getClass("mapreduce.map.output.key.class", null, Object.class);
    if (retv == null) {
      retv = getOutputKeyClass(conf);
    }
    return 
        retv;
  }

  public static <K> Class<K> getOutputKeyClass(Configuration conf) {
    return 
        (Class<K>) 
        conf.getClass(
            "mapreduce.job.output.key.class", 
            LongWritable.class, Object.class);
}
  
  public static <K> RawComparator<K> getOutputKeyComparator(Configuration conf) {
    Class<? extends RawComparator> theClass = 
        conf.getClass(
            "mapreduce.job.output.key.comparator.class", null, 
            RawComparator.class);
      if (theClass != null)
        return ReflectionUtils.newInstance(theClass, conf);
      return WritableComparator.get(
          getMapOutputKeyClass(conf).asSubclass(WritableComparable.class));
    }

  public static <V> RawComparator<V> getOutputValueGroupingComparator(
      Configuration conf) {
    Class<? extends RawComparator> theClass = 
        conf.getClass(
            "mapreduce.job.output.group.comparator.class", 
            null, RawComparator.class);
    if (theClass == null) {
      return getOutputKeyComparator(conf);
    }

    return ReflectionUtils.newInstance(theClass, conf);
  }

}
