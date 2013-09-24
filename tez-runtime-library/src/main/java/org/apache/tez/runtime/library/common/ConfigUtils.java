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

package org.apache.tez.runtime.library.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezJobConfig;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ConfigUtils {

  public static Class<? extends CompressionCodec> getIntermediateOutputCompressorClass(
      Configuration conf, Class<DefaultCodec> defaultValue) {
    Class<? extends CompressionCodec> codecClass = defaultValue;
    String name = conf
        .get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_COMPRESS_CODEC);
    if (name != null) {
      try {
        codecClass = conf.getClassByName(name).asSubclass(
            CompressionCodec.class);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Compression codec " + name
            + " was not found.", e);
      }
    }
    return codecClass;
  }
  
  public static Class<? extends CompressionCodec> getIntermediateInputCompressorClass(
      Configuration conf, Class<DefaultCodec> defaultValue) {
    Class<? extends CompressionCodec> codecClass = defaultValue;
    String name = conf
        .get(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_COMPRESS_CODEC);
    if (name != null) {
      try {
        codecClass = conf.getClassByName(name).asSubclass(
            CompressionCodec.class);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Compression codec " + name
            + " was not found.", e);
      }
    }
    return codecClass;
  }


  // TODO Move defaults over to a constants file.
  
  public static boolean shouldCompressIntermediateOutput(Configuration conf) {
    return conf.getBoolean(
        TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_SHOULD_COMPRESS, false);
  }

  public static boolean isIntermediateInputCompressed(Configuration conf) {
    return conf.getBoolean(
        TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_IS_COMPRESSED, false);
  }

  public static <V> Class<V> getIntermediateOutputValueClass(Configuration conf) {
    Class<V> retv = (Class<V>) conf.getClass(
        TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_VALUE_CLASS, null,
        Object.class);
    return retv;
  }
  
  public static <V> Class<V> getIntermediateInputValueClass(Configuration conf) {
    Class<V> retv = (Class<V>) conf.getClass(
        TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_VALUE_CLASS, null,
        Object.class);
    return retv;
  }

  public static <K> Class<K> getIntermediateOutputKeyClass(Configuration conf) {
    Class<K> retv = (Class<K>) conf.getClass(
        TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_KEY_CLASS, null,
        Object.class);
    return retv;
  }

  public static <K> Class<K> getIntermediateInputKeyClass(Configuration conf) {
    Class<K> retv = (Class<K>) conf.getClass(
        TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_KEY_CLASS, null,
        Object.class);
    return retv;
  }

  public static <K> RawComparator<K> getIntermediateOutputKeyComparator(Configuration conf) {
    Class<? extends RawComparator> theClass = conf.getClass(
        TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_KEY_COMPARATOR_CLASS, null,
        RawComparator.class);
    if (theClass != null)
      return ReflectionUtils.newInstance(theClass, conf);
    return WritableComparator.get(getIntermediateOutputKeyClass(conf).asSubclass(
        WritableComparable.class));
  }

  public static <K> RawComparator<K> getIntermediateInputKeyComparator(Configuration conf) {
    Class<? extends RawComparator> theClass = conf.getClass(
        TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_KEY_COMPARATOR_CLASS, null,
        RawComparator.class);
    if (theClass != null)
      return ReflectionUtils.newInstance(theClass, conf);
    return WritableComparator.get(getIntermediateInputKeyClass(conf).asSubclass(
        WritableComparable.class));
  }

  
  
  // TODO Fix name
  public static <V> RawComparator<V> getInputKeySecondaryGroupingComparator(
      Configuration conf) {
    Class<? extends RawComparator> theClass = conf
        .getClass(
            TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_KEY_SECONDARY_COMPARATOR_CLASS,
            null, RawComparator.class);
    if (theClass == null) {
      return getIntermediateInputKeyComparator(conf);
    }

    return ReflectionUtils.newInstance(theClass, conf);
  }
  
  public static boolean useNewApi(Configuration conf) {
    return conf.getBoolean("mapred.mapper.new-api", false);
  }

}
