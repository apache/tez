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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

@SuppressWarnings({"unchecked", "rawtypes"})
@InterfaceAudience.Private
public class ConfigUtils {

  public static Class<? extends CompressionCodec> getIntermediateOutputCompressorClass(
      Configuration conf, Class<DefaultCodec> defaultValue) {
    Class<? extends CompressionCodec> codecClass = defaultValue;
    String name = conf
        .get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
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
        .get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
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
        TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, false);
  }

  public static boolean isIntermediateInputCompressed(Configuration conf) {
    return conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, false);
  }

  public static <V> Class<V> getIntermediateOutputValueClass(Configuration conf) {
    Class<V> retv = (Class<V>) conf.getClass(
        TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, null,
        Object.class);
    return retv;
  }
  
  public static <V> Class<V> getIntermediateInputValueClass(Configuration conf) {
    Class<V> retv = (Class<V>) conf.getClass(
        TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, null,
        Object.class);
    return retv;
  }

  public static <K> Class<K> getIntermediateOutputKeyClass(Configuration conf) {
    Class<K> retv = (Class<K>) conf.getClass(
        TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, null,
        Object.class);
    return retv;
  }

  public static <K> Class<K> getIntermediateInputKeyClass(Configuration conf) {
    Class<K> retv = (Class<K>) conf.getClass(
        TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, null,
        Object.class);
    return retv;
  }

  public static <K> RawComparator<K> getIntermediateOutputKeyComparator(Configuration conf) {
    Class<? extends RawComparator> theClass = conf.getClass(
        TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS, null,
        RawComparator.class);
    if (theClass != null)
      return ReflectionUtils.newInstance(theClass, conf);
    return WritableComparator.get(getIntermediateOutputKeyClass(conf).asSubclass(
        WritableComparable.class));
  }

  public static <K> RawComparator<K> getIntermediateInputKeyComparator(Configuration conf) {
    Class<? extends RawComparator> theClass = conf.getClass(
        TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS, null,
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
            TezRuntimeConfiguration.TEZ_RUNTIME_KEY_SECONDARY_COMPARATOR_CLASS,
            null, RawComparator.class);
    if (theClass == null) {
      return getIntermediateInputKeyComparator(conf);
    }

    return ReflectionUtils.newInstance(theClass, conf);
  }
  
  public static boolean useNewApi(Configuration conf) {
    return conf.getBoolean("mapred.mapper.new-api", false);
  }

  @InterfaceAudience.Private
  public static Map<String, String> extractConfigurationMap(Map<String, String> confMap, Set<String> allowedKeys) {
    Preconditions.checkArgument(confMap != null, "ConfMap cannot be null");
    Preconditions.checkArgument(allowedKeys != null, "Valid key set cannot be empty");
    Map<String, String> map = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : confMap.entrySet()) {
      if (allowedKeys.contains(entry.getKey())) {
        map.put(entry.getKey(), entry.getValue());
      }
    }
    return map;
  }

  @InterfaceAudience.Private
  public static void addConfigMapToConfiguration(Configuration conf, Map<String, String> confMap) {
    Preconditions.checkArgument(conf != null, "Configuration cannot be null");
    Preconditions.checkArgument(confMap != null, "Configuration map cannot be null");
    for (Map.Entry<String, String> entry : confMap.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
  }

  @InterfaceAudience.Private
  public static Map<String, String> extractConfigurationMap(Map<String, String> confMap,
                                                            List<Set<String>> validKeySets,
                                                            List<String> allowedPrefixes) {
    Preconditions.checkArgument(confMap != null, "ConfMap cannot be null");
    Preconditions.checkArgument(validKeySets != null, "Valid key set cannot be empty");
    Preconditions.checkArgument(allowedPrefixes != null, "Allowed prefixes cannot be null");

    return extractConfigurationMapInternal(confMap.entrySet(), validKeySets, allowedPrefixes);
  }

  @InterfaceAudience.Private
  public static Map<String, String> extractConfigurationMap(Configuration conf,
                                                            List<Set<String>> validKeySets,
                                                            List<String> allowedPrefixes) {
    Preconditions.checkArgument(conf != null, "conf cannot be null");
    Preconditions.checkArgument(validKeySets != null, "Valid key set cannot be empty");
    Preconditions.checkArgument(allowedPrefixes != null, "Allowed prefixes cannot be null");
    return extractConfigurationMapInternal(conf, validKeySets, allowedPrefixes);
  }

  @InterfaceAudience.Private
  public static boolean doesKeyQualify(String key, List<Set<String>> validKeySets, List<String> allowedPrefixes) {
    Preconditions.checkArgument(key != null, "key cannot be null");
    Preconditions.checkArgument(validKeySets != null, "Valid key set cannot be empty");
    Preconditions.checkArgument(allowedPrefixes != null, "Allowed prefixes cannot be null");
    for (Set<String> set : validKeySets) {
      if (set.contains(key)) {
        return true;
      }
    }
    for (String prefix : allowedPrefixes) {
      if (key.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  @InterfaceAudience.Private
  public static void mergeConfsWithExclusions(Configuration destConf, Map<String, String> srcConf, Set<String> excludedKeySet) {
    Preconditions.checkState(destConf != null, "Destination conf cannot be null");
    Preconditions.checkState(srcConf != null, "Source conf cannot be null");
    for (Map.Entry<String, String> entry : srcConf.entrySet()) {
      if (!excludedKeySet.contains(entry.getKey())) {
        destConf.set(entry.getKey(), entry.getValue());
      }
    }
  }

  private static Map<String, String> extractConfigurationMapInternal(
      Iterable<Map.Entry<String, String>> iterable, List<Set<String>> validKeySets, List<String> allowedPrefixes) {
    Set<String> validKeys = new HashSet<String>();
    for (Set<String> set : validKeySets) {
      validKeys.addAll(set);
    }
    Map<String, String> localConfMap = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : iterable) {
      if (validKeys.contains(entry.getKey())) {
        localConfMap.put(entry.getKey(), entry.getValue());
      } else {
        for (String prefix : allowedPrefixes) {
          if (entry.getKey().startsWith(prefix)) {
            localConfMap.put(entry.getKey(), entry.getValue());
          }
        }
      }
    }
    return localConfMap;
  }
}
