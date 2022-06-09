/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.runtime.library.common.serializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;

/**
 * SerializationContext is a wrapper class for serialization related fields.
 */
public class SerializationContext {

  private Class<?> keyClass;
  private Class<?> valueClass;
  private Serialization<?> keySerialization;
  private Serialization<?> valSerialization;

  public SerializationContext(Configuration conf) {
    this.keyClass = ConfigUtils.getIntermediateInputKeyClass(conf);
    this.valueClass = ConfigUtils.getIntermediateInputValueClass(conf);
    SerializationFactory serializationFactory = new SerializationFactory(conf);
    if (keyClass != null) {
      this.keySerialization = serializationFactory.getSerialization(keyClass);
    }
    if (valueClass != null) {
      this.valSerialization = serializationFactory.getSerialization(valueClass);
    }
  }

  public SerializationContext(Class<?> keyClass, Class<?> valueClass,
                              Serialization<?> keySerialization, Serialization<?> valSerialization) {
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.keySerialization = keySerialization;
    this.valSerialization = valSerialization;
  }

  public Class<?> getKeyClass() {
    return keyClass;
  }

  public Class<?> getValueClass() {
    return valueClass;
  }

  public Serialization<?> getKeySerialization() {
    return keySerialization;
  }

  public Serialization<?> getValSerialization() {
    return valSerialization;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public Serializer<?> getKeySerializer() {
    return keySerialization.getSerializer((Class) keyClass);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public Serializer<?> getValueSerializer() {
    return valSerialization.getSerializer((Class) valueClass);
  }

  public void applyToConf(Configuration conf) {
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, keyClass.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, valueClass.getName());
  }
}
