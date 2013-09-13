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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.engine.api.Partitioner;

public class TezEngineUtils {

  public static String getTaskIdentifier(String vertexName, int taskIndex) {
    return String.format("%s_%06d", vertexName, taskIndex);
  }

  public static String getTaskAttemptIdentifier(int taskIndex,
      int taskAttemptNumber) {
    return String.format("%d_%d", taskIndex, taskAttemptNumber);
  }

  // TODO Maybe include a dag name in this.
  public static String getTaskAttemptIdentifier(String vertexName,
      int taskIndex, int taskAttemptNumber) {
    return String.format("%s_%06d_%02d", vertexName, taskIndex,
        taskAttemptNumber);
  }

  @SuppressWarnings("unchecked")
  public static Partitioner instantiatePartitioner(Configuration conf)
      throws IOException {
    Class<? extends Partitioner> clazz = (Class<? extends Partitioner>) conf
        .getClass(TezJobConfig.TEZ_ENGINE_PARTITIONER_CLASS, Partitioner.class);

    Partitioner partitioner = null;

    try {
      Constructor<? extends Partitioner> ctorWithConf = clazz
          .getConstructor(Configuration.class);
      partitioner = ctorWithConf.newInstance(conf);
    } catch (SecurityException e) {
      throw new IOException(e);
    } catch (NoSuchMethodException e) {
      try {
        // Try a 0 argument constructor.
        partitioner = clazz.newInstance();
      } catch (InstantiationException e1) {
        throw new IOException(e1);
      } catch (IllegalAccessException e1) {
        throw new IOException(e1);
      }
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    } catch (InvocationTargetException e) {
      throw new IOException(e);
    }
    return partitioner;
  }
}
