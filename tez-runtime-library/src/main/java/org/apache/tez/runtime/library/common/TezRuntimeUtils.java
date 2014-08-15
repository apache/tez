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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;

@Private
public class TezRuntimeUtils {

  private static final Log LOG = LogFactory
      .getLog(TezRuntimeUtils.class);
  
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
  public static Combiner instantiateCombiner(Configuration conf, TaskContext taskContext) throws IOException {
    Class<? extends Combiner> clazz;
    String className = conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS);
    if (className == null) {
      LOG.info("No combiner specified via " + TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS + ". Combiner will not be used");
      return null;
    }
    LOG.info("Using Combiner class: " + className);
    try {
      clazz = (Class<? extends Combiner>) conf.getClassByName(className);
    } catch (ClassNotFoundException e) {
      throw new IOException("Unable to load combiner class: " + className);
    }
    
    Combiner combiner = null;
    
      Constructor<? extends Combiner> ctor;
      try {
        ctor = clazz.getConstructor(TaskContext.class);
        combiner = ctor.newInstance(taskContext);
      } catch (SecurityException e) {
        throw new IOException(e);
      } catch (NoSuchMethodException e) {
        throw new IOException(e);
      } catch (IllegalArgumentException e) {
        throw new IOException(e);
      } catch (InstantiationException e) {
        throw new IOException(e);
      } catch (IllegalAccessException e) {
        throw new IOException(e);
      } catch (InvocationTargetException e) {
        throw new IOException(e);
      }
      return combiner;
  }
  
  @SuppressWarnings("unchecked")
  public static Partitioner instantiatePartitioner(Configuration conf)
      throws IOException {
    Class<? extends Partitioner> clazz;
    try {
      clazz = (Class<? extends Partitioner>) conf.getClassByName(conf
          .get(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS));
    } catch (ClassNotFoundException e) {
      throw new IOException("Unable to find Partitioner class specified in config : "
          + conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS), e);
    }

    LOG.info("Using partitioner class: " + clazz.getName());

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
  
  public static TezTaskOutput instantiateTaskOutputManager(Configuration conf, OutputContext outputContext) {
    Class<?> clazz = conf.getClass(Constants.TEZ_RUNTIME_TASK_OUTPUT_MANAGER,
        TezTaskOutputFiles.class);
    try {
      Constructor<?> ctor = clazz.getConstructor(Configuration.class, String.class);
      ctor.setAccessible(true);
      TezTaskOutput instance = (TezTaskOutput) ctor.newInstance(conf, outputContext.getUniqueIdentifier());
      return instance;
    } catch (Exception e) {
      throw new TezUncheckedException(
          "Unable to instantiate configured TezOutputFileManager: "
              + conf.get(Constants.TEZ_RUNTIME_TASK_OUTPUT_MANAGER,
                  TezTaskOutputFiles.class.getName()), e);
    }
  }
}
