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

package org.apache.tez.engine.runtime;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.YarnException;
import org.apache.tez.common.InputSpec;
import org.apache.tez.common.OutputSpec;
import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.engine.api.Input;
import org.apache.tez.engine.api.Output;
import org.apache.tez.engine.api.Processor;
import org.apache.tez.engine.task.RuntimeTask;

public class RuntimeUtils {

  private static final Log LOG = LogFactory.getLog(RuntimeUtils.class);

  private static final Class<?>[] CONTEXT_ARRAY =
      new Class[] { TezEngineTaskContext.class };
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE =
    new ConcurrentHashMap<Class<?>, Constructor<?>>();

  @SuppressWarnings("unchecked")
  public static <T> T getNewInstance(Class<T> theClass,
      TezEngineTaskContext context) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(CONTEXT_ARRAY);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(context);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public static RuntimeTask createRuntimeTask(
      TezEngineTaskContext taskContext) {
    LOG.info("Creating a runtime task from TaskContext"
        + ", Processor: " + taskContext.getProcessorName()
        + ", InputCount=" + taskContext.getInputSpecList().size()
        + ", OutputCount=" + taskContext.getOutputSpecList().size());

    RuntimeTask t = null;
    try {
      Class<?> processorClazz =
          Class.forName(taskContext.getProcessorName());

      Processor processor = (Processor) getNewInstance(
          processorClazz, taskContext);

      Input[] inputs;
      Output[] outputs;
      if (taskContext.getInputSpecList().isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Initializing task with 0 inputs");
        }
        inputs = new Input[0];
      } else {
        int iSpecCount = taskContext.getInputSpecList().size();
        inputs = new Input[iSpecCount];
        for (int i = 0; i < iSpecCount; ++i) {
          InputSpec inSpec = taskContext.getInputSpecList().get(i);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Using Input"
                + ", index=" + i
                + ", inputClass=" + inSpec.getInputClassName());
          }
          Class<?> inputClazz = Class.forName(inSpec.getInputClassName());
          Input input = (Input) getNewInstance(inputClazz, taskContext);
          inputs[i] = input;
        }
      }
      if (taskContext.getOutputSpecList().isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Initializing task with 0 outputs");
        }
        outputs = new Output[0];
      } else {
        int oSpecCount = taskContext.getOutputSpecList().size();
        outputs = new Output[oSpecCount];
        for (int i = 0; i < oSpecCount; ++i) {
          OutputSpec outSpec = taskContext.getOutputSpecList().get(i);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Using Output"
                + ", index=" + i
                + ", output=" + outSpec.getOutputClassName());
          }
          Class<?> outputClazz = Class.forName(outSpec.getOutputClassName());
          Output output = (Output) getNewInstance(outputClazz, taskContext);
          outputs[i] = output;
        }
      }
      t = createRuntime(taskContext, processor, inputs, outputs);
    } catch (ClassNotFoundException e) {
      throw new YarnException("Unable to initialize RuntimeTask, context="
          + taskContext, e);
    }
    return t;
  }

  private static RuntimeTask createRuntime(TezEngineTaskContext taskContext,
      Processor processor, Input[] inputs, Output[] outputs) {
    try {
      // TODO Change this to use getNewInstance
      Class<?> runtimeClazz = Class.forName(taskContext.getRuntimeName());
      Constructor<?> ctor = runtimeClazz.getConstructor(
          TezEngineTaskContext.class, Processor.class, Input[].class,
          Output[].class);
      ctor.setAccessible(true);
      return (RuntimeTask) ctor.newInstance(taskContext, processor, inputs, outputs);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Unable to load runtimeClass: "
          + taskContext.getRuntimeName(), e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
