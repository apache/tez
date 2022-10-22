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

package org.apache.tez.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.helpers.ThreadLocalMap;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LoggingUtils {
  private static final Logger LOG = LoggerFactory.getLogger(LoggingUtils.class);

  private LoggingUtils() {}

  @SuppressWarnings("unchecked")
  public static void initLoggingContext(ThreadLocalMap threadLocalMap, Configuration conf,
      String dagId, String taskAttemptId) {
    Hashtable<String, String> data = (Hashtable<String, String>) threadLocalMap.get();
    if (data == null) {
      data = new NonClonableHashtable<String, String>();
      threadLocalMap.set(data);
    }
    data.put("dagId", dagId == null ? "" : dagId);
    data.put("taskAttemptId", taskAttemptId == null ? "" : taskAttemptId);

    String[] mdcKeys = conf.getStrings(TezConfiguration.TEZ_MDC_CUSTOM_KEYS,
        TezConfiguration.TEZ_MDC_CUSTOM_KEYS_DEFAULT);

    if (mdcKeys.length == 0) {
      return;
    }

    String[] mdcKeysValuesFrom = conf.getStrings(TezConfiguration.TEZ_MDC_CUSTOM_KEYS_VALUES_FROM,
        TezConfiguration.TEZ_MDC_CUSTOM_KEYS_VALUES_FROM_DEFAULT);
    LOG.info("MDC_LOGGING: setting up MDC keys: keys: {} / conf: {}", Arrays.asList(mdcKeys),
        Arrays.asList(mdcKeysValuesFrom));

    int i = 0;
    for (String mdcKey : mdcKeys) {
      // don't want to fail on incorrect mdc key settings, but warn in app logs
      if (mdcKey.isEmpty() || mdcKeysValuesFrom.length < i + 1) {
        LOG.warn("cannot set mdc key: {}", mdcKey);
        break;
      }

      String mdcValue = mdcKeysValuesFrom[i] == null ? "" : conf.get(mdcKeysValuesFrom[i]);
      // MDC is backed by a Hashtable, let's prevent NPE because of null values
      if (mdcValue != null) {
        data.put(mdcKey, mdcValue);
      } else {
        LOG.warn("MDC_LOGGING: mdc value is null for key: {}, config key: {}", mdcKey,
            mdcKeysValuesFrom[i]);
      }

      i++;
    }
  }

  public static String getPatternForAM(Configuration conf) {
    return conf.get(TezConfiguration.TEZ_LOG_PATTERN_LAYOUT_AM, null);
  }

  public static String getPatternForTask(Configuration conf) {
    return conf.get(TezConfiguration.TEZ_LOG_PATTERN_LAYOUT_TASK, null);
  }

  /**
   * This method is for setting a NonClonableHashtable into log4j's mdc. Reflection hacks are
   * needed, because MDC.mdc is well protected (final static MDC mdc = new MDC();). The logic below
   * is supposed to be called once per JVM, so it's not a subject to performance bottlenecks. For
   * further details of this solution, please check NonClonableHashtable class, which is set into
   * the ThreadLocalMap. A wrong outcome of this method (any kind of runtime/reflection problems)
   * should not affect the DAGAppMaster/TezChild. In case of an exception a ThreadLocalMap is
   * returned, but it won't affect the content of the MDC.
   */
  @SuppressWarnings("unchecked")
  public static ThreadLocalMap setupLog4j() {
    ThreadLocalMap mdcContext = new ThreadLocalMap();
    mdcContext.set(new NonClonableHashtable<String, String>());

    try {
      final Constructor<?>[] constructors = org.apache.log4j.MDC.class.getDeclaredConstructors();
      for (Constructor<?> c : constructors) {
        c.setAccessible(true);
      }

      org.apache.log4j.MDC mdc = (org.apache.log4j.MDC) constructors[0].newInstance();
      Field tlmField = org.apache.log4j.MDC.class.getDeclaredField("tlm");
      tlmField.setAccessible(true);
      tlmField.set(mdc, mdcContext);

      Field mdcField = org.apache.log4j.MDC.class.getDeclaredField("mdc");
      mdcField.setAccessible(true);

      Field modifiers = Field.class.getDeclaredField("modifiers");
      modifiers.setAccessible(true);
      modifiers.setInt(mdcField, mdcField.getModifiers() & ~Modifier.FINAL);

      mdcField.set(null, mdc);

    } catch (Exception e) {
      LOG.warn("Cannot set log4j global MDC, mdcContext won't be applied to log4j's MDC class", e);
    }

    return mdcContext;
  }

  /**
   * NonClonableHashtable is a special class for hacking the log4j MDC context. By design, log4j's
   * MDC uses a ThreadLocalMap, which clones parent thread's context before propagating it to child
   * thread (see: @see {@link org.apache.log4j.helpers.ThreadLocalMap#childValue()}). In our
   * usecase, this is not suitable, as we want to maintain only one context globally (and set e.g.
   * dagId, taskAttemptId), then update it as easy as possible when dag/taskattempt changes, without
   * having to propagate the update parameters to all the threads in the JVM.
   */
  private static class NonClonableHashtable<K, V> extends Hashtable<String, String> {
    private static final long serialVersionUID = 1L;

    @Override
    public synchronized Object clone() {
      return this;
    }
  }
}
