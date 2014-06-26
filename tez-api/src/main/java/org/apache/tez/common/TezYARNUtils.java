/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.common;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

@Private
public class TezYARNUtils {
  
  private static Pattern ENV_VARIABLE_PATTERN = Pattern.compile(Shell.getEnvironmentVariableRegex());
  
  public static String getFrameworkClasspath(Configuration conf) {
    Map<String, String> environment = new HashMap<String, String>();

    TezYARNUtils.addToEnvironment(environment,
        Environment.CLASSPATH.name(),
        Environment.PWD.$(),
        File.pathSeparator);

    TezYARNUtils.addToEnvironment(environment,
        Environment.CLASSPATH.name(),
        Environment.PWD.$() + File.separator + "*",
        File.pathSeparator);

    // Add YARN/COMMON/HDFS jars and conf locations to path
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      TezYARNUtils.addToEnvironment(environment, Environment.CLASSPATH.name(),
          c.trim(), File.pathSeparator);
    }
    return StringInterner.weakIntern(environment.get(Environment.CLASSPATH.name()));
  }

  public static void appendToEnvFromInputString(Map<String, String> env,
      String envString, String classPathSeparator) {
    if (envString != null && envString.length() > 0) {
      String childEnvs[] = envString.split(",");
      for (String cEnv : childEnvs) {
        String[] parts = cEnv.split("="); // split on '='
        Matcher m = ENV_VARIABLE_PATTERN.matcher(parts[1]);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
          String var = m.group(1);
          // replace $env with the child's env constructed by tt's
          String replace = env.get(var);
          // if this key is not configured by the tt for the child .. get it
          // from the tt's env
          if (replace == null)
            replace = System.getenv(var);
          // If the env key is not present leave it as it is and assume it will
          // be set by YARN ContainerLauncher. For eg: $HADOOP_COMMON_HOME
          if (replace != null)
            m.appendReplacement(sb, Matcher.quoteReplacement(replace));
        }
        m.appendTail(sb);
        addToEnvironment(env, parts[0], sb.toString(), classPathSeparator);
      }
    }
  }
  
  public static void setEnvIfAbsentFromInputString(Map<String, String> env,
      String envString) {
    if (envString != null && envString.length() > 0) {
      String childEnvs[] = envString.split(",");
      for (String cEnv : childEnvs) {
        String[] parts = cEnv.split("="); // split on '='
        Matcher m = ENV_VARIABLE_PATTERN.matcher(parts[1]);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
          String var = m.group(1);
          // replace $env with the child's env constructed by tt's
          String replace = env.get(var);
          // if this key is not configured by the tt for the child .. get it
          // from the tt's env
          if (replace == null)
            replace = System.getenv(var);
          // If the env key is not present leave it as it is and assume it will
          // be set by YARN ContainerLauncher. For eg: $HADOOP_COMMON_HOME
          if (replace != null)
            m.appendReplacement(sb, Matcher.quoteReplacement(replace));
        }
        m.appendTail(sb);
        putIfAbsent(env, parts[0], sb.toString());
      }
    }
  }
  
  public static void addToEnvironment(
      Map<String, String> environment,
      String variable, String value, String classPathSeparator) {
    String val = environment.get(variable);
    if (val == null) {
      val = value;
    } else {
      val = val + classPathSeparator + value;
    }
    environment.put(StringInterner.weakIntern(variable), 
        StringInterner.weakIntern(val));
  }

  private static void putIfAbsent(Map<String, String> env, String key, String value) {
    if (!env.containsKey(key)) {
      env.put(StringInterner.weakIntern(key), StringInterner.weakIntern(value));
    }
  }

  public static void setupDefaultEnv(Map<String, String> env,
      Configuration conf,  String confEnvKey, String confEnvKeyDefault) {
    // Setup the CLASSPATH in environment
    // i.e. add { Hadoop jars, job jar, CWD } to classpath.
    String classpath = getFrameworkClasspath(conf);
    TezYARNUtils.addToEnvironment(env,
        ApplicationConstants.Environment.CLASSPATH.name(),
        classpath, File.pathSeparator);

    // set any env from config if it is not set already
    TezYARNUtils.setEnvIfAbsentFromInputString(env, conf.get(
        confEnvKey, confEnvKeyDefault));
    
    // Append pwd to LD_LIBRARY_PATH
    // Done separately here because this is known to work platform independent
    TezYARNUtils.addToEnvironment(env, Environment.LD_LIBRARY_PATH.name(),
        Environment.PWD.$(), File.pathSeparator);
  }

}
