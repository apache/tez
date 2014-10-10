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
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;

@Private
public class TezYARNUtils {
  
  private static Pattern ENV_VARIABLE_PATTERN = Pattern.compile(Shell.getEnvironmentVariableRegex());

  public static String getFrameworkClasspath(Configuration conf, boolean usingArchive) {
    StringBuilder classpathBuilder = new StringBuilder();

    // Add any additional user-specified classpath
    String additionalClasspath = conf.get(TezConfiguration.TEZ_CLUSTER_ADDITIONAL_CLASSPATH_PREFIX);
    if (additionalClasspath != null && !additionalClasspath.trim().isEmpty()) {
      classpathBuilder.append(additionalClasspath)
          .append(File.pathSeparator);
    }

    // Add PWD:PWD/*
    classpathBuilder.append(Environment.PWD.$())
        .append(File.pathSeparator)
        .append(Environment.PWD.$() + File.separator + "*")
        .append(File.pathSeparator);

    // Next add the tez libs, if specified via an archive.
    if (usingArchive) {
      // Add PWD/tezlib/*
      classpathBuilder.append(Environment.PWD.$())
          .append(File.separator)
          .append(TezConstants.TEZ_TAR_LR_NAME)
          .append(File.separator)
          .append("*")
          .append(File.pathSeparator);

      // Add PWD/tezlib/lib/*
      classpathBuilder.append(Environment.PWD.$())
          .append(File.separator)
          .append(TezConstants.TEZ_TAR_LR_NAME)
          .append(File.separator)
          .append("lib")
          .append(File.separator)
          .append("*")
          .append(File.pathSeparator);
    }

    // Last add HADOOP_CLASSPATH, if it's required.
    if (conf.getBoolean(TezConfiguration.TEZ_USE_CLUSTER_HADOOP_LIBS,
        TezConfiguration.TEZ_USE_CLUSTER_HADOOP_LIBS_DEFAULT)) {
      for (String c : conf.getStrings(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
        classpathBuilder.append(c.trim())
            .append(File.pathSeparator);
      }
    } else {
      // Setup HADOOP_CONF_DIR after PWD and tez-libs, if it's required.
      classpathBuilder.append(Environment.HADOOP_CONF_DIR.$())
          .append(File.pathSeparator);
    }

    String classpath = classpathBuilder.toString();
    return StringInterner.weakIntern(classpath);
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
      Configuration conf,  String confEnvKey, String confEnvKeyDefault, boolean usingArchive) {
    // Setup the CLASSPATH in environment
    // i.e. add { Hadoop jars, job jar, CWD } to classpath.
    String classpath = getFrameworkClasspath(conf, usingArchive);
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
