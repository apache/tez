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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
  private static Logger LOG = LoggerFactory.getLogger(TezYARNUtils.class);

  public static final String ENV_NAME_REGEX = "[A-Za-z_][A-Za-z0-9_]*";

  private static final Pattern VAR_SUBBER =
    Pattern.compile(Shell.getEnvironmentVariableRegex());
  private static final Pattern VARVAL_SPLITTER = Pattern.compile(
    "(?<=^|,)"                            // preceded by ',' or line begin
      + '(' + ENV_NAME_REGEX + ')'      // var group
      + '='
      + "([^,]*)"                             // val group
  );

  public static String getFrameworkClasspath(Configuration conf, boolean usingArchive) {
    StringBuilder classpathBuilder = new StringBuilder();
    boolean userClassesTakesPrecedence =
      conf.getBoolean(TezConfiguration.TEZ_USER_CLASSPATH_FIRST,
          TezConfiguration.TEZ_USER_CLASSPATH_FIRST_DEFAULT);
    if (userClassesTakesPrecedence) {
      addUserSpecifiedClasspath(classpathBuilder, conf);
    }

    String [] tezLibUrisClassPath = conf.getStrings(TezConfiguration.TEZ_LIB_URIS_CLASSPATH);

    if(!conf.getBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, false) &&
       tezLibUrisClassPath != null && tezLibUrisClassPath.length != 0) {
      for(String c : tezLibUrisClassPath) {
        classpathBuilder.append(c.trim())
        .append(File.pathSeparator);
      }
    } else {
      if(conf.getBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, false)) {
        LOG.info("Ignoring '" + TezConfiguration.TEZ_LIB_URIS + "' since  '" +
            TezConfiguration.TEZ_IGNORE_LIB_URIS + "' is set to true ");
      }

      // Legacy: Next add the tez libs, if specified via an archive.
      if (usingArchive) {
        // Add PWD/tezlib/*
        classpathBuilder.append(Environment.PWD.$())
            .append(File.separator)
            .append(TezConstants.TEZ_TAR_LR_NAME)
            .append(File.separator)
            .append("*")
            .append(File.pathSeparator);

        // Legacy: Add PWD/tezlib/lib/*
        classpathBuilder.append(Environment.PWD.$())
            .append(File.separator)
            .append(TezConstants.TEZ_TAR_LR_NAME)
            .append(File.separator)
            .append("lib")
            .append(File.separator)
            .append("*")
            .append(File.pathSeparator);
      }
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
    } else if (conf.getBoolean(TezConfiguration.TEZ_CLASSPATH_ADD_HADOOP_CONF,
        TezConfiguration.TEZ_CLASSPATH_ADD_HADOOP_CONF_DEFAULT)) {
      // Setup HADOOP_CONF_DIR after PWD and tez-libs, if it's required.
      classpathBuilder.append(Environment.HADOOP_CONF_DIR.$()).append(File.pathSeparator);
    }

    if (!userClassesTakesPrecedence) {
      addUserSpecifiedClasspath(classpathBuilder, conf);
    }
    String classpath = classpathBuilder.toString();
    return StringInterner.weakIntern(classpath);
  }

  private static void addUserSpecifiedClasspath(StringBuilder classpathBuilder,
      Configuration conf) {
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
  }

  public static void appendToEnvFromInputString(Map<String, String> env,
      String envString, String classPathSeparator) {
    if (envString != null && envString.length() > 0) {
      Matcher varValMatcher = VARVAL_SPLITTER.matcher(envString);
      while (varValMatcher.find()) {
        String envVar = varValMatcher.group(1);
        Matcher m = VAR_SUBBER.matcher(varValMatcher.group(2));
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
        addToEnvironment(env, envVar, sb.toString(), classPathSeparator);
      }
    }
  }
  
  public static void setEnvIfAbsentFromInputString(Map<String, String> env,
      String envString) {
    if (envString != null && envString.length() > 0) {
      String childEnvs[] = envString.split(",");
      for (String cEnv : childEnvs) {
        String[] parts = cEnv.split("="); // split on '='
        Matcher m = VAR_SUBBER .matcher(parts[1]);
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

  public static void setupDefaultEnv(Map<String, String> env, Configuration conf,  String userEnvKey, String userEnvDefault,
      String clusterDefaultEnvKey, String clusterDefaultEnvDefault, boolean usingArchive) {
    // Setup the CLASSPATH in environment
    // i.e. add { Hadoop jars, job jar, CWD } to classpath.
    String classpath = getFrameworkClasspath(conf, usingArchive);
    TezYARNUtils.addToEnvironment(env,
        ApplicationConstants.Environment.CLASSPATH.name(),
        classpath, File.pathSeparator);

    // Pre-pend pwd to LD_LIBRARY_PATH
    // Done separately here because this is known to work platform
    // independent
    TezYARNUtils.addToEnvironment(env,
        Environment.LD_LIBRARY_PATH.name(), Environment.PWD.$(), File.pathSeparator);
    TezYARNUtils.appendToEnvFromInputString(env,
        conf.get(userEnvKey, userEnvDefault), File.pathSeparator);
    // set any env from config if it is not set already
    TezYARNUtils.appendToEnvFromInputString(env,
        conf.get(clusterDefaultEnvKey, clusterDefaultEnvDefault), File.pathSeparator);
  }

  public static void replaceInEnv(Map<String, String> env, String key, String value) {
    env.put(StringInterner.weakIntern(key), StringInterner.weakIntern(value));
  }

}
