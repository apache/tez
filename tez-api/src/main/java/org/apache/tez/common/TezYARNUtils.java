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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringInterner;

@Private
public class TezYARNUtils {

  public static void setEnvFromInputString(Map<String, String> env,
      String envString,  String classPathSeparator) {
    if (envString != null && envString.length() > 0) {
      String childEnvs[] = envString.split(",");
      Pattern p = Pattern.compile(Shell.getEnvironmentVariableRegex());
      for (String cEnv : childEnvs) {
        String[] parts = cEnv.split("="); // split on '='
        Matcher m = p.matcher(parts[1]);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
          String var = m.group(1);
          // replace $env with the child's env constructed by tt's
          String replace = env.get(var);
          // if this key is not configured by the tt for the child .. get it
          // from the tt's env
          if (replace == null)
            replace = System.getenv(var);
          // the env key is note present anywhere .. simply set it
          if (replace == null)
            replace = "";
          m.appendReplacement(sb, Matcher.quoteReplacement(replace));
        }
        m.appendTail(sb);
        addToEnvironment(env, parts[0], sb.toString(), classPathSeparator);
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

}
