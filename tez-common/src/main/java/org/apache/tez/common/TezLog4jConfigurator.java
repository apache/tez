/*
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

package org.apache.tez.common;

import java.util.Locale;
import java.util.Properties;


import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.tez.dag.api.TezConstants;

public class TezLog4jConfigurator extends PropertyConfigurator {

  public void doConfigure(Properties properties, LoggerRepository repository) {
    String logParams = System.getenv(TezConstants.TEZ_CONTAINER_LOG_PARAMS);
    if (logParams != null) {
      String []parts = logParams.split(TezConstants.TEZ_CONTAINER_LOG_PARAMS_SEPARATOR);
      for (String logParam : parts) {
        String [] logParamParts = logParam.split("=");
        if (logParamParts.length == 2) {
          String loggerName = "log4j.logger." + logParamParts[0];
          String logLevel = logParamParts[1].toUpperCase(Locale.ENGLISH);
          properties.setProperty(loggerName, logLevel);
        } else {
          // Cannot use Log4J logging from here.
          System.out.println("TezLog4jConfigurator Ignoring invalid log parameter [" + logParam + "]");
          continue;
        }
      }
    }
    super.doConfigure(properties, repository);
  }

}
