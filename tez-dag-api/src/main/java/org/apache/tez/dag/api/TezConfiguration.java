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

package org.apache.tez.dag.api;

import org.apache.hadoop.conf.Configuration;

public class TezConfiguration extends Configuration {

  public final static String TEZ_SITE_XML = "tez-site.xml";
  public final static String TEZ_DEFAULT_XML = "tez-default.xml";

  static {
    addDefaultResource(TEZ_DEFAULT_XML);
    addDefaultResource(TEZ_SITE_XML);
  }

  public TezConfiguration() {
    super();
  }

  public TezConfiguration(Configuration conf) {
    super(conf);
  }

  public static final String TEZ_PREFIX = "tez.";
  public static final String DAG_AM_PREFIX = TEZ_PREFIX + "dag,am.";

  public static final String DAG_AM_RESOURCE_MEMORY_MB = DAG_AM_PREFIX
      + "resource.memory.mb";
  public static final int DEFAULT_DAG_AM_RESOURCE_MEMORY_MB = 1024;

  public static final String DAG_AM_RESOURCE_CPU_VCORES = DAG_AM_PREFIX
      + "resource.cpu.vcores";
  public static final int DEFAULT_DAG_AM_RESOURCE_CPU_VCORES = 1;

  private static final String TEZ_CONF_DIR_ENV = "TEZ_CONF_DIR";
  private static final String TEZ_HOME_ENV = "TEZ_HOME";

  public static final String TEZ_APPLICATION_CLASSPATH = TEZ_PREFIX
      + "application.classpath";
  public static final String[] DEFAULT_TEZ_APPLICATION_CLASSPATH = {
    TEZ_CONF_DIR_ENV,
    TEZ_HOME_ENV + "/*",
    TEZ_HOME_ENV + "/lib/*"
  };

  public static final String APPLICATION_ATTEMPT_ID_ENV = "APPLICATION_ATTEMPT_ID_ENV";
  
  
  public static final String DAG_AM_PLAN_CONFIG_XML = "tez-dag.xml";

}
