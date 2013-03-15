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

package org.apache.tez.ampool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;

public class AMPoolConfiguration extends Configuration {

  private static final String AMPOOL_DEFAULT_XML_FILE =
      "tez-ampool-default.xml";
  private static final String AMPOOL_SITE_XML_FILE = "tez-ampool-site.xml";
  public static final String AMPOOL_APP_XML_FILE = "tez-ampool-app.xml";

  public static final String APP_NAME = "AMPoolService";

  static {
    Configuration.addDefaultResource(AMPOOL_DEFAULT_XML_FILE);
    Configuration.addDefaultResource(AMPOOL_SITE_XML_FILE);
    Configuration.addDefaultResource(AMPOOL_APP_XML_FILE);
  }

  //Configurations

  public static final String AMPOOL_PREFIX = "tez.ampool.";

  // Client

  public static final String AM_MASTER_MEMORY_MB =
      AMPOOL_PREFIX + "am.master_memory";
  public static final int DEFAULT_AM_MASTER_MEMORY_MB = 1024;

  public static final String AM_MASTER_QUEUE =
      AMPOOL_PREFIX + "am.master_queue";
  public static final String DEFAULT_AM_MASTER_QUEUE = "default";

  // Server
  public static final String WS_PORT =
      AMPOOL_PREFIX + "ws.port";
  public static final int DEFAULT_WS_PORT = 12999;

  // AM

  public static final String AM_POOL_SIZE =
      AMPOOL_PREFIX + "am-pool-size";
  public static final int DEFAULT_AM_POOL_SIZE = 3;

  public static final String MAX_AM_POOL_SIZE =
      AMPOOL_PREFIX + "max-am-pool-size";
  public static final int DEFAULT_MAX_AM_POOL_SIZE = 5;

  public static final String AM_LAUNCH_NEW_AM_AFTER_APP_COMPLETION =
      AMPOOL_PREFIX + "launch-new-am-after-app-completion";
  public static final boolean DEFAULT_AM_LAUNCH_NEW_AM_AFTER_APP_COMPLETION =
      true;

  public static final String MAX_AM_LAUNCH_FAILURES =
      AMPOOL_PREFIX + "max-am-launch-failures";
  public static final int DEFAULT_MAX_AM_LAUNCH_FAILURES = 10;

  public static final String AM_STAGING_DIR =
      AMPOOL_PREFIX + "am.staging-dir";
  public static final String DEFAULT_AM_STAGING_DIR =
      "/tmp/tez/ampool/staging/";

  // RM Client proxy

  public static final String RM_PROXY_CLIENT_THREAD_COUNT =
      AMPOOL_PREFIX + "rm-proxy-client.thread-count";
  public static final int DEFAULT_RM_PROXY_CLIENT_THREAD_COUNT = 10;

  public static final String RM_PROXY_CLIENT_ADDRESS =
      AMPOOL_PREFIX + "address";
  public static final int DEFAULT_RM_PROXY_CLIENT_PORT = 10030;
  public static final String DEFAULT_RM_PROXY_CLIENT_ADDRESS = "0.0.0.0:" +
      DEFAULT_RM_PROXY_CLIENT_PORT;

  // MR AM related

  // Memory to allocate for lazy AM
  public static final String MR_AM_MEMORY_ALLOCATION_MB =
      AMPOOL_PREFIX + "mr-am.memory-allocation-mb";
  public static final int DEFAULT_MR_AM_MEMORY_ALLOCATION_MB = 1536;

  // Queue to launch LazyMRAM
  public static final String MR_AM_QUEUE_NAME =
      AMPOOL_PREFIX + "mr-am.queue-name";

  public static final String MR_AM_JOB_JAR_PATH =
      AMPOOL_PREFIX + "mr-am.job-jar-path";
  public static final String DEFAULT_MR_AM_JOB_JAR_PATH =
      "";

  public static final String APPLICATION_MASTER_CLASS =
      AMPOOL_PREFIX + "mr-am.application-master-class";
  public static final String DEFAULT_APPLICATION_MASTER_CLASS =
      "org.apache.hadoop.mapreduce.v2.app2.lazy.LazyMRAppMaster";

  public static final String LAZY_AM_POLLING_URL_ENV =
      "LAZY_AM_POLLING_URL";

  public static final String TMP_DIR_PATH =
      AMPOOL_PREFIX + "tmp-dir-path";
  public static final String DEFAULT_TMP_DIR_PATH = "/tmp/ampoolservice/";

  public static final String LAZY_AM_CONF_FILE_PATH =
      AMPOOL_PREFIX + "lazy-am-conf-file-path";

  /**
   * CLASSPATH for YARN applications. A comma-separated list of CLASSPATH
   * entries
   */
  public static final String AMPOOL_APPLICATION_CLASSPATH = AMPOOL_PREFIX
      + "application.classpath";

  /**
   * Default CLASSPATH for YARN applications. A comma-separated list of
   * CLASSPATH entries
   */
  public static final String[] DEFAULT_AMPOOL_APPLICATION_CLASSPATH = {
      ApplicationConstants.Environment.HADOOP_CONF_DIR.$(),
      ApplicationConstants.Environment.HADOOP_COMMON_HOME.$()
          + "/share/hadoop/common/*",
      ApplicationConstants.Environment.HADOOP_COMMON_HOME.$()
          + "/share/hadoop/common/lib/*",
      ApplicationConstants.Environment.HADOOP_HDFS_HOME.$()
          + "/share/hadoop/hdfs/*",
      ApplicationConstants.Environment.HADOOP_HDFS_HOME.$()
          + "/share/hadoop/hdfs/lib/*",
      ApplicationConstants.Environment.HADOOP_YARN_HOME.$()
          + "/share/hadoop/yarn/*",
      ApplicationConstants.Environment.HADOOP_YARN_HOME.$()
          + "/share/hadoop/yarn/lib/*"
  };

  public AMPoolConfiguration() {
    super();
  }

  public AMPoolConfiguration(Configuration conf) {
    super(conf);
    if (! (conf instanceof AMPoolConfiguration)) {
      this.reloadConfiguration();
    }
  }

}
