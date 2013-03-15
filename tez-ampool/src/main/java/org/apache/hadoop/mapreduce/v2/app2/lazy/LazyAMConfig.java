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

package org.apache.hadoop.mapreduce.v2.app2.lazy;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;

public class LazyAMConfig extends Configuration {

  private static final String LAZY_AM_DEFAULT_XML_FILE = "lazy-mram-default.xml";
  private static final String LAZY_AM_SITE_XML_FILE = "lazy-mram-site.xml";
  public static final String LAZY_AM_JOB_XML_FILE = "lazy-mram-job.xml";

  static {
    Configuration.addDefaultResource(LAZY_AM_DEFAULT_XML_FILE);
    Configuration.addDefaultResource(LAZY_AM_SITE_XML_FILE);
    Configuration.addDefaultResource(LAZY_AM_JOB_XML_FILE);
  }

  public static final String LAZY_MR_AM_PREFIX =
      MRJobConfig.MR_AM_PREFIX + "lazy.";

  public static final String POLLING_INTERVAL_SECONDS =
      LAZY_MR_AM_PREFIX + "polling-interval.secs";
  public static final int DEFAULT_POLLING_INTERVAL_SECONDS = 1;

  public static final String PREALLOC_CONTAINER_COUNT =
      LAZY_MR_AM_PREFIX + "prealloc-container-count";
  public static final int DEFAULT_PREALLOC_CONTAINER_COUNT = 0;

  public LazyAMConfig() {
    super();
  }

  public LazyAMConfig(Configuration conf) {
    super(conf);
    if (! (conf instanceof LazyAMConfig)) {
      this.reloadConfiguration();
    }
  }

}
