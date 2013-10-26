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

package org.apache.tez.mapreduce.client;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;

public class ClientCache {

  private final Configuration conf;
  private final ResourceMgrDelegate rm;

  private Map<JobID, ClientServiceDelegate> cache = 
      new HashMap<JobID, ClientServiceDelegate>();

  public ClientCache(Configuration conf, ResourceMgrDelegate rm) {
    this.conf = conf;
    this.rm = rm;
  }

  //TODO: evict from the cache on some threshold
  public synchronized ClientServiceDelegate getClient(JobID jobId) {
    ClientServiceDelegate client = cache.get(jobId);
    if (client == null) {
      client = new ClientServiceDelegate(conf, rm, jobId);
      cache.put(jobId, client);
    }
    return client;
  }

}
