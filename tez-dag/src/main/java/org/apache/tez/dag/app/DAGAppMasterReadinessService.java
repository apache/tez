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
package org.apache.tez.dag.app;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an artifical service to be used in DAGAppMaster,
 * which can be added to have dependencies that are crucial in order to be
 * able to run DAGs.
 *
 */
public class DAGAppMasterReadinessService extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(DAGAppMasterReadinessService.class);

  private AtomicBoolean ready = new AtomicBoolean(false);
  private int timeoutMs;

  public DAGAppMasterReadinessService(String name) {
    super(name);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    timeoutMs = getConfig().getInt(TezConfiguration.TEZ_AM_READY_FOR_SUBMIT_TIMEOUT_MS,
        TezConfiguration.TEZ_AM_READY_FOR_SUBMIT_TIMEOUT_MS_DEFAULT);
    if (timeoutMs <= 0) {
      throw new TezException(
          "timeout <= 0 is not supported for " + TezConfiguration.TEZ_AM_READY_FOR_SUBMIT_TIMEOUT_MS);
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    ready.set(true);
  }

  /**
   * The waitToBeReady waits until this service really starts. When the serviceStart
   * is called and this service is ready, we can make sure that the dependency services
   * has already been started too.
   * @throws TezException
   */
  public void waitToBeReady() throws TezException {
    long start = System.currentTimeMillis();
    while (!ready.get()) {
      if (System.currentTimeMillis() - start > timeoutMs) {
        throw new TezException("App Master is not ready within the configured time period (" + timeoutMs + "ms). "
            + "Please check logs for AM service states.");
      }
      try {
        LOG.info("App is not ready yet, waiting 100ms");
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new TezException(e);
      }
    }
  }
}
