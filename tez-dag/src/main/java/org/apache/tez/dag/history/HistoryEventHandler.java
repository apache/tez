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

package org.apache.tez.dag.history;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.history.ats.ATSService;
import org.apache.tez.dag.history.recovery.RecoveryService;
import org.apache.tez.dag.records.TezDAGID;

import java.util.concurrent.atomic.AtomicBoolean;

public class HistoryEventHandler extends CompositeService {

  private static Log LOG = LogFactory.getLog(HistoryEventHandler.class);

  private final AppContext context;
  private boolean yarnATSEnabled;
  private AtomicBoolean stopped = new AtomicBoolean(false);
  private ATSService atsService;
  private RecoveryService recoveryService;
  private boolean recoveryEnabled;

  public HistoryEventHandler(AppContext context) {
    super(HistoryEventHandler.class.getName());
    this.context = context;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    LOG.info("Initializing HistoryEventHandler");
    this.yarnATSEnabled = context.getAMConf().getBoolean(TezConfiguration.YARN_ATS_ENABLED,
        TezConfiguration.YARN_ATS_ENABLED_DEFAULT);
    this.recoveryEnabled = context.getAMConf().getBoolean(TezConfiguration.DAG_RECOVERY_ENABLED,
        TezConfiguration.DAG_RECOVERY_ENABLED_DEFAULT);
    if (yarnATSEnabled) {
      atsService = new ATSService();
      addService(atsService);
    }
    if (recoveryEnabled) {
      recoveryService = new RecoveryService(context);
      addService(recoveryService);
    }
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    LOG.info("Starting HistoryEventHandler");
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    LOG.info("Stopping HistoryEventHandler");
    super.serviceStop();
  }

  public void handle(DAGHistoryEvent event) {
    TezDAGID dagId = event.getDagID();
    String dagIdStr = "N/A";
    if(dagId != null) {
      dagIdStr = dagId.toString();
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Handling history event"
          + ", eventType=" + event.getHistoryEvent().getEventType());
    }
    if (recoveryEnabled && event.getHistoryEvent().isRecoveryEvent()) {
      recoveryService.handle(event);
    }
    if (yarnATSEnabled && event.getHistoryEvent().isHistoryEvent()) {
      atsService.handle(event);
    }

    // TODO at some point we should look at removing this once
    // there is a UI in place
    LOG.info("[HISTORY]"
        + "[DAG:" + dagIdStr + "]"
        + "[Event:" + event.getType().name() + "]"
        + ": " + event.getHistoryEvent().toString());
  }





}
