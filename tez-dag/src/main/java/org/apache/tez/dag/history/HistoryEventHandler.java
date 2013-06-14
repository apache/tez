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
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.tez.dag.app.AppContext;

public class HistoryEventHandler extends AbstractService
implements EventHandler<DAGHistoryEvent> {

  private static Log LOG = LogFactory.getLog(HistoryEventHandler.class);

  private final AppContext context;

  public HistoryEventHandler(AppContext context) {
    super(HistoryEventHandler.class.getName());
    this.context = context;
  }

  @Override
  public void serviceStart() {
    LOG.info("Starting HistoryEventHandler");
  }

  @Override
  public void serviceStop() {
    LOG.info("Stopping HistoryEventHandler");
  }

  @Override
  public void handle(DAGHistoryEvent event) {
    LOG.info("[HISTORY]"
        + "[DAG:" + context.getDAGID().toString() + "]"
        + "[Event:" + event.getType().name() + "]"
        + ": " + event.getHistoryEvent().getBlob().toString());
  }

}
