/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.history.logging.impl;

import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.logging.HistoryLoggingService;

/**
 * No-op history logger.
 */
public class DevNullHistoryLoggingService extends HistoryLoggingService {

  public DevNullHistoryLoggingService() {
    super(DevNullHistoryLoggingService.class.getName());
  }

  @Override
  public void handle(DAGHistoryEvent event) {
    // Sending all events to /dev/null
    // Done.
  }

}
