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

import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.client.AMRMClient;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.service.AbstractService;

public class RMHeartbeatService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(RMHeartbeatService.class);

  private long heartbeatInterval = 1000l;
  Timer scheduleTimer;
  RMAllocateTimerTask rmAllocateTimerTask;
  private final AMRMClient amRmClient;
  private final AMPoolContext context;
  private final String trackerUrl;
  private final int trackerPort;

  public RMHeartbeatService(AMPoolContext context, String trackerUrl,
      int trackerPort) {
    super(RMHeartbeatService.class.getName());
    // TODO Auto-generated constructor stub
    this.context = context;
    this.amRmClient = context.getAMRMClient();
    this.trackerUrl = trackerUrl;
    this.trackerPort = trackerPort;
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
  }

  @Override
  public void start() {
    try {
      LOG.info("Registering AMPoolService with RM"
          + ", nmHost=" + context.getNMHost()
          + ", trackerPort=" + trackerPort
          + ", trackerUrl=" + trackerUrl);
      RegisterApplicationMasterResponse response =
          amRmClient.registerApplicationMaster(
              context.getNMHost(), trackerPort, trackerUrl);
      context.setMinResourceCapability(response.getMinimumResourceCapability());
      context.setMaxResourceCapability(response.getMaximumResourceCapability());
    } catch (YarnRemoteException e) {
      throw new RuntimeException(e);
    }

    scheduleTimer = new Timer("RMAllocateTimer", true);
    rmAllocateTimerTask = new RMAllocateTimerTask();
    scheduleTimer.scheduleAtFixedRate(rmAllocateTimerTask, heartbeatInterval,
        heartbeatInterval);
    super.start();
  }

  @Override
  public void stop() {
    if (rmAllocateTimerTask != null) {
      rmAllocateTimerTask.stop();
    }
  }

  private class RMAllocateTimerTask extends TimerTask {
    private volatile boolean shouldRun = true;

    @Override
    public void run() {
      if (shouldRun) {
        try {
          amRmClient.allocate(0);
        } catch (YarnRemoteException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }

    public void stop() {
      shouldRun = false;
      this.cancel();
    }
  }
}
