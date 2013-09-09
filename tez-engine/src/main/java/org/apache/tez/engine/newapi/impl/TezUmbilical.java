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

package org.apache.tez.engine.newapi.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezTaskAttemptID;

/**
 * Interface to the RPC layer ( umbilical ) between the Tez AM and
 * a Tez Container's JVM.
 */
public class TezUmbilical extends AbstractService {

  private static final Log LOG = LogFactory.getLog(TezUmbilical.class);

  private final TezTaskUmbilicalProtocol umbilical;
  private Thread heartbeatThread;
  private Thread eventRouterThread;
  private AtomicBoolean stopped = new AtomicBoolean(false);
  private long amPollInterval;
  private final String containerIdStr;

  private TezTaskAttemptID currentTaskAttemptID;
  private int eventCounter = 0;
  private int maxEventsToGet = 0;
  private LinkedList<TezEvent> eventsToSend;
  private ConcurrentLinkedQueue<TezEvent> eventsToBeProcessed;

  public TezUmbilical(TezTaskUmbilicalProtocol umbilical,
      String containerIdStr) {
    super(TezUmbilical.class.getName());
    this.umbilical = umbilical;
    this.containerIdStr = containerIdStr;
    this.eventsToSend = new LinkedList<TezEvent>();
    this.eventsToBeProcessed = new ConcurrentLinkedQueue<TezEvent>();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    amPollInterval = conf.getLong(
        TezConfiguration.TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS,
        TezConfiguration.TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS_DEFAULT);
    maxEventsToGet = conf.getInt(
        TezConfiguration.TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT,
        TezConfiguration.TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT_DEFAULT);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    startHeartbeatThread();
    startRouterThread();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped.set(true);
    eventRouterThread.interrupt();
    super.serviceStop();
  }

  private void startHeartbeatThread() {
    heartbeatThread = new Thread(new Runnable() {
      public void run() {
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(amPollInterval);
            try {
              heartbeat();
            } catch (TezException e) {
              LOG.error("Error communicating with AM: " + e.getMessage() , e);
              // TODO TODONEWTEZ
            } catch (InvalidToken e) {
              LOG.error("Error in authencating with AM: ", e);
              // TODO TODONEWTEZ
            } catch (Exception e) {
              LOG.error("Error in heartbeating with AM. ", e);
              // TODO TODONEWTEZ
            }
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.warn("Heartbeat thread interrupted. Returning.");
            }
            return;
          }
        }
      }
    });
    heartbeatThread.setName("Tez Container Heartbeat Thread ["
        + containerIdStr + "]");
    heartbeatThread.start();
  }

  private void startRouterThread() {
    eventRouterThread = new Thread(new Runnable() {
      public void run() {
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            TezEvent e = eventsToBeProcessed.poll();
            if (e == null) {
              eventsToBeProcessed.wait();
            }
            // TODO TODONEWTEZ
            switch (e.getEventType()) {
            case DATA_MOVEMENT_EVENT:
              // redirect to input of current task
              if (!e.getDestinationInfo().getTaskAttemptID().equals(
                  currentTaskAttemptID)) {
                // error? or block?
              }
              // route to appropriate input
              break;
            case TASK_FAILED_EVENT:
              // route to ???
              break;
            case INPUT_DATA_ERROR_EVENT:
              // invalid event? ignore?
              break;
            }
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.warn("Event Router thread interrupted. Returning.");
            }
            return;
          }
        }
      }
    });
    eventRouterThread.setName("Tez Container Event Router Thread ["
        + containerIdStr + "]");
    eventRouterThread.start();
  }

  private synchronized void heartbeat() throws TezException, IOException {
    List<TezEvent> events = new ArrayList<TezEvent>();
    events.addAll(eventsToSend);
    TezHeartbeatRequest request = new TezHeartbeatRequest(events,
        currentTaskAttemptID, eventCounter, maxEventsToGet);
    TezHeartbeatResponse response = umbilical.heartbeat(request);
    eventsToSend.clear();
    eventCounter += response.getEvents().size();
    eventsToBeProcessed.addAll(response.getEvents());
    eventsToBeProcessed.notifyAll();
  }

  /**
   * Hook to ask the Tez AM for the next task to be run on the Container
   * @return Next task to be run
   * @throws IOException
   */
  public synchronized ContainerTask getNextTask(
      ContainerContext containerContext) throws IOException {
    ContainerTask task = umbilical.getTask(containerContext);
    if (task.getTaskSpec().getTaskAttemptID() != currentTaskAttemptID) {
      currentTaskAttemptID = task.getTaskSpec().getTaskAttemptID();
    }
    return task;
  }

  /**
   * Hook to query the Tez AM whether a particular Task Attempt can commit its
   * output.
   * @param attemptID Attempt ID of the Task that is waiting to commit.
   * attempts can commit.
   * @throws IOException
   */
  public synchronized boolean canCommit(TezTaskAttemptID attemptID)
      throws IOException {
    return umbilical.canCommit(attemptID);
  }

  /**
   * Inform the Tez AM that an attempt has failed.
   * @param attemptID Task Attempt ID of the failed attempt.
   * @param taskFailedEvent Event with details on the attempt failure.
   * @throws IOException
   */
  public synchronized void taskFailed(TezTaskAttemptID attemptID,
      TezEvent taskFailedEvent) throws IOException {
    umbilical.taskFailed(attemptID, taskFailedEvent);
  }

  public synchronized void addEvents(Collection<TezEvent> events) {
    eventsToSend.addAll(events);
  }
}
