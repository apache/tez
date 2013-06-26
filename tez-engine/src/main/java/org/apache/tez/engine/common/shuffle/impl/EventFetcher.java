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
package org.apache.tez.engine.common.shuffle.impl;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.engine.api.Master;
import org.apache.tez.engine.records.TezDependentTaskCompletionEvent;
import org.apache.tez.engine.records.TezTaskDependencyCompletionEventsUpdate;

class EventFetcher extends Thread {
  private static final long SLEEP_TIME = 1000;
  private static final int MAX_RETRIES = 10;
  private static final int RETRY_PERIOD = 5000;
  private static final Log LOG = LogFactory.getLog(EventFetcher.class);

  private final TezTaskAttemptID reduce;
  private final Master umbilical;
  private final ShuffleScheduler scheduler;
  private int fromEventIdx = 0;
  private int maxEventsToFetch;
  private ExceptionReporter exceptionReporter = null;
  
  private int maxMapRuntime = 0;

  private volatile boolean stopped = false;
  
  public EventFetcher(TezTaskAttemptID reduce,
                      Master umbilical,
                      ShuffleScheduler scheduler,
                      ExceptionReporter reporter,
                      int maxEventsToFetch) {
    setName("EventFetcher for fetching Map Completion Events");
    setDaemon(true);    
    this.reduce = reduce;
    this.umbilical = umbilical;
    this.scheduler = scheduler;
    exceptionReporter = reporter;
    this.maxEventsToFetch = maxEventsToFetch;
  }

  @Override
  public void run() {
    int failures = 0;
    LOG.info(reduce + " Thread started: " + getName());
    
    try {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        try {
          int numNewMaps = getMapCompletionEvents();
          failures = 0;
          if (numNewMaps > 0) {
            LOG.info(reduce + ": " + "Got " + numNewMaps + " new map-outputs");
          }
          LOG.debug("GetMapEventsThread about to sleep for " + SLEEP_TIME);
          if (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(SLEEP_TIME);
          }
        } catch (InterruptedException e) {
          LOG.info("EventFetcher is interrupted.. Returning");
          return;
        } catch (IOException ie) {
          LOG.info("Exception in getting events", ie);
          // check to see whether to abort
          if (++failures >= MAX_RETRIES) {
            throw new IOException("too many failures downloading events", ie);
          }
          // sleep for a bit
          if (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(RETRY_PERIOD);
          }
        }
      }
    } catch (InterruptedException e) {
      return;
    } catch (Throwable t) {
      exceptionReporter.reportException(t);
      return;
    }
  }

  public void shutDown() {
    this.stopped = true;
    interrupt();
    try {
      join(5000);
    } catch(InterruptedException ie) {
      LOG.warn("Got interrupted while joining " + getName(), ie);
    }
  }
  
  /** 
   * Queries the {@link TaskTracker} for a set of map-completion events 
   * from a given event ID.
   * @throws IOException
   */  
  protected int getMapCompletionEvents() throws IOException {
    
    int numNewMaps = 0;
    TezDependentTaskCompletionEvent events[] = null;

    do {
      TezTaskDependencyCompletionEventsUpdate update =
          umbilical.getDependentTasksCompletionEvents(
              fromEventIdx,
              maxEventsToFetch,
              reduce);
      events = update.getDependentTaskCompletionEvents();
      LOG.debug("Got " + events.length + " map completion events from " +
               fromEventIdx);
      // Check if the reset is required.
      // Since there is no ordering of the task completion events at the
      // reducer, the only option to sync with the new jobtracker is to reset
      // the events index
      if (update.shouldReset()) {
        fromEventIdx = 0;
        scheduler.resetKnownMaps();
      }

      // Update the last seen event ID
      fromEventIdx += events.length;

      // Process the TaskCompletionEvents:
      // 1. Save the SUCCEEDED maps in knownOutputs to fetch the outputs.
      // 2. Save the OBSOLETE/FAILED/KILLED maps in obsoleteOutputs to stop
      //    fetching from those maps.
      // 3. Remove TIPFAILED maps from neededOutputs since we don't need their
      //    outputs at all.
      for (TezDependentTaskCompletionEvent event : events) {
        switch (event.getStatus()) {
        case SUCCEEDED:
          URI u = getBaseURI(event.getTaskTrackerHttp());
          scheduler.addKnownMapOutput(u.getHost() + ":" + u.getPort(),
              u.toString(),
              event.getTaskAttemptID());
          numNewMaps ++;
          int duration = event.getTaskRunTime();
          if (duration > maxMapRuntime) {
            maxMapRuntime = duration;
            scheduler.informMaxMapRunTime(maxMapRuntime);
          }
          break;
        case FAILED:
        case KILLED:
        case OBSOLETE:
          scheduler.obsoleteMapOutput(event.getTaskAttemptID());
          LOG.info("Ignoring obsolete output of " + event.getStatus() + 
              " map-task: '" + event.getTaskAttemptID() + "'");
          break;
        case TIPFAILED:
          scheduler.tipFailed(event.getTaskAttemptID().getTaskID());
          LOG.info("Ignoring output of failed map TIP: '" +  
              event.getTaskAttemptID() + "'");
          break;
        }
      }
    } while (events.length == maxEventsToFetch);

    return numNewMaps;
  }
  
  private URI getBaseURI(String url) {
    StringBuffer baseUrl = new StringBuffer(url);
    if (!url.endsWith("/")) {
      baseUrl.append("/");
    }
    baseUrl.append("mapOutput?job=");
    // TODO TEZ HACK to get shuffle working. ApplicationId vs JobId shuffle handler.
    // FIXME dag or application or ???
    String jobID = reduce.getTaskID().getVertexID().getDAGId().
        getApplicationId().toString().replace("application", "job");

    baseUrl.append(jobID);
    baseUrl.append("&reduce=");
    baseUrl.append(reduce.getTaskID().getId());
    baseUrl.append("&map=");
    URI u = URI.create(baseUrl.toString());
    return u;
  }
}
