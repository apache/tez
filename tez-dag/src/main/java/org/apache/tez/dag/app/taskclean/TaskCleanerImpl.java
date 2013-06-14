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

package org.apache.tez.dag.app.taskclean;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.tez.dag.app.AppContext;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TaskCleanerImpl extends AbstractService implements TaskCleaner {

  private static final Log LOG = LogFactory.getLog(TaskCleanerImpl.class);

  private final AppContext context;
  private ThreadPoolExecutor launcherPool;
  private Thread eventHandlingThread;
  private BlockingQueue<TaskCleanupEvent> eventQueue =
      new LinkedBlockingQueue<TaskCleanupEvent>();

  public TaskCleanerImpl(AppContext context) {
    super("TaskCleaner");
    this.context = context;
  }

  public void serviceStart() {
    ThreadFactory tf = new ThreadFactoryBuilder()
      .setNameFormat("TaskCleaner #%d")
      .build();
    launcherPool = new ThreadPoolExecutor(5, 5, 1, 
        TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>(), tf);
    eventHandlingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        TaskCleanupEvent event = null;
        while (!Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
            return;
          }
          // the events from the queue are handled in parallel
          // using a thread pool
          launcherPool.execute(new EventProcessor(event));        }
      }
    });
    eventHandlingThread.setName("TaskCleaner Event Handler");
    eventHandlingThread.start();
  }

  public void serviceStop() {
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }
    if (launcherPool != null) {
      launcherPool.shutdown();
    }
    Iterator<TaskCleanupEvent> it = eventQueue.iterator();
    while (it.hasNext()) {
      TaskCleanupEvent ev = it.next();
      LOG.info("TaskCleaner.stop: Cleanup for: " + ev.getAttemptID());
      new EventProcessor(ev).run();
    }
  }

  private class EventProcessor implements Runnable {
    private TaskCleanupEvent event;

    EventProcessor(TaskCleanupEvent event) {
      this.event = event;
    }

    @Override
    public void run() {
      LOG.info("Processing the event " + event.toString());
      try {
        event.getCommitter().abortTask(event.getAttemptContext());
      } catch (Exception e) {
        LOG.warn("Task cleanup failed for attempt " + event.getAttemptID(), e);
      }
    }
  }

  @Override
  public void handle(TaskCleanupEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new TezUncheckedException(e);
    }
  }

}
