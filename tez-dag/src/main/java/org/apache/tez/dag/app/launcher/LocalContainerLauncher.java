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

package org.apache.tez.dag.app.launcher;


import java.io.IOException;
import java.util.Random;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Clock;

import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.rm.NMCommunicatorEventType;
import org.apache.tez.dag.app.rm.NMCommunicatorEvent;
import org.apache.tez.dag.app.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunchFailed;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunched;
import org.apache.tez.dag.app.rm.container.AMContainerEventStopFailed;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;
import org.apache.tez.runtime.task.TezChild;


/**
 * Runs the container task locally in a thread.
 * Since all (sub)tasks share the same local directory, they must be executed
 * sequentially in order to avoid creating/deleting the same files/dirs.
 */
public class LocalContainerLauncher extends AbstractService implements
  ContainerLauncher {

  private static final Log LOG = LogFactory.getLog(LocalContainerLauncher.class);
  private final AppContext context;
  private TaskAttemptListener taskAttemptListener;
  private BlockingQueue<NMCommunicatorEvent> eventQueue =
    new LinkedBlockingQueue<NMCommunicatorEvent>();
  private static AtomicBoolean serviceStopped;

  private Clock clock;
  private LinkedBlockingQueue<Runnable> taskQueue =
    new LinkedBlockingQueue<Runnable>();

  private ThreadPoolExecutor taskExecutor;
  private ListeningExecutorService listeningExecutorService;
  private int poolSize;
  private final Random sleepTime = new Random();

  public LocalContainerLauncher(AppContext context,
                                TaskAttemptListener taskAttemptListener) {
    super(LocalContainerLauncher.class.getName());
    this.context = context;
    this.taskAttemptListener = taskAttemptListener;
  }

  @Override
  public void serviceStart() throws Exception {
    Thread eventHandlingThread = new Thread(new TezSubTaskRunner(),
      "LocalContainerLauncher-SubTaskRunner");
    eventHandlingThread.start();
    super.serviceStart();
  }

  @Override
  public synchronized void serviceInit(Configuration config) {
    serviceStopped = new AtomicBoolean(false);
    this.clock = context.getClock();
    this.poolSize = config.getInt(TezConfiguration.TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS,
      TezConfiguration.TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS_DEFAULT);
    int maxPoolSize = poolSize;

    this.taskExecutor = new ThreadPoolExecutor(poolSize, maxPoolSize, 60*1000,
      TimeUnit.SECONDS, taskQueue);
    this.listeningExecutorService = MoreExecutors.listeningDecorator(taskExecutor);
  }

  @SuppressWarnings("unchecked")
  void sendContainerLaunchFailedMsg(ContainerId containerId, String message) {
    LOG.error(message);
    context.getEventHandler().handle(new AMContainerEventLaunchFailed(containerId, message));
  }

  //should mimic container using threads
  //need to start all MapProcessor and RedProcessor here
  private class TezSubTaskRunner implements Runnable {

    ListenableFuture<Object> runningTask;
    TezSubTaskRunner() {}

    //launch tasks
    private void launch(NMCommunicatorEvent event) {
      NMCommunicatorLaunchRequestEvent launchEv = (NMCommunicatorLaunchRequestEvent)event;

      String containerIdStr = event.getContainerId().toString();
      String host = taskAttemptListener.getAddress().getAddress().getHostAddress();
      int port = taskAttemptListener.getAddress().getPort();
      String tokenIdentifier = context.getApplicationID().toString();

      String[] localDirs =
        StringUtils.getTrimmedStrings(System.getenv(Environment.LOCAL_DIRS.name()));

      try {
        runningTask = listeningExecutorService.submit(createSubTask(context.getAMConf(),
          host, port, containerIdStr, tokenIdentifier, context.getApplicationAttemptId().getAttemptId(),
          localDirs, (TezTaskUmbilicalProtocol) taskAttemptListener));
        Futures.addCallback(runningTask,
          new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
            }

            @Override
            public void onFailure(Throwable t) {
              LOG.error("Container launching failed", t);
            }
          }
          , taskExecutor);
      } catch (Throwable throwable) {
        LOG.info("Failed to start runSubTask thread!", throwable);
        sendContainerLaunchFailedMsg(event.getContainerId(), "Container Launching Failed!");
      }

      try{
        context.getEventHandler().handle(
          new AMContainerEventLaunched(launchEv.getContainerId()));
        ContainerLaunchedEvent lEvt = new ContainerLaunchedEvent(
          event.getContainerId(), clock.getTime(), context.getApplicationAttemptId());
        context.getHistoryHandler().handle(new DAGHistoryEvent(context.getCurrentDAGID(),lEvt));
      } catch (Throwable t) {
        String message = "Container launch failed for " +  event.getContainerId() + " : " +
          StringUtils.stringifyException(t);
        t.printStackTrace();
        LOG.error(message);
        context.getEventHandler().handle(new AMContainerEventLaunchFailed(event.getContainerId(), message));
      }
    }

    private void stop(NMCommunicatorEvent event) {
      try{
        context.getEventHandler().handle(
          new AMContainerEvent(event.getContainerId(), AMContainerEventType.C_NM_STOP_SENT));
      } catch (Throwable t) {
        // ignore the cleanup failure
        String message = "cleanup failed for container " +  event.getContainerId() + " : " +
          StringUtils.stringifyException(t);
        context.getEventHandler().handle(
          new AMContainerEventStopFailed(event.getContainerId(), message));
        LOG.warn(message);
      }
    }

    @Override
    public void run() {
      NMCommunicatorEvent event;
      while (!Thread.currentThread().isInterrupted()) {
        while (taskExecutor.getActiveCount() >= poolSize){
          try {
            LOG.info("Number of Running Tasks reach the uppper bound, sleep 1 seconds!:" +
              taskExecutor.getActiveCount());
            Thread.sleep(1000 + sleepTime.nextInt(10) * 1000);
          } catch (InterruptedException e) {
            LOG.warn("Thread Sleep has been interrupted!", e);
          }
        }

        try {
          event = eventQueue.take();
        } catch (InterruptedException e) {  // mostly via T_KILL? JOB_KILL?
          LOG.error("Returning, interrupted : ", e);
          return;
        }

        LOG.info("Processing the event " + event.toString());
        if (event.getType() == NMCommunicatorEventType.CONTAINER_LAUNCH_REQUEST) {
          launch(event);
        } else if (event.getType() == NMCommunicatorEventType.CONTAINER_STOP_REQUEST) {
          stop(event);
        } else {
          LOG.warn("Ignoring unexpected event " + event.toString());
        }
      }
    }
  } //end SubTaskRunner

  //create a SubTask
  private synchronized Callable<Object> createSubTask(final Configuration defaultConf, final String host,
      final int port, final String containerId, final String tokenIdentifier, final int attemptNumber,
      final String[] localDirs, final TezTaskUmbilicalProtocol tezTaskUmbilicalProtocol) {
    return new Callable<Object>() {
      @Override
      public Object call() {
        // Pull in configuration specified for the session.
        try {
          TezChild tezChild;
          tezChild = TezChild.newTezChild(defaultConf, host, port, containerId, tokenIdentifier,
            attemptNumber, localDirs);
          tezChild.setUmbilical(tezTaskUmbilicalProtocol);
          tezChild.run();
        } catch (TezException e) {
          //need to report the TezException and stop this task
          LOG.error("Failed to add User Specified TezConfiguration!", e);
        } catch (IOException e) {
          //need to report the IOException and stop this task
          LOG.error("IOE in launching task!", e);
        } catch (InterruptedException e) {
          //need to report the IOException and stop this task
          LOG.error("Interruption happened during launching task!", e);
        }
        return null;
      }
    };
  }

  @Override
  public void serviceStop() throws Exception {
    if (taskExecutor != null) {
      taskExecutor.shutdownNow();
    }

    if (listeningExecutorService != null) {
      listeningExecutorService.shutdownNow();
    }

    serviceStopped.set(true);
    super.serviceStop();
  }

  @Override
  public void handle(NMCommunicatorEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new TezUncheckedException(e);  // FIXME? YarnRuntimeException is "for runtime exceptions only"
    }
  }
}