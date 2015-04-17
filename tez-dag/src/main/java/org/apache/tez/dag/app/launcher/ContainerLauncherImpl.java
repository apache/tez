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
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy;
import org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.rm.NMCommunicatorEvent;
import org.apache.tez.dag.app.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunchFailed;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunched;
import org.apache.tez.dag.app.rm.container.AMContainerEventStopFailed;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;

import com.google.common.util.concurrent.ThreadFactoryBuilder;


// TODO XXX: See what part of this lifecycle and state management can be simplified.
// Ideally, no state - only sendStart / sendStop.

// TODO XXX: Review this entire code and clean it up.

/**
 * This class is responsible for launching of containers.
 */
public class ContainerLauncherImpl extends AbstractService implements
    ContainerLauncher {

  // TODO XXX Ensure the same thread is used to launch / stop the same container. Or - ensure event ordering.
  static final Logger LOG = LoggerFactory.getLogger(ContainerLauncherImpl.class);

  private ConcurrentHashMap<ContainerId, Container> containers =
    new ConcurrentHashMap<ContainerId, Container>();
  private AppContext context;
  protected ThreadPoolExecutor launcherPool;
  protected static final int INITIAL_POOL_SIZE = 10;
  private int limitOnPoolSize;
  private Thread eventHandlingThread;
  protected BlockingQueue<NMCommunicatorEvent> eventQueue =
      new LinkedBlockingQueue<NMCommunicatorEvent>();
  private Clock clock;
  private ContainerManagementProtocolProxy cmProxy;
  private AtomicBoolean serviceStopped = new AtomicBoolean(false);

  private Container getContainer(NMCommunicatorEvent event) {
    ContainerId id = event.getContainerId();
    Container c = containers.get(id);
    if(c == null) {
      c = new Container(event.getContainerId(),
          event.getNodeId().toString(), event.getContainerToken());
      Container old = containers.putIfAbsent(id, c);
      if(old != null) {
        c = old;
      }
    }
    return c;
  }

  private void removeContainerIfDone(ContainerId id) {
    Container c = containers.get(id);
    if(c != null && c.isCompletelyDone()) {
      containers.remove(id);
    }
  }

  private static enum ContainerState {
    PREP, FAILED, RUNNING, DONE, KILLED_BEFORE_LAUNCH
  }

  private class Container {
    private ContainerState state;
    // store enough information to be able to cleanup the container
    private ContainerId containerID;
    final private String containerMgrAddress;
    private Token containerToken;

    public Container(ContainerId containerID,
        String containerMgrAddress, Token containerToken) {
      this.state = ContainerState.PREP;
      this.containerMgrAddress = containerMgrAddress;
      this.containerID = containerID;
      this.containerToken = containerToken;
    }

    public synchronized boolean isCompletelyDone() {
      return state == ContainerState.DONE || state == ContainerState.FAILED;
    }

    @SuppressWarnings("unchecked")
    public synchronized void launch(NMCommunicatorLaunchRequestEvent event) {
      LOG.info("Launching Container with Id: " + event.getContainerId());
      if(this.state == ContainerState.KILLED_BEFORE_LAUNCH) {
        state = ContainerState.DONE;
        sendContainerLaunchFailedMsg(event.getContainerId(),
            "Container was killed before it was launched");
        return;
      }

      ContainerManagementProtocolProxyData proxy = null;
      try {

        proxy = getCMProxy(containerID, containerMgrAddress,
            containerToken);

        // Construct the actual Container
        ContainerLaunchContext containerLaunchContext =
          event.getContainerLaunchContext();

        // Now launch the actual container
        StartContainerRequest startRequest = Records
          .newRecord(StartContainerRequest.class);
        startRequest.setContainerToken(event.getContainerToken());
        startRequest.setContainerLaunchContext(containerLaunchContext);

        StartContainersResponse response =
            proxy.getContainerManagementProtocol().startContainers(
                StartContainersRequest.newInstance(
                    Collections.singletonList(startRequest)));
        if (response.getFailedRequests() != null
            && !response.getFailedRequests().isEmpty()) {
          throw response.getFailedRequests().get(containerID).deSerialize();
        }

        // after launching, send launched event to task attempt to move
        // it from ASSIGNED to RUNNING state
        context.getEventHandler().handle(
            new AMContainerEventLaunched(containerID));
        ContainerLaunchedEvent lEvt = new ContainerLaunchedEvent(
            containerID, clock.getTime(), context.getApplicationAttemptId());
        context.getHistoryHandler().handle(new DAGHistoryEvent(
            null, lEvt));

        this.state = ContainerState.RUNNING;
      } catch (Throwable t) {
        String message = "Container launch failed for " + containerID + " : "
            + ExceptionUtils.getStackTrace(t);
        this.state = ContainerState.FAILED;
        sendContainerLaunchFailedMsg(containerID, message);
      } finally {
        if (proxy != null) {
          cmProxy.mayBeCloseProxy(proxy);
        }
      }
    }

    @SuppressWarnings("unchecked")
    public synchronized void kill() {

      if(isCompletelyDone()) {
        return;
      }
      if(this.state == ContainerState.PREP) {
        this.state = ContainerState.KILLED_BEFORE_LAUNCH;
      } else {
        LOG.info("Sending a stop request to the NM for ContainerId: "
            + containerID);

        ContainerManagementProtocolProxyData proxy = null;
        try {
          proxy = getCMProxy(this.containerID, this.containerMgrAddress,
              this.containerToken);

            // kill the remote container if already launched
            StopContainersRequest stopRequest = Records
              .newRecord(StopContainersRequest.class);
            stopRequest.setContainerIds(Collections.singletonList(containerID));

            proxy.getContainerManagementProtocol().stopContainers(stopRequest);

            // If stopContainer returns without an error, assuming the stop made
            // it over to the NodeManager.
          context.getEventHandler().handle(
              new AMContainerEvent(containerID, AMContainerEventType.C_NM_STOP_SENT));
        } catch (Throwable t) {

          // ignore the cleanup failure
          String message = "cleanup failed for container "
            + this.containerID + " : "
            + ExceptionUtils.getStackTrace(t);
          context.getEventHandler().handle(
              new AMContainerEventStopFailed(containerID, message));
          LOG.warn(message);
          this.state = ContainerState.DONE;
          return;
        } finally {
          if (proxy != null) {
            cmProxy.mayBeCloseProxy(proxy);
          }
        }
        this.state = ContainerState.DONE;
      }
    }
  }

  public ContainerLauncherImpl(AppContext context) {
    super(ContainerLauncherImpl.class.getName());
    this.context = context;
    this.clock = context.getClock();
  }

  @Override
  public synchronized void serviceInit(Configuration config) {
    Configuration conf = new Configuration(config);
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        0);
    this.limitOnPoolSize = conf.getInt(
        TezConfiguration.TEZ_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT,
        TezConfiguration.TEZ_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT_DEFAULT);
    LOG.info("Upper limit on the thread pool size is " + this.limitOnPoolSize);
  }

  @Override
  public void serviceStart() {
    // pass a copy of config to ContainerManagementProtocolProxy until YARN-3497 is fixed
    cmProxy =
        new ContainerManagementProtocolProxy(new Configuration(getConfig()));

    ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat(
        "ContainerLauncher #%d").setDaemon(true).build();

    // Start with a default core-pool size of 10 and change it dynamically.
    launcherPool = new ThreadPoolExecutor(INITIAL_POOL_SIZE,
        Integer.MAX_VALUE, 1, TimeUnit.HOURS,
        new LinkedBlockingQueue<Runnable>(),
        tf, new CustomizedRejectedExecutionHandler());
    eventHandlingThread = new Thread() {
      @Override
      public void run() {
        NMCommunicatorEvent event = null;
        while (!Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            if(!serviceStopped.get()) {
              LOG.error("Returning, interrupted : " + e);
            }
            return;
          }
          int poolSize = launcherPool.getCorePoolSize();

          // See if we need up the pool size only if haven't reached the
          // maximum limit yet.
          if (poolSize != limitOnPoolSize) {

            // nodes where containers will run at *this* point of time. This is
            // *not* the cluster size and doesn't need to be.
            int numNodes = context.getNodeTracker().getNumNodes();
            int idealPoolSize = Math.min(limitOnPoolSize, numNodes);

            if (poolSize < idealPoolSize) {
              // Bump up the pool size to idealPoolSize+INITIAL_POOL_SIZE, the
              // later is just a buffer so we are not always increasing the
              // pool-size
              int newPoolSize = Math.min(limitOnPoolSize, idealPoolSize
                  + INITIAL_POOL_SIZE);
              LOG.info("Setting ContainerLauncher pool size to " + newPoolSize
                  + " as number-of-nodes to talk to is " + numNodes);
              launcherPool.setCorePoolSize(newPoolSize);
            }
          }

          // the events from the queue are handled in parallel
          // using a thread pool
          launcherPool.execute(createEventProcessor(event));

          // TODO: Group launching of multiple containers to a single
          // NodeManager into a single connection
        }
      }
    };
    eventHandlingThread.setName("ContainerLauncher Event Handler");
    eventHandlingThread.start();
  }

  private void shutdownAllContainers() {
    for (Container ct : this.containers.values()) {
      if (ct != null) {
        ct.kill();
      }
    }
  }

  @Override
  public void serviceStop() {
    if(!serviceStopped.compareAndSet(false, true)) {
      LOG.info("Ignoring multiple stops");
      return;
    }
    // shutdown any containers that might be left running
    shutdownAllContainers();
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }
    if (launcherPool != null) {
      launcherPool.shutdownNow();
    }
  }

  protected EventProcessor createEventProcessor(NMCommunicatorEvent event) {
    return new EventProcessor(event);
  }

  protected ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData getCMProxy(
      ContainerId containerID, final String containerManagerBindAddr,
      Token containerToken) throws IOException {
    return cmProxy.getProxy(containerManagerBindAddr, containerID);
  }

  /**
   * Setup and start the container on remote nodemanager.
   */
  class EventProcessor implements Runnable {
    private NMCommunicatorEvent event;

    EventProcessor(NMCommunicatorEvent event) {
      this.event = event;
    }

    @Override
    public void run() {
      LOG.info("Processing the event " + event.toString());

      // Load ContainerManager tokens before creating a connection.
      // TODO: Do it only once per NodeManager.
      ContainerId containerID = event.getContainerId();

      Container c = getContainer(event);
      switch(event.getType()) {

      case CONTAINER_LAUNCH_REQUEST:
        NMCommunicatorLaunchRequestEvent launchEvent
            = (NMCommunicatorLaunchRequestEvent) event;
        c.launch(launchEvent);
        break;

      case CONTAINER_STOP_REQUEST:
        c.kill();
        break;
      }
      removeContainerIfDone(containerID);
    }
  }

  /**
   * ThreadPoolExecutor.submit may fail if you are submitting task
   * when ThreadPoolExecutor is shutting down (DAGAppMaster is shutting down).
   * Use this CustomizedRejectedExecutionHandler to just logging rather than abort the application.
   */
  private static class CustomizedRejectedExecutionHandler implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      LOG.warn("Can't submit task to ThreadPoolExecutor:" + executor);
    }
  }

  @SuppressWarnings("unchecked")
  void sendContainerLaunchFailedMsg(ContainerId containerId,
      String message) {
    LOG.error(message);
    context.getEventHandler().handle(new AMContainerEventLaunchFailed(containerId, message));
  }

  @Override
  public void handle(NMCommunicatorEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new TezUncheckedException(e);
    }
  }
}
