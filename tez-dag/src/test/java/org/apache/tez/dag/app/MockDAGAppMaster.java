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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;
import org.apache.tez.serviceplugins.api.ContainerLaunchRequest;
import org.apache.tez.serviceplugins.api.ContainerLauncher;
import org.apache.tez.serviceplugins.api.ContainerLauncherContext;
import org.apache.tez.serviceplugins.api.ContainerStopRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.client.TezApiVersionInfo;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.launcher.ContainerLauncherManager;
import org.apache.tez.dag.app.rm.ContainerLauncherEvent;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.runtime.api.events.TaskAttemptCompletedEvent;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TaskStatistics;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class MockDAGAppMaster extends DAGAppMaster {
  
  private static final Logger LOG = LoggerFactory.getLogger(MockDAGAppMaster.class);
  MockContainerLauncher containerLauncher;
  private final AtomicBoolean launcherGoFlag;
  boolean initFailFlag;
  boolean startFailFlag;
  boolean recoveryFatalError = false;
  EventsDelegate eventsDelegate;
  CountersDelegate countersDelegate;
  StatisticsDelegate statsDelegate;
  ContainerDelegate containerDelegate;
  long launcherSleepTime = 1;
  boolean doSleep = true;
  int handlerConcurrency = 1;
  int numConcurrentContainers = 1;
  
  ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
  AtomicLong heartbeatCpu = new AtomicLong(0);
  AtomicLong heartbeatTime = new AtomicLong(0);
  AtomicLong numHearbeats = new AtomicLong(0);
  
  public static interface StatisticsDelegate {
    public TaskStatistics getStatistics(TaskSpec taskSpec);
  }
  public static interface CountersDelegate {
    public TezCounters getCounters(TaskSpec taskSpec);
  }
  
  public static interface EventsDelegate {
    public void getEvents(TaskSpec taskSpec, List<TezEvent> events, long time);
  }
  
  public static interface ContainerDelegate {
    public void stop(ContainerStopRequest event);
    public void launch(ContainerLaunchRequest event);
  }

  // mock container launcher does not launch real tasks.
  // Upon, launch of a container is simulates the container asking for tasks
  // Upon receiving a task it simulates completion of the tasks
  // It can be used to preempt the container for a given task
  public class MockContainerLauncher extends ContainerLauncher implements Runnable {

    BlockingQueue<ContainerLauncherEvent> eventQueue = new LinkedBlockingQueue<ContainerLauncherEvent>();
    Thread eventHandlingThread;
    ListeningExecutorService executorService;
    
    Map<ContainerId, ContainerData> containers = Maps.newConcurrentMap();
    ArrayBlockingQueue<Worker> workers;
    TaskCommunicatorManager taskCommunicatorManager;
    TezTaskCommunicatorImpl taskCommunicator;
    
    AtomicBoolean startScheduling = new AtomicBoolean(true);
    AtomicBoolean goFlag;
    boolean updateProgress = true;

    LinkedBlockingQueue<ContainerData> containersToProcess = new LinkedBlockingQueue<ContainerData>();
    
    Map<TezTaskID, Integer> preemptedTasks = Maps.newConcurrentMap();
    
    Map<TezTaskAttemptID, Integer> tasksWithStatusUpdates = Maps.newConcurrentMap();

    public MockContainerLauncher(AtomicBoolean goFlag,
                                 ContainerLauncherContext containerLauncherContext) {
      super(containerLauncherContext);
      this.goFlag = goFlag;
    }


    public class ContainerData {
      ContainerId cId;
      TezTaskAttemptID taId;
      String vName;
      TaskSpec taskSpec;
      ContainerLaunchContext launchContext;
      int numUpdates = 0;
      int nextFromEventId = 0;
      int nextPreRoutedFromEventId = 0;
      boolean completed;
      String cIdStr;
      AtomicBoolean remove = new AtomicBoolean(false);
      
      public ContainerData(ContainerId cId, ContainerLaunchContext context) {
        this.cId = cId;
        this.cIdStr = cId.toString();
        this.launchContext = context;
      }
      
      void remove() {
        remove.set(true);
      }
      
      void clear() {
        taId = null;
        vName = null;
        taskSpec = null;
        completed = false;
        launchContext = null;
        numUpdates = 0;
        nextFromEventId = 0;
        nextPreRoutedFromEventId = 0;
        cIdStr = null;
        remove.set(false);
      }
    }
    
    @Override
    public void start() throws Exception {
      taskCommunicatorManager = (TaskCommunicatorManager) getTaskCommunicatorManager();
      taskCommunicator = (TezTaskCommunicatorImpl) taskCommunicatorManager.getTaskCommunicator(0).getTaskCommunicator();
      eventHandlingThread = new Thread(this);
      eventHandlingThread.start();
      ExecutorService rawExecutor = Executors.newFixedThreadPool(handlerConcurrency,
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("MockLauncherExecutionThread [%d]")
              .build());
      this.executorService = MoreExecutors.listeningDecorator(rawExecutor);
      int numWorkers = numConcurrentContainers*2; // handle races that cause extra
      workers = new ArrayBlockingQueue<Worker>(numWorkers);
      for (int i=0; i<numWorkers; ++i) {
        workers.add(new Worker());
      }
    }

    @Override
    public void shutdown() throws Exception {
      if (eventHandlingThread != null) {
        eventHandlingThread.interrupt();
        eventHandlingThread.join(2000l);
      }
      if (executorService != null) {
        executorService.shutdownNow();
      }
    }


    @Override
    public void launchContainer(ContainerLaunchRequest launchRequest) {
      launch(launchRequest);
    }

    @Override
    public void stopContainer(ContainerStopRequest stopRequest) {
      stop(stopRequest);
    }

    void waitToGo() {
      if (goFlag == null) {
        return;
      }
      synchronized (goFlag) {
        goFlag.set(true);
        goFlag.notify();
        try {
          goFlag.wait();
        } catch (InterruptedException e) {
          throw new TezUncheckedException(e);
        }
      }
    }
    
    public void startScheduling(boolean value) {
      startScheduling.set(value);
    }
    
    public void updateProgress(boolean value) {
      this.updateProgress = value;
    }

    public Map<ContainerId, ContainerData> getContainers() {
      return containers;
    }
    
    public void preemptContainerForTask(TezTaskID tId, int uptoVersion) {
      preemptedTasks.put(tId, uptoVersion);
    }
    
    public void preemptContainer(ContainerData cData) {
      getTaskSchedulerManager().containerCompleted(0, null,
          ContainerStatus.newInstance(cData.cId, null, "Preempted", ContainerExitStatus.PREEMPTED));
      cData.clear();
    }
    
    public void setStatusUpdatesForTask(TezTaskAttemptID tId, int numUpdates) {
      tasksWithStatusUpdates.put(tId, numUpdates);
    }
    
    void stop(ContainerStopRequest event) {
      // remove from simulated container list
      containers.remove(event.getContainerId());
      if (containerDelegate != null) {
        containerDelegate.stop(event);
      }
      getContext().containerStopRequested(event.getContainerId());
    }

    void launch(ContainerLaunchRequest event) {
      // launch container by putting it in simulated container list
      ContainerData cData = new ContainerData(event.getContainerId(),
          event.getContainerLaunchContext());
      containers.put(event.getContainerId(), cData);
      containersToProcess.add(cData);
      if (containerDelegate != null) {
        containerDelegate.launch(event);
      }
      getContext().containerLaunched(event.getContainerId());
    }
    
    public void waitTillContainersLaunched() throws InterruptedException {
      while (containers.isEmpty()) {
        Thread.sleep(50);
      }
    }
    
    void incrementTime(long inc) {
      Clock clock = MockDAGAppMaster.this.getContext().getClock();
      if (clock instanceof MockClock) {
        ((MockClock) clock).incrementTime(inc);
      }
    }
    
    @Override
    public void run() {
      Thread.currentThread().setName("MockLauncher");
      // wait for test to sync with us and get a reference to us. Go when sync is done
      LOG.info("Waiting to go");
      waitToGo();
      LOG.info("Signal to go");
      try {
        while (true) {
          if (!startScheduling.get()) { // schedule when asked to do so by the test code
            Thread.sleep(launcherSleepTime);
            continue;
          }
          incrementTime(1000);
          ContainerData cData = containersToProcess.take();
          if (!cData.remove.get()) {
            Worker worker = workers.remove();
            worker.setContainerData(cData);
            ListenableFuture<Void> future = executorService.submit(worker);
            Futures.addCallback(future, worker.getCallback());            
          } else {
            containers.remove(cData.cId);
          }
          if (doSleep) {
            Thread.sleep(launcherSleepTime);
          }
        }
      } catch (InterruptedException ie) {
        LOG.warn("Exception in mock container launcher thread", ie);
      }
    }
    
    private void doHeartbeat(TezHeartbeatRequest request, ContainerData cData) throws Exception {
      long startTime = System.nanoTime();
      long startCpuTime = threadMxBean.getCurrentThreadCpuTime();
      TezHeartbeatResponse response = taskCommunicator.getUmbilical().heartbeat(request);
      if (response.shouldDie()) {
        cData.remove();
      } else {
        cData.nextFromEventId = response.getNextFromEventId();
        cData.nextPreRoutedFromEventId = response.getNextPreRoutedEventId();
        if (!response.getEvents().isEmpty()) {
          long stopTime = System.nanoTime();
          long stopCpuTime = threadMxBean.getCurrentThreadCpuTime();
          heartbeatTime.addAndGet((stopTime-startTime)/1000);
          heartbeatCpu.addAndGet((stopCpuTime-startCpuTime)/1000);
          numHearbeats.incrementAndGet();
        }
      }
    }
        
    class Worker implements Callable<Void> {
      class WorkerCallback implements FutureCallback<Void> {
        @Override
        public void onSuccess(Void arg) {
          completeOperation();
        }

        @Override
        public void onFailure(Throwable t) {
          LOG.error("Unexpected error during processing", t);
          Worker.this.cData.remove();
          completeOperation();
        }

        void completeOperation() {
          workers.add(Worker.this);
          containersToProcess.add(Worker.this.cData);
        }
      }

      volatile ContainerData cData;
      WorkerCallback callback = new WorkerCallback();
      
      WorkerCallback getCallback() {
        return callback;
      }
      
      void setContainerData(ContainerData cData) {
        this.cData = cData;
      }
      
      @Override
      public Void call() throws Exception {
        try {
          if (cData.taId == null) {
            // if container is not assigned a task, ask for a task
            ContainerTask cTask =
                taskCommunicator.getUmbilical().getTask(new ContainerContext(cData.cIdStr));
            if (cTask != null) {
              if (cTask.shouldDie()) {
                cData.remove();
              } else {
                cData.taId = cTask.getTaskSpec().getTaskAttemptID();
                cData.vName = cTask.getTaskSpec().getVertexName();
                cData.taskSpec = cTask.getTaskSpec();
              }
            }
          } else if (!cData.completed) {
            // container is assigned a task and task is not completed
            // complete the task or preempt the task
            Integer version = preemptedTasks.get(cData.taId.getTaskID());
            Integer updatesToMake = tasksWithStatusUpdates.get(cData.taId);
            if (cData.numUpdates == 0 || // do at least one update
                updatesToMake != null && cData.numUpdates < updatesToMake) {
              List<TezEvent> events = Lists.newArrayListWithCapacity(
                                      cData.taskSpec.getOutputs().size() + 1);
              if (cData.numUpdates == 0 && eventsDelegate != null) {
                eventsDelegate.getEvents(cData.taskSpec, events, MockDAGAppMaster.this.getContext().getClock().getTime());
              }
              TezCounters counters = null;
              if (countersDelegate != null) {
                 counters = countersDelegate.getCounters(cData.taskSpec);
              }
              TaskStatistics stats = null;
              if (statsDelegate != null) {
                stats = statsDelegate.getStatistics(cData.taskSpec);
              }
              cData.numUpdates++;
              float maxUpdates = (updatesToMake != null) ? updatesToMake.intValue() : 1;
              float progress = updateProgress ? cData.numUpdates/maxUpdates : 0f;
              events.add(new TezEvent(new TaskStatusUpdateEvent(counters, progress, stats, false), 
                  new EventMetaData(
                  EventProducerConsumerType.SYSTEM, cData.vName, "", cData.taId),
                  MockDAGAppMaster.this.getContext().getClock().getTime()));
              TezHeartbeatRequest request = new TezHeartbeatRequest(cData.numUpdates, events,
                  cData.nextPreRoutedFromEventId, cData.cIdStr, cData.taId, cData.nextFromEventId, 50000);
              doHeartbeat(request, cData);
            } else if (version != null && cData.taId.getId() <= version.intValue()) {
              preemptContainer(cData);
            } else {
              // send a done notification
              cData.completed = true;
              List<TezEvent> events = Collections.singletonList(new TezEvent(
                  new TaskAttemptCompletedEvent(), new EventMetaData(
                      EventProducerConsumerType.SYSTEM, cData.vName, "", cData.taId),
                  MockDAGAppMaster.this.getContext().getClock().getTime()));
              TezHeartbeatRequest request = new TezHeartbeatRequest(++cData.numUpdates, events,
                  cData.nextPreRoutedFromEventId, cData.cIdStr, cData.taId, cData.nextFromEventId, 10000);
              doHeartbeat(request, cData);
              cData.clear();
            }
          }
        } catch (Exception e) {
          // exception from TA listener. Behave like real. Die and continue with others 
          LOG.warn("Exception in mock container launcher thread for cId: " + cData.cIdStr, e);
          cData.remove();
        }
        return null;
      }
      
    }
  }
  
  public class MockHistoryEventHandler extends HistoryEventHandler {

    public MockHistoryEventHandler(AppContext context) {
      super(context);
    }

    @Override
    public boolean hasRecoveryFailed() {
      return recoveryFatalError;
    }
  }

  public class MockDAGAppMasterShutdownHandler extends DAGAppMasterShutdownHandler {
    public AtomicInteger shutdownInvoked = new AtomicInteger(0);
    public AtomicInteger shutdownInvokedWithoutDelay = new AtomicInteger(0);

    @Override
    public void shutdown() {
      shutdownInvokedWithoutDelay.incrementAndGet();
    }

    @Override
    public void shutdown(boolean now) {
      shutdownInvoked.incrementAndGet();
    }

    public boolean wasShutdownInvoked() {
      return shutdownInvoked.get() > 0 ||
          shutdownInvokedWithoutDelay.get() > 0;
    }

  }

  public MockDAGAppMaster(ApplicationAttemptId applicationAttemptId, ContainerId containerId,
      String nmHost, int nmPort, int nmHttpPort, Clock clock, long appSubmitTime,
      boolean isSession, String workingDirectory, String[] localDirs, String[] logDirs,
      AtomicBoolean launcherGoFlag, boolean initFailFlag, boolean startFailFlag,
      Credentials credentials, String jobUserName, int handlerConcurrency, int numConcurrentContainers) {
    super(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort, clock, appSubmitTime,
        isSession, workingDirectory, localDirs, logDirs,  new TezApiVersionInfo().getVersion(), 1,
        credentials, jobUserName, null);
    shutdownHandler = new MockDAGAppMasterShutdownHandler();
    this.launcherGoFlag = launcherGoFlag;
    this.initFailFlag = initFailFlag;
    this.startFailFlag = startFailFlag;
    Preconditions.checkArgument(handlerConcurrency > 0);
    this.handlerConcurrency = handlerConcurrency;
    this.numConcurrentContainers = numConcurrentContainers;
  }

  // use mock container launcher for tests
  @Override
  protected ContainerLauncherManager createContainerLauncherManager(
      List<NamedEntityDescriptor> containerLauncherDescirptors,
      boolean isLocal)
      throws UnknownHostException {
    UserPayload userPayload;
    try {
      userPayload = TezUtils.createUserPayloadFromConf(new Configuration(false));
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    ContainerLauncherContext containerLauncherContext =
        new ContainerLauncherContextImpl(getContext(), getTaskCommunicatorManager(), userPayload);
    containerLauncher = new MockContainerLauncher(launcherGoFlag, containerLauncherContext);
    return new ContainerLauncherManager(containerLauncher, getContext());
  }

  @Override
  protected HistoryEventHandler createHistoryEventHandler(AppContext appContext) {
    return new MockHistoryEventHandler(appContext);
  }

  public MockContainerLauncher getContainerLauncher() {
    return containerLauncher;
  }

  public MockDAGAppMasterShutdownHandler getShutdownHandler() {
    return (MockDAGAppMasterShutdownHandler) this.shutdownHandler;
  }
  
  public void clearStats() {
    heartbeatCpu.set(0);
    heartbeatTime.set(0);
    numHearbeats.set(0);
  }

  @Override
  public synchronized void serviceInit(Configuration conf) throws Exception {
    conf.setInt(TezConfiguration.TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS, numConcurrentContainers);
    super.serviceInit(conf);
    if (initFailFlag) {
      throw new Exception("FailInit");
    }
  }

  @Override
  public synchronized void serviceStart() throws Exception {
    super.serviceStart();
    if (startFailFlag) {
      throw new Exception("FailStart");
    }
  }
}
