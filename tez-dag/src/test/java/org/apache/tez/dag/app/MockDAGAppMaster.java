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
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.launcher.ContainerLauncher;
import org.apache.tez.dag.app.rm.NMCommunicatorEvent;
import org.apache.tez.dag.app.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.tez.dag.app.rm.NMCommunicatorStopRequestEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunched;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.events.TaskAttemptCompletedEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;

import com.google.common.collect.Maps;

@SuppressWarnings("unchecked")
public class MockDAGAppMaster extends DAGAppMaster {
  
  MockContainerLauncher containerLauncher;
  
  // mock container launcher does not launch real tasks.
  // Upon, launch of a container is simulates the container asking for tasks
  // Upon receiving a task it simulates completion of the tasks
  // It can be used to preempt the container for a given task
  public class MockContainerLauncher extends AbstractService implements ContainerLauncher, Runnable {

    BlockingQueue<NMCommunicatorEvent> eventQueue = new LinkedBlockingQueue<NMCommunicatorEvent>();
    Thread eventHandlingThread;
    
    Map<ContainerId, ContainerData> containers = Maps.newConcurrentMap();
    TaskAttemptListenerImpTezDag taListener;
    
    AtomicBoolean startScheduling = new AtomicBoolean(true);
    AtomicBoolean goFlag;
    
    Map<TezTaskID, Integer> preemptedTasks = Maps.newConcurrentMap();
    
    public MockContainerLauncher(AtomicBoolean goFlag) {
      super("MockContainerLauncher");
      this.goFlag = goFlag;
    }

    public class ContainerData {
      ContainerId cId;
      TezTaskAttemptID taId;
      String vName;
      boolean completed;
      
      public ContainerData(ContainerId cId) {
        this.cId = cId;
      }
      
      void clear() {
        taId = null;
        vName = null;
        completed = false;
      }
    }
    
    @Override
    public void serviceStart() throws Exception {
      taListener = (TaskAttemptListenerImpTezDag) getTaskAttemptListener();
      eventHandlingThread = new Thread(this);
      eventHandlingThread.start();
    }

    @Override
    public void serviceStop() throws Exception {
      if (eventHandlingThread != null) {
        eventHandlingThread.interrupt();
        eventHandlingThread.join(2000l);
      }
    }
    
    @Override
    public void handle(NMCommunicatorEvent event) {
      switch (event.getType()) {
      case CONTAINER_LAUNCH_REQUEST:
        launch((NMCommunicatorLaunchRequestEvent) event);
        break;
      case CONTAINER_STOP_REQUEST:
        stop((NMCommunicatorStopRequestEvent)event);
        break;
      }
    }
    
    
    void waitToGo() {
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

    public Map<ContainerId, ContainerData> getContainers() {
      return containers;
    }
    
    public void preemptContainerForTask(TezTaskID tId, int uptoVersion) {
      preemptedTasks.put(tId, uptoVersion);
    }
    
    public void preemptContainer(ContainerData cData) {
      getTaskSchedulerEventHandler().containerCompleted(null, 
          ContainerStatus.newInstance(cData.cId, null, "Preempted", ContainerExitStatus.PREEMPTED));
      cData.clear();
    }
    
    void stop(NMCommunicatorStopRequestEvent event) {
      // remove from simulated container list
      containers.remove(event.getContainerId());
      getContext().getEventHandler().handle(
          new AMContainerEvent(event.getContainerId(), AMContainerEventType.C_NM_STOP_SENT));
    }

    void launch(NMCommunicatorLaunchRequestEvent event) {
      // launch container by putting it in simulated container list
      containers.put(event.getContainerId(), new ContainerData(event.getContainerId()));
      getContext().getEventHandler().handle(new AMContainerEventLaunched(event.getContainerId()));      
    }

    @Override
    public void run() {
      // wait for test to sync with us and get a reference to us. Go when sync is done
      waitToGo();
      while(true) {
        if (!startScheduling.get()) { // schedule when asked to do so by the test code
          continue;
        }
        for (Map.Entry<ContainerId, ContainerData> entry : containers.entrySet()) {
          ContainerData cData = entry.getValue();
          ContainerId cId = entry.getKey();
          if (cData.taId == null) {
            // if container is not assigned a task, ask for a task
            try {
              ContainerTask cTask = taListener.getTask(new ContainerContext(cId.toString()));
              if (cTask == null) {
                continue;
              }
              if (cTask.shouldDie()) {
                containers.remove(cId);
              } else {
                cData.taId = cTask.getTaskSpec().getTaskAttemptID();
                cData.vName = cTask.getTaskSpec().getVertexName();
              }
            } catch (IOException e) {
              e.printStackTrace();
            }
          } else if (!cData.completed) {
            // container is assigned a task and task is not completed
            // complete the task or preempt the task
            Integer version = preemptedTasks.get(cData.taId.getTaskID()); 
            if (version != null && cData.taId.getId() <= version.intValue()) {
              preemptContainer(cData);
            } else {
              // send a done notification
              TezVertexID vertexId = cData.taId.getTaskID().getVertexID();
              cData.completed = true;
              getContext().getEventHandler().handle(
                  new VertexEventRouteEvent(vertexId, Collections.singletonList(new TezEvent(
                      new TaskAttemptCompletedEvent(), new EventMetaData(
                          EventProducerConsumerType.SYSTEM, cData.vName, "", cData.taId)))));
              cData.clear();
            }
          }
        }
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          System.out.println("Interrupted in mock container launcher thread");
          break;
        }
      }
    }
    
  }

  public MockDAGAppMaster(ApplicationAttemptId applicationAttemptId, ContainerId containerId,
      String nmHost, int nmPort, int nmHttpPort, Clock clock, long appSubmitTime,
      boolean isSession, String workingDirectory, AtomicBoolean launcherGoFlag) {
    super(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort, clock, appSubmitTime,
        isSession, workingDirectory);
    containerLauncher = new MockContainerLauncher(launcherGoFlag);
  }
  
  // use mock container launcher for tests
  @Override
  protected ContainerLauncher createContainerLauncher(final AppContext context)
      throws UnknownHostException {
    return containerLauncher;
  }
  
  public MockContainerLauncher getContainerLauncher() {
    return containerLauncher;
  }
}
