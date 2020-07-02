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

package org.apache.tez.dag.app.dag;

import static org.apache.tez.dag.app.dag.VertexState.FAILED;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.Preconditions;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.RootInputLeafOutput;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.event.VertexEventRootInputFailed;
import org.apache.tez.dag.app.dag.event.VertexEventRootInputInitialized;
import org.apache.tez.dag.app.dag.impl.AMUserCodeException;
import org.apache.tez.dag.app.dag.impl.AMUserCodeException.Source;
import org.apache.tez.dag.app.dag.impl.TezRootInputInitializerContextImpl;
import org.apache.tez.dag.app.dag.impl.VertexImpl;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;

public class RootInputInitializerManager {

  private static final Logger LOG = LoggerFactory.getLogger(RootInputInitializerManager.class);

  @VisibleForTesting
  protected ListeningExecutorService executor;
  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  private volatile boolean isStopped = false;
  private final UserGroupInformation dagUgi;
  private final StateChangeNotifier entityStateTracker;

  private final Vertex vertex;
  private final AppContext appContext;

  @VisibleForTesting
  final Map<String, InitializerWrapper> initializerMap = new ConcurrentHashMap<>();

  public RootInputInitializerManager(Vertex vertex, AppContext appContext,
                                     UserGroupInformation dagUgi, StateChangeNotifier stateTracker) {
    this.appContext = appContext;
    this.vertex = vertex;
    this.eventHandler = appContext.getEventHandler();
    this.executor = appContext.getExecService();
    this.dagUgi = dagUgi;
    this.entityStateTracker = stateTracker;
  }


  public void runInputInitializers(
          List<RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> inputs, List<TezEvent> pendingInitializerEvents) {

    executor.submit(() -> createAndStartInitializing(inputs, pendingInitializerEvents));
  }

  private void createAndStartInitializing(List<RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> inputs, List<TezEvent> pendingInitializerEvents) {
    String current = null;
    try {
      List<InitializerWrapper> result = new ArrayList<>();
      for (RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> each : inputs) {
        current = each.getName();
        InitializerWrapper initializer = createInitializerWrapper(each);
        initializerMap.put(each.getName(), initializer);
        registerPendingVertex(each, initializer);
        result.add(initializer);
      }
      handleInitializerEvents(pendingInitializerEvents);
      pendingInitializerEvents.clear();
      for (InitializerWrapper inputWrapper : result) {
        executor.submit(() -> runInitializerAndProcessResult(inputWrapper));
      }
    } catch (Throwable t) {
      VertexImpl vertexImpl = (VertexImpl) vertex;
      String msg = "Fail to create InputInitializerManager, " + ExceptionUtils.getStackTrace(t);
      LOG.info(msg);
      vertexImpl.finished(FAILED, VertexTerminationCause.INIT_FAILURE, msg);
      eventHandler.handle(new VertexEventRootInputFailed(vertex.getVertexId(), current,
              new AMUserCodeException(AMUserCodeException.Source.InputInitializer, t)));

    }
  }

  private void runInitializerAndProcessResult(InitializerWrapper initializer) {
    try {
      List<Event> result = runInitializer(initializer);
      LOG.info("Succeeded InputInitializer for Input: " + initializer.getInput().getName() +
                  " on vertex " + initializer.getVertexLogIdentifier());
      eventHandler.handle(new VertexEventRootInputInitialized(vertex.getVertexId(),
          initializer.getInput().getName(), result));
    } catch (Throwable t) {
        if (t instanceof UndeclaredThrowableException) {
          t = t.getCause();
        }
        LOG.info("Failed InputInitializer for Input: " + initializer.getInput().getName() +
                    " on vertex " + initializer.getVertexLogIdentifier());
        eventHandler.handle(new VertexEventRootInputFailed(vertex.getVertexId(), initializer.getInput().getName(),
                    new AMUserCodeException(Source.InputInitializer,t)));
    } finally {
      initializer.setComplete();
    }
  }

  private List<Event> runInitializer(InitializerWrapper initializer) throws IOException, InterruptedException {
    return dagUgi.doAs((PrivilegedExceptionAction<List<Event>>) () -> {
      LOG.info(
              "Starting InputInitializer for Input: " + initializer.getInput().getName() +
                      " on vertex " + initializer.getVertexLogIdentifier());
      try {
        TezUtilsInternal.setHadoopCallerContext(appContext.getHadoopShim(),
                initializer.vertexId);
        return initializer.getInitializer().initialize();
      } finally {
        appContext.getHadoopShim().clearHadoopCallerContext();
      }
    });
  }

  private InitializerWrapper createInitializerWrapper(RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> input) throws TezException {
    InputInitializerContext context =
            new TezRootInputInitializerContextImpl(input, vertex, appContext, this);
    try {
      TezUtilsInternal.setHadoopCallerContext(appContext.getHadoopShim(), vertex.getVertexId());
      InputInitializer initializer = createInitializer(input, context);
      return new InitializerWrapper(input, initializer, context, vertex, entityStateTracker, appContext);
    } finally {
      appContext.getHadoopShim().clearHadoopCallerContext();
    }
  }

  private void registerPendingVertex(RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> input, InitializerWrapper initializerWrapper) {
    // Register pending vertex update registrations
    List<VertexUpdateRegistrationHolder> vertexUpdateRegistrations = pendingVertexRegistrations.removeAll(input.getName());
    if (vertexUpdateRegistrations != null) {
      for (VertexUpdateRegistrationHolder h : vertexUpdateRegistrations) {
        initializerWrapper.registerForVertexStateUpdates(h.vertexName, h.stateSet);
      }
    }
  }

  @VisibleForTesting
  protected InputInitializer createInitializer(final RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>
      input, final InputInitializerContext context) throws TezException {
    try {
      return dagUgi.doAs(new PrivilegedExceptionAction<InputInitializer>() {
        @Override
        public InputInitializer run() throws Exception {
          InputInitializer initializer = ReflectionUtils
              .createClazzInstance(input.getControllerDescriptor().getClassName(),
                  new Class[]{InputInitializerContext.class}, new Object[]{context});
          return initializer;
        }
      });
    } catch (IOException e) {
      throw new TezException(e);
    } catch (InterruptedException e) {
      throw new TezException(e);
    } catch (UndeclaredThrowableException e) {
      if (e.getCause() instanceof TezException) {
        throw (TezException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  public void handleInitializerEvents(List<TezEvent> events) {
    ListMultimap<InitializerWrapper, TezEvent> eventMap = LinkedListMultimap.create();

    for (TezEvent tezEvent : events) {
      Preconditions.checkState(tezEvent.getEvent() instanceof InputInitializerEvent);
      InputInitializerEvent event = (InputInitializerEvent)tezEvent.getEvent();
      Preconditions.checkState(vertex.getName().equals(event.getTargetVertexName()),
          "Received event for incorrect vertex");
      Objects.requireNonNull(event.getTargetInputName(), "target input name must be set");
      InitializerWrapper initializer = initializerMap.get(event.getTargetInputName());
      Preconditions.checkState(initializer != null,
          "Received event for unknown input : " + event.getTargetInputName());
      eventMap.put(initializer, tezEvent);
    }

    // This is a restriction based on current flow - i.e. events generated only by initialize().
    // TODO Rework the flow as per the first comment on TEZ-1076
    if (isStopped) {
      LOG.warn("InitializerManager already stopped for " + vertex.getLogIdentifier() +
          " Dropping " + events.size() + " events");
    }

    for (Map.Entry<InitializerWrapper, Collection<TezEvent>> entry : eventMap.asMap().entrySet()) {
      InitializerWrapper initializerWrapper = entry.getKey();
      if (initializerWrapper.isComplete()) {
        LOG.warn(entry.getValue().size() +
            " events targeted at vertex " + vertex.getLogIdentifier() +
            ", initializerWrapper for Input: " +
            initializerWrapper.getInput().getName() +
            " will be dropped, since Input has already been initialized.");
      } else {
        initializerWrapper.handleInputInitializerEvents(entry.getValue());
      }

    }
  }

  private static class VertexUpdateRegistrationHolder {
    private VertexUpdateRegistrationHolder(String vertexName, Set<org.apache.tez.dag.api.event.VertexState> stateSet) {
      this.vertexName = vertexName;
      this.stateSet = stateSet;
    }
    private final String vertexName;
    private final Set<org.apache.tez.dag.api.event.VertexState> stateSet;
  }

  // This doesn't need to be thread safe, since initializers are not created in separate threads,
  // they're only executed in separate threads.
  private final ListMultimap<String, VertexUpdateRegistrationHolder> pendingVertexRegistrations =
      LinkedListMultimap.create();

  public void registerForVertexUpdates(String vertexName, String inputName,
                                       @Nullable Set<org.apache.tez.dag.api.event.VertexState> stateSet) {
    Objects.requireNonNull(vertexName, "VertexName cannot be null: " + vertexName);
    Objects.requireNonNull(inputName, "InputName cannot be null");
    InitializerWrapper initializer = initializerMap.get(inputName);
    if (initializer == null) {
      pendingVertexRegistrations.put(inputName, new VertexUpdateRegistrationHolder(vertexName, stateSet));
    } else {
      initializer.registerForVertexStateUpdates(vertexName, stateSet);
    }
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  public InitializerWrapper getInitializerWrapper(String inputName) {
    return initializerMap.get(inputName);
  }

  public void shutdown() {
    isStopped = true;
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  public static class InitializerWrapper implements VertexStateUpdateListener, TaskStateUpdateListener {


    private final RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> input;
    private final InputInitializer initializer;
    private final InputInitializerContext context;
    private final AtomicBoolean isComplete = new AtomicBoolean(false);
    private final String vertexLogIdentifier;
    private final TezVertexID vertexId;
    private final StateChangeNotifier stateChangeNotifier;
    private final List<String> notificationRegisteredVertices = Lists.newArrayList();
    private final AppContext appContext;

    InitializerWrapper(RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> input,
                       InputInitializer initializer, InputInitializerContext context,
                       Vertex vertex, StateChangeNotifier stateChangeNotifier,
                       AppContext appContext) {
      this.input = input;
      this.initializer = initializer;
      this.context = context;
      this.vertexLogIdentifier = vertex.getLogIdentifier();
      this.vertexId = vertex.getVertexId();
      this.stateChangeNotifier = stateChangeNotifier;
      this.appContext = appContext;
    }

    public RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> getInput() {
      return input;
    }

    public InputInitializer getInitializer() {
      return initializer;
    }

    public String getVertexLogIdentifier() {
      return vertexLogIdentifier;
    }

    public boolean isComplete() {
      return isComplete.get();
    }

    public void setComplete() {
      this.isComplete.set(true);
      unregisterForVertexStatusUpdates();
      unregisterForTaskStatusUpdates();
    }

    public void registerForVertexStateUpdates(String vertexName, Set<VertexState> stateSet) {
      synchronized(notificationRegisteredVertices) {
        notificationRegisteredVertices.add(vertexName);
      }
      stateChangeNotifier.registerForVertexUpdates(vertexName, stateSet, this);
    }

    private void unregisterForVertexStatusUpdates() {
      synchronized (notificationRegisteredVertices) {
        for (String vertexName : notificationRegisteredVertices) {
          stateChangeNotifier.unregisterForVertexUpdates(vertexName, this);
        }

      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onStateUpdated(VertexStateUpdate event) {
      if (isComplete()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Dropping state update for vertex=" + event.getVertexName() + ", state=" +
              event.getVertexState() +
              " since initializer " + input.getName() + " is already complete.");
        }
      } else {
        try {
          initializer.onVertexStateUpdated(event);
        } catch (Exception e) {
          appContext.getEventHandler().handle(
              new VertexEventRootInputFailed(vertexId, input.getName(),
                  new AMUserCodeException(Source.InputInitializer,e)));
        }
      }
    }

    private final Map<String, Map<Integer, Integer>> firstSuccessfulAttemptMap = new HashMap<String, Map<Integer, Integer>>();
    private final ListMultimap<String, TezEvent> pendingEvents = LinkedListMultimap.create();
    private final List<String> taskNotificationRegisteredVertices = Lists.newLinkedList();

    @InterfaceAudience.Private
    @VisibleForTesting
    public Map<String, Map<Integer, Integer>> getFirstSuccessfulAttemptMap() {
      return this.firstSuccessfulAttemptMap;
    }

    @InterfaceAudience.Private
    @VisibleForTesting
    public ListMultimap<String, TezEvent> getPendingEvents() {
      return this.pendingEvents;
    }

    @Override
    public void onTaskSucceeded(String vertexName, TezTaskID taskId, int attemptId) {
      // Notifications will only start coming in after an event is received, which is when we register for notifications.
      // TODO TEZ-1577. Get rid of this.
      if (attemptId == -1) {
        throw new TezUncheckedException(
            "AttemptId is -1. This is likely caused by TEZ-1577; recovery not supported when InputInitializerEvents are used");
      }
      Map<Integer, Integer> vertexSuccessfulAttemptMap = firstSuccessfulAttemptMap.get(vertexName);
      Integer successfulAttempt = vertexSuccessfulAttemptMap.get(taskId.getId());
      if (successfulAttempt == null) {
        successfulAttempt = attemptId;
        vertexSuccessfulAttemptMap.put(taskId.getId(), successfulAttempt);
      }

      // Run through all the pending events for this srcVertex to see if any of them need to be dispatched.
      List<TezEvent> events = pendingEvents.get(vertexName);
      if (events != null && !events.isEmpty()) {
        List<InputInitializerEvent> toForwardEvents = new LinkedList<InputInitializerEvent>();
        Iterator<TezEvent> eventIterator = events.iterator();
        while (eventIterator.hasNext()) {
          TezEvent tezEvent = eventIterator.next();
          int taskIndex = tezEvent.getSourceInfo().getTaskAttemptID().getTaskID().getId();
          int taskAttemptIndex = tezEvent.getSourceInfo().getTaskAttemptID().getId();
          if (taskIndex == taskId.getId()) {
            // Process only if there's a pending event for the specific succeeded task
            if (taskAttemptIndex == successfulAttempt) {
              toForwardEvents.add((InputInitializerEvent) tezEvent.getEvent());
            }
            // Drop all other events which have the same source task Id.
            eventIterator.remove();
          }
        }
        sendEvents(toForwardEvents);
      }
    }

    public void handleInputInitializerEvents(Collection<TezEvent> tezEvents) {
      List<InputInitializerEvent> toForwardEvents = new LinkedList<InputInitializerEvent>();
      for (TezEvent tezEvent : tezEvents) {
        String srcVertexName = tezEvent.getSourceInfo().getTaskVertexName();
        int taskIndex = tezEvent.getSourceInfo().getTaskAttemptID().getTaskID().getId();
        int taskAttemptIndex = tezEvent.getSourceInfo().getTaskAttemptID().getId();

        Map<Integer, Integer> vertexSuccessfulAttemptMap =
            firstSuccessfulAttemptMap.get(srcVertexName);
        if (vertexSuccessfulAttemptMap == null) {
          vertexSuccessfulAttemptMap = new HashMap<Integer, Integer>();
          firstSuccessfulAttemptMap.put(srcVertexName, vertexSuccessfulAttemptMap);
          // Seen first time. Register for task updates
          stateChangeNotifier.registerForTaskSuccessUpdates(srcVertexName, this);
          taskNotificationRegisteredVertices.add(srcVertexName);
        }

        // Determine the successful attempt for the task
        Integer successfulAttemptInteger = vertexSuccessfulAttemptMap.get(taskIndex);
        if (successfulAttemptInteger == null) {
          // Check immediately if this task has succeeded, in case the notification came in before the event
          Vertex srcVertex = appContext.getCurrentDAG().getVertex(srcVertexName);
          Task task = srcVertex.getTask(taskIndex);
          if (task.getState() == TaskState.SUCCEEDED) {
            successfulAttemptInteger = task.getSuccessfulAttempt().getID().getId();
            vertexSuccessfulAttemptMap.put(taskIndex, successfulAttemptInteger);
          }
        }

        if (successfulAttemptInteger == null) {
          // Queue events and await a notification
          pendingEvents.put(srcVertexName, tezEvent);
        } else {
          // Handle the event immediately.
          if (taskAttemptIndex == successfulAttemptInteger) {
            toForwardEvents.add((InputInitializerEvent) tezEvent.getEvent());
          } // Otherwise the event can be dropped
        }
      }
      sendEvents(toForwardEvents);
    }

    @SuppressWarnings("unchecked")
    private void sendEvents(List<InputInitializerEvent> events) {
      if (events != null && !events.isEmpty()) {
        try {
          initializer.handleInputInitializerEvent(events);
        } catch (Exception e) {
          appContext.getEventHandler().handle(
              new VertexEventRootInputFailed(vertexId, input.getName(),
                  new AMUserCodeException(Source.InputInitializer,e)));
        }
      }
    }

    private void unregisterForTaskStatusUpdates() {
      for (String vertexName : taskNotificationRegisteredVertices) {
        stateChangeNotifier.unregisterForTaskSuccessUpdates(vertexName, this);
      }
    }
  }

}
