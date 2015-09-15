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

package org.apache.tez.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A dispatcher that can schedule events concurrently. Uses a fixed size threadpool 
 * to schedule events. Events that have the same serializing hash will get scheduled
 * on the same thread in the threadpool. This can be used to prevent concurrency issues
 * for events that may not be independently processed.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@Private
public class AsyncDispatcherConcurrent extends CompositeService implements Dispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncDispatcher.class);

  private final String name;
  private final ArrayList<LinkedBlockingQueue<Event>> eventQueues;
  private volatile boolean stopped = false;

  // Configuration flag for enabling/disabling draining dispatcher's events on
  // stop functionality.
  private volatile boolean drainEventsOnStop = false;

  // Indicates all the remaining dispatcher's events on stop have been drained
  // and processed.
  private volatile boolean drained = true;
  private Object waitForDrained = new Object();

  // For drainEventsOnStop enabled only, block newly coming events into the
  // queue while stopping.
  private volatile boolean blockNewEvents = false;
  private EventHandler handlerInstance = new GenericEventHandler();

  private ExecutorService execService;
  private final int numThreads;
  
  protected final Map<Class<? extends Enum>, EventHandler> eventHandlers = Maps.newHashMap();
  protected final Map<Class<? extends Enum>, AsyncDispatcherConcurrent> eventDispatchers = 
      Maps.newHashMap();
  private boolean exitOnDispatchException;

  AsyncDispatcherConcurrent(String name, int numThreads) {
    super(name);
    Preconditions.checkArgument(numThreads > 0);
    this.name = name;
    this.eventQueues = Lists.newArrayListWithCapacity(numThreads);
    this.numThreads = numThreads;
  }
  
  class DispatchRunner implements Runnable {
    final LinkedBlockingQueue<Event> queue;
    
    public DispatchRunner(LinkedBlockingQueue<Event> queue) {
      this.queue = queue;
    }
    
    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        drained = queue.isEmpty();
        // blockNewEvents is only set when dispatcher is draining to stop,
        // adding this check is to avoid the overhead of acquiring the lock
        // and calling notify every time in the normal run of the loop.
        if (blockNewEvents) {
          synchronized (waitForDrained) {
            if (drained) {
              waitForDrained.notify();
            }
          }
        }
        Event event;
        try {
          event = queue.take();
        } catch(InterruptedException ie) {
          if (!stopped) {
            LOG.warn("AsyncDispatcher thread interrupted", ie);
          }
          return;
        }
        if (event != null) {
          dispatch(event);
        }
      }
    }
  };
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // TODO TEZ-2049 remove YARN reference
    this.exitOnDispatchException =
        conf.getBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY,
          Dispatcher.DEFAULT_DISPATCHER_EXIT_ON_ERROR);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    execService = Executors.newFixedThreadPool(numThreads, new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("Dispatcher {" + this.name + "} #%d").build());
    for (int i=0; i<numThreads; ++i) {
      eventQueues.add(new LinkedBlockingQueue<Event>());
    }
    for (int i=0; i<numThreads; ++i) {
      execService.execute(new DispatchRunner(eventQueues.get(i)));
    }
    //start all the components
    super.serviceStart();
  }

  public void setDrainEventsOnStop() {
    drainEventsOnStop = true;
  }

  @Override
  protected void serviceStop() throws Exception {
    if (execService != null) {
      if (drainEventsOnStop) {
        blockNewEvents = true;
        LOG.info("AsyncDispatcher is draining to stop, ignoring any new events.");
        synchronized (waitForDrained) {
          while (!drained && !execService.isShutdown()) {
            LOG.info("Waiting for AsyncDispatcher to drain.");
            waitForDrained.wait(1000);
          }
        }
      }

      stopped = true;

      for (int i=0; i<numThreads; ++i) {
        LOG.info("AsyncDispatcher stopping with events: " + eventQueues.get(i).size()
            + " in queue: " + i);
      }
      execService.shutdownNow();
    }

    // stop all the components
    super.serviceStop();
  }

  protected void dispatch(Event event) {
    //all events go thru this loop
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatching the event " + event.getClass().getName() + "."
          + event.toString());
    }

    Class<? extends Enum> type = event.getType().getDeclaringClass();

    try{
      EventHandler handler = eventHandlers.get(type);
      if(handler != null) {
        handler.handle(event);
      } else {
        throw new Exception("No handler for registered for " + type);
      }
    } catch (Throwable t) {
      LOG.error("Error in dispatcher thread", t);
      // If serviceStop is called, we should exit this thread gracefully.
      if (exitOnDispatchException
          && (ShutdownHookManager.get().isShutdownInProgress()) == false
          && stopped == false) {
        Thread shutDownThread = new Thread(createShutDownThread());
        shutDownThread.setName("AsyncDispatcher ShutDown handler");
        shutDownThread.start();
      }
    }
  }

  private void checkForExistingHandler(Class<? extends Enum> eventType) {
    EventHandler<Event> registeredHandler = (EventHandler<Event>) eventHandlers.get(eventType);
    Preconditions.checkState(registeredHandler == null, 
        "Cannot register same event on multiple dispatchers");
  }

  private void checkForExistingDispatcher(Class<? extends Enum> eventType) {
    AsyncDispatcherConcurrent registeredDispatcher = eventDispatchers.get(eventType);
    Preconditions.checkState(registeredDispatcher == null,
        "Multiple dispatchers cannot be registered for: " + eventType.getName());
  }

  private void checkForExistingDispatchers(boolean checkHandler, Class<? extends Enum> eventType) {
    if (checkHandler) {
      checkForExistingHandler(eventType);
    }
    checkForExistingDispatcher(eventType);
  }

  /**
   * Add an EventHandler for events handled inline on this dispatcher
   */
  @Override
  public void register(Class<? extends Enum> eventType,
      EventHandler handler) {
    Preconditions.checkState(getServiceState() == STATE.NOTINITED);
    /* check to see if we have a listener registered */
    EventHandler<Event> registeredHandler = (EventHandler<Event>) eventHandlers.get(eventType);
    checkForExistingDispatchers(false, eventType);
    LOG.info("Registering " + eventType + " for " + handler.getClass());
    if (registeredHandler == null) {
      eventHandlers.put(eventType, handler);
    } else if (!(registeredHandler instanceof MultiListenerHandler)){
      /* for multiple listeners of an event add the multiple listener handler */
      MultiListenerHandler multiHandler = new MultiListenerHandler();
      multiHandler.addHandler(registeredHandler);
      multiHandler.addHandler(handler);
      eventHandlers.put(eventType, multiHandler);
    } else {
      /* already a multilistener, just add to it */
      MultiListenerHandler multiHandler
      = (MultiListenerHandler) registeredHandler;
      multiHandler.addHandler(handler);
    }
  }
  
  /**
   * Add an EventHandler for events handled in their own dispatchers with given name and threads
   */
  
  public AsyncDispatcherConcurrent registerAndCreateDispatcher(Class<? extends Enum> eventType,
      EventHandler handler, String dispatcherName, int numThreads) {
    Preconditions.checkState(getServiceState() == STATE.NOTINITED);
    
    /* check to see if we have a listener registered */
    checkForExistingDispatchers(true, eventType);
    LOG.info(
          "Registering " + eventType + " for independent dispatch using: " + handler.getClass());
    AsyncDispatcherConcurrent dispatcher = new AsyncDispatcherConcurrent(dispatcherName, numThreads);
    dispatcher.register(eventType, handler);
    eventDispatchers.put(eventType, dispatcher);
    addIfService(dispatcher);
    return dispatcher;
  }
  
  public void registerWithExistingDispatcher(Class<? extends Enum> eventType,
      EventHandler handler, AsyncDispatcherConcurrent dispatcher) {
    Preconditions.checkState(getServiceState() == STATE.NOTINITED);
    
    /* check to see if we have a listener registered */
    checkForExistingDispatchers(true, eventType);
    LOG.info("Registering " + eventType + " wit existing concurrent dispatch using: "
          + handler.getClass());
    dispatcher.register(eventType, handler);
    eventDispatchers.put(eventType, dispatcher);
  }

  @Override
  public EventHandler getEventHandler() {
    return handlerInstance;
  }

  class GenericEventHandler implements EventHandler<TezAbstractEvent> {
    public void handle(TezAbstractEvent event) {
      if (stopped) {
        return;
      }
      if (blockNewEvents) {
        return;
      }
      drained = false;
      
      // offload to specific dispatcher if one exists
      Class<? extends Enum> type = event.getType().getDeclaringClass();
      AsyncDispatcherConcurrent registeredDispatcher = eventDispatchers.get(type);
      if (registeredDispatcher != null) {
        registeredDispatcher.getEventHandler().handle(event);
        return;
      }
      
      int index = numThreads > 1 ? event.getSerializingHash() % numThreads : 0;

     // no registered dispatcher. use internal dispatcher.
      LinkedBlockingQueue<Event> queue = eventQueues.get(index);
      /* all this method does is enqueue all the events onto the queue */
      int qSize = queue.size();
      if (qSize !=0 && qSize %1000 == 0) {
        LOG.info("Size of event-queue is " + qSize);
      }
      int remCapacity = queue.remainingCapacity();
      if (remCapacity < 1000) {
        LOG.warn("Very low remaining capacity in the event-queue: "
            + remCapacity);
      }
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        if (!stopped) {
          LOG.warn("AsyncDispatcher thread interrupted", e);
        }
        throw new YarnRuntimeException(e);
      }
    };
  }

  /**
   * Multiplexing an event. Sending it to different handlers that
   * are interested in the event.
   * @param <T> the type of event these multiple handlers are interested in.
   */
  static class MultiListenerHandler implements EventHandler<Event> {
    List<EventHandler<Event>> listofHandlers;

    public MultiListenerHandler() {
      listofHandlers = new ArrayList<EventHandler<Event>>();
    }

    @Override
    public void handle(Event event) {
      for (EventHandler<Event> handler: listofHandlers) {
        handler.handle(event);
      }
    }

    void addHandler(EventHandler<Event> handler) {
      listofHandlers.add(handler);
    }

  }

  Runnable createShutDownThread() {
    return new Runnable() {
      @Override
      public void run() {
        LOG.info("Exiting, bbye..");
        System.exit(-1);
      }
    };
  }
}
