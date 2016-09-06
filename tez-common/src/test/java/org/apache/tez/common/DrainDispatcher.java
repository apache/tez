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

import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DrainDispatcher extends AsyncDispatcher {
  static final String DEFAULT_NAME = "dispatcher";
  private volatile boolean drained = false;
  private volatile boolean stopped = false;
  private final BlockingQueue<Event> queue;
  private final Object mutex;
  private static final Logger LOG = LoggerFactory.getLogger(DrainDispatcher.class);

  public DrainDispatcher() {
    this(DEFAULT_NAME, new LinkedBlockingQueue<Event>());
  }

  public DrainDispatcher(String name, BlockingQueue<Event> eventQueue) {
    super(name, eventQueue);
    this.queue = eventQueue;
    this.mutex = this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void register(Class<? extends Enum> eventType,
                       EventHandler handler) {
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
   * Busy loop waiting for all queued events to drain.
   */
  public void await() {
    while (!drained) {
      Thread.yield();
    }
  }

  @Override
  public Runnable createThread() {
    return new Runnable() {
      @Override
      public void run() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          synchronized (mutex) {
            // !drained if dispatch queued new events on this dispatcher
            drained = queue.isEmpty();
          }
          Event event;
          try {
            event = queue.take();
          } catch (InterruptedException ie) {
            return;
          }
          if (event != null) {
            dispatch(event);
          }
        }
      }
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public EventHandler getEventHandler() {
    final EventHandler actual = super.getEventHandler();
    return new EventHandler() {
      @Override
      public void handle(Event event) {
        synchronized (mutex) {
          actual.handle(event);
          drained = false;
        }
      }
    };
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    super.serviceStop();
  }
}
