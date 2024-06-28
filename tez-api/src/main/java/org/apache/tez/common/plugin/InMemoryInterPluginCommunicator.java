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

package org.apache.tez.common.plugin;

import org.apache.tez.serviceplugins.api.InterPluginId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Class for storing values that will be passed around plugins. Every queue is
 * identified by a key. The write lock is only needed when adding a new queue.
 * If we are writing/reading from a queue we only need the read lock.
 */
public class InMemoryInterPluginCommunicator
    implements InterPluginCommunicator {
  /**
   * Logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      InMemoryInterPluginCommunicator.class);

  /**
   * Structure to hold the data.
   */
  private HashMap<Object, LinkedBlockingQueue<Object>> data = new HashMap<>();

  /**
   * Structure to hold the listeners.
   */
  private HashMap<Object, CopyOnWriteArrayList<InterPluginListener>>
      subscribers = new HashMap<>();

  /**
   * Read lock for the data.
   */
  private final Lock dataReadLock;

  /**
   * Write lock for the data.
   */
  private final Lock dataWriteLock;

  /**
   * Read lock for the subscribers.
   */
  private final Lock subscribeReadLock;

  /**
   * Write lock for the subscribers.
   */
  private final Lock subscribeWriteLock;

  /**
   * Creates an object of this class.
   */
  public InMemoryInterPluginCommunicator() {
    ReadWriteLock dataReadWriteLock = new ReentrantReadWriteLock();
    dataReadLock = dataReadWriteLock.readLock();
    dataWriteLock = dataReadWriteLock.writeLock();

    ReadWriteLock subscribeReadWriteLock = new ReentrantReadWriteLock();
    subscribeReadLock = subscribeReadWriteLock.readLock();
    subscribeWriteLock = subscribeReadWriteLock.writeLock();
  }

  /**
   * Put value in the queue defined by key. Right now for every key
   * a new {@link LinkedBlockingQueue} is created and although it
   * can be empty it will never be removed.
   * @param key to find the right queue
   * @param value value to put in the queue
   */
  @Override
  public final void put(final InterPluginId key, final Object value) {
    // No null values in the queue
    if (value == null) {
      LOG.warn("Couldn't save in InterPluginCommunicator value null");
      return;
    }

    boolean contained;
    try {
      dataReadLock.lock();
      contained = data.containsKey(key);
      // We will get in the first if most of the times
      if (contained) {
        data.get(key).add(value);
      }
    } finally {
      dataReadLock.unlock();
    }

    if (!contained) {
      try {
        dataWriteLock.lock();
        if (data.containsKey(key)) {
          data.get(key).add(value);
        } else {
          LinkedBlockingQueue<Object> linkedBlockingQueue =
              new LinkedBlockingQueue<>();
          linkedBlockingQueue.add(value);
          data.put(key, linkedBlockingQueue);
        }
      } finally {
        dataWriteLock.unlock();
      }
    }
  }

  /**
   * @param key key to find the queue.
   * @return the first element of the queue or null
   * if it doesn't exist. It doesn't remove the element
   * from the queue
   */
  @Override
  public final @Nullable Object peek(final InterPluginId key) {
    try {
      dataReadLock.lock();
      if (data.containsKey(key)) {
        return data.get(key).peek();
      }
      return null;
    } finally {
      dataReadLock.unlock();
    }
  }

  /**
   * @param key key to find the queue.
   * @return the first element of the queue or null.
   * if it doesn't exist. It removes the element from
   * the queue.
   */
  @Override
  public final @Nullable Object get(final InterPluginId key) {
    try {
      dataReadLock.lock();
      if (data.containsKey(key)) {
        return data.get(key).poll();
      }
      return null;
    } finally {
      dataReadLock.unlock();
    }
  }

  /**
   * send object to subscribed listeners.
   * @param key key that determines the listeners.
   * @param value value to send.
   */
  @Override
  public final void send(final InterPluginId key, final Object value) {
    List<InterPluginListener> keySubscribers = null;
    try {
      subscribeReadLock.lock();
      keySubscribers = subscribers.get(key);
    } finally {
      subscribeReadLock.unlock();
    }
    if (keySubscribers != null) {
      keySubscribers.forEach(subscriber -> subscriber.onSentValue(value));
    }
  }

  /**
   * @param key subscribe to this key.
   * @param interPluginListener subscriber.
   */
  @Override
  public final void subscribe(final InterPluginId key, final InterPluginListener
      interPluginListener) {
    try {
      subscribeWriteLock.lock();
      if (!subscribers.containsKey(key)) {
        CopyOnWriteArrayList<InterPluginListener> copyOnWriteArrayList =
            new CopyOnWriteArrayList<>(Collections.singleton(interPluginListener));
        subscribers.put(key, copyOnWriteArrayList);
      }
    } finally {
      subscribeWriteLock.unlock();
    }
  }

  /**
   * @param key to unsubscribe from.
   * @param interPluginListener to unsubscribe.
   */
  @Override
  public final void unsubscribe(final InterPluginId key,
      final InterPluginListener interPluginListener) {
    try {
      subscribeWriteLock.lock();
      if (subscribers.containsKey(key)) {
        CopyOnWriteArrayList<InterPluginListener> copyOnWriteArrayList =
            subscribers.get(key);
        copyOnWriteArrayList.remove(interPluginListener);
        if (copyOnWriteArrayList.isEmpty()) {
          subscribers.remove(key);
        }
      }
    } finally {
      subscribeWriteLock.unlock();
    }
  }
}
