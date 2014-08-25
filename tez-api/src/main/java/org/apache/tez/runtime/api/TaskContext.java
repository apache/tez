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

package org.apache.tez.runtime.api;

import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.UserPayload;

/**
 * Base interface for Context classes used to initialize the Input, Output
 * and Processor instances.
 * This interface is not supposed to be implemented by users
 */
@Public
public interface TaskContext {
  /**
   * Get the {@link ApplicationId} for the running app
   * @return the {@link ApplicationId}
   */
  public ApplicationId getApplicationId();

  /**
   * Get the current DAG Attempt Number
   * @return DAG Attempt Number
   */
  public int getDAGAttemptNumber();

  /**
   * Get the index of this Task among the tasks of this vertex
   * @return Task Index
   */
  public int getTaskIndex();

  /**
   * Get the current Task Attempt Number
   * @return Task Attempt Number
   */
  public int getTaskAttemptNumber();

  /**
   * Get the name of the DAG
   * @return the DAG name
   */
  public String getDAGName();

  /**
   * Get the name of the Vertex in which the task is running
   * @return Vertex Name
   */
  public String getTaskVertexName();
  
  /**
   * Get the index of this task's vertex in the set of vertices in the DAG. This 
   * is consistent and valid across all tasks/vertices in the same DAG.
   * @return index
   */
  public int getTaskVertexIndex();

  public TezCounters getCounters();

  /**
   * Send Events to the AM and/or dependent Vertices
   * @param events Events to be sent
   */
  public void sendEvents(List<Event> events);

  /**
   * Get the User Payload for the Input/Output/Processor
   * @return User Payload
   */
  public UserPayload getUserPayload();

  /**
   * Get the work directories for the Input/Output/Processor
   * @return an array of work dirs
   */
  public String[] getWorkDirs();

  /**
   * Returns an identifier which is unique to the specific Input, Processor or
   * Output
   * 
   * @return a unique identifier
   */
  public String getUniqueIdentifier();
  
  /**
   * Returns a shared {@link ObjectRegistry} to hold user objects in memory 
   * between tasks. 
   * @return {@link ObjectRegistry}
   */
  public ObjectRegistry getObjectRegistry();

  /**
   * Report a fatal error to the framework. This will cause the entire task to
   * fail and should not be used for reporting temporary or recoverable errors
   *
   * @param exception an exception representing the error
   */
  public void fatalError(@Nullable Throwable exception, @Nullable String message);

  /**
   * Returns meta-data for the specified service. As an example, when the MR
   * ShuffleHandler is used - this would return the jobToken serialized as bytes
   *
   * @param serviceName
   *          the name of the service for which meta-data is required
   * @return a ByteBuffer representing the meta-data
   */
  public ByteBuffer getServiceConsumerMetaData(String serviceName);

  /**
   * Return Provider meta-data for the specified service As an example, when the
   * MR ShuffleHandler is used - this would return the shuffle port serialized
   * as bytes
   *
   * @param serviceName
   *          the name of the service for which provider meta-data is required
   * @return a ByteBuffer representing the meta-data
   */
  @Nullable
  public ByteBuffer getServiceProviderMetaData(String serviceName);
  
  /**
   * Request a specific amount of memory during initialization
   * (initialize(..*Context)) The requester is notified of allocation via the
   * provided callback handler.
   * 
   * Currently, (post TEZ-668) the caller will be informed about the available
   * memory after initialization (I/P/O initialize(...)), and before the
   * start/run invocation. There will be no other invocations on the callback.
   * 
   * This method can be called only once by any component. Calling it multiple
   * times from within the same component will result in an error.
   * 
   * Each Input / Output must request memory. For Inputs / Outputs which do not
   * have a specific ask, a null callback handler can be specified with a
   * request size of 0.
   * 
   * @param size
   *          request size in bytes.
   * @param callbackHandler
   *          the callback handler to be invoked once memory is assigned
   */
  public void requestInitialMemory(long size, MemoryUpdateCallback callbackHandler);
  
  /**
   * Gets the total memory available to all components of the running task. This
   * values will always be constant, and does not factor in any allocations.
   * 
   * @return the total available memory for all components of the task
   */
  public long getTotalMemoryAvailableToTask();

  /**
   * Get the vertex parallelism of the vertex to which this task belongs.
   *
   * @return Parallelism of the current vertex.
   */
  public int getVertexParallelism();
    
}
