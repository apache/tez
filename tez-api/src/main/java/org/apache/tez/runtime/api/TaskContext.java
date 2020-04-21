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
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
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
   * Get container configuration
   * @return Container configuration
   */
  public Configuration getContainerConfiguration();

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

  /**
   * Get a numeric identifier for the dag to which the task belongs. This will be unique within the
   * running application.
   *
   * @return the dag identifier
   */
  public int getDagIdentifier();

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
   * Notifies the framework that progress is being made by this component. 
   * This is used to identify hung components that are not making progress.
   * Must be called periodically until processing has completed for this component.
   * Care must be taken to call this when real progress has been made. Simply 
   * calling this continuously from a thread without regard to real work may prevent 
   * identification of hung components and delay/stall job completion.
   */
  public void notifyProgress();

  /**
   * Report an error to the framework. This will cause the taskAttempt to fail, and should not be used
   * to report errors which can be handled locally in the TaskAttempt. A new TaskAttempt will be launched
   * depending upon how many retries are available for the task.
   *
   * @deprecated Replaced by {@link #reportFailure(TaskFailureType, Throwable, String)} (FailureType, Throwable, String)}
   *
   * Note: To maintain compatibility, even though this method is named 'fatalError' - this method
   * operates as {@link #reportFailure(TaskFailureType, Throwable, String)}
   * with the TaskFailureType set to {@link TaskFailureType#NON_FATAL}.
   *
   * @param exception an exception representing the error
   * @param message a diagnostic message which may be associated with the error
   */
  @Deprecated
  public void fatalError(@Nullable Throwable exception, @Nullable String message);


  /**
   * Report an error to the framework. This will cause the entire task to be terminated.
   *
   * @param taskFailureType the type of the error
   * @param exception any exception that may be associated with the error
   * @param message a diagnostic message which may be associated with the error
   */
  void reportFailure(TaskFailureType taskFailureType, @Nullable Throwable exception, @Nullable String message);

  /**
   * Kill the currently running attempt.
   * @param exception an associated exception
   * @param message an associated diagnostic message
   */
  @Private
  @Unstable
  void killSelf(@Nullable Throwable exception, @Nullable String message);

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

  /**
   * Get the context for the executor. This may be shared across multiple tasks
   * @return the execution context
   */
  public ExecutionContext getExecutionContext();

  /**
   * Create a new ExecutorService with the given parallelism and thread name format. The parallelism
   * might not be guaranteed. The service returned works with tez framework, currently it provides
   * thread reuse across tasks.
   * Note: This is an unstable api, and is not recommended to be used by external users. Please wait
   * until API and code is stablized by use in Tez processors, input and outputs.
   * @param parallelism The expected parallelism for for this ExecutorService.
   * @param threadNameFormat The thread name format, format will be given one parameter, threadId.
   * @return An ExecutorService instance.
   */
  @Private
  @Unstable
  public ExecutorService createTezFrameworkExecutorService(
      int parallelism, String threadNameFormat);
}
