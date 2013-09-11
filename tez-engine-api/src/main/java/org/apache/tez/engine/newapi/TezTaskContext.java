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

package org.apache.tez.engine.newapi;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.counters.TezCounters;

/**
 * Base interface for Context classes used to initialize the Input, Output
 * and Processor instances.
 */
public interface TezTaskContext {

  // TODO NEWTEZ
  // Scale the maximum events we fetch per RPC call to mitigate OOM issues
  // on the ApplicationMaster when a thundering herd of reducers fetch events
  // This should not be necessary after HADOOP-8942

  /**
   * Get the {@link ApplicationId} for the running app
   * @return the {@link ApplicationId}
   */
  public ApplicationId getApplicationId();
  
  /**
   * Get the index of this Task
   * @return Task Index
   */
  public int getTaskIndex();

  /**
   * Get the current Task Attempt Number
   * @return Attempt Number
   */
  public int getAttemptNumber();

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
  public byte[] getUserPayload();

  /**
   * Get the work diectories for the Input/Output/Processor
   * @return an array of work dirs
   */
  public String[] getWorkDirs();
  
  /**
   * Returns an identifier which is unique to the specific Input, Processor or
   * Output
   * 
   * @return
   */
  public String getUniqueIdentifier();
  
  /**
   * Report a fatal error to the framework. This will cause the entire task to
   * fail and should not be used for reporting temporary or recoverable errors
   * 
   * @param exception an exception representing the error
   */
  public void fatalError(Throwable exception, String message);
  
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
  public ByteBuffer getServiceProviderMetaData(String serviceName);
}
