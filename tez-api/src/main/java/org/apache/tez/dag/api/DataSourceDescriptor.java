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

package org.apache.tez.dag.api;

import javax.annotation.Nullable;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;

/**
 * Defines the input and input initializer for a data source 
 *
 */
public class DataSourceDescriptor {
  private final InputDescriptor inputDescriptor;
  private final InputInitializerDescriptor initializerDescriptor;
  
  private final Credentials credentials;
  private final int numShards;
  private final VertexLocationHint locationHint;
  private final Map<String, LocalResource> additionalLocalResources;

  /**
   * Create a {@link DataSourceDescriptor} when the data shard calculation 
   * happens in the App Master at runtime
   * @param inputDescriptor
   *          An {@link InputDescriptor} for the Input
   * @param credentials Credentials needed to access the data
   * @param initializerDescriptor
   *          An initializer for this Input which may run within the AM. This
   *          can be used to set the parallelism for this vertex and generate
   *          {@link InputDataInformationEvent}s for the actual Input.</p>
   *          If this is not specified, the parallelism must be set for the
   *          vertex. In addition, the Input should know how to access data for
   *          each of it's tasks. </p> If a {@link InputInitializer} is
   *          meant to determine the parallelism of the vertex, the initial
   *          vertex parallelism should be set to -1. Can be null.
   */
  public DataSourceDescriptor(InputDescriptor inputDescriptor,
      @Nullable InputInitializerDescriptor initializerDescriptor, 
      @Nullable Credentials credentials) {
    this(inputDescriptor, initializerDescriptor, -1, credentials, null, null);
  }

  /**
   * Create a {@link DataSourceDescriptor} when the data shard calculation
   * happens in the client at compile time
   *
   * @param inputDescriptor          An {@link InputDescriptor} for the Input
   * @param initializerDescriptor    An initializer for this Input which may run within the AM.
   *                                 This can be used to set the parallelism for this vertex and
   *                                 generate {@link org.apache.tez.runtime.api.events.InputDataInformationEvent}s
   *                                 for the actual Input.</p>
   *                                 If this is not specified, the parallelism must be set for the
   *                                 vertex. In addition, the Input should know how to access data
   *                                 for each of it's tasks. </p> If a {@link org.apache.tez.runtime.api.InputInitializer}
   *                                 is
   *                                 meant to determine the parallelism of the vertex, the initial
   *                                 vertex parallelism should be set to -1. Can be null.
   * @param numShards                Number of shards of data
   * @param credentials              Credentials needed to access the data
   * @param locationHint             Location hints for the vertex tasks
   * @param additionalLocalResources additional local resources required by this Input. An attempt
   *                                 will be made to add these resources to the Vertex as Private
   *                                 resources. If a name conflict occurs, a {@link
   *                                 org.apache.tez.dag.api.TezException} will be thrown
   */
  public DataSourceDescriptor(InputDescriptor inputDescriptor,
      @Nullable InputInitializerDescriptor initializerDescriptor, int numShards,
      @Nullable Credentials credentials, @Nullable VertexLocationHint locationHint,
      @Nullable Map<String, LocalResource> additionalLocalResources) {
    this.inputDescriptor = inputDescriptor;
    this.initializerDescriptor = initializerDescriptor;
    this.numShards = numShards;
    this.credentials = credentials;
    this.locationHint = locationHint;
    this.additionalLocalResources = additionalLocalResources;
  }

  public InputDescriptor getInputDescriptor() {
    return inputDescriptor;
  }
  
  public @Nullable InputInitializerDescriptor getInputInitializerDescriptor() {
    return initializerDescriptor;
  }
  
  /**
   * Number of shards for this data source. If a vertex has only one
   * data source this the number of tasks in the vertex should be set to 
   * the number of shards
   * Returns -1 when this is determined at runtime in the AM.
   * @return number of tasks
   */
  @InterfaceAudience.Private
  public int getNumberOfShards() {
    return numShards;
  }
  
  /**
   * Returns any credentials needed to access this data source.
   * Is null when this calculation happens on the AppMaster (default)
   * @return credentials.
   */
  @InterfaceAudience.Private
  public @Nullable Credentials getCredentials() {
    return credentials;
  }
  
  /**
   * Get the location hints for the tasks in the vertex for this data source.
   * Is null when shard calculation happens on the AppMaster (default)
   * @return List of {@link TaskLocationHint}
   */
  @InterfaceAudience.Private
  public @Nullable VertexLocationHint getLocationHint() {
    return locationHint;
  }

  /**
   * Get the list of additional local resources which were specified during creation.
   * @return
   */
  @InterfaceAudience.Private
  public @Nullable Map<String, LocalResource> getAdditionalLocalResources() {
    return additionalLocalResources;
  }


}
