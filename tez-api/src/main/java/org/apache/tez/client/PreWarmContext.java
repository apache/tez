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

package org.apache.tez.client;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.VertexLocationHint;

import java.util.Map;

/**
 * Context to define how the pre-warm containers should be launched within a
 * session.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PreWarmContext {

  private final ProcessorDescriptor processorDescriptor;
  private final Resource resource;
  private final int numTasks;
  private final VertexLocationHint locationHints;
  private Map<String, LocalResource> localResources;
  private Map<String, String> environment;
  private String javaOpts;

  /**
   * Context to define how to pre-warm a TezSession.
   * 
   * @param processorDescriptor
   *          The processor to run within a Tez Task after launching a container
   * @param resource
   *          The resource requirements for each container
   * @param numTasks
   *          The number of tasks to run. The num of tasks can drive how many
   *          containers are launched. However, as containers are re-used, the
   *          total number of launched containers will likely be less than the
   *          specified number of tasks.
   * @param locationHints
   *          The location hints for the containers to be launched.
   * 
   */
  public PreWarmContext(ProcessorDescriptor processorDescriptor,
      Resource resource,
      int numTasks,
      VertexLocationHint locationHints) {
    this.processorDescriptor =  processorDescriptor;
    this.resource = resource;
    this.numTasks = numTasks;
    this.locationHints = locationHints;
  }

  /**
   * Set the LocalResources for the pre-warm containers.
   * @param localResources LocalResources for the container
   * @return this
   */
  public PreWarmContext setLocalResources(
      Map<String, LocalResource> localResources) {
    this.localResources = localResources;
    return this;
  }


  /**
   * Set the Environment for the pre-warm containers.
   * @param environment Container environment
   * @return this
   */
  public PreWarmContext setEnvironment(
      Map<String, String> environment) {
    this.environment = environment;
    return this;
  }

  /**
   * Set the Java opts for the pre-warm containers.
   * @param javaOpts Container java opts
   * @return this
   */
  public PreWarmContext setJavaOpts(String javaOpts) {
    this.javaOpts = javaOpts;
    return this;
  }

  public ProcessorDescriptor getProcessorDescriptor() {
    return processorDescriptor;
  }

  public Resource getResource() {
    return resource;
  }

  public int getNumTasks() {
    return numTasks;
  }

  public VertexLocationHint getLocationHints() {
    return locationHints;
  }

  public Map<String, LocalResource> getLocalResources() {
    return localResources;
  }

  public Map<String, String> getEnvironment() {
    return environment;
  }

  public String getJavaOpts() {
    return javaOpts;
  }

}
