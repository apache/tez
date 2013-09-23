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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;

public class Vertex { // FIXME rename to Task

  private final String vertexName;
  private final ProcessorDescriptor processorDescriptor;

  private final int parallelism;
  private VertexLocationHint taskLocationsHint;
  private final Resource taskResource;
  private Map<String, LocalResource> taskLocalResources;
  private Map<String, String> taskEnvironment;

  private final List<Vertex> inputVertices = new ArrayList<Vertex>();
  private final List<Vertex> outputVertices = new ArrayList<Vertex>();
  private final List<String> inputEdgeIds = new ArrayList<String>();
  private final List<String> outputEdgeIds = new ArrayList<String>();
  private String javaOpts = "";


  public Vertex(String vertexName,
      ProcessorDescriptor processorDescriptor,
      int parallelism,
      Resource taskResource) {
    this.vertexName = vertexName;
    this.processorDescriptor = processorDescriptor;
    this.parallelism = parallelism;
    this.taskResource = taskResource;
    if (parallelism == 0) {
      throw new IllegalArgumentException("Parallelism cannot be 0");
    }
    if (taskResource == null) {
      throw new IllegalArgumentException("Resource cannot be null");
    }
  }

  public String getVertexName() { // FIXME rename to getName()
    return vertexName;
  }

  public ProcessorDescriptor getProcessorDescriptor() {
    return this.processorDescriptor;
  }

  public int getParallelism() {
    return parallelism;
  }

  public Resource getTaskResource() {
    return taskResource;
  }

  public Vertex setTaskLocationsHint(List<TaskLocationHint> locations) {
    if (locations == null) {
      return this;
    }
    assert locations.size() == parallelism;
    taskLocationsHint = new VertexLocationHint(parallelism, locations);
    return this;
  }

  // used internally to create parallelism location resource file
  VertexLocationHint getTaskLocationsHint() {
    return taskLocationsHint;
  }

  public Vertex setTaskLocalResources(Map<String, LocalResource> localResources) {
    this.taskLocalResources = localResources;
    return this;
  }

  public Map<String, LocalResource> getTaskLocalResources() {
    return taskLocalResources;
  }

  public Vertex setTaskEnvironment(Map<String, String> environment) {
    this.taskEnvironment = environment;
    return this;
  }

  public Map<String, String> getTaskEnvironment() {
    return taskEnvironment;
  }

  public Vertex setJavaOpts(String javaOpts){
     this. javaOpts = javaOpts;
     return this;
  }

  public String getJavaOpts(){
	  return javaOpts;
  }

  @Override
  public String toString() {
    return "[" + vertexName + " : " + processorDescriptor.getClassName() + "]";
  }

  void addInputVertex(Vertex inputVertex, String edgeId) {
    inputVertices.add(inputVertex);
    inputEdgeIds.add(edgeId);
  }

  void addOutputVertex(Vertex outputVertex, String edgeId) {
    outputVertices.add(outputVertex);
    outputEdgeIds.add(edgeId);
  }

  List<Vertex> getInputVertices() {
    return inputVertices;
  }

  List<Vertex> getOutputVertices() {
    return outputVertices;
  }

  List<String> getInputEdgeIds() {
    return inputEdgeIds;
  }

  List<String> getOutputEdgeIds() {
    return outputEdgeIds;
  }

  // FIXME how do we support profiling? Can't profile all tasks.

}
