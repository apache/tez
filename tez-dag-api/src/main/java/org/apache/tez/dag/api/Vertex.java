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
  private final String processorName;

  private final int parallelism;
  private VertexLocationHint taskLocationsHint;
  private Resource taskResource;
  private Map<String, LocalResource> taskLocalResources;
  private Map<String, String> taskEnvironment;

  private final List<Vertex> inputVertices = new ArrayList<Vertex>();
  private final List<Vertex> outputVertices = new ArrayList<Vertex>();
  private final List<String> inputEdgeIds = new ArrayList<String>();
  private final List<String> outputEdgeIds = new ArrayList<String>();
  private String javaOpts = "";


  public Vertex(String vertexName, String processorName, int parallelism) {
    this.vertexName = vertexName;
    this.processorName = processorName;
    this.parallelism = parallelism;
  }

  public String getVertexName() { // FIXME rename to getName()
    return vertexName;
  }

  public String getProcessorName() {
    return processorName;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setTaskResource(Resource resource) {
    this.taskResource = resource;
  }

  public Resource getTaskResource() {
    return taskResource;
  }

  public void setTaskLocationsHint(TaskLocationHint[] locations) {
    if (locations == null) {
      return;
    }
    assert locations.length == parallelism;
    taskLocationsHint = new VertexLocationHint(parallelism, locations);
  }

  // used internally to create parallelism location resource file
  VertexLocationHint getTaskLocationsHint() {
    return taskLocationsHint;
  }

  public void setTaskLocalResources(Map<String, LocalResource> localResources) {
    this.taskLocalResources = localResources;
  }

  public Map<String, LocalResource> getTaskLocalResources() {
    return taskLocalResources;
  }

  public void setTaskEnvironment(Map<String, String> environment) {
    this.taskEnvironment = environment;
  }

  public Map<String, String> getTaskEnvironment() {
    return taskEnvironment;
  }

  public void setJavaOpts(String javaOpts){
     this. javaOpts = javaOpts;
  }

  public String getJavaOpts(){
	  return javaOpts;
  }

  @Override
  public String toString() {
    return "[" + vertexName + " : " + processorName + "]";
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
