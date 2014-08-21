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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.client.TezClient;
import org.apache.tez.runtime.api.Processor;

/**
 * A {@link PreWarmVertex} is used to specify parameters to be used to setup
 * prewarmed containers for Tez session mode. Sessions allow re-use of execution
 * slots (containers) across DAG's. Pre- warming allows pre-allocation of
 * containers so that the first DAG has some execution resources already
 * available to re-use. In order to get re-use containers they must be setup
 * identically. So the prewarm vertex must be setup identically to the real DAG
 * vertex (typically the first vertex to execute in the read DAG). Identical
 * settings include same execution resources, same task local files etc. This
 * works best in use cases where all DAGs share the same files/jars/resource
 * settings from a common template<br>
 * The parallelism of the pre-warm vertex determines the number of containers to
 * be pre-warmed. This would ideally ensures a viable number of containers to
 * provide performance while sharing resources with other applications.
 * Typically the session would also hold onto the same number of containers
 * in-between DAGs in session mode via the
 * {@link TezConfiguration#TEZ_AM_SESSION_MIN_HELD_CONTAINERS} property. The
 * prewarm vertex by default runs the PreWarmProcessor from the Tez runtime
 * library. This processor can be overridden to get the default behavior along
 * with any app specific customizations. Alternatively, the application can
 * provide any {@link Processor} to prewarm the containers. Pre-warming
 * processors can be used to initialize classes etc. and setup the environment
 * for the actual processing to reduce latency.
 */
@Unstable
@Public
public class PreWarmVertex extends Vertex {


  private PreWarmVertex(String vertexName, ProcessorDescriptor processorDescriptor, int parallelism,
                        Resource taskResource) {
    super(vertexName, processorDescriptor, parallelism, taskResource);
  }
  

  private PreWarmVertex(String vertexName, int parallelism, Resource taskResource) {
    this(vertexName, ProcessorDescriptor.create(
        "org.apache.tez.runtime.library.processor.PreWarmProcessor"), parallelism, taskResource);
  }
  
  /**
   * Create a config builder for the @link {@link PreWarmVertex}. This may be used to construct the
   * pre-warm vertex more flexibly.
   * @param conf
   * @return a new config builder for {@link PreWarmVertex}
   */
  public static PreWarmVertexConfigBuilder createConfigBuilder(Configuration conf) {
    return new PreWarmVertexConfigBuilder(conf);
  }

  /**
   * Create a {@link PreWarmVertex} to be used in
   * {@link TezClient#preWarm(PreWarmVertex)} It may be necessary to call more
   * methods to add local files etc on the pre-warm vertex post creation so that
   * it matches the real DAG vertices.
   *
   * @param vertexName
   *          Name of the vertex
   * @param processorDescriptor
   *          Descriptor of the processor to be run
   * @param parallelism
   *          Number of containers to be pre-warmed
   * @param taskResource
   *          Execution cpu/memory resources etc needed
   */
  public static PreWarmVertex create(String vertexName, ProcessorDescriptor processorDescriptor,
                                     int parallelism,
                                     Resource taskResource) {
    return new PreWarmVertex(vertexName, processorDescriptor, parallelism, taskResource);
  }

  /**
   * Create a {@link PreWarmVertex} to be used in @link
   * {@link TezClient#preWarm(PreWarmVertex)} This uses a built in pre-warm
   * processor that implements common functionality. Users may derive from this
   * processor to add custom functionality but then they must add the jar for
   * that class to the prewarm vertex and other vertices in their DAG for which
   * they want the containers to be reused. It may be necessary to call more
   * methods to add local files etc on the pre-warm vertex post creation so that
   * it matches the real DAG vertices.
   *
   * @param vertexName
   *          Name of the vertex
   * @param parallelism
   *          Number of containers to be pre-warmed
   * @param taskResource
   *          Execution cpu/memory resources etc needed
   */
  public static PreWarmVertex create(String vertexName, int parallelism, Resource taskResource) {
    return new PreWarmVertex(vertexName, parallelism, taskResource);
  }

  /**
   * Setup the prewarm vertex constructor. By default is uses the built-in
   * PreWarmProcessor and sets up the prewarm container number equal to
   * {@link TezConfiguration#TEZ_AM_SESSION_MIN_HELD_CONTAINERS}
   */
  public static class PreWarmVertexConfigBuilder {
    String name;
    int parallelism;
    ProcessorDescriptor proc;
    Resource resource;
    Configuration conf;
    
    PreWarmVertexConfigBuilder(Configuration conf) {
      this.conf = conf;
    }
    
    public PreWarmVertexConfigBuilder setName(String name) {
      this.name = name;
      return this;
    }
    
    public PreWarmVertexConfigBuilder setProcessorDescriptor(ProcessorDescriptor proc) {
      this.proc = proc;
      return this;
    }
    
    public PreWarmVertexConfigBuilder setResource(Resource resource) {
      this.resource = resource;
      return this;
    }
    
    public PreWarmVertexConfigBuilder setParallelism(int parallelism) {
      this.parallelism = parallelism;
      return this;
    }
    
    public PreWarmVertex build() {
      if (name == null) {
        name = "_PreWarm_";
      }
      if (parallelism == 0) {
        parallelism = conf.getInt(TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS, -1);
        if (parallelism == -1) {
          throw new TezUncheckedException("Prewarm parallelism must be set or specified in conf via " 
              + TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS);
        }
      }
      if (proc == null) {
        proc =
            ProcessorDescriptor.create("org.apache.tez.runtime.library.processor.PreWarmProcessor");
      }
      
      return create(name, proc, parallelism, resource);
    }
  }

}
