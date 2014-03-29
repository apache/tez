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

package org.apache.tez.dag.app.dag;

import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.RuntimeUtils;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.app.dag.event.VertexEventRootInputFailed;
import org.apache.tez.dag.app.dag.event.VertexEventRootInputInitialized;
import org.apache.tez.dag.app.dag.impl.RootInputLeafOutputDescriptor;
import org.apache.tez.dag.app.dag.impl.TezRootInputInitializerContextImpl;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezRootInputInitializer;
import org.apache.tez.runtime.api.TezRootInputInitializerContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class RootInputInitializerRunner {

  private static final Log LOG = LogFactory.getLog(RootInputInitializerRunner.class);
  
  private final ExecutorService rawExecutor;
  private final ListeningExecutorService executor;
  private final String dagName;
  private final String vertexName;
  private final TezVertexID vertexID;
  private final int numTasks;
  private final Resource vertexTaskResource;
  private final Resource totalResource;
  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  private volatile boolean isStopped = false;
  private final UserGroupInformation dagUgi;
  private final int numClusterNodes;

  @SuppressWarnings("rawtypes")
  public RootInputInitializerRunner(String dagName, String vertexName,
      TezVertexID vertexID, EventHandler eventHandler, UserGroupInformation dagUgi,
      Resource vertexTaskResource, Resource totalResource, int numTasks, int numNodes) {
    this.dagName = dagName;
    this.vertexName = vertexName;
    this.vertexID = vertexID;
    this.eventHandler = eventHandler;
    this.vertexTaskResource = vertexTaskResource;
    this.totalResource = totalResource;
    this.numTasks = numTasks;
    this.rawExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("InputInitializer [" + this.vertexName + "] #%d").build());
    this.executor = MoreExecutors.listeningDecorator(rawExecutor);
    this.dagUgi = dagUgi;
    this.numClusterNodes = numNodes;
  }
  
  public void runInputInitializers(List<RootInputLeafOutputDescriptor<InputDescriptor>> inputs) {
    for (RootInputLeafOutputDescriptor<InputDescriptor> input : inputs) {
      ListenableFuture<List<Event>> future = executor
          .submit(new InputInitializerCallable(input, vertexID, dagName,
              vertexName, dagUgi, numTasks, numClusterNodes, vertexTaskResource, totalResource));
      Futures.addCallback(future, createInputInitializerCallback(input.getEntityName()));
    }
  }

  @VisibleForTesting
  protected InputInitializerCallback createInputInitializerCallback(String entityName) {
    return new InputInitializerCallback(entityName, eventHandler, vertexID);
  }
  
  public void shutdown() {
    if (executor != null && !isStopped) {
      // Don't really care about what is running if an error occurs. If no error
      // occurs, all execution is complete.
      executor.shutdownNow();
      isStopped = true;
    }
  }

  private static class InputInitializerCallable implements
      Callable<List<Event>> {

    private final RootInputLeafOutputDescriptor<InputDescriptor> input;
    private final TezVertexID vertexID;
    private final String dagName;
    private final String vertexName;
    private final int numTasks;
    private final Resource vertexTaskResource;
    private final Resource totalResource;
    private final UserGroupInformation ugi;
    private final int numClusterNodes;

    public InputInitializerCallable(RootInputLeafOutputDescriptor<InputDescriptor> input,
        TezVertexID vertexID, String dagName, String vertexName, UserGroupInformation ugi, 
        int numTasks, int numClusterNodes, Resource vertexTaskResource, Resource totalResource) {
      this.input = input;
      this.vertexID = vertexID;
      this.dagName = dagName;
      this.vertexName = vertexName;
      this.numTasks = numTasks;
      this.vertexTaskResource = vertexTaskResource;
      this.totalResource = totalResource;
      this.ugi = ugi;
      this.numClusterNodes = numClusterNodes;
    }

    @Override
    public List<Event> call() throws Exception {
      List<Event> events = ugi.doAs(new PrivilegedExceptionAction<List<Event>>() {
        @Override
        public List<Event> run() throws Exception {
          TezRootInputInitializer initializer = createInitializer();
          TezRootInputInitializerContext context = new TezRootInputInitializerContextImpl(vertexID,
              dagName, vertexName, input.getEntityName(), input.getDescriptor(), 
              numTasks, numClusterNodes, vertexTaskResource, totalResource);
          return initializer.initialize(context);
        }
      });
      return events;
    }

    private TezRootInputInitializer createInitializer() throws InstantiationException,
        IllegalAccessException {
      String className = input.getInitializerClassName();
      @SuppressWarnings("unchecked")
      Class<? extends TezRootInputInitializer> clazz = (Class<? extends TezRootInputInitializer>) RuntimeUtils
          .getClazz(className);
      TezRootInputInitializer initializer = clazz.newInstance();
      return initializer;
    }
  }

  @SuppressWarnings("rawtypes")
  @VisibleForTesting
  private static class InputInitializerCallback implements
      FutureCallback<List<Event>> {

    private final String inputName;
    private final EventHandler eventHandler;
    private final TezVertexID vertexID;

    public InputInitializerCallback(String inputName,
        EventHandler eventHandler, TezVertexID vertexID) {
      this.inputName = inputName;
      this.eventHandler = eventHandler;
      this.vertexID = vertexID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onSuccess(List<Event> result) {
      eventHandler.handle(new VertexEventRootInputInitialized(vertexID,
          inputName, result));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onFailure(Throwable t) {
      eventHandler
          .handle(new VertexEventRootInputFailed(vertexID, inputName, t));
    }
  }
}
