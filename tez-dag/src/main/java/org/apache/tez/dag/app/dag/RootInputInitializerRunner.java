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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.app.dag.event.VertexEventRootInputFailed;
import org.apache.tez.dag.app.dag.event.VertexEventRootInputInitialized;
import org.apache.tez.dag.app.dag.impl.RootInputLeafOutputDescriptor;
import org.apache.tez.dag.app.dag.impl.TezRootInputInitializerContextImpl;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezRootInputInitializer;
import org.apache.tez.runtime.api.TezRootInputInitializerContext;

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
  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  private volatile boolean isStopped = false;

  @SuppressWarnings("rawtypes")
  public RootInputInitializerRunner(String dagName, String vertexName, TezVertexID vertexID, EventHandler eventHandler) {
    this.dagName = dagName;
    this.vertexName = vertexName;
    this.vertexID = vertexID;
    this.eventHandler = eventHandler;
    this.rawExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("InputInitializer [" + this.vertexName + "] #%d").build());
    this.executor = MoreExecutors.listeningDecorator(rawExecutor);
  }

  public void runInputInitializers(List<RootInputLeafOutputDescriptor<InputDescriptor>> inputs) {
    for (RootInputLeafOutputDescriptor<InputDescriptor> input : inputs) {
      ListenableFuture<List<Event>> future = executor
          .submit(new InputInitializerCallable(input, vertexID, dagName,
              vertexName));
      Futures.addCallback(future, new InputInitializerCallback(input.getEntityName()));
    }
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

    public InputInitializerCallable(RootInputLeafOutputDescriptor<InputDescriptor> input,
        TezVertexID vertexID, String dagName, String vertexName) {
      this.input = input;
      this.vertexID = vertexID;
      this.dagName = dagName;
      this.vertexName = vertexName;
    }

    @Override
    public List<Event> call() throws Exception {
      TezRootInputInitializer initializer = createInitializer();
      TezRootInputInitializerContext context = new TezRootInputInitializerContextImpl(
          vertexID, dagName, vertexName, input.getEntityName(), input.getDescriptor());
      return initializer.initialize(context);
    }

    private TezRootInputInitializer createInitializer()
        throws ClassNotFoundException, InstantiationException,
        IllegalAccessException {
      String className = input.getInitializerClassName();
      @SuppressWarnings("unchecked")
      Class<? extends TezRootInputInitializer> clazz = (Class<? extends TezRootInputInitializer>) Class
          .forName(className);
      TezRootInputInitializer initializer = clazz.newInstance();
      return initializer;
    }
  }

  private class InputInitializerCallback implements FutureCallback<List<Event>> {

    private final String inputName;

    public InputInitializerCallback(String inputName) {
      this.inputName = inputName;
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
