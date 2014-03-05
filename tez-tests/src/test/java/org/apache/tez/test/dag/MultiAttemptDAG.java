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

package org.apache.tez.test.dag;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.test.TestInput;
import org.apache.tez.test.TestOutput;
import org.apache.tez.test.TestProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiAttemptDAG {

  private static final Log LOG =
      LogFactory.getLog(MultiAttemptDAG.class);

  static Resource defaultResource = Resource.newInstance(100, 0);
  public static String MULTI_ATTEMPT_DAG_VERTEX_NUM_TASKS =
      "tez.multi-attempt-dag.vertex.num-tasks";
  public static int MULTI_ATTEMPT_DAG_VERTEX_NUM_TASKS_DEFAULT = 2;

  public static class FailOnAttemptVertexManagerPlugin
      implements VertexManagerPlugin {
    private int numSourceTasks = 0;
    private AtomicInteger numCompletions = new AtomicInteger();
    private VertexManagerPluginContext context;
    private boolean tasksScheduled = false;

    @Override
    public void initialize(VertexManagerPluginContext context) {
      this.context = context;
      for (String input :
          context.getInputVertexEdgeProperties().keySet()) {
        LOG.info("Adding sourceTasks for Vertex " + input);
        numSourceTasks += context.getVertexNumTasks(input);
        LOG.info("Current numSourceTasks=" + numSourceTasks);
      }
    }

    @Override
    public void onVertexStarted(Map<String, List<Integer>> completions) {
      if (completions != null) {
        for (Entry<String, List<Integer>> entry : completions.entrySet()) {
          LOG.info("Received completion events on vertexStarted"
              + ", vertex=" + entry.getKey()
              + ", completions=" + entry.getValue().size());
          numCompletions.addAndGet(entry.getValue().size());
        }
      }
      maybeScheduleTasks();
    }

    private synchronized void maybeScheduleTasks() {
      if (numCompletions.get() >= numSourceTasks
          && !tasksScheduled) {
        tasksScheduled = true;
        String payload = new String(context.getUserPayload());
        int successAttemptId = Integer.valueOf(payload);
        LOG.info("Checking whether to crash AM or schedule tasks"
            + ", successfulAttemptID=" + successAttemptId
            + ", currentAttempt=" + context.getDAGAttemptNumber());
        if (successAttemptId > context.getDAGAttemptNumber()) {
          Runtime.getRuntime().halt(-1);
        } else if (successAttemptId == context.getDAGAttemptNumber()) {
          LOG.info("Scheduling tasks for vertex=" + context.getVertexName());
          int numTasks = context.getVertexNumTasks(context.getVertexName());
          List<Integer> scheduledTasks = new ArrayList<Integer>(numTasks);
          for (int i=0; i<numTasks; ++i) {
            scheduledTasks.add(new Integer(i));
          }
          context.scheduleVertexTasks(scheduledTasks);
        }
      }
    }

    @Override
    public void onSourceTaskCompleted(String srcVertexName, Integer taskId) {
      LOG.info("Received completion events for source task"
          + ", vertex=" + srcVertexName
          + ", taskIdx=" + taskId);
      numCompletions.incrementAndGet();
      maybeScheduleTasks();
    }

    @Override
    public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
      // Nothing to do
    }

    @Override
    public void onRootVertexInitialized(String inputName, InputDescriptor inputDescriptor, List<Event> events) {
      // Do nothing
    }
  }


  public static DAG createDAG(String name,
      Configuration conf) throws Exception {
    byte[] payload = null;
    int taskCount = MULTI_ATTEMPT_DAG_VERTEX_NUM_TASKS_DEFAULT;
    if (conf != null) {
      taskCount = conf.getInt(MULTI_ATTEMPT_DAG_VERTEX_NUM_TASKS, MULTI_ATTEMPT_DAG_VERTEX_NUM_TASKS_DEFAULT);
      payload = TezUtils.createUserPayloadFromConf(conf);
    }
    DAG dag = new DAG(name);
    Vertex v1 = new Vertex("v1", TestProcessor.getProcDesc(payload), taskCount, defaultResource);
    Vertex v2 = new Vertex("v2", TestProcessor.getProcDesc(payload), taskCount, defaultResource);
    Vertex v3 = new Vertex("v3", TestProcessor.getProcDesc(payload), taskCount, defaultResource);

    // Make each vertex manager fail on appropriate attempt
    v1.setVertexManagerPlugin(new VertexManagerPluginDescriptor(
        FailOnAttemptVertexManagerPlugin.class.getName())
        .setUserPayload(new String("1").getBytes()));
    v2.setVertexManagerPlugin(new VertexManagerPluginDescriptor(
        FailOnAttemptVertexManagerPlugin.class.getName())
        .setUserPayload(new String("2").getBytes()));
    v3.setVertexManagerPlugin(new VertexManagerPluginDescriptor(
        FailOnAttemptVertexManagerPlugin.class.getName())
        .setUserPayload(new String("3").getBytes()));
    dag.addVertex(v1).addVertex(v2).addVertex(v3);
    dag.addEdge(new Edge(v1, v2,
        new EdgeProperty(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED,
            SchedulingType.SEQUENTIAL,
            TestOutput.getOutputDesc(payload),
            TestInput.getInputDesc(payload))));
    dag.addEdge(new Edge(v2, v3,
        new EdgeProperty(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED,
            SchedulingType.SEQUENTIAL,
            TestOutput.getOutputDesc(payload),
            TestInput.getInputDesc(payload))));
    return dag;
  }

  public static DAG createDAG(Configuration conf) throws Exception {
    return createDAG("SimpleVTestDAG", conf);
  }

}
