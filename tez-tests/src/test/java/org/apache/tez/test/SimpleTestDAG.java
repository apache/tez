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

package org.apache.tez.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;

/**
 * Simple Test DAG with 2 vertices using TestProcessor/TestInput/TestOutput.
 * 
 * v1
 * |
 * v2
 *
 */
public class SimpleTestDAG {
  static Resource defaultResource = Resource.newInstance(100, 0);
  public static String TEZ_SIMPLE_DAG_NUM_TASKS =
      "tez.simple-test-dag.num-tasks";
  public static int TEZ_SIMPLE_DAG_NUM_TASKS_DEFAULT = 2;
  
  public static DAG createDAG(String name, 
      Configuration conf) throws Exception {
    UserPayload payload = UserPayload.create(null);
    int taskCount = TEZ_SIMPLE_DAG_NUM_TASKS_DEFAULT;
    if (conf != null) {
      taskCount = conf.getInt(TEZ_SIMPLE_DAG_NUM_TASKS, TEZ_SIMPLE_DAG_NUM_TASKS_DEFAULT);
      payload = TezUtils.createUserPayloadFromConf(conf);
    }
    DAG dag = DAG.create(name);
    Vertex v1 = Vertex.create("v1", TestProcessor.getProcDesc(payload), taskCount, defaultResource);
    Vertex v2 = Vertex.create("v2", TestProcessor.getProcDesc(payload), taskCount, defaultResource);
    dag.addVertex(v1).addVertex(v2).addEdge(Edge.create(v1, v2,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED,
            SchedulingType.SEQUENTIAL,
            TestOutput.getOutputDesc(payload),
            TestInput.getInputDesc(payload))));
    return dag;
  }
  
  public static DAG createDAG(Configuration conf) throws Exception {
    return createDAG("SimpleTestDAG", conf);
  }

  /**
   *  v1  v2
   *   \  /
   *    v3
   *   /  \
   *  v4  v5
   *   \  /
   *    v6
   * @param name
   * @param conf
   * @return
   * @throws Exception
   */
  public static DAG createDAGForVertexOrder(String name, Configuration conf) throws Exception{
    UserPayload payload = UserPayload.create(null);
    int taskCount = TEZ_SIMPLE_DAG_NUM_TASKS_DEFAULT;
    if (conf != null) {
      taskCount = conf.getInt(TEZ_SIMPLE_DAG_NUM_TASKS, TEZ_SIMPLE_DAG_NUM_TASKS_DEFAULT);
      payload = TezUtils.createUserPayloadFromConf(conf);
    }
    DAG dag = DAG.create(name);

    Vertex v1 = Vertex.create("v1", TestProcessor.getProcDesc(payload), taskCount, defaultResource);
    Vertex v2 = Vertex.create("v2", TestProcessor.getProcDesc(payload), taskCount, defaultResource);
    Vertex v3 = Vertex.create("v3", TestProcessor.getProcDesc(payload), taskCount, defaultResource);
    Vertex v4 = Vertex.create("v4", TestProcessor.getProcDesc(payload), taskCount, defaultResource);
    Vertex v5 = Vertex.create("v5", TestProcessor.getProcDesc(payload), taskCount, defaultResource);
    Vertex v6 = Vertex.create("v6", TestProcessor.getProcDesc(payload), taskCount, defaultResource);

    // add vertex not in the topological order, since we are using this dag for testing vertex topological order
    dag.addVertex(v4)
      .addVertex(v5)
      .addVertex(v6)
      .addVertex(v1)
      .addVertex(v2)
      .addVertex(v3)
      .addEdge(Edge.create(v1, v3,
          EdgeProperty.create(DataMovementType.SCATTER_GATHER,
              DataSourceType.PERSISTED,
              SchedulingType.SEQUENTIAL,
              TestOutput.getOutputDesc(payload),
              TestInput.getInputDesc(payload))))
      .addEdge(Edge.create(v2, v3,
          EdgeProperty.create(DataMovementType.SCATTER_GATHER,
              DataSourceType.PERSISTED,
              SchedulingType.SEQUENTIAL,
              TestOutput.getOutputDesc(payload),
              TestInput.getInputDesc(payload))))
      .addEdge(Edge.create(v3, v4,
          EdgeProperty.create(DataMovementType.SCATTER_GATHER,
              DataSourceType.PERSISTED,
              SchedulingType.SEQUENTIAL,
              TestOutput.getOutputDesc(payload),
              TestInput.getInputDesc(payload))))
      .addEdge(Edge.create(v3, v5,
          EdgeProperty.create(DataMovementType.SCATTER_GATHER,
              DataSourceType.PERSISTED,
              SchedulingType.SEQUENTIAL,
              TestOutput.getOutputDesc(payload),
              TestInput.getInputDesc(payload))))
      .addEdge(Edge.create(v4, v6,
          EdgeProperty.create(DataMovementType.SCATTER_GATHER,
              DataSourceType.PERSISTED,
              SchedulingType.SEQUENTIAL,
              TestOutput.getOutputDesc(payload),
              TestInput.getInputDesc(payload))))
      .addEdge(Edge.create(v5, v6,
          EdgeProperty.create(DataMovementType.SCATTER_GATHER,
              DataSourceType.PERSISTED,
              SchedulingType.SEQUENTIAL,
              TestOutput.getOutputDesc(payload),
              TestInput.getInputDesc(payload))));

    return dag;
  }
}
