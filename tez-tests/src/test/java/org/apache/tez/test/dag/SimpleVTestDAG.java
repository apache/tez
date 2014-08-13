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
import org.apache.tez.test.TestInput;
import org.apache.tez.test.TestOutput;
import org.apache.tez.test.TestProcessor;

/**
 * A DAG with 3 vertices of which their graph can be depicted easily with V shape.
 *  v1     v2
 *    \   /
 *     v3
 *
 */
public class SimpleVTestDAG {
  static Resource defaultResource = Resource.newInstance(100, 0);
  public static String TEZ_SIMPLE_V_DAG_NUM_TASKS =
      "tez.simple-v-test-dag.num-tasks";
  public static int TEZ_SIMPLE_V_DAG_NUM_TASKS_DEFAULT = 2;
  
  public static DAG createDAG(String name, 
      Configuration conf) throws Exception {
    UserPayload payload = new UserPayload(null);
    int taskCount = TEZ_SIMPLE_V_DAG_NUM_TASKS_DEFAULT;
    if (conf != null) {
      taskCount = conf.getInt(TEZ_SIMPLE_V_DAG_NUM_TASKS, TEZ_SIMPLE_V_DAG_NUM_TASKS_DEFAULT);
      payload = TezUtils.createUserPayloadFromConf(conf);
    }
    DAG dag = new DAG(name);
    Vertex v1 = new Vertex("v1", TestProcessor.getProcDesc(payload), taskCount, defaultResource);
    Vertex v2 = new Vertex("v2", TestProcessor.getProcDesc(payload), taskCount, defaultResource);
    Vertex v3 = new Vertex("v3", TestProcessor.getProcDesc(payload), taskCount, defaultResource);
    dag.addVertex(v1).addVertex(v2).addVertex(v3);
    dag.addEdge(new Edge(v1, v3, 
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
