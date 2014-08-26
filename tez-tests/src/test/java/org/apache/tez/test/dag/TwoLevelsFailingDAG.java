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
 * A DAG with vertices divided into 2 levels.
 * Vertex name is "l<level number>v<vertex number>". Level/vertex numbers start at 1.
 * Each vertex has failing processor and failing inputs. The constructor can accept Tez Configuration to indicate failing patterns.
 * 
 * DAG is shown with a diagram below.
 * Each vertex has its degree of parallelism indicated in a bracket following its name.
 * Each edge annotates with data movement (s = scatter/gather, b = broadcast)
 * 
 * l1v1(1)     l1v2(2)     l1v3(3)     l1v4(2)
 *   |s          |s          |s          |b
 *   |           |           |           |
 * l2v1(1)     l2v2(3)     l2v3(2)     l2v4(3)
 *
 */
public class TwoLevelsFailingDAG {
    static Resource defaultResource = Resource.newInstance(100, 0);
    protected static DAG dag;
    protected static UserPayload payload = UserPayload.create(null);
    protected static Vertex l1v1, l1v2, l1v3, l1v4;
    protected static Vertex l2v1, l2v2, l2v3, l2v4;

    public static DAG createDAG(String name, 
            Configuration conf) throws Exception {
        if (conf != null) {
          payload = TezUtils.createUserPayloadFromConf(conf);
        } 
        dag = DAG.create(name);
        addDAGVerticesAndEdges();
        return dag;
    }
    
    protected static void addDAGVerticesAndEdges() {
        l1v1 = Vertex.create("l1v1", TestProcessor.getProcDesc(payload), 1, defaultResource);
        l2v1 = Vertex.create("l2v1", TestProcessor.getProcDesc(payload), 1, defaultResource);
        addVerticesAndEdgeInternal(l1v1, l2v1, DataMovementType.SCATTER_GATHER);
        l1v2 = Vertex.create("l1v2", TestProcessor.getProcDesc(payload), 2, defaultResource);
        l2v2 = Vertex.create("l2v2", TestProcessor.getProcDesc(payload), 3, defaultResource);
        addVerticesAndEdgeInternal(l1v2, l2v2, DataMovementType.SCATTER_GATHER);
        l1v3 = Vertex.create("l1v3", TestProcessor.getProcDesc(payload), 3, defaultResource);
        l2v3 = Vertex.create("l2v3", TestProcessor.getProcDesc(payload), 2, defaultResource);
        addVerticesAndEdgeInternal(l1v3, l2v3, DataMovementType.SCATTER_GATHER);
        l1v4 = Vertex.create("l1v4", TestProcessor.getProcDesc(payload), 2, defaultResource);
        l2v4 = Vertex.create("l2v4", TestProcessor.getProcDesc(payload), 3, defaultResource);
        addVerticesAndEdgeInternal(l1v4, l2v4, DataMovementType.BROADCAST);
    }
    
    /**
     * Adds 2 vertices and an edge connecting them.
     * Given two vertices must not exist.
     * 
     * @param v1 vertice 1
     * @param v2 vertice 2
     * @param dataMovementType Data movement type
     */
    protected static void addVerticesAndEdgeInternal(Vertex v1, Vertex v2, DataMovementType dataMovementType) {
        dag.addVertex(v1).addVertex(v2);
        addEdge(v1, v2, dataMovementType);
    }
    
    /**
     * Adds an edge to given 2 vertices.
     * @param v1 vertice 1
     * @param v2 vertice 2
     * @param dataMovementType Data movement type
     */
    protected static void addEdge(Vertex v1, Vertex v2, DataMovementType dataMovementType) {
        dag.addEdge(Edge.create(v1, v2,
            EdgeProperty.create(dataMovementType,
                DataSourceType.PERSISTED,
                SchedulingType.SEQUENTIAL,
                TestOutput.getOutputDesc(payload),
                TestInput.getInputDesc(payload))));
    }
    
    public static DAG createDAG(Configuration conf) throws Exception {
      return createDAG("TwoLevelsFailingDAG", conf);
    }
}
