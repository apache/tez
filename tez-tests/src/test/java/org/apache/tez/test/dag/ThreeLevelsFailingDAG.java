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
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.test.TestProcessor;

/**
 * A DAG with vertices divided into 3 levels.
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
 *    \s        /s    \b     |s       /s
 *     \       /        \    |      /
 *      l3v1(4)            l3v2(4)
 *
 */
public class ThreeLevelsFailingDAG extends TwoLevelsFailingDAG {
    
    protected static Vertex l3v1, l3v2;
    
    protected static void addDAGVerticesAndEdges() {
        TwoLevelsFailingDAG.addDAGVerticesAndEdges();
        l3v1 = Vertex.create("l3v1", TestProcessor.getProcDesc(payload), 4, defaultResource);
        dag.addVertex(l3v1);
        addEdge(l2v1, l3v1, DataMovementType.SCATTER_GATHER);
        addEdge(l2v2, l3v1, DataMovementType.SCATTER_GATHER);
        l3v2 = Vertex.create("l3v2", TestProcessor.getProcDesc(payload), 4, defaultResource);
        dag.addVertex(l3v2);
        addEdge(l2v2, l3v2, DataMovementType.BROADCAST);
        addEdge(l2v3, l3v2, DataMovementType.SCATTER_GATHER);
        addEdge(l2v4, l3v2, DataMovementType.SCATTER_GATHER);
    }
    
    public static DAG createDAG(String name, 
            Configuration conf) throws Exception {
        if (conf != null) {
          payload = TezUtils.createUserPayloadFromConf(conf);
        } 
        dag = DAG.create(name);
        addDAGVerticesAndEdges();
        return dag;
    }
    
    public static DAG createDAG(Configuration conf) throws Exception {
      return createDAG("ThreeLevelsFailingDAG", conf);
    }
}
