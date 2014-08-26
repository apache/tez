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
 * A DAG with vertices divided into 6 levels.
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
 *          \s              /s
 *            \            /
 *               l4v1 (10)
 *              /s   |s   \s
 *            /      |      \
 *     l5v1(2)    l5v2(4)     l5v3(1)
 *           \s      |s      /s
 *             \     |      /
 *                l6v1(4)
 *
 */
public class SixLevelsFailingDAG extends ThreeLevelsFailingDAG {
    
    protected static Vertex l4v1;
    protected static Vertex l5v1, l5v2, l5v3;
    protected static Vertex l6v1;
    
    protected static void addDAGVerticesAndEdges() {
        ThreeLevelsFailingDAG.addDAGVerticesAndEdges();
        l4v1 = Vertex.create("l4v1", TestProcessor.getProcDesc(payload), 10, defaultResource);
        dag.addVertex(l4v1);
        addEdge(l3v1, l4v1, DataMovementType.SCATTER_GATHER);
        addEdge(l3v2, l4v1, DataMovementType.SCATTER_GATHER);
        l5v1 = Vertex.create("l5v1", TestProcessor.getProcDesc(payload), 2, defaultResource);
        dag.addVertex(l5v1);
        addEdge(l4v1, l5v1, DataMovementType.SCATTER_GATHER);
        l5v2 = Vertex.create("l5v2", TestProcessor.getProcDesc(payload), 4, defaultResource);
        dag.addVertex(l5v2);
        addEdge(l4v1, l5v2, DataMovementType.SCATTER_GATHER);
        l5v3 = Vertex.create("l5v3", TestProcessor.getProcDesc(payload), 1, defaultResource);
        dag.addVertex(l5v3);
        addEdge(l4v1, l5v3, DataMovementType.SCATTER_GATHER);
        l6v1 = Vertex.create("l6v1", TestProcessor.getProcDesc(payload), 4, defaultResource);
        dag.addVertex(l6v1);
        addEdge(l5v1, l6v1, DataMovementType.SCATTER_GATHER);
        addEdge(l5v2, l6v1, DataMovementType.SCATTER_GATHER);
        addEdge(l5v3, l6v1, DataMovementType.SCATTER_GATHER);
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
      return createDAG("SixLevelsFailingDAG", conf);
    }
}
