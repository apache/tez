/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.tez.dag.api.*;
import org.apache.tez.test.TestInput;
import org.apache.tez.test.TestOutput;
import org.apache.tez.test.TestProcessor;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * A builder for a DAG with vertices divided into a maximum of 6 levels.
 * Vertex name is "l<level number>v<vertex number>". Level/vertex numbers start at 1.
 * Each vertex has failing processor and failing inputs.
 * The builder can accept Tez Configuration to indicate failing patterns.
 * The number of levels in the built DAG can be configured.
 * <p>
 * DAG is shown with a diagram below.
 * Each vertex has its degree of parallelism indicated in a bracket following its name.
 * Each edge annotates with data movement (s = scatter/gather, b = broadcast)
 * <p>
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

public class FailingDagBuilder {

  private final static Resource DEFAULT_RESOURCE = org.apache.hadoop.yarn.api.records.Resource.newInstance(100, 0);

  private final Levels levels;
  private String name;
  private Configuration conf;

  public enum Levels {
    TWO("TwoLevelsFailingDAG", (dag, payload) -> {
      Vertex l1v1 = Vertex.create("l1v1", TestProcessor.getProcDesc(payload), 1, DEFAULT_RESOURCE);
      Vertex l2v1 = Vertex.create("l2v1", TestProcessor.getProcDesc(payload), 1, DEFAULT_RESOURCE);
      addVerticesAndEdgeInternal(dag, l1v1, l2v1, EdgeProperty.DataMovementType.SCATTER_GATHER, payload);
      Vertex l1v2 = Vertex.create("l1v2", TestProcessor.getProcDesc(payload), 2, DEFAULT_RESOURCE);
      Vertex l2v2 = Vertex.create("l2v2", TestProcessor.getProcDesc(payload), 3, DEFAULT_RESOURCE);
      addVerticesAndEdgeInternal(dag, l1v2, l2v2, EdgeProperty.DataMovementType.SCATTER_GATHER, payload);
      Vertex l1v3 = Vertex.create("l1v3", TestProcessor.getProcDesc(payload), 3, DEFAULT_RESOURCE);
      Vertex l2v3 = Vertex.create("l2v3", TestProcessor.getProcDesc(payload), 2, DEFAULT_RESOURCE);
      addVerticesAndEdgeInternal(dag, l1v3, l2v3, EdgeProperty.DataMovementType.SCATTER_GATHER, payload);
      Vertex l1v4 = Vertex.create("l1v4", TestProcessor.getProcDesc(payload), 2, DEFAULT_RESOURCE);
      Vertex l2v4 = Vertex.create("l2v4", TestProcessor.getProcDesc(payload), 3, DEFAULT_RESOURCE);
      addVerticesAndEdgeInternal(dag, l1v4, l2v4, EdgeProperty.DataMovementType.BROADCAST, payload);
    }),
    THREE("ThreeLevelsFailingDAG", (dag, payload) -> {
      TWO.levelAdder.accept(dag, payload);
      Vertex l3v1 = Vertex.create("l3v1", TestProcessor.getProcDesc(payload), 4, DEFAULT_RESOURCE);
      dag.addVertex(l3v1);
      addEdge(dag, dag.getVertex("l2v1"), l3v1, EdgeProperty.DataMovementType.SCATTER_GATHER, payload);
      addEdge(dag, dag.getVertex("l2v2"), l3v1, EdgeProperty.DataMovementType.SCATTER_GATHER, payload);
      Vertex l3v2 = Vertex.create("l3v2", TestProcessor.getProcDesc(payload), 4, DEFAULT_RESOURCE);
      dag.addVertex(l3v2);
      addEdge(dag, dag.getVertex("l2v2"), l3v2, EdgeProperty.DataMovementType.BROADCAST, payload);
      addEdge(dag, dag.getVertex("l2v3"), l3v2, EdgeProperty.DataMovementType.SCATTER_GATHER, payload);
      addEdge(dag, dag.getVertex("l2v4"), l3v2, EdgeProperty.DataMovementType.SCATTER_GATHER, payload);
    }),
    SIX("SixLevelsFailingDAG", (dag, payload) -> {
      THREE.levelAdder.accept(dag, payload);
      Vertex l4v1 = Vertex.create("l4v1", TestProcessor.getProcDesc(payload), 10, DEFAULT_RESOURCE);
      dag.addVertex(l4v1);
      addEdge(dag, dag.getVertex("l3v1"), l4v1, EdgeProperty.DataMovementType.SCATTER_GATHER, payload);
      addEdge(dag, dag.getVertex("l3v2"), l4v1, EdgeProperty.DataMovementType.SCATTER_GATHER, payload);
      Vertex l5v1 = Vertex.create("l5v1", TestProcessor.getProcDesc(payload), 2, DEFAULT_RESOURCE);
      dag.addVertex(l5v1);
      addEdge(dag, l4v1, l5v1, EdgeProperty.DataMovementType.SCATTER_GATHER, payload);
      Vertex l5v2 = Vertex.create("l5v2", TestProcessor.getProcDesc(payload), 4, DEFAULT_RESOURCE);
      dag.addVertex(l5v2);
      addEdge(dag, l4v1, l5v2, EdgeProperty.DataMovementType.SCATTER_GATHER, payload);
      Vertex l5v3 = Vertex.create("l5v3", TestProcessor.getProcDesc(payload), 1, DEFAULT_RESOURCE);
      dag.addVertex(l5v3);
      addEdge(dag, l4v1, l5v3, EdgeProperty.DataMovementType.SCATTER_GATHER, payload);
      Vertex l6v1 = Vertex.create("l6v1", TestProcessor.getProcDesc(payload), 4, DEFAULT_RESOURCE);
      dag.addVertex(l6v1);
      addEdge(dag, l5v1, l6v1, EdgeProperty.DataMovementType.SCATTER_GATHER, payload);
      addEdge(dag, l5v2, l6v1, EdgeProperty.DataMovementType.SCATTER_GATHER, payload);
      addEdge(dag, l5v3, l6v1, EdgeProperty.DataMovementType.SCATTER_GATHER, payload);
    });

    private final String defaultName;
    private final BiConsumer<DAG, UserPayload> levelAdder;
    Levels(String defaultName, BiConsumer<DAG, UserPayload> levelAdder) {
      this.defaultName = defaultName;
      this.levelAdder = levelAdder;
    }

    private static void addVerticesAndEdgeInternal(
        DAG dag, Vertex v1, Vertex v2, EdgeProperty.DataMovementType dataMovementType, UserPayload payload) {
      dag.addVertex(v1).addVertex(v2);
      addEdge(dag, v1, v2, dataMovementType, payload);
    }

    private static void addEdge(
        DAG dag, Vertex v1, Vertex v2, EdgeProperty.DataMovementType dataMovementType, UserPayload payload) {
      dag.addEdge(Edge.create(v1, v2,
          EdgeProperty.create(dataMovementType,
              EdgeProperty.DataSourceType.PERSISTED,
              EdgeProperty.SchedulingType.SEQUENTIAL,
              TestOutput.getOutputDesc(payload),
              TestInput.getInputDesc(payload))));
    }
  }

  public FailingDagBuilder(Levels levels) {
    this.levels = levels;
    this.name = levels.defaultName;
  }

  public FailingDagBuilder withConf(Configuration config) {
    conf = config;
    return this;
  }

  public FailingDagBuilder withName(String dagName) {
    name = dagName;
    return this;
  }

  public DAG build() throws IOException {
    UserPayload payload = UserPayload.create(null);
    if (conf != null) {
      payload = TezUtils.createUserPayloadFromConf(conf);
    }

    DAG dag = DAG.create(name);

    levels.levelAdder.accept(dag, payload);

    return dag;
  }
}
