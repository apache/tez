/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.test;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;

/**
 * A shuffle vertex manager that will set the vertex's parallelism upon
 * completion of its single grandparent simulating a very simplified
 * version of PigGraceShuffleVertexManager for testing purposes.
 *
 * This manager plugin should only be used for vertices that have a single
 * grandparent.
 */
public final class GraceShuffleVertexManagerForTest extends ShuffleVertexManager {

  private static final Logger logger = LoggerFactory.getLogger(GraceShuffleVertexManagerForTest.class);

  private GraceConf graceConf;
  private boolean isParallelismSet = false;

  public GraceShuffleVertexManagerForTest(VertexManagerPluginContext context) {
    super(context);
  }

  @Override
  public void initialize() {
    try {
      Configuration conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
      graceConf = GraceConf.fromConfiguration(conf);
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    getContext().registerForVertexStateUpdates(graceConf.grandparentVertex,
        EnumSet.of(VertexState.SUCCEEDED));
    logger.info("Watching {}", graceConf.grandparentVertex);
    super.initialize();
  }

  @Override
  public synchronized void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
    logger.info("Received onVertexStateUpdated");

    String vertexName = stateUpdate.getVertexName();
    VertexState vertexState = stateUpdate.getVertexState();

    Preconditions.checkState(graceConf != null,
        "Received state notification {} for vertex {} in vertex {} before manager was initialized",
        vertexState, vertexName, getContext().getVertexName());

    if (!shouldSetParallelism(stateUpdate)) {
      return;
    }
    getContext().reconfigureVertex(graceConf.desiredParallelism, null, null);
    isParallelismSet = true;

    logger.info("Initialize parallelism for {} to {}",
        getContext().getVertexName(), graceConf.desiredParallelism);
  }

  private boolean shouldSetParallelism(VertexStateUpdate update) {
    return !isParallelismSet &&
        update.getVertexState().equals(VertexState.SUCCEEDED) &&
        update.getVertexName().equals(graceConf.grandparentVertex);
  }

  private static final class GraceConf {

    static final String TEST_GRACE_GRANDPARENT_VERTEX = "test.grace.grandparent-vertex";
    static final String TEST_GRACE_DESIRED_PARALLELISM = "test.grace.desired-parallelism";

    final String grandparentVertex;
    final int desiredParallelism;

    GraceConf(GraceConfBuilder builder) {
      grandparentVertex = builder.grandparentVertex;
      desiredParallelism = builder.desiredParallelism;
    }

    static GraceConf fromConfiguration(Configuration conf) {
      return newConfBuilder()
          .setGrandparentVertex(conf.get(TEST_GRACE_GRANDPARENT_VERTEX))
          .setDesiredParallelism(conf.getInt(TEST_GRACE_DESIRED_PARALLELISM, -1))
          .build();
    }

    Configuration toConfiguration() {
      Configuration conf = new Configuration();
      conf.setStrings(TEST_GRACE_GRANDPARENT_VERTEX, grandparentVertex);
      conf.setInt(TEST_GRACE_DESIRED_PARALLELISM, desiredParallelism);
      return conf;
    }
  }

  public static GraceConfBuilder newConfBuilder() {
    return new GraceConfBuilder();
  }

  public static final class GraceConfBuilder {

    private String grandparentVertex;
    private int desiredParallelism;

    private GraceConfBuilder() {
    }

    public GraceConfBuilder setGrandparentVertex(String grandparentVertex) {
      this.grandparentVertex = grandparentVertex;
      return this;
    }

    public GraceConfBuilder setDesiredParallelism(int desiredParallelism) {
      this.desiredParallelism = desiredParallelism;
      return this;
    }

    public ByteString toByteString() throws IOException {
      return TezUtils.createByteStringFromConf(build().toConfiguration());
    }

    private GraceConf build() {
      Preconditions.checkNotNull(grandparentVertex,
          "Grandparent vertex is required");
      Preconditions.checkArgument(desiredParallelism > 0,
          "Desired parallelism must be greater than 0");
      return new GraceConf(this);
    }
  }
}
