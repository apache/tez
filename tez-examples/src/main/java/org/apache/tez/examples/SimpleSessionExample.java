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

package org.apache.tez.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.PreWarmVertex;
import org.apache.tez.dag.api.TezConfiguration;

/**
 * Simple example that shows how Tez session mode can be used to run multiple DAGs in the same 
 * session for efficiency and performance. Tez session mode enables the execution environment to 
 * hold onto execution resources so that they can be reused to across multiple DAGs. Typical use 
 * cases for this would be drill-down queries from a user shell or maintaining a pool of resources
 * to provide low latency execution of DAGs for an application.
 * In this example we will be submitting multiple OrderedWordCount DAGs on different inputs to the 
 * same session.
 */
public class SimpleSessionExample extends TezExampleBase {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleSessionExample.class);
  private static final String enablePrewarmConfig = "simplesessionexample.prewarm";

  @Override
  protected void printUsage() {
    System.err.println("Usage: " + " simplesessionexample"
        + " <in1,in2> <out1, out2> [numPartitions]");
  }

  @Override
  protected int validateArgs(String[] otherArgs) {
    if (otherArgs.length < 2 || otherArgs.length > 3) {
      return 2;
    }
    return 0;
  }

  @Override
  protected int runJob(String[] args, TezConfiguration tezConf,
      TezClient tezClient) throws Exception {
    System.out.println("Running SimpleSessionExample");
    String[] inputPaths = args[0].split(",");
    String[] outputPaths = args[1].split(",");
    if (inputPaths.length != outputPaths.length) {
      System.err.println("Inputs and outputs must be equal in number");
      return 3;
    }
    int numPartitions = args.length == 3 ? Integer.parseInt(args[2]) : 1;

    // Session pre-warming allows the user to hide initial startup, resource acquisition latency etc.
    // by pre-allocating execution resources in the Tez session. They can run initialization logic 
    // in these pre-allocated resources (containers) to pre-warm the containers.
    // In between DAG executions, the session can hold on to a minimum number of containers.
    // Ideally, this would be enough to provide desired balance of efficiency for the application 
    // and sharing of resources with other applications. Typically, the number of containers to be 
    // pre-warmed equals the number of containers to be held between DAGs.
    if (tezConf.getBoolean(enablePrewarmConfig, false)) {
      // the above parameter is not a Tez parameter. Its only for this example.
      // In this example we are pre-warming enough containers to run all the sum tasks in parallel.
      // This means pre-warming numPartitions number of containers.
      // We are making the pre-warm and held containers to be the same and using the helper API to 
      // set up pre-warming. They can be made different and also custom initialization logic can be 
      // specified using other API's. We know that the OrderedWordCount dag uses default files and 
      // resources. Otherwise we would have to specify matching parameters in the preWarm API too.
      tezConf.setInt(TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS, numPartitions);
      tezClient.preWarm(PreWarmVertex.createConfigBuilder(tezConf).build());
    }

    for (int i = 0; i < inputPaths.length; ++i) {
      DAG dag = OrderedWordCount.createDAG(tezConf, inputPaths[i], outputPaths[i], numPartitions,
          isDisableSplitGrouping(), ("DAG-Iteration-" + i)); // the names of the DAGs must be unique in a session

      LOG.info("Running dag number " + i);
      if(runDag(dag, false, LOG) != 0) {
        return -1;
      }
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    TezConfiguration tezConf = new TezConfiguration();
    tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true);
    int res = ToolRunner.run(tezConf, new SimpleSessionExample(), args);
    System.exit(res);
  }
}
