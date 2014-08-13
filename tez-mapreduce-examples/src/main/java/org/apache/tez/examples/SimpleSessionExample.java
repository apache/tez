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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.PreWarmVertex;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;

/**
 * Simple example that shows how Tez session mode can be used to run multiple DAGs in the same 
 * session for efficiency and performance. Tez session mode enables the execution environment to 
 * hold onto execution resources so that they can be reused to across multiple DAGs. Typical use 
 * cases for this would be drill-down queries from a user shell or maintaining a pool of resources
 * to provide low latency execution of DAGs for an application.
 * In this example we will be submitting multiple OrderedWordCount DAGs on different inputs to the 
 * same session.
 */
public class SimpleSessionExample extends Configured implements Tool {
  
  private static final String enablePrewarmConfig = "simplesessionexample.prewarm";

  public boolean run(String[] inputPaths, String[] outputPaths, Configuration conf,
      int numPartitions) throws Exception {
    TezConfiguration tezConf;
    if (conf != null) {
      tezConf = new TezConfiguration(conf);
    } else {
      tezConf = new TezConfiguration();
    }
    
    // start TezClient in session mode. The same code run in session mode or non-session mode. The 
    // mode can be changed via configuration. However if the application wants to run exclusively in 
    // session mode then it can do so in code directly using the appropriate constructor
    
    // tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true); // via config OR via code
    TezClient tezClient = new TezClient("SimpleSessionExample", tezConf, true);
    tezClient.start();
    
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
      tezClient.preWarm(PreWarmVertex.createConfigurer(tezConf).create());
    }

    // the remaining code is the same as submitting any DAG.
    try {
      for (int i=0; i<inputPaths.length; ++i) {
        DAG dag = OrderedWordCount.createDAG(tezConf, inputPaths[i], outputPaths[i], numPartitions,
            ("DAG-Iteration-" + i)); // the names of the DAGs must be unique in a session

        tezClient.waitTillReady();
        System.out.println("Running dag number " + i);
        DAGClient dagClient = tezClient.submitDAG(dag);

        // wait to finish
        DAGStatus dagStatus = dagClient.waitForCompletion();
        if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
          System.out.println("Iteration " + i + " failed with diagnostics: "
              + dagStatus.getDiagnostics());
          return false;
        }
      }
      return true;
    } finally {
      tezClient.stop();
    }
  }

  private static void printUsage() {
    System.err.println("Usage: " + " simplesessionexample <in1,in2> <out1, out2> [numPartitions]");
    ToolRunner.printGenericCommandUsage(System.err);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    System.out.println("Running SimpleSessionExample");
    Configuration conf = getConf();
    String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length < 2 || otherArgs.length > 3) {
      printUsage();
      return 2;
    }
    
    String[] inputPaths = otherArgs[0].split(",");
    String[] outputPaths = otherArgs[1].split(",");
    if (inputPaths.length != outputPaths.length) {
      System.err.println("Inputs and outputs must be equal in number");
      return 3;
    }
    
    SimpleSessionExample job = new SimpleSessionExample();
    if (job.run(inputPaths, outputPaths, conf,
        (otherArgs.length == 3 ? Integer.parseInt(otherArgs[2]) : 1))) {
      return 0;
    }
    return 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SimpleSessionExample(), args);
    System.exit(res);
  }

}
