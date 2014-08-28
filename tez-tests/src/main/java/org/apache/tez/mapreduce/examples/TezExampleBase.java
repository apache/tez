/*
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

package org.apache.tez.mapreduce.examples;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;

public abstract class TezExampleBase extends Configured implements Tool {

  private TezClient tezClientInternal;

  @Override
  public final int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    return _execute(otherArgs, null, null);
  }

  /**
   * Utility method to use the example from within code or a test.
   *
   * @param conf      the tez configuration instance which will be used to crate the DAG and
   *                  possible the Tez Client.
   * @param args      arguments to the example
   * @param tezClient an existing running {@link org.apache.tez.client.TezClient} instance if one
   *                  exists. If no TezClient is specified (null), one will be created based on the
   *                  provided configuration
   * @return
   * @throws IOException
   * @throws TezException
   */
  public int run(TezConfiguration conf, String[] args, @Nullable TezClient tezClient) throws
      IOException,
      TezException, InterruptedException {
    setConf(conf);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    return _execute(otherArgs, conf, tezClient);
  }

  /**
   * @param dag           the dag to execute
   * @param printCounters whether to print counters or not
   * @param logger        the logger to use while printing diagnostics
   * @return
   * @throws TezException
   * @throws InterruptedException
   * @throws IOException
   */
  public int runDag(DAG dag, boolean printCounters, Log logger) throws TezException,
      InterruptedException, IOException {
    tezClientInternal.waitTillReady();
    DAGClient dagClient = tezClientInternal.submitDAG(dag);
    Set<StatusGetOpts> getOpts = Sets.newHashSet();
    if (printCounters) {
      getOpts.add(StatusGetOpts.GET_COUNTERS);
    }

    DAGStatus dagStatus;
    dagStatus = dagClient.waitForCompletionWithStatusUpdates(getOpts);

    if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
      logger.info("DAG diagnostics: " + dagStatus.getDiagnostics());
      return -1;
    }
    return 0;
  }

  private int _validateArgs(String[] args) {
    int res = validateArgs(args);
    if (res != 0) {
      printUsage();
      return res;
    }
    return 0;
  }

  private int _execute(String[] otherArgs, TezConfiguration tezConf, TezClient tezClient) throws
      IOException, TezException, InterruptedException {

    int result = _validateArgs(otherArgs);
    if (result != 0) {
      return result;
    }

    if (tezConf == null) {
      tezConf = new TezConfiguration(getConf());
    }
    UserGroupInformation.setConfiguration(tezConf);
    boolean ownTezClient = false;
    if (tezClient == null) {
      ownTezClient = true;
      tezClientInternal = createTezClient(tezConf);
    }
    try {
      return runJob(otherArgs, tezConf, tezClientInternal);
    } finally {
      if (ownTezClient && tezClientInternal != null) {
        tezClientInternal.stop();
      }
    }
  }

  private TezClient createTezClient(TezConfiguration tezConf) throws IOException, TezException {
    TezClient tezClient = TezClient.create(getClass().getSimpleName(), tezConf);
    tezClient.start();
    return tezClient;
  }

  /**
   * Print usage instructions for this example
   */
  protected abstract void printUsage();

  /**
   * Validate the arguments
   *
   * @param otherArgs arguments, if any
   * @return
   */
  protected abstract int validateArgs(String[] otherArgs);

  /**
   * Create and execute the actual DAG for the example
   *
   * @param args      arguments for execution
   * @param tezConf   the tez configuration instance to be used while processing the DAG
   * @param tezClient the tez client instance to use to run the DAG if any custom monitoring is
   *                  required. Otherwise the utility method {@link #runDag(org.apache.tez.dag.api.DAG,
   *                  boolean, org.apache.commons.logging.Log)} should be used
   * @return
   * @throws IOException
   * @throws TezException
   */
  protected abstract int runJob(String[] args, TezConfiguration tezConf,
                                TezClient tezClient) throws IOException, TezException,
      InterruptedException;
}
