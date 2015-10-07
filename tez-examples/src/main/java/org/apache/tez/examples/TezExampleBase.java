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

package org.apache.tez.examples;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.commons.cli.Options;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.CallerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

@InterfaceAudience.Private
public abstract class TezExampleBase extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(TezExampleBase.class);

  private TezClient tezClientInternal;
  protected static final String DISABLE_SPLIT_GROUPING = "disableSplitGrouping";
  protected static final String LOCAL_MODE = "local";

  private boolean disableSplitGrouping = false;
  private boolean isLocalMode = false;

  protected boolean isDisableSplitGrouping() {
    return disableSplitGrouping;
  }

  private Options getExtraOptions() {
    Options options = new Options();
    options.addOption(LOCAL_MODE, false, "run it as local mode");
    options.addOption(DISABLE_SPLIT_GROUPING, false , "disable split grouping");
    return options;
  }

  @Override
  public final int run(String[] args) throws Exception {
    Configuration conf = getConf();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, getExtraOptions(), args);
    String[] otherArgs = optionParser.getRemainingArgs();
    if (optionParser.getCommandLine().hasOption(LOCAL_MODE)) {
      isLocalMode = true;
    }
    if (optionParser.getCommandLine().hasOption(DISABLE_SPLIT_GROUPING)) {
      disableSplitGrouping = true;
    }
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
   *                  provided configuration. If TezClient is specified, local mode option can not been
   *                  specified in arguments, it takes no effect.
   * @return Zero indicates success, non-zero indicates failure
   * @throws Exception 
   */
  public int run(TezConfiguration conf, String[] args, @Nullable TezClient tezClient) throws
      Exception {
    setConf(conf);
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, getExtraOptions(), args);
    if (optionParser.getCommandLine().hasOption(LOCAL_MODE)) {
      isLocalMode = true;
      if (tezClient != null) {
        throw new RuntimeException("can't specify local mode when TezClient is created, it takes no effect");
      }
    }
    if (optionParser.getCommandLine().hasOption(DISABLE_SPLIT_GROUPING)) {
      disableSplitGrouping = true;
    }
    String[] otherArgs = optionParser.getRemainingArgs();
    return _execute(otherArgs, conf, tezClient);
  }

  /**
   * @param dag           the dag to execute
   * @param printCounters whether to print counters or not
   * @param logger        the logger to use while printing diagnostics
   * @return Zero indicates success, non-zero indicates failure
   * @throws TezException
   * @throws InterruptedException
   * @throws IOException
   */
  public int runDag(DAG dag, boolean printCounters, Logger logger) throws TezException,
      InterruptedException, IOException {
    tezClientInternal.waitTillReady();

    CallerContext callerContext = CallerContext.create("TezExamples",
        "Tez Example DAG: " + dag.getName());
    ApplicationId appId = tezClientInternal.getAppMasterApplicationId();
    if (appId != null) {
      callerContext.setCallerIdAndType(appId.toString(), "TezExampleApplication");
    }
    dag.setCallerContext(callerContext);

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
      _printUsage();
      return res;
    }
    return 0;
  }

  private int _execute(String[] otherArgs, TezConfiguration tezConf, TezClient tezClient) throws
      Exception {

    int result = _validateArgs(otherArgs);
    if (result != 0) {
      return result;
    }

    if (tezConf == null) {
      tezConf = new TezConfiguration(getConf());
    }
    if (isLocalMode) {
      LOG.info("Running in local mode...");
      tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
      tezConf.set("fs.defaultFS", "file:///");
      tezConf.setBoolean(
          TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
    }
    UserGroupInformation.setConfiguration(tezConf);
    boolean ownTezClient = false;
    if (tezClient == null) {
      ownTezClient = true;
      tezClientInternal = createTezClient(tezConf);
    } else {
      tezClientInternal = tezClient;
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

  private void _printUsage() {
    printUsage();
    System.err.println();
    printExtraOptionsUsage(System.err);
    System.err.println();
    ToolRunner.printGenericCommandUsage(System.err);
  }

  /**
   * Print usage instructions for this example
   */
  protected abstract void printUsage();

  protected void printExtraOptionsUsage(PrintStream ps) {
    ps.println("Tez example extra options supported are");
    // TODO TEZ-1348 make it able to access dfs in tez local mode
    ps.println("-" + LOCAL_MODE + "\t\trun it in tez local mode, currently it can only access local file system in tez local mode,"
        + " run it in distributed mode without this option");
    ps.println("-" + DISABLE_SPLIT_GROUPING + "\t\t disable split grouping for MRInput,"
        + " enable split grouping without this option.");
    ps.println();
    ps.println("The Tez example extra options usage syntax is ");
    ps.println("example_name [extra_options] [example_parameters]");
  }

  /**
   * Validate the arguments
   *
   * @param otherArgs arguments, if any
   * @return Zero indicates success, non-zero indicates failure
   */
  protected abstract int validateArgs(String[] otherArgs);

  /**
   * Create and execute the actual DAG for the example
   *
   * @param args      arguments for execution
   * @param tezConf   the tez configuration instance to be used while processing the DAG
   * @param tezClient the tez client instance to use to run the DAG if any custom monitoring is
   *                  required. Otherwise the utility method {@link #runDag(org.apache.tez.dag.api.DAG,
   *                  boolean, org.slf4j.Logger)} should be used
   * @return Zero indicates success, non-zero indicates failure
   * @throws IOException
   * @throws TezException
   */
  protected abstract int runJob(String[] args, TezConfiguration tezConf,
                                TezClient tezClient) throws Exception;
}
