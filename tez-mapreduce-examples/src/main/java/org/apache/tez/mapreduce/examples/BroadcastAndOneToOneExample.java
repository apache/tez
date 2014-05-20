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

package org.apache.tez.mapreduce.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezSession;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.library.vertexmanager.InputReadyVertexManager;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.runtime.common.objectregistry.ObjectLifeCycle;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistry;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryFactory;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.input.ShuffledUnorderedKVInput;
import org.apache.tez.runtime.library.output.OnFileUnorderedKVOutput;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.Preconditions;

public class BroadcastAndOneToOneExample extends Configured implements Tool {
  public static class InputProcessor extends SimpleProcessor {
    Text word = new Text();

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getOutputs().size() == 1);
      OnFileUnorderedKVOutput output = (OnFileUnorderedKVOutput) getOutputs().values().iterator()
          .next();
      KeyValueWriter kvWriter = (KeyValueWriter) output.getWriter();
      kvWriter.write(word, new IntWritable(getContext().getTaskIndex()));
      byte[] userPayload = getContext().getUserPayload();
      if (userPayload != null) {
        boolean doLocalityCheck = userPayload[0] > 0 ? true : false;
        if (doLocalityCheck) {
          ObjectRegistry objectRegistry = ObjectRegistryFactory.getObjectRegistry();
          String entry = String.valueOf(getContext().getTaskIndex());
          objectRegistry.add(ObjectLifeCycle.DAG, entry, entry);
        }
      }
    }
  }

  public static class OneToOneProcessor extends SimpleProcessor {
    Text word = new Text();

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(inputs.size() == 2);

      KeyValueReader inputKvReader = (KeyValueReader) getInputs().get("Input").getReader();
      KeyValueReader broadcastKvReader = (KeyValueReader) getInputs().get("Broadcast").getReader();
      int sum = 0;
      while (broadcastKvReader.next()) {
        sum += ((IntWritable) broadcastKvReader.getCurrentValue()).get();
      }
      while (inputKvReader.next()) {
        sum += ((IntWritable) inputKvReader.getCurrentValue()).get();
      }
      boolean doLocalityCheck = getContext().getUserPayload()[0] > 0 ? true : false;
      int broadcastSum = getContext().getUserPayload()[1];
      int expectedSum = broadcastSum + getContext().getTaskIndex();
      System.out.println("Index: " + getContext().getTaskIndex() + 
          " sum: " + sum + " expectedSum: " + expectedSum + " broadcastSum: " + broadcastSum);
      Preconditions.checkState((sum == expectedSum), "Sum = " + sum);      
      
      if (doLocalityCheck) {
        ObjectRegistry objectRegistry = ObjectRegistryFactory.getObjectRegistry();
        String index = (String) objectRegistry.get(String.valueOf(getContext().getTaskIndex()));
        if (index == null || Integer.valueOf(index).intValue() != getContext().getTaskIndex()) {
          String msg = "Did not find expected local producer "
              + getContext().getTaskIndex() + " in the same JVM";
          System.out.println(msg);
          throw new TezUncheckedException(msg);
        }
      }
    }

  }

  private DAG createDAG(FileSystem fs, TezConfiguration tezConf,
      Path stagingDir, boolean doLocalityCheck) throws IOException, YarnException {
    Configuration kvInputConf = new JobConf((Configuration)tezConf);
    kvInputConf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        Text.class.getName());
    kvInputConf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        IntWritable.class.getName());
    MultiStageMRConfToTezTranslator.translateVertexConfToTez(kvInputConf,
        null);

    Configuration kvOneToOneConf = new JobConf((Configuration)tezConf);

    MultiStageMRConfToTezTranslator.translateVertexConfToTez(kvOneToOneConf,
        kvInputConf);

    MRHelpers.doJobClientMagic(kvInputConf);
    MRHelpers.doJobClientMagic(kvOneToOneConf);

    int numBroadcastTasks = 2;
    int numOneToOneTasks = 3;
    if (doLocalityCheck) {
      YarnClient yarnClient = YarnClient.createYarnClient();
      yarnClient.init(tezConf);
      yarnClient.start();
      int numNMs = yarnClient.getNodeReports(NodeState.RUNNING).size();
      yarnClient.stop();
      // create enough 1-1 tasks to run in parallel
      numOneToOneTasks = numNMs - numBroadcastTasks - 1;// 1 AM
      if (numOneToOneTasks < 1) {
        numOneToOneTasks = 1;
      }
    }
    byte[] procPayload = {(byte) (doLocalityCheck ? 1 : 0), 1};

    System.out.println("Using " + numOneToOneTasks + " 1-1 tasks");

    byte[] kvInputPayload = MRHelpers.createUserPayloadFromConf(kvInputConf);
    Vertex broadcastVertex = new Vertex("Broadcast", new ProcessorDescriptor(
        InputProcessor.class.getName()),
        numBroadcastTasks, MRHelpers.getMapResource(kvInputConf));
    broadcastVertex.setJavaOpts(MRHelpers.getMapJavaOpts(kvInputConf));
    
    Vertex inputVertex = new Vertex("Input", new ProcessorDescriptor(
        InputProcessor.class.getName()).setUserPayload(procPayload),
        numOneToOneTasks, MRHelpers.getMapResource(kvInputConf));
    inputVertex.setJavaOpts(MRHelpers.getMapJavaOpts(kvInputConf));
    
    byte[] kvOneToOnePayload = MRHelpers.createUserPayloadFromConf(kvOneToOneConf);
    Vertex oneToOneVertex = new Vertex("OneToOne",
        new ProcessorDescriptor(
            OneToOneProcessor.class.getName()).setUserPayload(procPayload),
            numOneToOneTasks, MRHelpers.getReduceResource(kvOneToOneConf));
    oneToOneVertex.setJavaOpts(
        MRHelpers.getReduceJavaOpts(kvOneToOneConf)).setVertexManagerPlugin(
            new VertexManagerPluginDescriptor(InputReadyVertexManager.class.getName()));
    
    DAG dag = new DAG("BroadcastAndOneToOneExample");
    dag.addVertex(inputVertex)
        .addVertex(broadcastVertex)
        .addVertex(oneToOneVertex)
        .addEdge(
            new Edge(inputVertex, oneToOneVertex, new EdgeProperty(
                DataMovementType.ONE_TO_ONE, DataSourceType.PERSISTED,
                SchedulingType.SEQUENTIAL, 
                new OutputDescriptor(OnFileUnorderedKVOutput.class.getName())
                        .setUserPayload(kvInputPayload), 
                new InputDescriptor(ShuffledUnorderedKVInput.class.getName())
                        .setUserPayload(kvOneToOnePayload))))
        .addEdge(
            new Edge(broadcastVertex, oneToOneVertex, new EdgeProperty(
                DataMovementType.BROADCAST, DataSourceType.PERSISTED,
                SchedulingType.SEQUENTIAL, 
                new OutputDescriptor(OnFileUnorderedKVOutput.class.getName())
                        .setUserPayload(kvInputPayload), 
                new InputDescriptor(ShuffledUnorderedKVInput.class.getName())
                        .setUserPayload(kvOneToOnePayload))));
    return dag;
  }
  
  private Credentials credentials = new Credentials();
  
  public boolean run(Configuration conf, boolean doLocalityCheck) throws Exception {
    System.out.println("Running BroadcastAndOneToOneExample");
    // conf and UGI
    TezConfiguration tezConf;
    if (conf != null) {
      tezConf = new TezConfiguration(conf);
    } else {
      tezConf = new TezConfiguration();
    }
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    UserGroupInformation.setConfiguration(tezConf);
    String user = UserGroupInformation.getCurrentUser().getShortUserName();

    TezClient tezClient = new TezClient(tezConf);
    ApplicationId appId = tezClient.createApplication();
    
    // staging dir
    FileSystem fs = FileSystem.get(tezConf);
    String stagingDirStr = Path.SEPARATOR + "user" + Path.SEPARATOR
        + user + Path.SEPARATOR+ ".staging" + Path.SEPARATOR
        + Path.SEPARATOR + appId.toString();    
    Path stagingDir = new Path(stagingDirStr);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirStr);
    stagingDir = fs.makeQualified(stagingDir);
    
    // security
    TokenCache.obtainTokensForNamenodes(credentials, new Path[] {stagingDir}, tezConf);
    TezClientUtils.ensureStagingDirExists(tezConf, stagingDir);

    tezConf.set(TezConfiguration.TEZ_AM_JAVA_OPTS,
        MRHelpers.getMRAMJavaOpts(tezConf));

    // No need to add jar containing this class as assumed to be part of
    // the tez jars.

    // TEZ-674 Obtain tokens based on the Input / Output paths. For now assuming staging dir
    // is the same filesystem as the one used for Input/Output.
    TezSession tezSession = null;
    AMConfiguration amConfig = new AMConfiguration(null,
        null, tezConf, credentials);
    
    TezSessionConfiguration sessionConfig =
        new TezSessionConfiguration(amConfig, tezConf);
    tezSession = new TezSession("WordCountSession", appId,
        sessionConfig);
    tezSession.start();

    DAGClient dagClient = null;

    try {
        DAG dag = createDAG(fs, tezConf, stagingDir, doLocalityCheck);

        tezSession.waitTillReady();
        dagClient = tezSession.submitDAG(dag);

        // monitoring
        DAGStatus dagStatus = dagClient.waitForCompletionWithAllStatusUpdates(null);
        if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
          System.out.println("DAG diagnostics: " + dagStatus.getDiagnostics());
          return false;
        }
        return true;
    } finally {
      fs.delete(stagingDir, true);
      tezSession.stop();
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    boolean doLocalityCheck = true;
    if (args.length == 1) {
      if (args[0].equals(skipLocalityCheck)) {
        doLocalityCheck = false;
      } else {
        printUsage();
        throw new TezException("Invalid command line");
      }
    } else if (args.length > 1) {
      printUsage();
      throw new TezException("Invalid command line");
    }
    boolean status = run(getConf(), doLocalityCheck);
    return status ? 0 : 1;
  }
  
  private static void printUsage() {
    System.err.println("broadcastAndOneToOneExample " + skipLocalityCheck);
    ToolRunner.printGenericCommandUsage(System.err);
  }
  
  static String skipLocalityCheck = "-skipLocalityCheck";

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    BroadcastAndOneToOneExample job = new BroadcastAndOneToOneExample();
    int status = ToolRunner.run(conf, job, args);
    System.exit(status);
  }
}
