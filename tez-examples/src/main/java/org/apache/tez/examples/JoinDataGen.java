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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import com.google.common.base.Preconditions;

public class JoinDataGen extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(JoinDataGen.class);

  private static final String STREAM_OUTPUT_NAME = "streamoutput";
  private static final String HASH_OUTPUT_NAME = "hashoutput";
  private static final String EXPECTED_OUTPUT_NAME = "expectedoutput";

  public static void main(String[] args) throws Exception {
    JoinDataGen dataGen = new JoinDataGen();
    int status = ToolRunner.run(new Configuration(), dataGen, args);
    System.exit(status);
  }

  private static void printUsage() {
    System.err
        .println("Usage: "
            + "joindatagen <outPath1> <path1Size> <outPath2> <path2Size> <expectedResultPath> <numTasks>");
    ToolRunner.printGenericCommandUsage(System.err);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    int result = validateArgs(otherArgs);
    if (result != 0) {
      return result;
    }
    return execute(otherArgs);
  }
  
  public int run(Configuration conf, String[] args, TezClient tezClient) throws Exception {
    setConf(conf);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    int result = validateArgs(otherArgs);
    if (result != 0) {
      return result;
    }
    return execute(otherArgs, tezClient);
  }
  
  private int validateArgs(String[] otherArgs) {
    if (otherArgs.length != 6) {
      printUsage();
      return 2;
    }
    return 0;
  }

  private int execute(String [] args) throws TezException, IOException, InterruptedException {
    TezConfiguration tezConf = new TezConfiguration(getConf());
    TezClient tezClient = null;
    try {
      tezClient = createTezClient(tezConf);
      return execute(args, tezConf, tezClient);
    } finally {
      if (tezClient != null) {
        tezClient.stop();
      }
    }
  }
  
  private int execute(String[] args, TezClient tezClient) throws IOException, TezException,
      InterruptedException {
    TezConfiguration tezConf = new TezConfiguration(getConf());
    return execute(args, tezConf, tezClient);
  }
  
  private TezClient createTezClient(TezConfiguration tezConf) throws TezException, IOException {
    TezClient tezClient = TezClient.create("JoinDataGen", tezConf);
    tezClient.start();
    return tezClient;
  }
  
  private int execute(String[] args, TezConfiguration tezConf, TezClient tezClient)
      throws IOException, TezException, InterruptedException {
    LOG.info("Running JoinDataGen");

    UserGroupInformation.setConfiguration(tezConf);

    String outDir1 = args[0];
    long outDir1Size = Long.parseLong(args[1]);
    String outDir2 = args[2];
    long outDir2Size = Long.parseLong(args[3]);
    String expectedOutputDir = args[4];
    int numTasks = Integer.parseInt(args[5]);

    Path largeOutPath = null;
    Path smallOutPath = null;
    long largeOutSize = 0;
    long smallOutSize = 0;

    if (outDir1Size >= outDir2Size) {
      largeOutPath = new Path(outDir1);
      largeOutSize = outDir1Size;
      smallOutPath = new Path(outDir2);
      smallOutSize = outDir2Size;
    } else {
      largeOutPath = new Path(outDir2);
      largeOutSize = outDir2Size;
      smallOutPath = new Path(outDir1);
      smallOutSize = outDir1Size;
    }

    Path expectedOutputPath = new Path(expectedOutputDir);

    // Verify output path existence
    FileSystem fs = FileSystem.get(tezConf);
    int res = 0;
    res = checkOutputDirectory(fs, largeOutPath) + checkOutputDirectory(fs, smallOutPath)
        + checkOutputDirectory(fs, expectedOutputPath);
    if (res != 0) {
      return 3;
    }

    if (numTasks <= 0) {
      System.err.println("NumTasks must be > 0");
      return 4;
    }

    DAG dag = createDag(tezConf, largeOutPath, smallOutPath, expectedOutputPath, numTasks,
        largeOutSize, smallOutSize);

    tezClient.waitTillReady();
    DAGClient dagClient = tezClient.submitDAG(dag);
    DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
    if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
      LOG.info("DAG diagnostics: " + dagStatus.getDiagnostics());
      return -1;
    }
    return 0;

  }

  private DAG createDag(TezConfiguration tezConf, Path largeOutPath, Path smallOutPath,
      Path expectedOutputPath, int numTasks, long largeOutSize, long smallOutSize)
      throws IOException {

    long largeOutSizePerTask = largeOutSize / numTasks;
    long smallOutSizePerTask = smallOutSize / numTasks;

    DAG dag = DAG.create("JoinDataGen");

    Vertex genDataVertex = Vertex.create("datagen", ProcessorDescriptor.create(
        GenDataProcessor.class.getName()).setUserPayload(
        UserPayload.create(ByteBuffer.wrap(GenDataProcessor.createConfiguration(largeOutSizePerTask,
            smallOutSizePerTask)))), numTasks);
    genDataVertex.addDataSink(STREAM_OUTPUT_NAME, 
        MROutput.createConfigBuilder(new Configuration(tezConf),
            TextOutputFormat.class, largeOutPath.toUri().toString()).build());
    genDataVertex.addDataSink(HASH_OUTPUT_NAME, 
        MROutput.createConfigBuilder(new Configuration(tezConf),
            TextOutputFormat.class, smallOutPath.toUri().toString()).build());
    genDataVertex.addDataSink(EXPECTED_OUTPUT_NAME, 
        MROutput.createConfigBuilder(new Configuration(tezConf),
            TextOutputFormat.class, expectedOutputPath.toUri().toString()).build());

    dag.addVertex(genDataVertex);

    return dag;
  }

  public static class GenDataProcessor extends SimpleMRProcessor {

    private static final Log LOG = LogFactory.getLog(GenDataProcessor.class);

    long streamOutputFileSize;
    long hashOutputFileSize;
    float overlapApprox = 0.2f;

    public GenDataProcessor(ProcessorContext context) {
      super(context);
    }

    public static byte[] createConfiguration(long streamOutputFileSize, long hashOutputFileSize)
        throws IOException {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(bos);
      dos.writeLong(streamOutputFileSize);
      dos.writeLong(hashOutputFileSize);
      dos.close();
      bos.close();
      return bos.toByteArray();
    }

    @Override
    public void initialize() throws Exception {
      byte[] payload = getContext().getUserPayload().deepCopyAsArray();
      ByteArrayInputStream bis = new ByteArrayInputStream(payload);
      DataInputStream dis = new DataInputStream(bis);
      streamOutputFileSize = dis.readLong();
      hashOutputFileSize = dis.readLong();
      LOG.info("Initialized with largeFileTargetSize=" + streamOutputFileSize
          + ", smallFileTragetSize=" + hashOutputFileSize);
      dis.close();
      bis.close();
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkState(getInputs().size() == 0);
      Preconditions.checkState(getOutputs().size() == 3);

      KeyValueWriter streamOutputWriter = (KeyValueWriter) getOutputs().get(STREAM_OUTPUT_NAME)
          .getWriter();
      KeyValueWriter hashOutputWriter = (KeyValueWriter) getOutputs().get(HASH_OUTPUT_NAME)
          .getWriter();
      KeyValueWriter expectedOutputWriter = (KeyValueWriter) getOutputs().get(EXPECTED_OUTPUT_NAME)
          .getWriter();

      float fileSizeFraction = hashOutputFileSize / (float) streamOutputFileSize;
      Preconditions.checkState(fileSizeFraction > 0.0f && fileSizeFraction <= 1.0f);
      int mod = 1;
      int extraKeysMod = 0;
      if (fileSizeFraction > overlapApprox) {
        // Common keys capped by overlap. Additional ones required in the hashFile.
        mod = (int) (1 / overlapApprox);
        extraKeysMod = (int) (1 / (fileSizeFraction - overlapApprox));
      } else {
        // All keys in hashFile must exist in stream file.
        mod = (int) (1 / fileSizeFraction);
      }
      LOG.info("Using mod=" + mod + ", extraKeysMod=" + extraKeysMod);

      long count = 0;
      long sizeLarge = 0;
      long sizeSmall = 0;
      long numLargeFileKeys = 0;
      long numSmallFileKeys = 0;
      long numExpectedKeys = 0;
      while (sizeLarge < streamOutputFileSize) {
        String str = createOverlapString(13, count);
        Text text = new Text(str);
        int size = text.getLength();
        streamOutputWriter.write(text, NullWritable.get());
        sizeLarge += size;
        numLargeFileKeys++;
        if (count % mod == 0) {
          hashOutputWriter.write(text, NullWritable.get());
          sizeSmall += size;
          numSmallFileKeys++;
          expectedOutputWriter.write(text, NullWritable.get());
          numExpectedKeys++;
        }
        if (extraKeysMod != 0 && count % extraKeysMod == 0) {
          String nStr = createNonOverlaptring(13, count);
          Text nText = new Text(nStr);
          hashOutputWriter.write(nText, NullWritable.get());
          sizeSmall += nText.getLength();
          numSmallFileKeys++;
        }
        count++;
      }
      LOG.info("OutputStats: " + "largeFileNumKeys=" + numLargeFileKeys + ", smallFileNumKeys="
          + numSmallFileKeys + ", expFileNumKeys=" + numExpectedKeys + ", largeFileSize="
          + sizeLarge + ", smallFileSize=" + sizeSmall);
    }

    private String createOverlapString(int size, long count) {
      StringBuilder sb = new StringBuilder();
      Random random = new Random();
      for (int i = 0; i < size; i++) {
        int r = Math.abs(random.nextInt()) % 26;
        // Random a-z followed by the count
        sb.append((char) (97 + r));
      }
      sb.append("_").append(getContext().getTaskIndex()).append("_").append(count);
      return sb.toString();
    }

    private String createNonOverlaptring(int size, long count) {
      StringBuilder sb = new StringBuilder();
      Random random = new Random();
      for (int i = 0; i < size; i++) {
        int r = Math.abs(random.nextInt()) % 26;
        // Random A-Z followed by the count
        sb.append((char) (65 + r));
      }
      sb.append("_").append(getContext().getTaskIndex()).append("_").append(count);
      return sb.toString();
    }

  }

  private int checkOutputDirectory(FileSystem fs, Path path) throws IOException {
    if (fs.exists(path)) {
      System.err.println("Output directory: " + path + " already exists");
      return 2;
    }
    return 0;
  }

}
