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

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.Preconditions;
import org.apache.tez.common.io.NonSyncByteArrayInputStream;
import org.apache.tez.common.io.NonSyncByteArrayOutputStream;
import org.apache.tez.common.io.NonSyncDataOutputStream;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates records with data skew.
 */
public class DemuxerDataGen extends TezExampleBase {
  private static final Logger LOG = LoggerFactory.getLogger(DemuxerDataGen.class);

  private static final String OUTPUT_NAME = "output";

  public static void main(String[] args) throws Exception {
    DemuxerDataGen dataGen = new DemuxerDataGen();
    int status = ToolRunner.run(new Configuration(), dataGen, args);
    System.exit(status);
  }

  @Override
  protected void printUsage() {
    System.err.println("Usage: demuxerdatagen <outPath> <numCategories> <parallelism>");
    ToolRunner.printGenericCommandUsage(System.err);
  }

  @Override
  protected int runJob(String[] args, TezConfiguration tezConf, TezClient tezClient)
      throws Exception {
    LOG.info("Running DemuxerDataGen");

    String outDir = args[0];
    int numCategories = Integer.parseInt(args[1]);
    int numTasks = args.length == 2 ? 1 : Integer.parseInt(args[2]);

    Path outputPath = new Path(outDir);
    FileSystem fs = outputPath.getFileSystem(tezConf);
    if (fs.exists(outputPath)) {
      System.err.println("Output directory: " + outDir + " already exists");
      return 2;
    }

    DAG dag = createDag(tezConf, outputPath, numCategories, numTasks);

    return runDag(dag, isCountersLog(), LOG);
  }


  @Override
  protected int validateArgs(String[] otherArgs) {
    if (otherArgs.length < 2 || otherArgs.length > 3) {
      return 2;
    }
    return 0;
  }

  private DAG createDag(TezConfiguration tezConf, Path outputPath, int numCategories, int numTasks)
      throws IOException {
    DAG dag = DAG.create("DemuxerDataGen");

    Vertex genDataVertex = Vertex
        .create(
            "datagen",
            ProcessorDescriptor
                .create(GenDataProcessor.class.getName())
                .setUserPayload(
                    UserPayload.create(
                        ByteBuffer.wrap(
                            GenDataProcessor.createConfiguration(numCategories, numTasks)))),
            numTasks)
        .addDataSink(OUTPUT_NAME,
            MROutput.createConfigBuilder(new Configuration(tezConf),
                TextOutputFormat.class, outputPath.toUri().toString()).build());

    dag.addVertex(genDataVertex);

    return dag;
  }

  public static class GenDataProcessor extends SimpleMRProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(GenDataProcessor.class);

    private int numCategories;
    private int numTasks;

    public GenDataProcessor(ProcessorContext context) {
      super(context);
    }

    public static byte[] createConfiguration(int numCategories, int numTasks) throws IOException {
      NonSyncByteArrayOutputStream bos = new NonSyncByteArrayOutputStream();
      NonSyncDataOutputStream dos = new NonSyncDataOutputStream(bos);
      dos.writeInt(numCategories);
      dos.writeInt(numTasks);
      dos.close();
      bos.close();
      return bos.toByteArray();
    }

    @Override
    public void initialize() throws Exception {
      byte[] payload = getContext().getUserPayload().deepCopyAsArray();
      NonSyncByteArrayInputStream bis = new NonSyncByteArrayInputStream(payload);
      DataInputStream dis = new DataInputStream(bis);
      numCategories = dis.readInt();
      numTasks = dis.readInt();
      LOG.info("Initialized with numCategories={} and numTasks={}", numCategories, numTasks);
      dis.close();
      bis.close();
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkState(getInputs().isEmpty());
      Preconditions.checkState(getOutputs().size() == 1);

      KeyValueWriter outputWriter = (KeyValueWriter) getOutputs().get(OUTPUT_NAME).getWriter();

      for (int i = 0; i < numCategories; i++) {
        Text name = new Text(String.format("category-%05d", i));
        long numValues = Math.max(((long) Math.pow(2, i)) / numTasks, 1);
        for (int j = 0; j < numValues; j++) {
          outputWriter.write(name, NullWritable.get());
        }
      }
    }
  }
}
