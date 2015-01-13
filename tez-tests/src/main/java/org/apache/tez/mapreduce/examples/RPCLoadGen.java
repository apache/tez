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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.google.common.base.Stopwatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.examples.TezExampleBase;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import sun.misc.IOUtils;

public class RPCLoadGen extends TezExampleBase {

  private static final Log LOG = LogFactory.getLog(RPCLoadGen.class);

  private static final String VIA_RPC = "viaRpc";
  private static final byte VIA_RPC_BYTE = (byte) 0x00;
  private static final String VIA_HDFS_DIST_CACHE = "viaHdfsDistCache";
  private static final byte VIA_HDFS_DIST_CACHE_BYTE = (byte) 0x01;
  private static final String VIA_HDFS_DIRECT_READ = "viaHdfsDirectRead";
  private static final byte VIA_HDFS_DIRECT_READ_BYTE = (byte) 0x02;

  private static final String DISK_PAYLOAD_NAME = RPCLoadGen.class.getSimpleName() + "_payload";

  private FileSystem fs;
  private Path resourcePath;

  @Override
  protected final int runJob(String[] args, TezConfiguration tezConf, TezClient tezClient) throws
      TezException, InterruptedException, IOException {
    LOG.info("Running: " +
        this.getClass().getSimpleName());
    String mode = VIA_RPC;
    if (args.length == 4) {
      if (args[3].equals(VIA_RPC) || args[3].equals(VIA_HDFS_DIRECT_READ) ||
          args[3].equals(VIA_HDFS_DIST_CACHE)) {
        mode = args[3];
      } else {
        printUsage();
        return 2;
      }
    }

    int numTasks = Integer.parseInt(args[0]);
    int maxSleepTimeMillis = Integer.parseInt(args[1]);
    int payloadSizeBytes = Integer.parseInt(args[2]);
    LOG.info("Parameters: numTasks=" + numTasks + ", maxSleepTime(ms)=" + maxSleepTimeMillis +
        ", payloadSize(bytes)=" + payloadSizeBytes + ", mode=" + mode);

    DAG dag = createDAG(tezConf, numTasks, maxSleepTimeMillis, payloadSizeBytes, mode);
    try {
      return runDag(dag, false, LOG);
    } finally {
      if (fs != null) {
        if (resourcePath != null) {
          fs.delete(resourcePath, false);
        }
      }
    }
  }

  @Override
  protected void printUsage() {
    System.err.println(
        "Usage: " + "RPCLoadGen <numTasks> <max_sleep_time_millis> <get_task_payload_size> [" +
            "<" + VIA_RPC + ">|" + VIA_HDFS_DIST_CACHE + "|" + VIA_HDFS_DIRECT_READ + "]");
    ToolRunner.printGenericCommandUsage(System.err);
  }

  @Override
  protected final int validateArgs(String[] otherArgs) {
    return (otherArgs.length >=3 && otherArgs.length <=4) ? 0 : 2;
  }

  private DAG createDAG(TezConfiguration conf, int numTasks, int maxSleepTimeMillis,
                        int payloadSize, String mode) throws IOException {

    Map<String, LocalResource> localResourceMap = new HashMap<String, LocalResource>();
    UserPayload payload =
        createUserPayload(conf, maxSleepTimeMillis, payloadSize, mode, localResourceMap);

    Vertex vertex = Vertex.create("RPCLoadVertex",
        ProcessorDescriptor.create(RPCSleepProcessor.class.getName()).setUserPayload(
            payload), numTasks).addTaskLocalFiles(localResourceMap);

    return DAG.create("RPCLoadGen").addVertex(vertex);
  }

  private UserPayload createUserPayload(TezConfiguration conf, int maxSleepTimeMillis,
                                        int payloadSize, String mode,
                                        Map<String, LocalResource> localResources) throws
      IOException {
    ByteBuffer payload;
    if (mode.equals(VIA_RPC)) {
      if (payloadSize < 5) {
        payloadSize = 5; // To Configure the processor
      }
      byte[] payloadBytes = new byte[payloadSize];
      new Random().nextBytes(payloadBytes);
      payload = ByteBuffer.wrap(payloadBytes);
      payload.put(4, VIA_RPC_BYTE); // ViaRPC
    } else {
      // Actual payload
      byte[] payloadBytes = new byte[5];
      payload = ByteBuffer.wrap(payloadBytes);

      // Disk payload
      byte[] diskPayload = new byte[payloadSize];
      new Random().nextBytes(diskPayload);
      fs = FileSystem.get(conf);
      resourcePath = new Path(Path.SEPARATOR + "tmp", DISK_PAYLOAD_NAME);
      System.err.println("ZZZ: HDFSPath: " + resourcePath);
      resourcePath = fs.makeQualified(resourcePath);
      System.err.println("ZZZ: HDFSPathResolved: " + resourcePath);
      FSDataOutputStream dataOut = fs.create(resourcePath, true);
      dataOut.write(diskPayload);
      dataOut.close();
      fs.setReplication(resourcePath, (short)10);
      FileStatus fileStatus = fs.getFileStatus(resourcePath);

      if (mode.equals(VIA_HDFS_DIST_CACHE)) {
        LocalResource lr = LocalResource.newInstance(ConverterUtils.getYarnUrlFromPath(resourcePath),
            LocalResourceType.ARCHIVE.FILE, LocalResourceVisibility.PRIVATE, fileStatus.getLen(),
            fileStatus.getModificationTime());
        localResources.put(DISK_PAYLOAD_NAME, lr);
        payload.put(4, VIA_HDFS_DIST_CACHE_BYTE); // ViaRPC
      } else if (mode.equals(VIA_HDFS_DIRECT_READ)) {
        payload.put(4, VIA_HDFS_DIRECT_READ_BYTE); // ViaRPC
      }
    }

    payload.putInt(0, maxSleepTimeMillis);
    return UserPayload.create(payload);
  }

  public static class RPCSleepProcessor extends SimpleProcessor {

    private final int sleepTimeMax;
    private final byte modeByte;

    public RPCSleepProcessor(ProcessorContext context) {
      super(context);
      sleepTimeMax = getContext().getUserPayload().getPayload().getInt(0);
      modeByte = getContext().getUserPayload().getPayload().get(4);
    }

    @Override
    public void run() throws Exception {
      Stopwatch sw = new Stopwatch().start();
      long sleepTime = new Random().nextInt(sleepTimeMax);
      if (modeByte == VIA_RPC_BYTE) {
        LOG.info("Received via RPC.");
      } else if (modeByte == VIA_HDFS_DIST_CACHE_BYTE) {
        LOG.info("Reading from local filesystem");
        FileSystem localFs = FileSystem.getLocal(new Configuration());
        FSDataInputStream is = localFs.open(new Path(DISK_PAYLOAD_NAME));
        IOUtils.readFully(is, -1, false);
      } else if (modeByte == VIA_HDFS_DIRECT_READ_BYTE) {
        LOG.info("Reading from HDFS");
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream is = fs.open(new Path(Path.SEPARATOR + "tmp", DISK_PAYLOAD_NAME));
        IOUtils.readFully(is, -1, false);
      } else {
        throw new IllegalArgumentException("Unknown execution mode: [" + modeByte + "]");
      }
      LOG.info("TimeTakenToAccessPayload=" + sw.stop().elapsedMillis());
      LOG.info("Sleeping for: " + sleepTime);
      Thread.sleep(sleepTime);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new RPCLoadGen(), args);
    System.exit(res);
  }
}