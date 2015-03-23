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

package org.apache.tez.mapreduce.processor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.JobSplit.SplitMetaInfo;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReaderTez;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.tez.common.MRFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.mapreduce.TezTestUtils;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.processor.map.MapProcessor;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezUmbilical;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;

import com.google.common.collect.HashMultimap;


public class MapUtils {

  private static final Logger LOG = LoggerFactory.getLogger(MapUtils.class);
  
  public static void configureLocalDirs(Configuration conf, String localDir)
      throws IOException {
    String[] localSysDirs = new String[1];
    localSysDirs[0] = localDir;

    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, localSysDirs);
    conf.set(MRFrameworkConfigs.TASK_LOCAL_RESOURCE_DIR, localDir);

    LOG.info(TezRuntimeFrameworkConfigs.LOCAL_DIRS + " for child: "
        + conf.get(TezRuntimeFrameworkConfigs.LOCAL_DIRS));
    LOG.info(MRFrameworkConfigs.TASK_LOCAL_RESOURCE_DIR + " for child: "
        + conf.get(MRFrameworkConfigs.TASK_LOCAL_RESOURCE_DIR));

    LocalDirAllocator lDirAlloc = new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);
    Path workDir = null;
    // First, try to find the JOB_LOCAL_DIR on this host.
    try {
      workDir = lDirAlloc.getLocalPathToRead("work", conf);
    } catch (DiskErrorException e) {
      // DiskErrorException means dir not found. If not found, it will
      // be created below.
    }
    if (workDir == null) {
      // JOB_LOCAL_DIR doesn't exist on this host -- Create it.
      workDir = lDirAlloc.getLocalPathForWrite("work", conf);
      FileSystem lfs = FileSystem.getLocal(conf).getRaw();
      boolean madeDir = false;
      try {
        madeDir = lfs.mkdirs(workDir);
      } catch (FileAlreadyExistsException e) {
        // Since all tasks will be running in their own JVM, the race condition
        // exists where multiple tasks could be trying to create this directory
        // at the same time. If this task loses the race, it's okay because
        // the directory already exists.
        madeDir = true;
        workDir = lDirAlloc.getLocalPathToRead("work", conf);
      }
      if (!madeDir) {
        throw new IOException("Mkdirs failed to create " + workDir.toString());
      }
    }
    conf.set(MRFrameworkConfigs.JOB_LOCAL_DIR, workDir.toString());
  }
  
  private static InputSplit 
  createInputSplit(FileSystem fs, Path workDir, JobConf job, Path file) 
      throws IOException {
    FileInputFormat.setInputPaths(job, workDir);

    LOG.info("Generating data at path: " + file);
    // create a file with length entries
    @SuppressWarnings("deprecation")
    SequenceFile.Writer writer = 
        SequenceFile.createWriter(fs, job, file, 
            LongWritable.class, Text.class);
    try {
      Random r = new Random(System.currentTimeMillis());
      LongWritable key = new LongWritable();
      Text value = new Text();
      for (int i = 10; i > 0; i--) {
        key.set(r.nextInt(1000));
        value.set(Integer.toString(i));
        writer.append(key, value);
        LOG.info("<k, v> : <" + key.get() + ", " + value + ">");
      }
    } finally {
      writer.close();
    }
    
    SequenceFileInputFormat<LongWritable, Text> format = 
        new SequenceFileInputFormat<LongWritable, Text>();
    InputSplit[] splits = format.getSplits(job, 1);
    System.err.println("#split = " + splits.length + " ; " +
        "#locs = " + splits[0].getLocations().length + "; " +
        "loc = " + splits[0].getLocations()[0] + "; " + 
        "off = " + splits[0].getLength() + "; " +
        "file = " + ((FileSplit)splits[0]).getPath());
    return splits[0];
  }
  
  final private static FsPermission JOB_FILE_PERMISSION = FsPermission
      .createImmutable((short) 0644); // rw-r--r--

  // Will write files to PWD, from where they are read.
  
  private static void writeSplitFiles(FileSystem fs, JobConf conf,
      InputSplit split) throws IOException {
    Path jobSplitFile = new Path(conf.get(MRFrameworkConfigs.TASK_LOCAL_RESOURCE_DIR,
        MRFrameworkConfigs.TASK_LOCAL_RESOURCE_DIR_DEFAULT), MRJobConfig.JOB_SPLIT);
    LOG.info("Writing split to: " + jobSplitFile);
    FSDataOutputStream out = FileSystem.create(fs, jobSplitFile,
        new FsPermission(JOB_FILE_PERMISSION));

    long offset = out.getPos();
    Text.writeString(out, split.getClass().getName());
    split.write(out);
    out.close();

    String[] locations = split.getLocations();

    SplitMetaInfo info = null;
    info = new JobSplit.SplitMetaInfo(locations, offset, split.getLength());

    Path jobSplitMetaInfoFile = new Path(
        conf.get(MRFrameworkConfigs.TASK_LOCAL_RESOURCE_DIR),
        MRJobConfig.JOB_SPLIT_METAINFO);

    FSDataOutputStream outMeta = FileSystem.create(fs, jobSplitMetaInfoFile,
        new FsPermission(JOB_FILE_PERMISSION));
    outMeta.write(SplitMetaInfoReaderTez.META_SPLIT_FILE_HEADER);
    WritableUtils.writeVInt(outMeta, SplitMetaInfoReaderTez.META_SPLIT_VERSION);
    WritableUtils.writeVInt(outMeta, 1); // Only 1 split meta info being written
    info.write(outMeta);
    outMeta.close();
  }

  public static void generateInputSplit(FileSystem fs, Path workDir, JobConf jobConf, Path mapInput) throws IOException {
    jobConf.setInputFormat(SequenceFileInputFormat.class);
    InputSplit split = createInputSplit(fs, workDir, jobConf, mapInput);
    writeSplitFiles(fs, jobConf, split);
  }
  
  public static LogicalIOProcessorRuntimeTask createLogicalTask(FileSystem fs, Path workDir,
      JobConf jobConf, int mapId, Path mapInput,
      TezUmbilical umbilical, String dagName,
      String vertexName, List<InputSpec> inputSpecs,
      List<OutputSpec> outputSpecs) throws Exception {
    jobConf.setInputFormat(SequenceFileInputFormat.class);

    ProcessorDescriptor mapProcessorDesc = ProcessorDescriptor.create(
        MapProcessor.class.getName()).setUserPayload(
        TezUtils.createUserPayloadFromConf(jobConf));
    
    Token<JobTokenIdentifier> shuffleToken = new Token<JobTokenIdentifier>();

    TaskSpec taskSpec = new TaskSpec(
        TezTestUtils.getMockTaskAttemptId(0, 0, mapId, 0),
        dagName, vertexName, -1,
        mapProcessorDesc,
        inputSpecs,
        outputSpecs, null);

    Map<String, ByteBuffer> serviceConsumerMetadata = new HashMap<String, ByteBuffer>();
    serviceConsumerMetadata.put(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID,
        ShuffleUtils.convertJobTokenToBytes(shuffleToken));
    Map<String, String> envMap = new HashMap<String, String>();
    ByteBuffer shufflePortBb = ByteBuffer.allocate(4).putInt(0, 8000);
    AuxiliaryServiceHelper
        .setServiceDataIntoEnv(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID, shufflePortBb,
            envMap);

    LogicalIOProcessorRuntimeTask task = new LogicalIOProcessorRuntimeTask(
        taskSpec,
        0,
        jobConf,
        new String[] {workDir.toString()},
        umbilical,
        serviceConsumerMetadata,
        envMap,
        HashMultimap.<String, String>create(), null, "", new ExecutionContextImpl("localhost"),
        Runtime.getRuntime().maxMemory());
    return task;
  }
}
