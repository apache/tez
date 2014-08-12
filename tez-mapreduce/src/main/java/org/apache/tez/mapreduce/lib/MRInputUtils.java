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

package org.apache.tez.mapreduce.lib;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReaderTez;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;

/**
 * Helper methods for InputFormat based Inputs. Private to Tez.
 */
@Private
public class MRInputUtils {

  private static final Log LOG = LogFactory.getLog(MRInputUtils.class);

  public static TaskSplitMetaInfo[] readSplits(Configuration conf) throws IOException {
    TaskSplitMetaInfo[] allTaskSplitMetaInfo;
    allTaskSplitMetaInfo = SplitMetaInfoReaderTez
        .readSplitMetaInfo(conf, FileSystem.getLocal(conf));
    return allTaskSplitMetaInfo;
  }

  public static org.apache.hadoop.mapreduce.InputSplit getNewSplitDetailsFromEvent(
      MRSplitProto splitProto, Configuration conf) throws IOException {
    SerializationFactory serializationFactory = new SerializationFactory(conf);
    return MRInputHelpers.createNewFormatSplitFromUserPayload(
        splitProto, serializationFactory);
  }
  
  @SuppressWarnings("unchecked")
  public static org.apache.hadoop.mapreduce.InputSplit getNewSplitDetailsFromDisk(
      TaskSplitIndex splitMetaInfo, JobConf jobConf, TezCounter splitBytesCounter)
      throws IOException {
    Path file = new Path(splitMetaInfo.getSplitLocation());
    long offset = splitMetaInfo.getStartOffset();

    // Split information read from local filesystem.
    FileSystem fs = FileSystem.getLocal(jobConf);
    file = fs.makeQualified(file);
    LOG.info("Reading input split file from : " + file);
    FSDataInputStream inFile = fs.open(file);
    inFile.seek(offset);
    String className = Text.readString(inFile);
    Class<org.apache.hadoop.mapreduce.InputSplit> cls;
    try {
      cls = (Class<org.apache.hadoop.mapreduce.InputSplit>) jobConf.getClassByName(className);
    } catch (ClassNotFoundException ce) {
      IOException wrap = new IOException("Split class " + className + " not found");
      wrap.initCause(ce);
      throw wrap;
    }
    SerializationFactory factory = new SerializationFactory(jobConf);
    Deserializer<org.apache.hadoop.mapreduce.InputSplit> deserializer = (Deserializer<org.apache.hadoop.mapreduce.InputSplit>) factory
        .getDeserializer(cls);
    deserializer.open(inFile);
    org.apache.hadoop.mapreduce.InputSplit split = deserializer.deserialize(null);
    long pos = inFile.getPos();
    if (splitBytesCounter != null) {
      splitBytesCounter.increment(pos - offset);
    }
    inFile.close();
    return split;
  }

  @SuppressWarnings("unchecked")
  public static InputSplit getOldSplitDetailsFromDisk(TaskSplitIndex splitMetaInfo,
      JobConf jobConf, TezCounter splitBytesCounter) throws IOException {
    Path file = new Path(splitMetaInfo.getSplitLocation());
    FileSystem fs = FileSystem.getLocal(jobConf);
    file = fs.makeQualified(file);
    LOG.info("Reading input split file from : " + file);
    long offset = splitMetaInfo.getStartOffset();

    FSDataInputStream inFile = fs.open(file);
    inFile.seek(offset);
    String className = Text.readString(inFile);
    Class<org.apache.hadoop.mapred.InputSplit> cls;
    try {
      cls = (Class<org.apache.hadoop.mapred.InputSplit>) jobConf.getClassByName(className);
    } catch (ClassNotFoundException ce) {
      IOException wrap = new IOException("Split class " + className + " not found");
      wrap.initCause(ce);
      throw wrap;
    }
    SerializationFactory factory = new SerializationFactory(jobConf);
    Deserializer<org.apache.hadoop.mapred.InputSplit> deserializer = (Deserializer<org.apache.hadoop.mapred.InputSplit>) factory
        .getDeserializer(cls);
    deserializer.open(inFile);
    org.apache.hadoop.mapred.InputSplit split = deserializer.deserialize(null);
    long pos = inFile.getPos();
    if (splitBytesCounter != null) {
      splitBytesCounter.increment(pos - offset);
    }
    inFile.close();
    return split;
  }
  
  @Private
  public static InputSplit getOldSplitDetailsFromEvent(MRSplitProto splitProto, Configuration conf)
      throws IOException {
    SerializationFactory serializationFactory = new SerializationFactory(conf);
    return MRInputHelpers.createOldFormatSplitFromUserPayload(splitProto, serializationFactory);
  }
}
