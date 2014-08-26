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

package org.apache.tez.mapreduce.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.security.Credentials;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;

import com.google.common.base.Preconditions;

/**
 * Represents InputSplitInfo for splits generated to memory. </p>
 * 
 * Since splits are generated in memory, the getSplitsMetaInfoFile and
 * getSplitsFile are not supported.
 * 
 */
public class InputSplitInfoMem implements InputSplitInfo {

//  private final MRSplitsProto splitsProto;
  private final boolean isNewSplit;
  private final int numTasks;
  private final Credentials credentials;
  private final Configuration conf;
  private final List<TaskLocationHint> taskLocationHints;
  
  private org.apache.hadoop.mapreduce.InputSplit[] newFormatSplits;
  private org.apache.hadoop.mapred.InputSplit[] oldFormatSplits;

  // TaskLocationHints accepted as a parameter since InputSplits don't have rack
  // info, and it can't always be derived.
  public InputSplitInfoMem(org.apache.hadoop.mapreduce.InputSplit[] newSplits,
      List<TaskLocationHint> taskLocationHints, int numTasks, Credentials credentials,
      Configuration conf) {
    this.isNewSplit = true;
    this.newFormatSplits = newSplits;
    this.taskLocationHints = taskLocationHints;
    this.numTasks = numTasks;
    this.credentials = credentials;
    this.conf = conf;
  }

  // TaskLocationHints accepted as a parameter since InputSplits don't have rack
  // info, and it can't always be derived.
  public InputSplitInfoMem(org.apache.hadoop.mapred.InputSplit[] oldSplits,
      List<TaskLocationHint> taskLocationHints, int numTasks, Credentials credentials,
      Configuration conf) {
    this.isNewSplit = false;
    this.oldFormatSplits = oldSplits;
    this.taskLocationHints = taskLocationHints;
    this.numTasks = numTasks;
    this.credentials = credentials;
    this.conf = conf;
  }

  @Override
  public List<TaskLocationHint> getTaskLocationHints() {
    return this.taskLocationHints;
  }

  @Override
  public Path getSplitsMetaInfoFile() {
    throw new UnsupportedOperationException("Not supported for Type: "
        + getType());
  }

  @Override
  public Path getSplitsFile() {
    throw new UnsupportedOperationException("Not supported for Type: "
        + getType());
  }

  @Override
  public int getNumTasks() {
    return this.numTasks;
  }

  @Override
  public Type getType() {
    return Type.MEM;
  }

  @Override
  public MRSplitsProto getSplitsProto() {
    if (isNewSplit) {
      try {
        return createSplitsProto(newFormatSplits, new SerializationFactory(conf));
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    } else {
      try {
        return createSplitsProto(oldFormatSplits);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Credentials getCredentials() {
    return this.credentials;
  }

  @Override
  public boolean holdsNewFormatSplits() {
    return this.isNewSplit;  
  }

  @Override
  public org.apache.hadoop.mapreduce.InputSplit[] getNewFormatSplits() {
    Preconditions
        .checkState(
            isNewSplit == true,
            "Cannot fetch newSplits for an instance handling oldFormatSplits. Use holdsNewFormatSplits() to check type");
    return newFormatSplits;
  }

  @Override
  public org.apache.hadoop.mapred.InputSplit[] getOldFormatSplits() {
    Preconditions
        .checkState(
            isNewSplit == false,
            "Cannot fetch newSplits for an instance handling newFormatSplits. Use holdsNewFormatSplits() to check type");
    return oldFormatSplits;
  }

  private static MRSplitsProto createSplitsProto(
      org.apache.hadoop.mapreduce.InputSplit[] newSplits,
      SerializationFactory serializationFactory) throws IOException,
      InterruptedException {
    MRSplitsProto.Builder splitsBuilder = MRSplitsProto.newBuilder();

    for (org.apache.hadoop.mapreduce.InputSplit newSplit : newSplits) {
      splitsBuilder.addSplits(MRInputHelpers.createSplitProto(newSplit, serializationFactory));
    }
    return splitsBuilder.build();
  }

  private static MRSplitsProto createSplitsProto(
      org.apache.hadoop.mapred.InputSplit[] oldSplits) throws IOException {
    MRSplitsProto.Builder splitsBuilder = MRSplitsProto.newBuilder();
    for (org.apache.hadoop.mapred.InputSplit oldSplit : oldSplits) {
      splitsBuilder.addSplits(MRInputHelpers.createSplitProto(oldSplit));
    }
    return splitsBuilder.build();
  }
}
