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

package org.apache.tez.runtime.library.common.shuffle.impl;

import java.io.IOException;
import java.net.URI;
import java.util.BitSet;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;

import com.google.protobuf.InvalidProtocolBufferException;

public class ShuffleInputEventHandler {
  
  private static final Log LOG = LogFactory.getLog(ShuffleInputEventHandler.class);

  private final ShuffleScheduler scheduler;
  private final InputContext inputContext;

  private int maxMapRuntime = 0;
  private final MergeManager merger;
  private final Configuration conf;
  private final boolean sslShuffle;
  private final boolean doLocalFetch;

  public ShuffleInputEventHandler(InputContext inputContext,
      ShuffleScheduler scheduler, MergeManager merger, Configuration conf, boolean sslShuffle) {
    this.inputContext = inputContext;
    this.scheduler = scheduler;
    this.merger = merger;
    this.conf = conf;
    this.sslShuffle = sslShuffle;
    this.doLocalFetch = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH,
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_DEFAULT);
  }

  public void handleEvents(List<Event> events) throws IOException {
    for (Event event : events) {
      handleEvent(event);
    }
  }
  
  
  private void handleEvent(Event event) throws IOException {
    if (event instanceof DataMovementEvent) {
      processDataMovementEvent((DataMovementEvent) event);      
    } else if (event instanceof InputFailedEvent) {
      processTaskFailedEvent((InputFailedEvent) event);
    }
  }

  private void processDataMovementEvent(DataMovementEvent dmEvent) throws IOException {
    DataMovementEventPayloadProto shufflePayload;
    try {
      shufflePayload = DataMovementEventPayloadProto.parseFrom(dmEvent.getUserPayload());
    } catch (InvalidProtocolBufferException e) {
      throw new TezUncheckedException("Unable to parse DataMovementEvent payload", e);
    } 
    int partitionId = dmEvent.getSourceIndex();
    LOG.info("DataMovementEvent partitionId:" + partitionId + ", targetIndex: " + dmEvent.getTargetIndex()
        + ", attemptNum: " + dmEvent.getVersion() + ", payload: " + ShuffleUtils.stringify(shufflePayload));
    // TODO NEWTEZ See if this duration hack can be removed.
    int duration = shufflePayload.getRunDuration();
    if (duration > maxMapRuntime) {
      maxMapRuntime = duration;
      scheduler.informMaxMapRunTime(maxMapRuntime);
    }
    if (shufflePayload.hasEmptyPartitions()) {
      try {
        byte[] emptyPartitions = TezCommonUtils.decompressByteStringToByteArray(shufflePayload.getEmptyPartitions());
        BitSet emptyPartitionsBitSet = TezUtils.fromByteArray(emptyPartitions);
        if (emptyPartitionsBitSet.get(partitionId)) {
          InputAttemptIdentifier srcAttemptIdentifier =
              new InputAttemptIdentifier(dmEvent.getTargetIndex(), dmEvent.getVersion());
          LOG.info("Source partition: " + partitionId + " did not generate any data. SrcAttempt: ["
              + srcAttemptIdentifier + "]. Not fetching.");
          scheduler.copySucceeded(srcAttemptIdentifier, null, 0, 0, 0, null);
          return;
        }
      } catch (IOException e) {
        throw new TezUncheckedException("Unable to set " +
                "the empty partition to succeeded", e);
      }
    }

    InputAttemptIdentifier srcAttemptIdentifier =
        new InputAttemptIdentifier(dmEvent.getTargetIndex(), dmEvent.getVersion(),
            shufflePayload.getPathComponent());
    if (doLocalFetch && shufflePayload.getHost().equals(System.getenv(
        ApplicationConstants.Environment.NM_HOST.toString()))) {
      LOG.info("SrcAttempt: [" + srcAttemptIdentifier +
          "] fetching input data using direct local access");
      Path filename = getShuffleInputFileName(shufflePayload.getPathComponent(), "");
      TezIndexRecord indexRecord = getIndexRecord(dmEvent, shufflePayload);
      MapOutput mapOut = new MapOutput(srcAttemptIdentifier, merger, conf, filename,
          indexRecord.getStartOffset(), indexRecord.getPartLength()); //TODO: do we need length?
      scheduler.copySucceeded(srcAttemptIdentifier, null, indexRecord.getPartLength(),
          indexRecord.getRawLength(), 0, mapOut);
    } else {
      URI baseUri = getBaseURI(shufflePayload.getHost(), shufflePayload.getPort(), partitionId);
      scheduler.addKnownMapOutput(shufflePayload.getHost(), shufflePayload.getPort(),
          partitionId, baseUri.toString(), srcAttemptIdentifier);
    }
  }
  
  private void processTaskFailedEvent(InputFailedEvent ifEvent) {
    InputAttemptIdentifier taIdentifier = new InputAttemptIdentifier(ifEvent.getTargetIndex(), ifEvent.getVersion());
    scheduler.obsoleteInput(taIdentifier);
    LOG.info("Obsoleting output of src-task: " + taIdentifier);
  }

  // TODO NEWTEZ Handle encrypted shuffle
  @VisibleForTesting
  URI getBaseURI(String host, int port, int partitionId) {
    StringBuilder sb = ShuffleUtils.constructBaseURIForShuffleHandler(host, port,
      partitionId, inputContext.getApplicationId().toString(), sslShuffle);
    URI u = URI.create(sb.toString());
    return u;
  }

  private Path getShuffleInputFileName(String pathComponent, String suffix) throws IOException {
    LocalDirAllocator localDirAllocator = new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);
    suffix = suffix != null ? suffix : "";

    String pathFromLocalDir = Constants.TEZ_RUNTIME_TASK_OUTPUT_DIR + Path.SEPARATOR + pathComponent +
        Path.SEPARATOR + Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING + suffix;

    return localDirAllocator.getLocalPathToRead(pathFromLocalDir.toString(), conf);
  }

  private TezIndexRecord getIndexRecord(DataMovementEvent dme,
                                              DataMovementEventPayloadProto shufflePayload)
      throws IOException {
    Path indexFile = getShuffleInputFileName(shufflePayload.getPathComponent(),
        Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING);
    TezSpillRecord spillRecord = new TezSpillRecord(indexFile, conf);
    return spillRecord.getIndex(dme.getSourceIndex());
  }
}

