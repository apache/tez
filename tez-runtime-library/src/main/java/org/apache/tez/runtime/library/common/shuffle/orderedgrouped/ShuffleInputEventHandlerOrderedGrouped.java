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

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.IOException;
import java.net.URI;
import java.util.BitSet;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputIdentifier;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;

import com.google.protobuf.InvalidProtocolBufferException;

public class ShuffleInputEventHandlerOrderedGrouped {
  
  private static final Log LOG = LogFactory.getLog(ShuffleInputEventHandlerOrderedGrouped.class);

  private final ShuffleScheduler scheduler;
  private final InputContext inputContext;

  private int maxMapRuntime = 0;
  private final boolean sslShuffle;

  public ShuffleInputEventHandlerOrderedGrouped(InputContext inputContext,
                                                ShuffleScheduler scheduler, boolean sslShuffle) {
    this.inputContext = inputContext;
    this.scheduler = scheduler;
    this.sslShuffle = sslShuffle;
  }

  public void handleEvents(List<Event> events) throws IOException {
    for (Event event : events) {
      handleEvent(event);
    }
  }
  
  
  private void handleEvent(Event event) throws IOException {
    if (event instanceof DataMovementEvent) {
      processDataMovementEvent((DataMovementEvent) event);
      scheduler.updateEventReceivedTime();
    } else if (event instanceof InputFailedEvent) {
      processTaskFailedEvent((InputFailedEvent) event);
    }
  }

  private void processDataMovementEvent(DataMovementEvent dmEvent) throws IOException {
    DataMovementEventPayloadProto shufflePayload;
    try {
      shufflePayload = DataMovementEventPayloadProto.parseFrom(ByteString.copyFrom(dmEvent.getUserPayload()));
    } catch (InvalidProtocolBufferException e) {
      throw new TezUncheckedException("Unable to parse DataMovementEvent payload", e);
    } 
    int partitionId = dmEvent.getSourceIndex();
    LOG.info("DME srcIdx: " + partitionId + ", targetIdx: " + dmEvent.getTargetIndex()
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
        BitSet emptyPartitionsBitSet = TezUtilsInternal.fromByteArray(emptyPartitions);
        if (emptyPartitionsBitSet.get(partitionId)) {
          InputAttemptIdentifier srcAttemptIdentifier = constructInputAttemptIdentifier(dmEvent, shufflePayload);
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Source partition: " + partitionId + " did not generate any data. SrcAttempt: ["
                    + srcAttemptIdentifier + "]. Not fetching.");
          }
          scheduler.copySucceeded(srcAttemptIdentifier, null, 0, 0, 0, null);
          return;
        }
      } catch (IOException e) {
        throw new TezUncheckedException("Unable to set " +
                "the empty partition to succeeded", e);
      }
    }

    InputAttemptIdentifier srcAttemptIdentifier = constructInputAttemptIdentifier(dmEvent, shufflePayload);

    URI baseUri = getBaseURI(shufflePayload.getHost(), shufflePayload.getPort(), partitionId);
    scheduler.addKnownMapOutput(shufflePayload.getHost(), shufflePayload.getPort(),
        partitionId, baseUri.toString(), srcAttemptIdentifier);
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

  /**
   * Helper method to create InputAttemptIdentifier
   *
   * @param dmEvent
   * @param shufflePayload
   * @return InputAttemptIdentifier
   */
  private InputAttemptIdentifier constructInputAttemptIdentifier(DataMovementEvent dmEvent,
      DataMovementEventPayloadProto shufflePayload) {
    String pathComponent = (shufflePayload.hasPathComponent()) ? shufflePayload.getPathComponent() : null;
    int spillEventId = shufflePayload.getSpillId();
    InputAttemptIdentifier srcAttemptIdentifier = null;
    if (shufflePayload.hasSpillId()) {
      boolean lastEvent = shufflePayload.getLastEvent();
      InputAttemptIdentifier.SPILL_INFO info = (lastEvent) ? InputAttemptIdentifier.SPILL_INFO
          .FINAL_UPDATE : InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE;
      srcAttemptIdentifier =
          new InputAttemptIdentifier(new InputIdentifier(dmEvent.getTargetIndex()), dmEvent
              .getVersion(), pathComponent, false, info, spillEventId);
    } else {
      srcAttemptIdentifier =
          new InputAttemptIdentifier(dmEvent.getTargetIndex(), dmEvent.getVersion(), pathComponent);
    }
    return srcAttemptIdentifier;
  }
}

