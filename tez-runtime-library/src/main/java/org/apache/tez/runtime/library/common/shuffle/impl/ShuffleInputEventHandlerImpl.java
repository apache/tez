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
import java.util.BitSet;
import java.util.List;

import com.google.protobuf.ByteString;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputIdentifier;
import org.apache.tez.runtime.library.common.shuffle.DiskFetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.MemoryFetchedInput;
import org.apache.tez.runtime.library.common.shuffle.ShuffleEventHandler;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataProto;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A base class for generic Event handling for Inputs which need to Shuffle data.
 */
public class ShuffleInputEventHandlerImpl implements ShuffleEventHandler {

  private static final Log LOG = LogFactory.getLog(ShuffleInputEventHandlerImpl.class);
  
  private final ShuffleManager shuffleManager;
  private final FetchedInputAllocator inputAllocator;
  private final CompressionCodec codec;
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;
  private final boolean useSharedInputs;

  public ShuffleInputEventHandlerImpl(InputContext inputContext,
                                      ShuffleManager shuffleManager,
                                      FetchedInputAllocator inputAllocator, CompressionCodec codec,
                                      boolean ifileReadAhead, int ifileReadAheadLength) {
    this.shuffleManager = shuffleManager;
    this.inputAllocator = inputAllocator;
    this.codec = codec;
    this.ifileReadAhead = ifileReadAhead;
    this.ifileReadAheadLength = ifileReadAheadLength;
    // this currently relies on a user to enable the flag
    // expand on idea based on vertex parallelism and num inputs
    this.useSharedInputs = (inputContext.getTaskAttemptNumber() == 0);
  }

  @Override
  public void handleEvents(List<Event> events) throws IOException {
    for (Event event : events) {
      handleEvent(event);
    }
  }
  
  private void handleEvent(Event event) throws IOException {
    if (event instanceof DataMovementEvent) {
      processDataMovementEvent((DataMovementEvent)event);
      shuffleManager.updateEventReceivedTime();
    } else if (event instanceof InputFailedEvent) {
      processInputFailedEvent((InputFailedEvent)event);
    } else {
      throw new TezUncheckedException("Unexpected event type: " + event.getClass().getName());
    }
  }

  private void processDataMovementEvent(DataMovementEvent dme) throws IOException {
    DataMovementEventPayloadProto shufflePayload;
    try {
      shufflePayload = DataMovementEventPayloadProto.parseFrom(
          ByteString.copyFrom(dme.getUserPayload()));
    } catch (InvalidProtocolBufferException e) {
      throw new TezUncheckedException("Unable to parse DataMovementEvent payload", e);
    }
    int srcIndex = dme.getSourceIndex();
    String hostIdentifier = shufflePayload.getHost() + ":" + shufflePayload.getPort();
    LOG.info("DME srcIdx: " + srcIndex + ", targetIndex: " + dme.getTargetIndex()
        + ", attemptNum: " + dme.getVersion() + ", payload: " + ShuffleUtils
        .stringify(shufflePayload));

    if (shufflePayload.hasEmptyPartitions()) {
      byte[] emptyPartitions = TezCommonUtils.decompressByteStringToByteArray(shufflePayload
          .getEmptyPartitions());
      BitSet emptyPartionsBitSet = TezUtilsInternal.fromByteArray(emptyPartitions);
      if (emptyPartionsBitSet.get(srcIndex)) {
        InputAttemptIdentifier srcAttemptIdentifier =
            constructInputAttemptIdentifier(dme, shufflePayload, false);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Source partition: " + srcIndex + " did not generate any data. SrcAttempt: ["
              + srcAttemptIdentifier + "]. Not fetching.");
        }
        shuffleManager.addCompletedInputWithNoData(srcAttemptIdentifier);
        return;
      }
    }

    InputAttemptIdentifier srcAttemptIdentifier = constructInputAttemptIdentifier(dme,
        shufflePayload, (useSharedInputs && srcIndex == 0));

    if (shufflePayload.hasData()) {
      DataProto dataProto = shufflePayload.getData();
      FetchedInput fetchedInput = inputAllocator.allocate(dataProto.getRawLength(),
          dataProto.getCompressedLength(), srcAttemptIdentifier);
      moveDataToFetchedInput(dataProto, fetchedInput, hostIdentifier);
      shuffleManager.addCompletedInputWithData(srcAttemptIdentifier, fetchedInput);
    } else {
      shuffleManager.addKnownInput(shufflePayload.getHost(),
          shufflePayload.getPort(), srcAttemptIdentifier, srcIndex);
    }

  }

  private void moveDataToFetchedInput(DataProto dataProto,
      FetchedInput fetchedInput, String hostIdentifier) throws IOException {
    switch (fetchedInput.getType()) {
    case DISK:
      ShuffleUtils.shuffleToDisk(((DiskFetchedInput) fetchedInput).getOutputStream(),
        hostIdentifier, dataProto.getData().newInput(), dataProto.getCompressedLength(), LOG,
          fetchedInput.getInputAttemptIdentifier().toString());
      break;
    case MEMORY:
      ShuffleUtils.shuffleToMemory(((MemoryFetchedInput) fetchedInput).getBytes(),
        dataProto.getData().newInput(), dataProto.getRawLength(), dataProto.getCompressedLength(),
        codec, ifileReadAhead, ifileReadAheadLength, LOG,
        fetchedInput.getInputAttemptIdentifier().toString());
      break;
    case WAIT:
    default:
      throw new TezUncheckedException("Unexpected type: "
          + fetchedInput.getType());
    }
  }
  
  private void processInputFailedEvent(InputFailedEvent ife) {
    InputAttemptIdentifier srcAttemptIdentifier = new InputAttemptIdentifier(ife.getTargetIndex(), ife.getVersion());
    shuffleManager.obsoleteKnownInput(srcAttemptIdentifier);
  }

  /**
   * Helper method to create InputAttemptIdentifier
   *
   * @param dmEvent
   * @param shufflePayload
   * @return InputAttemptIdentifier
   */
  private InputAttemptIdentifier constructInputAttemptIdentifier(DataMovementEvent dmEvent,
      DataMovementEventPayloadProto shufflePayload, boolean isShared) {
    String pathComponent = (shufflePayload.hasPathComponent()) ? shufflePayload.getPathComponent() : null;
    InputAttemptIdentifier srcAttemptIdentifier = null;
    if (shufflePayload.hasSpillId()) {
      int spillEventId = shufflePayload.getSpillId();
      boolean lastEvent = shufflePayload.getLastEvent();
      InputAttemptIdentifier.SPILL_INFO spillInfo = (lastEvent) ? InputAttemptIdentifier.SPILL_INFO
          .FINAL_UPDATE : InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE;
      srcAttemptIdentifier =
          new InputAttemptIdentifier(new InputIdentifier(dmEvent.getTargetIndex()), dmEvent
              .getVersion(), pathComponent, isShared, spillInfo, spillEventId);
    } else {
      srcAttemptIdentifier =
          new InputAttemptIdentifier(dmEvent.getTargetIndex(), dmEvent.getVersion(),
              pathComponent, isShared);
    }
    return srcAttemptIdentifier;
  }

}

