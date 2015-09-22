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
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.ShuffleEventHandler;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A base class for generic Event handling for Inputs which need to Shuffle data.
 */
public class ShuffleInputEventHandlerImpl implements ShuffleEventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleInputEventHandlerImpl.class);
  
  private final ShuffleManager shuffleManager;
  //TODO: unused. Consider removing later?
  private final FetchedInputAllocator inputAllocator;
  private final CompressionCodec codec;
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;
  private final boolean useSharedInputs;
  private final InputContext inputContext;

  private final AtomicInteger nextToLogEventCount = new AtomicInteger(0);
  private final AtomicInteger numDmeEvents = new AtomicInteger(0);
  private final AtomicInteger numObsoletionEvents = new AtomicInteger(0);
  private final AtomicInteger numDmeEventsNoData = new AtomicInteger(0);

  public ShuffleInputEventHandlerImpl(InputContext inputContext,
                                      ShuffleManager shuffleManager,
                                      FetchedInputAllocator inputAllocator, CompressionCodec codec,
                                      boolean ifileReadAhead, int ifileReadAheadLength) {
    this.inputContext = inputContext;
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
      numDmeEvents.incrementAndGet();
      processDataMovementEvent((DataMovementEvent)event);
      shuffleManager.updateEventReceivedTime();
    } else if (event instanceof InputFailedEvent) {
      numObsoletionEvents.incrementAndGet();
      processInputFailedEvent((InputFailedEvent) event);
    } else {
      throw new TezUncheckedException("Unexpected event type: " + event.getClass().getName());
    }
    if (numDmeEvents.get() + numObsoletionEvents.get() > nextToLogEventCount.get()) {
      logProgress(false);
      // Log every 50 events seen.
      nextToLogEventCount.addAndGet(50);
    }
  }

  @Override
  public void logProgress(boolean updateOnClose) {
    LOG.info(inputContext.getSourceVertexName() + ": "
        + "numDmeEventsSeen=" + numDmeEvents.get()
        + ", numDmeEventsSeenWithNoData=" + numDmeEventsNoData.get()
        + ", numObsoletionEventsSeen=" + numObsoletionEvents.get()
        + (updateOnClose == true ? ", updateOnClose" : ""));
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("DME srcIdx: " + srcIndex + ", targetIndex: " + dme.getTargetIndex()
          + ", attemptNum: " + dme.getVersion() + ", payload: " + ShuffleUtils
          .stringify(shufflePayload));
    }

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
        numDmeEventsNoData.incrementAndGet();
        shuffleManager.addCompletedInputWithNoData(srcAttemptIdentifier);
        return;
      }
    }

    InputAttemptIdentifier srcAttemptIdentifier = constructInputAttemptIdentifier(dme,
        shufflePayload, (useSharedInputs && srcIndex == 0));

    shuffleManager.addKnownInput(shufflePayload.getHost(),
        shufflePayload.getPort(), srcAttemptIdentifier, srcIndex);
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

