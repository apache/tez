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
import java.util.zip.Inflater;

import com.google.protobuf.ByteString;

import org.apache.tez.runtime.api.events.CompositeRoutedDataMovementEvent;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
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
  private final boolean compositeFetch;
  private final Inflater inflater;

  private final AtomicInteger nextToLogEventCount = new AtomicInteger(0);
  private final AtomicInteger numDmeEvents = new AtomicInteger(0);
  private final AtomicInteger numObsoletionEvents = new AtomicInteger(0);
  private final AtomicInteger numDmeEventsNoData = new AtomicInteger(0);

  public ShuffleInputEventHandlerImpl(InputContext inputContext,
                                      ShuffleManager shuffleManager,
                                      FetchedInputAllocator inputAllocator, CompressionCodec codec,
                                      boolean ifileReadAhead, int ifileReadAheadLength, boolean compositeFetch) {
    this.inputContext = inputContext;
    this.shuffleManager = shuffleManager;
    this.inputAllocator = inputAllocator;
    this.codec = codec;
    this.ifileReadAhead = ifileReadAhead;
    this.ifileReadAheadLength = ifileReadAheadLength;
    // this currently relies on a user to enable the flag
    // expand on idea based on vertex parallelism and num inputs
    this.useSharedInputs = (inputContext.getTaskAttemptNumber() == 0);
    this.compositeFetch = compositeFetch;
    this.inflater = TezCommonUtils.newInflater();
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
      DataMovementEvent dmEvent = (DataMovementEvent)event;
      DataMovementEventPayloadProto shufflePayload;
      try {
        shufflePayload = DataMovementEventPayloadProto.parseFrom(ByteString.copyFrom(dmEvent.getUserPayload()));
      } catch (InvalidProtocolBufferException e) {
        throw new TezUncheckedException("Unable to parse DataMovementEvent payload", e);
      }
      BitSet emptyPartitionsBitSet = null;
      if (shufflePayload.hasEmptyPartitions()) {
        try {
          byte[] emptyPartitions = TezCommonUtils.decompressByteStringToByteArray(shufflePayload.getEmptyPartitions(), inflater);
          emptyPartitionsBitSet = TezUtilsInternal.fromByteArray(emptyPartitions);
        } catch (IOException e) {
          throw new TezUncheckedException("Unable to set the empty partition to succeeded", e);
        }
      }
      processDataMovementEvent(dmEvent, shufflePayload, emptyPartitionsBitSet);
      shuffleManager.updateEventReceivedTime();
    } else if (event instanceof CompositeRoutedDataMovementEvent) {
      CompositeRoutedDataMovementEvent crdme = (CompositeRoutedDataMovementEvent)event;
      DataMovementEventPayloadProto shufflePayload;
      try {
        shufflePayload = DataMovementEventPayloadProto.parseFrom(ByteString.copyFrom(crdme.getUserPayload()));
      } catch (InvalidProtocolBufferException e) {
        throw new TezUncheckedException("Unable to parse DataMovementEvent payload", e);
      }
      BitSet emptyPartitionsBitSet = null;
      if (shufflePayload.hasEmptyPartitions()) {
        try {
          byte[] emptyPartitions = TezCommonUtils.decompressByteStringToByteArray(shufflePayload.getEmptyPartitions(), inflater);
          emptyPartitionsBitSet = TezUtilsInternal.fromByteArray(emptyPartitions);
        } catch (IOException e) {
          throw new TezUncheckedException("Unable to set the empty partition to succeeded", e);
        }
      }
      if (compositeFetch) {
        numDmeEvents.addAndGet(crdme.getCount());
        processCompositeRoutedDataMovementEvent(crdme, shufflePayload, emptyPartitionsBitSet);
      } else {
        for (int offset = 0; offset < crdme.getCount(); offset++) {
          numDmeEvents.incrementAndGet();
          processDataMovementEvent(crdme.expand(offset), shufflePayload, emptyPartitionsBitSet);
        }
      }
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

  private void processDataMovementEvent(DataMovementEvent dme, DataMovementEventPayloadProto shufflePayload, BitSet emptyPartitionsBitSet) throws IOException {
    int srcIndex = dme.getSourceIndex();
    if (LOG.isDebugEnabled()) {
      LOG.debug("DME srcIdx: " + srcIndex + ", targetIndex: " + dme.getTargetIndex()
          + ", attemptNum: " + dme.getVersion() + ", payload: " + ShuffleUtils
          .stringify(shufflePayload));
    }

    if (shufflePayload.hasEmptyPartitions()) {
      if (emptyPartitionsBitSet.get(srcIndex)) {
        CompositeInputAttemptIdentifier srcAttemptIdentifier =
            constructInputAttemptIdentifier(dme.getTargetIndex(), 1, dme.getVersion(), shufflePayload, false);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Source partition: " + srcIndex + " did not generate any data. SrcAttempt: ["
              + srcAttemptIdentifier + "]. Not fetching.");
        }
        numDmeEventsNoData.getAndIncrement();
        shuffleManager.addCompletedInputWithNoData(srcAttemptIdentifier.expand(0));
        return;
      }
    }

    CompositeInputAttemptIdentifier srcAttemptIdentifier = constructInputAttemptIdentifier(dme.getTargetIndex(), 1, dme.getVersion(),
        shufflePayload, (useSharedInputs && srcIndex == 0));

    shuffleManager.addKnownInput(shufflePayload.getHost(), shufflePayload.getPort(), srcAttemptIdentifier, srcIndex);
  }

  private void processCompositeRoutedDataMovementEvent(CompositeRoutedDataMovementEvent crdme, DataMovementEventPayloadProto shufflePayload, BitSet emptyPartitionsBitSet) throws IOException {
    int partitionId = crdme.getSourceIndex();
    if (LOG.isDebugEnabled()) {
      LOG.debug("DME srcIdx: " + partitionId + ", targetIndex: " + crdme.getTargetIndex() + ", count:" + crdme.getCount()
          + ", attemptNum: " + crdme.getVersion() + ", payload: " + ShuffleUtils
          .stringify(shufflePayload));
    }

    if (shufflePayload.hasEmptyPartitions()) {
      CompositeInputAttemptIdentifier compositeInputAttemptIdentifier =
          constructInputAttemptIdentifier(crdme.getTargetIndex(), crdme.getCount(), crdme.getVersion(), shufflePayload, false);

      boolean allPartitionsEmpty = true;
      for (int i = 0; i < crdme.getCount(); i++) {
        int srcPartitionId = partitionId + i;
        allPartitionsEmpty &= emptyPartitionsBitSet.get(srcPartitionId);
        if (emptyPartitionsBitSet.get(srcPartitionId)) {
          InputAttemptIdentifier srcAttemptIdentifier = compositeInputAttemptIdentifier.expand(i);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Source partition: " + srcPartitionId + " did not generate any data. SrcAttempt: ["
                + srcAttemptIdentifier + "]. Not fetching.");
          }
          numDmeEventsNoData.getAndIncrement();
          shuffleManager.addCompletedInputWithNoData(srcAttemptIdentifier);
        }
      }

      if (allPartitionsEmpty) {
        return;
      }
    }

    CompositeInputAttemptIdentifier srcAttemptIdentifier = constructInputAttemptIdentifier(crdme.getTargetIndex(), crdme.getCount(), crdme.getVersion(),
        shufflePayload, (useSharedInputs && partitionId == 0));

    shuffleManager.addKnownInput(shufflePayload.getHost(), shufflePayload.getPort(), srcAttemptIdentifier, partitionId);
  }

  private void processInputFailedEvent(InputFailedEvent ife) {
    InputAttemptIdentifier srcAttemptIdentifier = new InputAttemptIdentifier(ife.getTargetIndex(), ife.getVersion());
    shuffleManager.obsoleteKnownInput(srcAttemptIdentifier);
  }

  /**
   * Helper method to create InputAttemptIdentifier
   *
   * @param targetIndex
   * @param targetIndexCount
   * @param version
   * @param shufflePayload
   * @param isShared
   * @return CompositeInputAttemptIdentifier
   */
  private CompositeInputAttemptIdentifier constructInputAttemptIdentifier(int targetIndex, int targetIndexCount, int version,
      DataMovementEventPayloadProto shufflePayload, boolean isShared) {
    String pathComponent = (shufflePayload.hasPathComponent()) ? shufflePayload.getPathComponent() : null;
    CompositeInputAttemptIdentifier srcAttemptIdentifier = null;
    if (shufflePayload.hasSpillId()) {
      int spillEventId = shufflePayload.getSpillId();
      boolean lastEvent = shufflePayload.getLastEvent();
      InputAttemptIdentifier.SPILL_INFO spillInfo = (lastEvent) ? InputAttemptIdentifier.SPILL_INFO
          .FINAL_UPDATE : InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE;
      srcAttemptIdentifier =
          new CompositeInputAttemptIdentifier(targetIndex, version, pathComponent, isShared, spillInfo, spillEventId, targetIndexCount);
    } else {
      srcAttemptIdentifier =
          new CompositeInputAttemptIdentifier(targetIndex, version, pathComponent, isShared, targetIndexCount);
    }
    return srcAttemptIdentifier;
  }

}

