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
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.Inflater;

import com.google.protobuf.ByteString;
import org.apache.tez.runtime.api.events.CompositeRoutedDataMovementEvent;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.ShuffleEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;

import com.google.protobuf.InvalidProtocolBufferException;

public class ShuffleInputEventHandlerOrderedGrouped implements ShuffleEventHandler {
  
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleInputEventHandlerOrderedGrouped.class);

  private final ShuffleScheduler scheduler;
  private final InputContext inputContext;
  private final boolean compositeFetch;
  private final Inflater inflater;

  private final AtomicInteger nextToLogEventCount = new AtomicInteger(0);
  private final AtomicInteger numDmeEvents = new AtomicInteger(0);
  private final AtomicInteger numObsoletionEvents = new AtomicInteger(0);
  private final AtomicInteger numDmeEventsNoData = new AtomicInteger(0);

  public ShuffleInputEventHandlerOrderedGrouped(InputContext inputContext,
                                                ShuffleScheduler scheduler,
                                                boolean compositeFetch) {
    this.inputContext = inputContext;
    this.scheduler = scheduler;
    this.compositeFetch = compositeFetch;
    this.inflater = TezCommonUtils.newInflater();
  }

  @Override
  public void handleEvents(List<Event> events) throws IOException {
    for (Event event : events) {
      handleEvent(event);
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
      scheduler.updateEventReceivedTime();
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
      scheduler.updateEventReceivedTime();
    } else if (event instanceof InputFailedEvent) {
      numObsoletionEvents.incrementAndGet();
      processTaskFailedEvent((InputFailedEvent) event);
    }
    if (numDmeEvents.get() + numObsoletionEvents.get() > nextToLogEventCount.get()) {
      logProgress(false);
      // Log every 50 events seen.
      nextToLogEventCount.addAndGet(50);
    }
  }

  private void processDataMovementEvent(DataMovementEvent dmEvent, DataMovementEventPayloadProto shufflePayload, BitSet emptyPartitionsBitSet) throws IOException {
    int partitionId = dmEvent.getSourceIndex();
    CompositeInputAttemptIdentifier srcAttemptIdentifier = constructInputAttemptIdentifier(dmEvent.getTargetIndex(), 1, dmEvent.getVersion(), shufflePayload);

    if (LOG.isDebugEnabled()) {
      LOG.debug("DME srcIdx: " + partitionId + ", targetIdx: " + dmEvent.getTargetIndex()
          + ", attemptNum: " + dmEvent.getVersion() + ", payload: " +
          ShuffleUtils.stringify(shufflePayload));
    }

    if (shufflePayload.hasEmptyPartitions()) {
      try {
        if (emptyPartitionsBitSet.get(partitionId)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Source partition: " + partitionId + " did not generate any data. SrcAttempt: ["
                    + srcAttemptIdentifier + "]. Not fetching.");
          }
          numDmeEventsNoData.getAndIncrement();
          scheduler.copySucceeded(srcAttemptIdentifier.expand(0), null, 0, 0, 0, null, true);
          return;
        }
      } catch (IOException e) {
        throw new TezUncheckedException("Unable to set the empty partition to succeeded", e);
      }
    }

    scheduler.addKnownMapOutput(StringInterner.weakIntern(shufflePayload.getHost()), shufflePayload.getPort(),
        partitionId, srcAttemptIdentifier);
  }

  private void processCompositeRoutedDataMovementEvent(CompositeRoutedDataMovementEvent crdmEvent, DataMovementEventPayloadProto shufflePayload, BitSet emptyPartitionsBitSet) throws IOException {
    int partitionId = crdmEvent.getSourceIndex();
    CompositeInputAttemptIdentifier compositeInputAttemptIdentifier = constructInputAttemptIdentifier(crdmEvent.getTargetIndex(), crdmEvent.getCount(), crdmEvent.getVersion(), shufflePayload);

    if (LOG.isDebugEnabled()) {
      LOG.debug("DME srcIdx: " + partitionId + ", targetIdx: " + crdmEvent.getTargetIndex() + ", count:" + crdmEvent.getCount()
          + ", attemptNum: " + crdmEvent.getVersion() + ", payload: " +
          ShuffleUtils.stringify(shufflePayload));
    }

    if (shufflePayload.hasEmptyPartitions()) {
      boolean allPartitionsEmpty = true;
      for (int i = 0; i < crdmEvent.getCount(); i++) {
        int srcPartitionId = partitionId + i;
        allPartitionsEmpty &= emptyPartitionsBitSet.get(srcPartitionId);
        if (emptyPartitionsBitSet.get(srcPartitionId)) {
          InputAttemptIdentifier srcInputAttemptIdentifier = compositeInputAttemptIdentifier.expand(i);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Source partition: " + srcPartitionId + " did not generate any data. SrcAttempt: ["
                + srcInputAttemptIdentifier + "]. Not fetching.");
          }
          numDmeEventsNoData.getAndIncrement();
          scheduler.copySucceeded(srcInputAttemptIdentifier, null, 0, 0, 0, null, true);
        }
      }

      if (allPartitionsEmpty) {
        return;
      }
    }

    scheduler.addKnownMapOutput(StringInterner.weakIntern(shufflePayload.getHost()), shufflePayload.getPort(),
        partitionId, compositeInputAttemptIdentifier);
  }

  private void processTaskFailedEvent(InputFailedEvent ifEvent) {
    InputAttemptIdentifier taIdentifier = new InputAttemptIdentifier(ifEvent.getTargetIndex(), ifEvent.getVersion());
    scheduler.obsoleteInput(taIdentifier);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Obsoleting output of src-task: " + taIdentifier);
    }
  }

  /**
   * Helper method to create InputAttemptIdentifier
   *
   * @param targetIndex
   * @param targetIndexCount
   * @param version
   * @param shufflePayload
   * @return CompositeInputAttemptIdentifier
   */
  private CompositeInputAttemptIdentifier constructInputAttemptIdentifier(int targetIndex, int targetIndexCount, int version,
                                                                          DataMovementEventPayloadProto shufflePayload) {
    String pathComponent = (shufflePayload.hasPathComponent()) ? StringInterner.weakIntern(shufflePayload.getPathComponent()) : null;
    int spillEventId = shufflePayload.getSpillId();
    CompositeInputAttemptIdentifier srcAttemptIdentifier = null;
    if (shufflePayload.hasSpillId()) {
      boolean lastEvent = shufflePayload.getLastEvent();
      InputAttemptIdentifier.SPILL_INFO info = (lastEvent) ? InputAttemptIdentifier.SPILL_INFO
          .FINAL_UPDATE : InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE;
      srcAttemptIdentifier =
          new CompositeInputAttemptIdentifier(targetIndex, version, pathComponent, false, info, spillEventId, targetIndexCount);
    } else {
      srcAttemptIdentifier =
          new CompositeInputAttemptIdentifier(targetIndex, version, pathComponent, targetIndexCount);
    }
    return srcAttemptIdentifier;
  }
}

