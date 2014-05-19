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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;

import com.google.protobuf.InvalidProtocolBufferException;

public class ShuffleInputEventHandler {
  
  private static final Log LOG = LogFactory.getLog(ShuffleInputEventHandler.class);

  private final ShuffleScheduler scheduler;
  private final TezInputContext inputContext;

  private int maxMapRuntime = 0;
  private final boolean sslShuffle;

  public ShuffleInputEventHandler(TezInputContext inputContext,
      ShuffleScheduler scheduler, boolean sslShuffle) {
    this.inputContext = inputContext;
    this.scheduler = scheduler;
    this.sslShuffle = sslShuffle;
  }

  public void handleEvents(List<Event> events) {
    for (Event event : events) {
      handleEvent(event);
    }
  }
  
  
  private void handleEvent(Event event) {
    if (event instanceof DataMovementEvent) {
      processDataMovementEvent((DataMovementEvent) event);      
    } else if (event instanceof InputFailedEvent) {
      processTaskFailedEvent((InputFailedEvent) event);
    }
  }

  private void processDataMovementEvent(DataMovementEvent dmEvent) {
    DataMovementEventPayloadProto shufflePayload;
    try {
      shufflePayload = DataMovementEventPayloadProto.parseFrom(dmEvent.getUserPayload());
    } catch (InvalidProtocolBufferException e) {
      throw new TezUncheckedException("Unable to parse DataMovementEvent payload", e);
    } 
    int partitionId = dmEvent.getSourceIndex();
    URI baseUri = getBaseURI(shufflePayload.getHost(), shufflePayload.getPort(), partitionId);
    InputAttemptIdentifier srcAttemptIdentifier = 
        new InputAttemptIdentifier(dmEvent.getTargetIndex(), dmEvent.getVersion(), shufflePayload.getPathComponent());
    LOG.info("DataMovementEvent baseUri:" + baseUri + ", src: " + srcAttemptIdentifier);
    
    // TODO NEWTEZ See if this duration hack can be removed.
    int duration = shufflePayload.getRunDuration();
    if (duration > maxMapRuntime) {
      maxMapRuntime = duration;
      scheduler.informMaxMapRunTime(maxMapRuntime);
    }
    if (shufflePayload.hasEmptyPartitions()) {
      try {
        byte[] emptyPartitions = TezUtils.decompressByteStringToByteArray(shufflePayload.getEmptyPartitions());
        BitSet emptyPartitionsBitSet = TezUtils.fromByteArray(emptyPartitions);
        if (emptyPartitionsBitSet.get(partitionId)) {
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
    scheduler.addKnownMapOutput(shufflePayload.getHost(), shufflePayload.getPort(), 
        partitionId, baseUri.toString(), srcAttemptIdentifier);
  }
  
  private void processTaskFailedEvent(InputFailedEvent ifEvent) {
    InputAttemptIdentifier taIdentifier = new InputAttemptIdentifier(ifEvent.getTargetIndex(), ifEvent.getVersion());
    scheduler.obsoleteInput(taIdentifier);
    LOG.info("Obsoleting output of src-task: " + taIdentifier);
  }

  // TODO NEWTEZ Handle encrypted shuffle
  private URI getBaseURI(String host, int port, int partitionId) {
    StringBuilder sb = ShuffleUtils.constructBaseURIForShuffleHandler(host, port,
      partitionId, inputContext.getApplicationId().toString(), sslShuffle);
    URI u = URI.create(sb.toString());
    return u;
  }
}

