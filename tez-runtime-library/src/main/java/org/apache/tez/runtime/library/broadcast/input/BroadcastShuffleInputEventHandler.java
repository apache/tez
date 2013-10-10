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


package org.apache.tez.runtime.library.broadcast.input;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;

public class BroadcastShuffleInputEventHandler {

  private static final Log LOG = LogFactory.getLog(BroadcastShuffleInputEventHandler.class);
  
  private final BroadcastShuffleManager shuffleManager;
  
  public BroadcastShuffleInputEventHandler(TezInputContext inputContext, BroadcastShuffleManager shuffleManager) {
    this.shuffleManager = shuffleManager;
  }

  public void handleEvents(List<Event> events) {
    for (Event event : events) {
      handleEvent(event);
    }
  }
  
  private void handleEvent(Event event) {
    if (event instanceof DataMovementEvent) {
      processDataMovementEvent((DataMovementEvent)event);
    } else if (event instanceof InputFailedEvent) {
      processInputFailedEvent((InputFailedEvent)event);
    } else {
      throw new TezUncheckedException("Unexpected event type: " + event.getClass().getName());
    }
  }
  
  
  private void processDataMovementEvent(DataMovementEvent dme) {
    Preconditions.checkArgument(dme.getSourceIndex() == 0,
        "Unexpected srcIndex: " + dme.getSourceIndex()
            + " on DataMovementEvent. Can only be 0");
    DataMovementEventPayloadProto shufflePayload;
    try {
      shufflePayload = DataMovementEventPayloadProto.parseFrom(dme.getUserPayload());
    } catch (InvalidProtocolBufferException e) {
      throw new TezUncheckedException("Unable to parse DataMovementEvent payload", e);
    }
    LOG.info("Processing DataMovementEvent with srcIndex: "
        + dme.getSourceIndex() + ", targetIndex: " + dme.getTargetIndex()
        + ", attemptNum: " + dme.getVersion() + ", payload: "
        + TextFormat.shortDebugString(shufflePayload));
    if (shufflePayload.getOutputGenerated()) {
      InputAttemptIdentifier srcAttemptIdentifier = new InputAttemptIdentifier(dme.getTargetIndex(), dme.getVersion(), shufflePayload.getPathComponent());
      shuffleManager.addKnownInput(shufflePayload.getHost(), shufflePayload.getPort(), srcAttemptIdentifier, 0);
    } else {
      shuffleManager.addCompletedInputWithNoData(new InputAttemptIdentifier(dme.getTargetIndex(), dme.getVersion()));
    }
  }
  
  private void processInputFailedEvent(InputFailedEvent ife) {
    InputAttemptIdentifier srcAttemptIdentifier = new InputAttemptIdentifier(ife.getTargetIndex(), ife.getVersion());
    shuffleManager.obsoleteKnownInput(srcAttemptIdentifier);
  }
}

