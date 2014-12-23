package org.apache.tez.runtime.api.event;

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
import java.nio.ByteBuffer;

import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.junit.Assert;
import org.junit.Test;

public class TestCompositeDataMovementEvent {
  ByteBuffer userPayload = ByteBuffer.wrap("Dummy userPayLoad".getBytes());

  @Test(timeout = 5000)
  public void testGetCount(){
    int numPartitions = 2;
    int startIndex = 2;
    CompositeDataMovementEvent cdme1 =
        CompositeDataMovementEvent.create(startIndex, numPartitions, userPayload);
    Assert.assertEquals(numPartitions, cdme1.getCount());
    Assert.assertEquals(startIndex, cdme1.getSourceIndexStart());
  }

  @Test(timeout = 5000)
  public void testGetEvents(){
    int numOutputs = 0;
    int startIndex = 1;
    CompositeDataMovementEvent cdme2 =
        CompositeDataMovementEvent.create(startIndex, numOutputs, userPayload);
    for(DataMovementEvent dme: cdme2.getEvents()){
      numOutputs++;
    }
    Assert.assertEquals(numOutputs, cdme2.getCount());
  }

}
