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

package org.apache.tez.runtime.api.events;

import org.apache.tez.runtime.api.Event;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.base.Function;

/**
 * A convenience class to specify multiple DataMovementEvents which share the
 * same payload. A contiguous range of srcIndices can be specified.
 * 
 * This event will NOT be seen by downstream Inputs - instead they will see
 * {@link DataMovementEvent}s which are generated based on the range specified
 * in this event.
 * 
 * This event should be used by an output which has the same payload for all of
 * the Physical Outputs that it generates.
 * 
 */
public class CompositeDataMovementEvent extends Event {

  protected final int sourceIndexStart;
  protected final int sourceIndexEnd;
  protected int version;

  protected final byte[] userPayload;

  /**
   * @param srcIndexStart
   *          the startIndex of the physical source which generated the event
   *          (inclusive)
   * @param srcIndexEnd
   *          the endIndex of the physical source which generated the event
   *          (non-inclusive)
   * @param userPayload
   *          the common payload between all the events.
   */
  public CompositeDataMovementEvent(int srcIndexStart, int srcIndexEnd, byte[] userPayload) {
    this.sourceIndexStart = srcIndexStart;
    this.sourceIndexEnd = srcIndexEnd;
    this.userPayload = userPayload;
  }

  public int getSourceIndexStart() {
    return sourceIndexStart;
  }

  public int getSourceIndexEnd() {
    return sourceIndexEnd;
  }

  public byte[] getUserPayload() {
    return userPayload;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public int getVersion() {
    return this.version;
  }

  public Iterable<DataMovementEvent> getEvents() {
    // Overkill. For now is enough to just run a loop over the ints.
    Range<Integer> range = Range.closedOpen(sourceIndexStart, sourceIndexEnd);
    ContiguousSet<Integer> intRange = ContiguousSet.create(range, DiscreteDomain.integers());
    return Iterables.transform(intRange, new Function<Integer, DataMovementEvent>() {
      public DataMovementEvent apply(Integer integer) {
        DataMovementEvent dmEvent = new DataMovementEvent(integer, userPayload);
        dmEvent.setVersion(version);
        return dmEvent;
      }
    });
  }

}
