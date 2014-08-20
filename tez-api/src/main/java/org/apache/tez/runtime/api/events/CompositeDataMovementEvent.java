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

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.tez.runtime.api.Event;

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
@Public
public class CompositeDataMovementEvent extends Event {

  protected final int sourceIndexStart;
  protected final int count;
  protected int version;

  protected final ByteBuffer userPayload;

  private CompositeDataMovementEvent(int srcIndexStart, int count, ByteBuffer userPayload) {
    this.sourceIndexStart = srcIndexStart;
    this.count = count;
    this.userPayload = userPayload;
  }

  /**
   * @param srcIndexStart
   *          the startIndex of the physical source which generated the event
   *          (inclusive)
   * @param count
   *          the number of physical sources represented by this event,
   *          starting from the srcIndexStart(non-inclusive)
   * @param userPayload
   *          the common payload between all the events.
   */
  public static CompositeDataMovementEvent create(int srcIndexStart, int count,
                                                  ByteBuffer userPayload) {
    return new CompositeDataMovementEvent(srcIndexStart, count, userPayload);
  }

  public int getSourceIndexStart() {
    return sourceIndexStart;
  }

  public int getCount() {
    return count;
  }

  public ByteBuffer getUserPayload() {
    return userPayload == null ? null : userPayload.asReadOnlyBuffer();
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public int getVersion() {
    return this.version;
  }

  public Iterable<DataMovementEvent> getEvents() {

    return new Iterable<DataMovementEvent>() {
      
      @Override
      public Iterator<DataMovementEvent> iterator() {
        return new Iterator<DataMovementEvent>() {
          
          int currentPos = sourceIndexStart;

          @Override
          public boolean hasNext() {
            return currentPos < (count + sourceIndexStart);
          }

          @Override
          public DataMovementEvent next() {
            DataMovementEvent dmEvent = DataMovementEvent.create(currentPos, userPayload);
            currentPos++;
            dmEvent.setVersion(version);
            return dmEvent;
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException("Remove not supported");
          }
        };
      }
    };
  }

}
