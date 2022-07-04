/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.task;

import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezOutputContextImpl;
import org.apache.tez.runtime.api.impl.TezUmbilical;
import org.apache.tez.runtime.internals.api.TezTrapEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Class that handles the events after the trap has been activated. At
 * this point no more events of some types shouldn't be sent and it's
 * a bug to do so. If the events arrive here probably the task will be
 * restarted because it has failed.
 */
public class TezTrapEventHandler implements EventHandler<TezTrapEvent> {
  /**
   * logger.
   */
  private static final Logger
      LOG = LoggerFactory.getLogger(TezOutputContextImpl.class);

  /**
   * Output context that will report the events.
   */
  private final OutputContext outputContext;

  /**
   * Protocol to send the events.
   */
  private final TezUmbilical tezUmbilical;

  /**
   * @param output    context that will report the events.
   * @param umbilical used to send the events to the AM.
   */
  TezTrapEventHandler(final OutputContext output,
                      final TezUmbilical umbilical) {
    this.outputContext = output;
    this.tezUmbilical = umbilical;
  }

  /**
   * Decide what to do with the events.
   *
   * @param tezTrapEvent event holding the tez events.
   */
  @Override
  public final void handle(final TezTrapEvent tezTrapEvent) {
    Preconditions.checkArgument(tezTrapEvent.getTezEvents() != null);
    List<TezEvent> tezEvents = new ArrayList<TezEvent>(
        tezTrapEvent.getTezEvents().size());
    for (TezEvent tezEvent : tezTrapEvent.getTezEvents()) {
      switch (tezEvent.getEventType()) {
        case COMPOSITE_DATA_MOVEMENT_EVENT:
        case DATA_MOVEMENT_EVENT:
          String errorMsg = "Some events won't be sent to the AM because all"
              + " the events should have been sent at this point. Most likely"
              + " this would result in a bug. "
              + " event:" + tezEvent.toString();
          Throwable throwable = new Throwable(errorMsg);
          LOG.error(errorMsg, throwable);
          break;
        default:
          LOG.info("Event of type " + tezEvent.getEventType() + " will be sent"
              + " to the AM after the task was closed ");
          tezEvents.add(tezEvent);
      }
    }
    tezUmbilical.addEvents(tezEvents);
  }
}
