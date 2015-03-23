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

package org.apache.tez.mapreduce;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezUmbilical;

public class TestUmbilical implements TezUmbilical {

  private static final Logger LOG = LoggerFactory.getLogger(TestUmbilical.class);

  private final List<TezEvent> events = Lists.newLinkedList();

  public TestUmbilical() {
  }

  @Override
  public void addEvents(Collection<TezEvent> events) {
    if (events != null && events.size() > 0) {
      this.events.addAll(events);
      LOG.info("#Events Received: " + events.size());
      for (TezEvent event : events) {
        LOG.info("Event: " + event);
      }
    }
  }

  public List<TezEvent> getEvents() {
    return this.events;
  }

  @Override
  public void signalFatalError(TezTaskAttemptID taskAttemptID, Throwable t, 
      String message, EventMetaData sourceInfo) {
    LOG.info("Received fatal error from task: " + taskAttemptID
        + ", Message: " + message);

  }

  @Override
  public boolean canCommit(TezTaskAttemptID taskAttemptID) throws IOException {
    LOG.info("Got canCommit from task: " + taskAttemptID);
    return true;
  }

}
