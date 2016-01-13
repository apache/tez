/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.serviceplugins.api;

import java.util.List;

import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TezEvent;

// TODO TEZ-2003 (post) TEZ-2665. Move to the tez-api module
public class TaskHeartbeatRequest {

  // TODO TEZ-2003 (post) TEZ-2666 Ideally containerIdentifier should not be part of the request.
  private final String containerIdentifier;
  private final TezTaskAttemptID taskAttemptId;
  private final List<TezEvent> events;
  private final int startIndex;
  private final int preRoutedStartIndex;
  private final int maxEvents;


  public TaskHeartbeatRequest(String containerIdentifier, TezTaskAttemptID taskAttemptId, List<TezEvent> events, int startIndex,
                              int preRoutedStartIndex,
                              int maxEvents) {
    this.containerIdentifier = containerIdentifier;
    this.taskAttemptId = taskAttemptId;
    this.events = events;
    this.startIndex = startIndex;
    this.preRoutedStartIndex = preRoutedStartIndex;
    this.maxEvents = maxEvents;
  }

  public String getContainerIdentifier() {
    return containerIdentifier;
  }

  public TezTaskAttemptID getTaskAttemptId() {
    return taskAttemptId;
  }

  public List<TezEvent> getEvents() {
    return events;
  }

  public int getStartIndex() {
    return startIndex;
  }

  public int getPreRoutedStartIndex() {
    return preRoutedStartIndex;
  }

  public int getMaxEvents() {
    return maxEvents;
  }
}
