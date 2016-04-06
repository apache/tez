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

package org.apache.tez.runtime.internals.api;

import java.io.IOException;
import java.util.Collection;

import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.RuntimeTask;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.task.ErrorReporter;

public interface TaskReporterInterface {

  void registerTask(RuntimeTask task, ErrorReporter errorReporter);

  void unregisterTask(TezTaskAttemptID taskAttemptId);

  boolean taskSucceeded(TezTaskAttemptID taskAttemptId) throws IOException, TezException;

  boolean taskFailed(TezTaskAttemptID taskAttemptId,
                              TaskFailureType taskFailureType,
                              Throwable cause,
                              String diagnostics, EventMetaData srcMeta) throws IOException,
      TezException;

  boolean taskKilled(TezTaskAttemptID taskAttemtpId, Throwable cause, String diagnostics,
                     EventMetaData srcMeta) throws IOException, TezException;

  void addEvents(TezTaskAttemptID taskAttemptId, Collection<TezEvent> events);

  boolean canCommit(TezTaskAttemptID taskAttemptId) throws IOException;

  void shutdown();

}