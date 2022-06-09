/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.api;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.yarn.event.EventHandler;

/**
 * Context handle for the Output to initialize itself.
 * This interface is not supposed to be implemented by users
 */
@Public
public interface OutputContext extends TaskContext {

  /**
   * Get the Vertex Name of the Destination that is the recipient of this
   * Output's data
   * @return Name of the Destination Vertex
   */
  String getDestinationVertexName();

  /**
   * Returns a convenient, human-readable string describing the input and output vertices.
   * @return the convenient string
   */
  String getInputOutputVertexNames();

  /**
   * Get the index of the output in the set of all outputs for the task. The 
   * index will be consistent and valid only among the tasks of this vertex.
   * @return index
   */
  int getOutputIndex();

  /**
   * Get an {@link OutputStatisticsReporter} for this {@link Output} that can
   * be used to report statistics like data size
   * @return {@link OutputStatisticsReporter}
   */
  OutputStatisticsReporter getStatisticsReporter();

  /**
   * Notify the context that at this point no more events should be sent.
   * This is used as a safety measure to prevent events being sent after close
   * or in cleanup. After this is called events being queued to be sent to the
   * AM will instead be passed to the event handler.
   * @param eventHandler should handle the events after the call.
   */
  void trapEvents(EventHandler eventHandler);
}
