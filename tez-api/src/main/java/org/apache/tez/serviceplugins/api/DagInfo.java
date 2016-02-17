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

public interface DagInfo {

  /**
   * The index of the current dag
   * @return a numerical identifier for the DAG. This is unique within the currently running application.
   */
  int getIndex();

  /**
   * Get the name of the dag
   * @return the name of the dag
   */
  String getName();
}
