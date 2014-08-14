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

package org.apache.tez.dag.api;

import org.apache.hadoop.classification.InterfaceAudience.Public;

/**
 * Exception thrown when the client cannot communicate with the Tez Session
 * as the Tez Session is no longer running.
 */
@Public
public class SessionNotRunning extends TezException {

  private static final long serialVersionUID = -287996170505550316L;

  public SessionNotRunning(String message, Throwable cause) {
    super(message, cause);
  }

  public SessionNotRunning(String message) {
    super(message);
  }

  public SessionNotRunning(Throwable cause) {
    super(cause);
  }

}
