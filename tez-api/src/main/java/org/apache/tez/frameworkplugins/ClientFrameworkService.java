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
package org.apache.tez.frameworkplugins;


import org.apache.tez.client.FrameworkClient;

/**
 * A {@code FrameworkService} that runs within the client process using {@code TezClient}.
 *
 * <p>This service bundles together a compatible {@code FrameworkClient} and
 * {@code AMRegistryClient} to enable communication and coordination with the
 * Application Master.</p>
 *
 * <p>Implementations must provide a {@link FrameworkClient} instance that will
 * be used by the Tez client layer.</p>
 */
public interface ClientFrameworkService extends FrameworkService {

  /**
   * Create a new {@link FrameworkClient} instance used by the client-side
   * Tez runtime.
   *
   * @return a new {@code FrameworkClient} instance
   */
  FrameworkClient newFrameworkClient();
}
