/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.http;

import java.io.DataInputStream;
import java.io.IOException;

public abstract class BaseHttpConnection {
  /**
   * Basic/unit connection timeout (in milliseconds)
   */
  protected final static int UNIT_CONNECT_TIMEOUT = 60 * 1000;

  /**
   * Connect to url
   *
   * @return boolean
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract boolean connect() throws IOException, InterruptedException;

  /**
   * Validate established connection
   *
   * @throws IOException
   */
  public abstract void validate() throws IOException;

  /**
   * Get inputstream
   *
   * @return DataInputStream
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract DataInputStream getInputStream() throws IOException, InterruptedException;

  /**
   * Clean up connection
   *
   * @param disconnect
   * @throws IOException
   */
  public abstract void cleanup(boolean disconnect) throws IOException;

}
