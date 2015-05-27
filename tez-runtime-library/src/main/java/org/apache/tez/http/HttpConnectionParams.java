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

public class HttpConnectionParams {
  private final boolean keepAlive;
  private final int keepAliveMaxConnections;
  private final int connectionTimeout;
  private final int readTimeout;
  private final int bufferSize;

  private final boolean sslShuffle;
  private final SSLFactory sslFactory;

  public HttpConnectionParams(boolean keepAlive, int keepAliveMaxConnections, int
      connectionTimeout, int readTimeout, int bufferSize, boolean sslShuffle, SSLFactory
      sslFactory) {
    this.keepAlive = keepAlive;
    this.keepAliveMaxConnections = keepAliveMaxConnections;
    this.connectionTimeout = connectionTimeout;
    this.readTimeout = readTimeout;
    this.bufferSize = bufferSize;
    this.sslShuffle = sslShuffle;
    this.sslFactory = sslFactory;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  public boolean isKeepAlive() {
    return keepAlive;
  }

  public int getKeepAliveMaxConnections() {
    return keepAliveMaxConnections;
  }

  public int getReadTimeout() {
    return readTimeout;
  }

  public boolean isSslShuffle() {
    return sslShuffle;
  }

  public SSLFactory getSslFactory() {
    return sslFactory;
  }


  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("keepAlive=").append(keepAlive).append(", ");
    sb.append("keepAliveMaxConnections=").append(keepAliveMaxConnections).append(", ");
    sb.append("connectionTimeout=").append(connectionTimeout).append(", ");
    sb.append("readTimeout=").append(readTimeout).append(", ");
    sb.append("bufferSize=").append(bufferSize).append(", ");
    sb.append("bufferSize=").append(bufferSize);
    return sb.toString();
  }
}
