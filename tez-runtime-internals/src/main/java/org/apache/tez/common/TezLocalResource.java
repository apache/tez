/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.io.Writable;

public class TezLocalResource implements Writable {

  private URI uri;
  private long size;
  private long timestamp;

  public TezLocalResource() {
  }
  
  public TezLocalResource(URI uri, long size, long timestamp) {
    this.uri = uri;
    this.size = size;
    this.timestamp = timestamp;
  }

  public URI getUri() {
    return uri;
  }

  public long getSize() {
    return size;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(uri.toString());
    out.writeLong(size);
    out.writeLong(timestamp);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    try {
      this.uri = new URI(in.readUTF());
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    this.size = in.readLong();
    this.timestamp = in.readLong();
  }

  @Override
  public String toString() {
    return "TezLocalResource [uri=" + uri + ", size=" + size + ", timestamp=" + timestamp + "]";
  }
}
