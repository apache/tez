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
package org.apache.tez.common.io;

import java.io.ByteArrayInputStream;

/**
 * A thread-not-safe version of ByteArrayInputStream, which removes all
 * synchronized modifiers.
 */
public class NonSyncByteArrayInputStream extends ByteArrayInputStream {
  public NonSyncByteArrayInputStream(byte[] bs) {
    super(bs);
  }

  public NonSyncByteArrayInputStream(byte[] buf, int offset, int length) {
    super(buf, offset, length);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int read() {
    return (pos < count) ? (buf[pos++] & 0xff) : -1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int read(byte b[], int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }

    if (pos >= count) {
      return -1;
    }

    int avail = count - pos;
    if (len > avail) {
      len = avail;
    }
    if (len <= 0) {
      return 0;
    }
    System.arraycopy(buf, pos, b, off, len);
    pos += len;
    return len;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long skip(long n) {
    long k = count - pos;
    if (n < k) {
      k = n < 0 ? 0 : n;
    }

    pos += k;
    return k;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int available() {
    return count - pos;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset() {
    pos = mark;
  }
}
