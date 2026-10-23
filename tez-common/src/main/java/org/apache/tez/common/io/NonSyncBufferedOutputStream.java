/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.common.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A thread-not-safe version of BufferedOutputStream, which removes all
 * synchronized modifiers. Use it where a stream is confined to one thread and
 * the writes are small and frequent: the lock is a measurable share of the cost
 * when the work per call is tiny.
 */
public class NonSyncBufferedOutputStream extends FilterOutputStream {
  private final byte[] buf;
  private int count;

  public NonSyncBufferedOutputStream(OutputStream stream, int size) {
    super(stream);
    if (size <= 0) {
      throw new IllegalArgumentException("Buffer size <= 0");
    }
    this.buf = new byte[size];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(int b) throws IOException {
    if (count == buf.length) {
      drain();
    }
    buf[count++] = (byte) b;
  }

  /**
   * {@inheritDoc}
   *
   * <p>A write at least as large as the buffer goes straight to the underlying
   * stream, so a large record is not copied twice.
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (len >= buf.length) {
      drain();
      out.write(b, off, len);
      return;
    }
    if (len > buf.length - count) {
      drain();
    }
    System.arraycopy(b, off, buf, count, len);
    count += len;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() throws IOException {
    drain();
    out.flush();
  }

  private void drain() throws IOException {
    if (count > 0) {
      out.write(buf, 0, count);
      count = 0;
    }
  }
}
