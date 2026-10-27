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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

public class TestNonSyncBufferedOutputStream {

  private static final int SIZE = 64;

  /** Records the size of every write that reaches the underlying stream. */
  private static final class RecordingStream extends OutputStream {
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final List<Integer> writes = new ArrayList<>();

    @Override
    public void write(int b) {
      bytes.write(b);
      writes.add(1);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      bytes.write(b, off, len);
      writes.add(len);
    }
  }

  private static byte[] pattern(int len, int seed) {
    byte[] b = new byte[len];
    for (int i = 0; i < len; i++) {
      b[i] = (byte) (seed + i);
    }
    return b;
  }

  @Test
  public void testWriteOfExactlyBufferSizeBypassesTheBuffer() throws IOException {
    RecordingStream sink = new RecordingStream();
    byte[] data = pattern(SIZE, 1);
    try (NonSyncBufferedOutputStream out = new NonSyncBufferedOutputStream(sink, SIZE)) {
      out.write(data, 0, data.length);
      // len >= buffer: handed to the underlying stream as it is, before any flush
      assertEquals(List.of(SIZE), sink.writes);
    }
    assertArrayEquals(data, sink.bytes.toByteArray());
  }

  @Test
  public void testWriteLargerThanTheBufferBypassesTheBuffer() throws IOException {
    RecordingStream sink = new RecordingStream();
    byte[] data = pattern(SIZE * 3 + 7, 2);
    try (NonSyncBufferedOutputStream out = new NonSyncBufferedOutputStream(sink, SIZE)) {
      out.write(data, 0, data.length);
      assertEquals(List.of(data.length), sink.writes);
    }
    assertArrayEquals(data, sink.bytes.toByteArray());
  }

  @Test
  public void testWritesThatCrossTheBufferAreDrainedOnce() throws IOException {
    RecordingStream sink = new RecordingStream();
    byte[] first = pattern(SIZE - 1, 3);
    byte[] second = pattern(2, 4);
    try (NonSyncBufferedOutputStream out = new NonSyncBufferedOutputStream(sink, SIZE)) {
      out.write(first, 0, first.length);
      assertEquals(List.of(), sink.writes, "the first write fits, so nothing reaches the stream");
      out.write(second, 0, second.length);
      // does not fit in what is left: the buffer is drained, then the new bytes are kept
      assertEquals(List.of(SIZE - 1), sink.writes);
      out.flush();
      assertEquals(List.of(SIZE - 1, 2), sink.writes);
    }
    byte[] expected = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, expected, first.length, second.length);
    assertArrayEquals(expected, sink.bytes.toByteArray());
  }

  @Test
  public void testSingleByteWritesFillAndDrainTheBuffer() throws IOException {
    RecordingStream sink = new RecordingStream();
    byte[] data = pattern(SIZE + 1, 5);
    try (NonSyncBufferedOutputStream out = new NonSyncBufferedOutputStream(sink, SIZE)) {
      for (byte b : data) {
        out.write(b);
      }
      // the byte after the buffer is full drains the full buffer first
      assertEquals(List.of(SIZE), sink.writes);
    }
    assertArrayEquals(data, sink.bytes.toByteArray());
  }

  @Test
  public void testCloseDrainsTheBuffer() throws IOException {
    RecordingStream sink = new RecordingStream();
    byte[] data = pattern(3, 6);
    NonSyncBufferedOutputStream out = new NonSyncBufferedOutputStream(sink, SIZE);
    out.write(data, 0, data.length);
    assertEquals(List.of(), sink.writes);
    out.close();
    assertArrayEquals(data, sink.bytes.toByteArray());
  }

  @Test
  public void testNonPositiveBufferSizeIsRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> new NonSyncBufferedOutputStream(new ByteArrayOutputStream(), 0));
  }
}
