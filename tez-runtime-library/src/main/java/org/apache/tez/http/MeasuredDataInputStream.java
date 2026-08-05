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
package org.apache.tez.http;

import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

public class MeasuredDataInputStream extends DataInputStream {

  private final MeasuredInputStream measuredIn;

  private MeasuredDataInputStream(MeasuredInputStream measuredIn) {
    super(measuredIn);
    this.measuredIn = measuredIn;
  }

  public MeasuredDataInputStream(InputStream in) {
    this(new MeasuredInputStream(in));
  }

  public long getElapsedTimeMs() {
    return measuredIn.getElapsedTimeMs();
  }

  private static class MeasuredInputStream extends FilterInputStream {
    private long elapsedTimeNanos = 0;

    public MeasuredInputStream(InputStream in) {
      super(in);
    }

    @Override
    public int read() throws IOException {
      long start = System.nanoTime();
      int ret = super.read();
      elapsedTimeNanos += (System.nanoTime() - start);
      return ret;
    }

    @Override
    public int read(byte[] b) throws IOException {
      long start = System.nanoTime();
      int ret = super.read(b);
      elapsedTimeNanos += (System.nanoTime() - start);
      return ret;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      long start = System.nanoTime();
      int ret = super.read(b, off, len);
      elapsedTimeNanos += (System.nanoTime() - start);
      return ret;
    }

    public long getElapsedTimeMs() {
      return TimeUnit.NANOSECONDS.toMillis(elapsedTimeNanos);
    }
  }
}
