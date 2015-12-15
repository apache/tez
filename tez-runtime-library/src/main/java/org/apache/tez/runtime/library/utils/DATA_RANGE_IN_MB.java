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

package org.apache.tez.runtime.library.utils;


public enum DATA_RANGE_IN_MB {
  THOUSAND(1000), HUNDRED(100), TEN(10), ONE(1), ZERO(0);

  private final int sizeInMB;

  private DATA_RANGE_IN_MB(int sizeInMB) {
    this.sizeInMB = sizeInMB;
  }

  public final int getSizeInMB() {
    return sizeInMB;
  }

  static long ceil(long a, long b) {
    return (a + (b - 1)) / b;
  }

  public static final DATA_RANGE_IN_MB getRange(long sizeInBytes) {
    long sizeInMB = ceil(sizeInBytes, (1024l * 1024l));
    for (DATA_RANGE_IN_MB range : values()) {
      if (sizeInMB >= range.sizeInMB) {
        return range;
      }
    }
    return ZERO;
  }
}
