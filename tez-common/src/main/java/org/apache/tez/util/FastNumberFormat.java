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

package org.apache.tez.util;

public class FastNumberFormat {

  public static final int MAX_COUNT = 19;
  private final char[] digits = new char[MAX_COUNT];
  private int minimumIntegerDigits;

  public static FastNumberFormat getInstance() {
    return new FastNumberFormat();
  }

  public void setMinimumIntegerDigits(int minimumIntegerDigits) {
    this.minimumIntegerDigits = minimumIntegerDigits;
  }

  public StringBuilder format(long source, StringBuilder sb) {
    int left = MAX_COUNT;
    if (source < 0) {
      sb.append('-');
      source = - source;
    }
    while (source > 0) {
      digits[--left] = (char)('0' + (source % 10));
      source /= 10;
    }
    while (MAX_COUNT - left < minimumIntegerDigits) {
      digits[--left] = '0';
    }
    sb.append(digits, left, MAX_COUNT - left);
    return sb;
  }

  public String format(long source) {
    return format(source, new StringBuilder()).toString();
  }
}
