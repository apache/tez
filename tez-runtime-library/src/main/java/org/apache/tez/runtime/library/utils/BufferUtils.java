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

package org.apache.tez.runtime.library.utils;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

@Private
public class BufferUtils {
  public static int compare(DataInputBuffer buf1, DataInputBuffer buf2) {
    byte[] b1 = buf1.getData();
    byte[] b2 = buf2.getData();
    int s1 = buf1.getPosition();
    int s2 = buf2.getPosition();
    int l1 = buf1.getLength();
    int l2 = buf2.getLength();
    return FastByteComparisons.compareTo(b1, s1, l1, b2, s2, l2);
  }

  public static int compare(DataOutputBuffer buf1, DataOutputBuffer buf2) {
    byte[] b1 = buf1.getData();
    byte[] b2 = buf2.getData();
    int s1 = 0;
    int s2 = 0;
    int l1 = buf1.getLength();
    int l2 = buf2.getLength();
    return FastByteComparisons.compareTo(b1, s1, l1, b2, s2, l2);
  }

  public static int compare(DataInputBuffer buf1, DataOutputBuffer buf2) {
    byte[] b1 = buf1.getData();
    byte[] b2 = buf2.getData();
    int s1 = buf1.getPosition();
    int s2 = 0;
    int l1 = buf1.getLength();
    int l2 = buf2.getLength();
    return FastByteComparisons.compareTo(b1, s1, (l1 - s1), b2, s2, l2);
  }

  public static int compare(DataOutputBuffer buf1, DataInputBuffer buf2) {
    return compare(buf2, buf1);
  }

  public static void copy(DataInputBuffer src, DataOutputBuffer dst) throws IOException {
    byte[] b1 = src.getData();
    int s1 = src.getPosition();
    int l1 = src.getLength();
    dst.reset();
    dst.write(b1, s1, l1 - s1);
  }

  public static void copy(DataOutputBuffer src, DataOutputBuffer dst) throws IOException {
    byte[] b1 = src.getData();
    int s1 = 0;
    int l1 = src.getLength();
    dst.reset();
    dst.write(b1, s1, l1);
  }

}
