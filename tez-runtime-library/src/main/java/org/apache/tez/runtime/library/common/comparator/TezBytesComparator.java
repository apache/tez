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
package org.apache.tez.runtime.library.common.comparator;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;

@Public
@Unstable
public class TezBytesComparator extends WritableComparator implements
    ProxyComparator<BytesWritable> {

  public TezBytesComparator() {
    super(BytesWritable.class);
  }

  /**
   * Compare the buffers in serialized form.
   */
  @Override
  public int compare(byte[] b1, int s1, int l1,
      byte[] b2, int s2, int l2) {
    return compareBytes(b1, s1, l1, b2, s2, l2);
  }

  @Override
  public int getProxy(BytesWritable key) {
    int prefix = 0;
    final int len = key.getLength();
    final byte[] content = key.getBytes();
    int b1 = 0, b2 = 0, b3 = 0;
    switch (len) {
    default:
    case 3:
      b3 = content[2] & 0xff;
    case 2:
      b2 = content[1] & 0xff;
    case 1:
      b1 = content[0] & 0xff;
    case 0:
    }
    prefix = (b1 << 16) | (b2 << 8) | (b3);
    return prefix;
  }

}
