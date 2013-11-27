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

package org.apache.tez.runtime.library.testutils;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class KVDataGen {

  public static List<KVPair> generateTestData(boolean repeat) {
    List<KVPair> data = new LinkedList<KVPair>();
    int repeatCount = 0;
    for (int i = 0; i < 5; i++) {
      Text key = new Text("key" + i);
      IntWritable value = new IntWritable(i + repeatCount);
      KVPair kvp = new KVPair(key, value);
      data.add(kvp);
      if (repeat && i == 2) { // Repeat this key
        repeatCount++;
        value.set(i + repeatCount);
        kvp = new KVPair(key, value);
        data.add(kvp);
      }
    }
    return data;
  }

  public static class KVPair {
    private Text key;
    private IntWritable value;

    public KVPair(Text key, IntWritable value) {
      this.key = key;
      this.value = value;
    }

    public Text getKey() {
      return this.key;
    }

    public IntWritable getvalue() {
      return this.value;
    }
  }
}
