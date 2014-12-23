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

import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestProxyComparator {
  private static final Log LOG = LogFactory.getLog(TestProxyComparator.class);

  final static String[] keys = {
    "",
    "A", "B",
    "AA", "BB", "BA", "CB",
    "AAA", "BBBB", "CCCCC",
    /* utf-8 comparisons */
    "\u00E6AAAA", "\u00F7", "A\u00F7", "\u00F7AAAAAAAAA",
    "\u00F7\u00F7", "\u00F7\u00F7\u00E6\u00E6A",
    "\u00F7\u00F7\u00E6\u00E6A"
  };

  private static final void set(BytesWritable bw, String s) {
    byte[] b = s.getBytes(Charset.forName("utf-8"));
    bw.set(b, 0, b.length);
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void cleanup() throws Exception {
  }

  @Test(timeout = 5000)
  public void testProxyComparator() {
    final ProxyComparator<BytesWritable> comparator = new TezBytesComparator();
    BytesWritable lhs = new BytesWritable();
    BytesWritable rhs = new BytesWritable();
    for (String l : keys) {
      for (String r : keys) {
        set(lhs, l);
        set(rhs, r);
        final int lproxy = comparator.getProxy(lhs);
        final int rproxy = comparator.getProxy(rhs);
        if (lproxy < rproxy) {
          assertTrue(String.format("(%s) %d < (%s) %d", l, lproxy, r, rproxy),
              comparator.compare(lhs, rhs) < 0);
        }
        if (lproxy > rproxy) {
          assertTrue(String.format("(%s) %d > (%s) %d", l, lproxy, r, rproxy),
              comparator.compare(lhs, rhs) > 0);
        }
      }
    }
  }
}
