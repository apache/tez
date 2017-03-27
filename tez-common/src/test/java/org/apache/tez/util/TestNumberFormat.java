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

import org.junit.Assert;
import org.junit.Test;

import java.text.NumberFormat;

public class TestNumberFormat {

  @Test(timeout = 1000)
  public void testLongWithPadding() throws Exception {
    FastNumberFormat fastNumberFormat = FastNumberFormat.getInstance();
    fastNumberFormat.setMinimumIntegerDigits(6);
    NumberFormat numberFormat = NumberFormat.getInstance();
    numberFormat.setGroupingUsed(false);
    numberFormat.setMinimumIntegerDigits(6);
    long[] testLongs = {1, 23, 456, 7890, 12345, 678901, 2345689, 0, -0, -1, -23, -456, -7890, -12345, -678901, -2345689};
    for (long l: testLongs) {
      Assert.assertEquals("Number formats should be equal", numberFormat.format(l), fastNumberFormat.format(l));
    }
  }
}
