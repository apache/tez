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

package org.apache.tez.runtime.library.input;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.RawComparator;
import org.apache.tez.runtime.InputReadyTracker;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.MergedLogicalInput;
import org.apache.tez.runtime.api.MergedInputContext;
import org.apache.tez.runtime.api.impl.TezMergedInputContextImpl;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.junit.Test;

public class TestSortedGroupedMergedInput {

  MergedInputContext createMergedInputContext() {
    return new TezMergedInputContextImpl(null, "mergedInputName", new HashMap<String, MergedLogicalInput>(),
        mock(InputReadyTracker.class), null);
  }
  
  @Test(timeout = 5000)
  public void testSimple() throws Exception {
    SortedTestKeyValuesReader kvsReader1 = new SortedTestKeyValuesReader(new int[] { 1, 2, 3 },
        new int[][] { { 1, 1 }, { 2, 2 }, { 3, 3 } });

    SortedTestKeyValuesReader kvsReader2 = new SortedTestKeyValuesReader(new int[] { 1, 2, 3 },
        new int[][] { { 1, 1 }, { 2, 2 }, { 3, 3 } });

    SortedTestKeyValuesReader kvsReader3 = new SortedTestKeyValuesReader(new int[] { 1, 2, 3 },
        new int[][] { { 1, 1 }, { 2, 2 }, { 3, 3 } });

    SortedTestInput sInput1 = new SortedTestInput(kvsReader1);
    SortedTestInput sInput2 = new SortedTestInput(kvsReader2);
    SortedTestInput sInput3 = new SortedTestInput(kvsReader3);

    List<Input> sInputs = new LinkedList<Input>();
    sInputs.add(sInput1);
    sInputs.add(sInput2);
    sInputs.add(sInput3);
    OrderedGroupedMergedKVInput input = new OrderedGroupedMergedKVInput(createMergedInputContext(), sInputs);

    KeyValuesReader kvsReader = input.getReader();
    int keyCount = 0;
    while (kvsReader.next()) {
      keyCount++;
      Integer key = (Integer) kvsReader.getCurrentKey();
      assertEquals(Integer.valueOf(keyCount), key);
      Iterator<Object> valuesIter = kvsReader.getCurrentValues().iterator();
      int valCount = 0;
      while (valuesIter.hasNext()) {
        valCount++;
        Integer val = (Integer) valuesIter.next();
        assertEquals(Integer.valueOf(keyCount), val);
      }
      assertEquals(6, valCount);
    }
  }

  @Test(timeout = 5000)
  public void testSkippedKey() throws Exception {


    SortedTestKeyValuesReader kvsReader1 = new SortedTestKeyValuesReader(new int[] { 1, 2, 3 },
        new int[][] { { 1, 1 }, { 2, 2 }, { 3, 3 } });

    SortedTestKeyValuesReader kvsReader2 = new SortedTestKeyValuesReader(new int[] { 1, 2, 3 },
        new int[][] { { 1, 1 }, { 2, 2 }, { 3, 3 } });

    SortedTestKeyValuesReader kvsReader3 = new SortedTestKeyValuesReader(new int[] { 1, 2, 3 },
        new int[][] { { 1, 1 }, { 2, 2 }, { 3, 3 } });

    SortedTestInput sInput1 = new SortedTestInput(kvsReader1);
    SortedTestInput sInput2 = new SortedTestInput(kvsReader2);
    SortedTestInput sInput3 = new SortedTestInput(kvsReader3);

    List<Input> sInputs = new LinkedList<Input>();
    sInputs.add(sInput1);
    sInputs.add(sInput2);
    sInputs.add(sInput3);

    OrderedGroupedMergedKVInput input = new OrderedGroupedMergedKVInput(createMergedInputContext(), sInputs);

    KeyValuesReader kvsReader = input.getReader();
    int keyCount = 0;
    while (kvsReader.next()) {
      keyCount++;
      if (keyCount == 2) {
        continue;
      }
      Integer key = (Integer) kvsReader.getCurrentKey();
      assertEquals(Integer.valueOf(keyCount), key);
      Iterator<Object> valuesIter = kvsReader.getCurrentValues().iterator();
      int valCount = 0;
      while (valuesIter.hasNext()) {
        valCount++;
        Integer val = (Integer) valuesIter.next();
        assertEquals(Integer.valueOf(keyCount), val);
      }
      assertEquals(6, valCount);
    }
  }

  @Test(timeout = 5000)
  public void testPartialValuesSkip() throws Exception {

    SortedTestKeyValuesReader kvsReader1 = new SortedTestKeyValuesReader(new int[] { 1, 2, 3 },
        new int[][] { { 1, 1 }, { 2, 2 }, { 3, 3 } });

    SortedTestKeyValuesReader kvsReader2 = new SortedTestKeyValuesReader(new int[] { 1, 2, 3 },
        new int[][] { { 1, 1 }, { 2, 2 }, { 3, 3 } });

    SortedTestKeyValuesReader kvsReader3 = new SortedTestKeyValuesReader(new int[] { 1, 2, 3 },
        new int[][] { { 1, 1 }, { 2, 2 }, { 3, 3 } });

    SortedTestInput sInput1 = new SortedTestInput(kvsReader1);
    SortedTestInput sInput2 = new SortedTestInput(kvsReader2);
    SortedTestInput sInput3 = new SortedTestInput(kvsReader3);

    List<Input> sInputs = new LinkedList<Input>();
    sInputs.add(sInput1);
    sInputs.add(sInput2);
    sInputs.add(sInput3);

    OrderedGroupedMergedKVInput input = new OrderedGroupedMergedKVInput(createMergedInputContext(), sInputs);
    KeyValuesReader kvsReader = input.getReader();
    int keyCount = 0;
    while (kvsReader.next()) {
      keyCount++;
      Integer key = (Integer) kvsReader.getCurrentKey();
      assertEquals(Integer.valueOf(keyCount), key);
      Iterator<Object> valuesIter = kvsReader.getCurrentValues().iterator();
      int valCount = 0;
      while (valuesIter.hasNext()) {
        valCount++;
        if (keyCount == 2 && valCount == 3) {
          break;
        }
        Integer val = (Integer) valuesIter.next();
        assertEquals(Integer.valueOf(keyCount), val);
      }
      if (keyCount == 2) {
        assertEquals(3, valCount);
      } else {
        assertEquals(6, valCount);
      }
    }
  }

  @Test(timeout = 5000)
  public void testOrdering() throws Exception {

    SortedTestKeyValuesReader kvsReader1 = new SortedTestKeyValuesReader(new int[] { 2, 4 },
        new int[][] { { 2, 2 }, { 4, 4 } });

    SortedTestKeyValuesReader kvsReader2 = new SortedTestKeyValuesReader(new int[] { 1, 2, 3 },
        new int[][] { { 1, 1 }, { 2, 2 }, { 3, 3 } });

    SortedTestKeyValuesReader kvsReader3 = new SortedTestKeyValuesReader(new int[] { 4, 5, 6, 7 },
        new int[][] { { 4, 4 }, { 5, 5 }, { 6, 6 }, { 7, 7 } });

    SortedTestInput sInput1 = new SortedTestInput(kvsReader1);
    SortedTestInput sInput2 = new SortedTestInput(kvsReader2);
    SortedTestInput sInput3 = new SortedTestInput(kvsReader3);

    List<Input> sInputs = new LinkedList<Input>();
    sInputs.add(sInput1);
    sInputs.add(sInput2);
    sInputs.add(sInput3);

    OrderedGroupedMergedKVInput input = new OrderedGroupedMergedKVInput(createMergedInputContext(), sInputs);
    KeyValuesReader kvsReader = input.getReader();
    int keyCount = 0;
    while (kvsReader.next()) {
      keyCount++;
      Integer key = (Integer) kvsReader.getCurrentKey();
      assertEquals(Integer.valueOf(keyCount), key);
      Iterator<Object> valuesIter = kvsReader.getCurrentValues().iterator();
      int valCount = 0;
      while (valuesIter.hasNext()) {
        valCount++;
        Integer val = (Integer) valuesIter.next();
        assertEquals(Integer.valueOf(keyCount), val);
      }
      if (keyCount == 1) {
        assertEquals(2, valCount);
      } else if (keyCount == 2) {
        assertEquals(4, valCount);
      } else if (keyCount == 3) {
        assertEquals(2, valCount);
      } else if (keyCount == 4) {
        assertEquals(4, valCount);
      } else if (keyCount == 5 || keyCount == 6 || keyCount == 7) {
        assertEquals(2, valCount);
      } else {
        fail("Unexpected key");
      }
    }
  }

  @Test(timeout = 5000)
  public void testSkippedKey2() throws Exception {

    SortedTestKeyValuesReader kvsReader1 = new SortedTestKeyValuesReader(new int[] { 2, 4 },
        new int[][] { { 2, 2 }, { 4, 4 } });

    SortedTestKeyValuesReader kvsReader2 = new SortedTestKeyValuesReader(new int[] { 1, 2, 3 },
        new int[][] { { 1, 1 }, { 2, 2 }, { 3, 3 } });

    SortedTestKeyValuesReader kvsReader3 = new SortedTestKeyValuesReader(new int[] { 4, 5, 6, 7 },
        new int[][] { { 4, 4 }, { 5, 5 }, { 6, 6 }, { 7, 7 } });

    SortedTestInput sInput1 = new SortedTestInput(kvsReader1);
    SortedTestInput sInput2 = new SortedTestInput(kvsReader2);
    SortedTestInput sInput3 = new SortedTestInput(kvsReader3);

    List<Input> sInputs = new LinkedList<Input>();
    sInputs.add(sInput1);
    sInputs.add(sInput2);
    sInputs.add(sInput3);

    OrderedGroupedMergedKVInput input = new OrderedGroupedMergedKVInput(createMergedInputContext(), sInputs);
    KeyValuesReader kvsReader = input.getReader();
    int keyCount = 0;
    while (kvsReader.next()) {
      keyCount++;
      if (keyCount == 4) {
        continue;
      }
      Integer key = (Integer) kvsReader.getCurrentKey();
      assertEquals(Integer.valueOf(keyCount), key);
      Iterator<Object> valuesIter = kvsReader.getCurrentValues().iterator();
      int valCount = 0;
      while (valuesIter.hasNext()) {
        valCount++;
        Integer val = (Integer) valuesIter.next();
        assertEquals(Integer.valueOf(keyCount), val);
      }
      if (keyCount == 1) {
        assertEquals(2, valCount);
      } else if (keyCount == 2) {
        assertEquals(4, valCount);
      } else if (keyCount == 3) {
        assertEquals(2, valCount);
      } else if (keyCount == 4) {
        fail("Key 4 should have been skipped");
      } else if (keyCount == 5 || keyCount == 6 || keyCount == 7) {
        assertEquals(2, valCount);
      } else {
        fail("Unexpected key");
      }
    }
  }
  
  // Reads all values for a key, but doesn't trigger the last hasNext() call.
  @Test(timeout = 5000)
  public void testSkippedKey3() throws Exception {

    SortedTestKeyValuesReader kvsReader1 = new SortedTestKeyValuesReader(new int[] { 1, 2, 3, 4 },
        new int[][] { { 1, 1 }, { 2, 2 }, { 3, 3 }, {4, 4} });

    SortedTestKeyValuesReader kvsReader2 = new SortedTestKeyValuesReader(new int[] { 1, 2, 3, 4 },
        new int[][] { { 1, 1 }, { 2, 2 }, { 3, 3 }, {4, 4} });

    SortedTestKeyValuesReader kvsReader3 = new SortedTestKeyValuesReader(new int[] { 1, 2, 3, 4 },
        new int[][] { { 1, 1 }, { 2, 2 }, { 3, 3 }, {4, 4} });

    SortedTestInput sInput1 = new SortedTestInput(kvsReader1);
    SortedTestInput sInput2 = new SortedTestInput(kvsReader2);
    SortedTestInput sInput3 = new SortedTestInput(kvsReader3);

    List<Input> sInputs = new LinkedList<Input>();
    sInputs.add(sInput1);
    sInputs.add(sInput2);
    sInputs.add(sInput3);

    OrderedGroupedMergedKVInput input = new OrderedGroupedMergedKVInput(createMergedInputContext(), sInputs);

    KeyValuesReader kvsReader = input.getReader();
    int keyCount = 0;
    while (kvsReader.next()) {
      keyCount++;
      if (keyCount == 2 || keyCount == 3) {
        continue;
      }
      Integer key = (Integer) kvsReader.getCurrentKey();
      assertEquals(Integer.valueOf(keyCount), key);
      Iterator<Object> valuesIter = kvsReader.getCurrentValues().iterator();
      int valCount = 0;
      while (valuesIter.hasNext()) {
        valCount++;
        Integer val = (Integer) valuesIter.next();
        assertEquals(Integer.valueOf(keyCount), val);
        if (keyCount == 1 && valCount == 6) { // Avoid last hasNext on iterator
          break;
        }
      }
      assertEquals(6, valCount);
    }
  }

  @Test(timeout = 5000)
  public void testEmptySources() throws Exception {

    SortedTestKeyValuesReader kvsReader1 = new SortedTestKeyValuesReader(new int[] {},
        new int[][] {});

    SortedTestKeyValuesReader kvsReader2 = new SortedTestKeyValuesReader(new int[] {},
        new int[][] {});

    SortedTestKeyValuesReader kvsReader3 = new SortedTestKeyValuesReader(new int[] {},
        new int[][] {});

    SortedTestInput sInput1 = new SortedTestInput(kvsReader1);
    SortedTestInput sInput2 = new SortedTestInput(kvsReader2);
    SortedTestInput sInput3 = new SortedTestInput(kvsReader3);

    List<Input> sInputs = new LinkedList<Input>();
    sInputs.add(sInput1);
    sInputs.add(sInput2);
    sInputs.add(sInput3);

    OrderedGroupedMergedKVInput input = new OrderedGroupedMergedKVInput(createMergedInputContext(), sInputs);

    KeyValuesReader kvsReader = input.getReader();
    assertFalse(kvsReader.next());
  }

  private static class SortedTestInput extends OrderedGroupedKVInput {

    final SortedTestKeyValuesReader reader;

    SortedTestInput(SortedTestKeyValuesReader reader) {
      super(null, 0);
      this.reader = reader;
    }

    @Override
    public List<Event> initialize() throws IOException {
      return null;
    }

    @Override
    public void start() throws IOException {
    }

    @Override
    public KeyValuesReader getReader() throws IOException {
      return reader;
    }

    @Override
    public void handleEvents(List<Event> inputEvents) {
    }

    @Override
    public List<Event> close() throws IOException {
      return null;
    }

    @SuppressWarnings("rawtypes")
    public RawComparator getInputKeyComparator() {
      return new RawComparatorForTest();
    }
  }

  private static class SortedTestKeyValuesReader extends KeyValuesReader {

    final int[] keys;
    final int[][] values;
    int currentIndex = -1;

    SortedTestKeyValuesReader(int[] keys, int[][] vals) {
      this.keys = keys;
      this.values = vals;
    }

    @Override
    public boolean next() throws IOException {
      currentIndex++;
      if (keys == null || currentIndex >= keys.length) {
        return false;
      }
      return true;
    }

    @Override
    public Object getCurrentKey() throws IOException {
      return keys[currentIndex];
    }

    @Override
    public Iterable<Object> getCurrentValues() throws IOException {
      List<Object> ints = new LinkedList<Object>();
      for (int i = 0; i < values[currentIndex].length; i++) {
        ints.add(Integer.valueOf(values[currentIndex][i]));
      }
      return ints;
    }
  }

  private static class RawComparatorForTest implements RawComparator<Integer> {

    @Override
    public int compare(Integer o1, Integer o2) {
      return o1 - o2;
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      throw new UnsupportedOperationException();
    }
  }

}
