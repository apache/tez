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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.io.RawComparator;

@Unstable
@Private
public interface ProxyComparator<KEY> extends RawComparator {
  /**
   * This comparator interface provides a fast-path for comparisons between keys.
   *
   * The implicit assumption is that integer returned from this function serves
   * as a transitive comparison proxy for the comparator that this implements.
   *
   * But this does not serve as a measure of equality.
   *
   * getProxy(k1) < getProxy(k2) implies k1 < k2 (transitive between different keys for sorting requirements)
   *
   * getProxy(k1) == getProxy(k2) does not imply ordering, but requires actual key comparisons.
   *
   * This serves as a way to short-circuit  the RawComparator speeds.
   *
   * @param key
   * @return proxy
   */
  int getProxy(KEY key);

}
