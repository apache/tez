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

package org.apache.tez.runtime.library.output;

import org.apache.tez.runtime.api.LogicalOutput;

/**
 * <code>OnFileUnorderedPartitionedKVOutput</code> is a {@link LogicalOutput}
 * which can be used to write Key-Value pairs. The key-value pairs are written
 * to the correct partition based on the configured Partitioner.
 * 
 * This currently acts as a usable placeholder for writing unordered output (the data is sorted,
 * which should be functionally correct since there's no guarantees on order without a sort).
 * TEZ-661 to add a proper implementation.
 * 
 */
public class OnFileUnorderedPartitionedKVOutput extends OnFileSortedOutput {

}