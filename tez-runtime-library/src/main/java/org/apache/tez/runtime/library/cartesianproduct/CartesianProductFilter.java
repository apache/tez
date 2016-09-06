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
package org.apache.tez.runtime.library.cartesianproduct;

import org.apache.tez.dag.api.UserPayload;

import java.util.Map;

import static org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * User can extend this base class and override <method>isValidCombination</method> to implement
 * custom filter
 */
@Evolving
public abstract class CartesianProductFilter {
  private UserPayload userPayload;

  public CartesianProductFilter(UserPayload payload) {
    this.userPayload = payload;
  }

  /**
   * @param vertexPartitionMap the map from vertex name to partition id
   * @return whether this combination of partitions is valid
   */
  public abstract boolean isValidCombination(Map<String, Integer> vertexPartitionMap);

  public UserPayload getUserPayload() {
    return userPayload;
  }
}
