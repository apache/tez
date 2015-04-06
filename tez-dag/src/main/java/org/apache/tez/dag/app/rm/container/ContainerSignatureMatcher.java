/* Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tez.dag.app.rm.container;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.LocalResource;

public interface ContainerSignatureMatcher {
  /**
   * Checks the compatibility between the specified container signatures.
   *
   * @return true if the first signature is a super set of the second
   *         signature.
   */
  public boolean isSuperSet(Object cs1, Object cs2);
  
  /**
   * Checks if the container signatures match exactly
   * @return true if exact match
   */
  public boolean isExactMatch(Object cs1, Object cs2);
  
  /**
   * Gets additional resources specified in lr2, which are not present for lr1
   * 
   * @param lr1
   * @param lr2
   * @return additional resources specified in lr2, which are not present for lr1
   */
  public Map<String, LocalResource> getAdditionalResources(Map<String, LocalResource> lr1,
      Map<String, LocalResource> lr2);


  /**
   * Do a union of 2 signatures
   * Pre-condition. This function should only be invoked iff cs1 is compatible with cs2.
   * i.e. isSuperSet should not return false.
   * @param cs1 Signature 1 Original signature
   * @param cs2 Signature 2 New signature
   * @return Union of 2 signatures
   */
  public Object union(Object cs1, Object cs2);

}
