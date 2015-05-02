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


package org.apache.tez.common;

/**
 * Event that allows running in parallel for different instances
 * 
 * @param <TYPE>
 *          Event type
 */
public abstract class TezAbstractEvent<TYPE extends Enum<TYPE>> extends
    org.apache.hadoop.yarn.event.AbstractEvent<TYPE> {

  public TezAbstractEvent(TYPE type) {
    super(type);
  }

  /**
   * Returning a number that is identical for event instances that need to be
   * serialized while processing.
   * 
   * @return Serializing identifier. Not overriding this causes serialization
   *         for all events instances
   */
  public int getSerializingHash() {
    return 0;
  }
}
