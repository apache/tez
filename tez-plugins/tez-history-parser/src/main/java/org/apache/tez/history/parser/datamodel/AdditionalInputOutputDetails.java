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

package org.apache.tez.history.parser.datamodel;

import static org.apache.hadoop.classification.InterfaceAudience.Public;
import static org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Additional input/output information present in DAG.
 */

@Public
@Evolving
public class AdditionalInputOutputDetails {
  private final String name;
  private final String clazz;
  private final String initializer;
  private final String userPayloadText;

  public AdditionalInputOutputDetails(String name, String clazz, String initializer,
      String userPayloadText) {
    this.name = name;
    this.clazz = clazz;
    this.initializer = initializer;
    this.userPayloadText = userPayloadText;
  }

  public final String getName() {
    return name;
  }

  public final String getClazz() {
    return clazz;
  }

  public final String getInitializer() {
    return initializer;
  }

  public final String getUserPayloadText() {
    return userPayloadText;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append("name=").append(name).append(", ");
    sb.append("clazz=").append(clazz).append(", ");
    sb.append("initializer=").append(initializer).append(", ");
    sb.append("userPayloadText=").append(userPayloadText);
    sb.append("]");
    return sb.toString();
  }
}
