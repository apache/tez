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

package org.apache.tez.tools.javadoc.model;

public class ConfigProperty {

  public String propertyName;
  public String defaultValue;
  public String description;
  public String type = "string";
  public boolean isPrivate = false;
  public boolean isUnstable = false;
  public boolean isEvolving = false;
  public boolean isValidConfigProp = false;
  public String[] validValues;
  public String inferredType;

  @Override
  public String toString() {
    return "name=" + propertyName
        + ", defaultValue=" + defaultValue
        + ", description=" + description
        + ", type=" + type
        + ", inferredType=" + inferredType
        + ", private=" + isPrivate
        + ", validValues=" + (validValues == null ? "null" : validValues)
        + ", isConfigProp=" + isValidConfigProp;
  }
}


