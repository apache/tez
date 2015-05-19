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

@Public
@Evolving
public class Event {
  private final String info;
  private final String type;
  private final long time;

  private long refTime; //typically dag start time.

  public Event(String info, String type, long time) {
    this.time = time;
    this.type = type;
    this.info = info;
  }

  void setReferenceTime(long refTime) {
    this.refTime = refTime;
  }

  public final String getInfo() {
    return info;
  }

  public final String getType() {
    return type;
  }

  public final long getAbsoluteTime() {
    return time;
  }

  public final long getTime() {
    return time - refTime;
  }

  @Override
  public String toString() {
    return "[info=" + info + ", type=" + type + ", time=" + time + "]";
  }
}
