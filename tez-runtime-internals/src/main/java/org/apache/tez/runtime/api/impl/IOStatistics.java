/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.runtime.api.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class IOStatistics implements Writable {
  private long dataSize = 0;
  private long numItems = 0;
  
  public void setDataSize(long size) {
    this.dataSize = size;
  }
  
  public long getDataSize() {
    return dataSize;
  }
  
  public void setItemsProcessed(long items) {
    this.numItems = items;
  }
  
  public long getItemsProcessed() {
    return numItems;
  }
  
  public void mergeFrom(org.apache.tez.runtime.api.impl.IOStatistics other) {
    this.setDataSize(this.getDataSize() + other.getDataSize());
    this.setItemsProcessed(this.getItemsProcessed() + other.getItemsProcessed());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(getDataSize());
    out.writeLong(getItemsProcessed());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    setDataSize(in.readLong());
    setItemsProcessed(in.readLong());
  }

}
