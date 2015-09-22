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

package org.apache.hadoop.mapred.split;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;

/**
 * Implements an InputSplit that provides a generic wrapper around 
 * a group of real InputSplits
 */
@Public
@Evolving
public class TezGroupedSplit implements InputSplit, Configurable {

  List<InputSplit> wrappedSplits = null;
  String wrappedInputFormatName = null;
  String[] locations = null;
  String rack = null;
  long length = 0;
  Configuration conf;
  
  public TezGroupedSplit() {
    
  }
  
  public TezGroupedSplit(int numSplits, String wrappedInputFormatName,
      String[] locations, String rack) {
    this.wrappedSplits = new ArrayList<InputSplit>(numSplits);
    this.wrappedInputFormatName = wrappedInputFormatName;
    this.locations = locations;
    this.rack = rack;
  }
  public TezGroupedSplit(int numSplits, String wrappedInputFormatName,
      String[] locations) {
    this(numSplits, wrappedInputFormatName, locations, null);
  }

  public List<InputSplit> getGroupedSplits() {
    return wrappedSplits;
  }

  public void addSplit(InputSplit split) {
    wrappedSplits.add(split);
    try {
      length += split.getLength();
    } catch (Exception e) {
      throw new TezUncheckedException(e);
    }
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    if (wrappedSplits == null) {
      throw new TezUncheckedException("Wrapped splits cannot be empty");
    }

    Text.writeString(out, wrappedInputFormatName);
    Text.writeString(out, wrappedSplits.get(0).getClass().getName());
    out.writeInt(wrappedSplits.size());
    for(InputSplit split : wrappedSplits) {
      writeWrappedSplit(split, out);
    }
    out.writeLong(length);
    
    if (locations == null || locations.length == 0) {
      out.writeInt(0);
    } else {
      out.writeInt(locations.length);
      for (String location : locations) {
        Text.writeString(out, location);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput in) throws IOException {
    wrappedInputFormatName = Text.readString(in);
    String inputSplitClassName = Text.readString(in);
    Class<? extends InputSplit> clazz = null;
    try {
      clazz = (Class<? extends InputSplit>)
      TezGroupedSplitsInputFormat.getClassFromName(inputSplitClassName);
    } catch (TezException e) {
      throw new IOException(e);
    }

    int numSplits = in.readInt();
    
    wrappedSplits = new ArrayList<InputSplit>(numSplits);
    for (int i=0; i<numSplits; ++i) {
      addSplit(readWrappedSplit(in, clazz));
    }
    
    long recordedLength = in.readLong();
    if(recordedLength != length) {
      throw new TezUncheckedException("Expected length: " + recordedLength
          + " actual length: " + length);
    }
    int numLocs = in.readInt();
    if (numLocs > 0) {
      locations = new String[numLocs];
      for (int i=0; i<numLocs; ++i) {
        locations[i] = Text.readString(in);
      }
    }
  }
  
  void writeWrappedSplit(InputSplit split, DataOutput out) throws IOException {
    split.write(out);
  }
  
  InputSplit readWrappedSplit(DataInput in, Class<? extends InputSplit> clazz) 
      throws IOException {
    InputSplit split;
    try {
      split = ReflectionUtils.newInstance(clazz, conf);
    } catch (Exception e) {
      throw new TezUncheckedException(e);
    }
    split.readFields(in);
    return split;
  }
  
  @Override
  public long getLength() throws IOException {
    return length;
  }

  @Override
  public String[] getLocations() throws IOException {
    return locations;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
  
  public String getRack() {
    return rack;
  }

  @Override
  public String toString() {
    return "TezGroupedSplit{" +
        "wrappedSplits=" + wrappedSplits +
        ", wrappedInputFormatName='" + wrappedInputFormatName + '\'' +
        ", locations=" + Arrays.toString(locations) +
        ", rack='" + rack + '\'' +
        ", length=" + length +
        '}';
  }
}
