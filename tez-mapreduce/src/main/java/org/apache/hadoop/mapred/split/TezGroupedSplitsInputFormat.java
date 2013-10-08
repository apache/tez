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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tez.dag.api.TezUncheckedException;

import com.google.common.base.Preconditions;

public class TezGroupedSplitsInputFormat<K, V> implements InputFormat<K, V> {
  private static final Log LOG = LogFactory.getLog(TezGroupedSplitsInputFormat.class);

  InputFormat<K, V> wrappedInputFormat;
  int desiredNumSplits = 0;
  
  public TezGroupedSplitsInputFormat() {
    
  }
  
  public void setInputFormat(InputFormat<K, V> wrappedInputFormat) {
    this.wrappedInputFormat = wrappedInputFormat;
    if (LOG.isDebugEnabled()) {
      LOG.debug("wrappedInputFormat: " + wrappedInputFormat.getClass().getName());
    }
  }
  
  public void setDesiredNumberOfSPlits(int num) {
    Preconditions.checkArgument(num > 0);
    this.desiredNumSplits = num;
    if (LOG.isDebugEnabled()) {
      LOG.debug("desiredNumSplits: " + desiredNumSplits);
    }
  }
  
  class SplitHolder {
    InputSplit split;
    boolean isProcessed = false;
    SplitHolder(InputSplit split) {
      this.split = split;
    }
  }
  
  class LocationHolder {
    List<SplitHolder> splits;
    int headIndex = 0;
    LocationHolder(int capacity) {
      splits = new ArrayList<SplitHolder>(capacity);
    }
    boolean isEmpty() {
      return (headIndex == splits.size());
    }
    SplitHolder getUnprocessedHeadSplit() {
      while (!isEmpty()) {
        SplitHolder holder = splits.get(headIndex);
        if (!holder.isProcessed) {
          return holder;
        }
        incrementHeadIndex();
      }
      return null;
    }
    void incrementHeadIndex() {
      headIndex++;
    }
  }
  
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    if (desiredNumSplits > 0) {
      // get the desired num splits directly if possible
      numSplits = desiredNumSplits;
    }
    InputSplit[] originalSplits = wrappedInputFormat.getSplits(job, numSplits);
    String wrappedInputFormatName = wrappedInputFormat.getClass().getCanonicalName();
    if (desiredNumSplits == 0 ||
        originalSplits.length == 0 ||
        desiredNumSplits >= originalSplits.length) {
      // nothing set. so return all the splits as is
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using original number of splits: " + originalSplits.length);
      }
      InputSplit[] groupedSplits = new TezGroupedSplit[originalSplits.length];
      int i=0;
      for (InputSplit split : originalSplits) {
        TezGroupedSplit newSplit = 
            new TezGroupedSplit(1, wrappedInputFormatName, split.getLocations());
        newSplit.addSplit(split);
        groupedSplits[i++] = newSplit;
      }
      return groupedSplits;
    }
    
    String[] emptyLocations = {"EmptyLocation"};
    List<InputSplit> groupedSplitsList = new ArrayList<InputSplit>(desiredNumSplits);
    
    // sort the splits by length
    Arrays.sort(originalSplits, new Comparator<InputSplit>() {
      @Override
      public int compare(InputSplit o1, InputSplit o2) {
        try {
          if (o1.getLength() < o2.getLength()) {
            return -1;
          } else if (o1.getLength() > o2.getLength()) {
            return 1;
          }
        } catch (Exception e) {
          throw new TezUncheckedException(e);
        }
        return 0;
      }
    });
    
    long totalLength = 0;
    Map<String, LocationHolder> distinctLocations = new HashMap<String, LocationHolder>();
    // go through splits in sorted order and add them to locations
    for (InputSplit split : originalSplits) {
      totalLength += split.getLength();
      String[] locations = split.getLocations();
      if (locations == null || locations.length == 0) {
        locations = emptyLocations;
      }
      for (String location : locations ) {
        distinctLocations.put(location, null);
      }
    }
    
    long lengthPerSplit = totalLength/desiredNumSplits;
    int numLocations = distinctLocations.size();
    int numSplitsPerLocation = originalSplits.length/numLocations;
    int numSplitsInGroup = originalSplits.length/desiredNumSplits;
    for (String location : distinctLocations.keySet()) {
      distinctLocations.put(location, new LocationHolder(numSplitsPerLocation));
    }
    
    for (InputSplit split : originalSplits) {
      SplitHolder splitHolder = new SplitHolder(split);
      String[] locations = split.getLocations();
      if (locations == null || locations.length == 0) {
        locations = emptyLocations;
      }
      for (String location : locations ) {
        LocationHolder holder = distinctLocations.get(location);
        holder.splits.add(splitHolder); // added smallest to largest
      }
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Desired lengthPerSplit: " + lengthPerSplit +
          " numLocations: " + numLocations +
          " numSplitsPerLocation: " + numSplitsPerLocation +
          " numSplitsInGroup: " + numSplitsInGroup + 
          " totalLength: " + totalLength);
    }
    
    // go through locations and group splits
    int splitsProcessed = 0;
    List<SplitHolder> group = new ArrayList<SplitHolder>(numSplitsInGroup+1);
    boolean allowSmallGroups = false;
    int iterations = 0;
    while (splitsProcessed < originalSplits.length) {
      group.clear();
      iterations++;
      int numFullGroupsCreated = 0;
      for (Map.Entry<String, LocationHolder> entry : distinctLocations.entrySet()) {
        String location = entry.getKey();
        LocationHolder holder = entry.getValue();
        SplitHolder splitHolder = holder.getUnprocessedHeadSplit();
        if (splitHolder == null) {
          // all splits on node processed
          continue;
        }
        int oldHeadIndex = holder.headIndex;
        long groupLength = 0;
        do {
          group.add(splitHolder);
          groupLength += splitHolder.split.getLength();
          holder.incrementHeadIndex();
          splitHolder = holder.getUnprocessedHeadSplit();
        } while(splitHolder != null && 
            groupLength + splitHolder.split.getLength() <= lengthPerSplit);

        if (holder.isEmpty() && groupLength < lengthPerSplit/2 && !allowSmallGroups) {
          // group too small, reset it
          holder.headIndex = oldHeadIndex;
          continue;
        }
        
        numFullGroupsCreated++;

        // One split group created
        String[] groupLocation = {location};
        if (location == emptyLocations[0]) {
          groupLocation = null;
        }
        TezGroupedSplit groupedSplit = 
            new TezGroupedSplit(group.size(), wrappedInputFormatName, groupLocation);
        for (SplitHolder groupedSplitHolder : group) {
          groupedSplit.addSplit(groupedSplitHolder.split);
          groupedSplitHolder.isProcessed = true;
          splitsProcessed++;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Grouped " + group.size() + " split at: " + groupLocation);
        }
        groupedSplitsList.add(groupedSplit);
      }
      
      if (!allowSmallGroups && numFullGroupsCreated < numLocations/4) {
        // a few nodes have a lot of data or data is thinly spread across nodes
        // so allow small groups now
        if (LOG.isDebugEnabled()) {
          LOG.debug("Allowing small groups");
        }
        allowSmallGroups = true;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Iteration: " + iterations +
            " splitsProcessed: " + splitsProcessed + 
            " numFullGroupsInRound: " + numFullGroupsCreated +
            " totalGroups: " + groupedSplitsList.size());
      }
    }
    InputSplit[] groupedSplits = new InputSplit[groupedSplitsList.size()];
    groupedSplitsList.toArray(groupedSplits);
    LOG.info("Number of splits created: " + groupedSplitsList.size());
    return groupedSplits;
  }
  
  @Override
  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    TezGroupedSplit groupedSplit = (TezGroupedSplit) split;
    initInputFormatFromSplit(groupedSplit);
    return new TezGroupedSplitsRecordReader(groupedSplit, job, reporter);
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  void initInputFormatFromSplit(TezGroupedSplit split) {
    if (wrappedInputFormat == null) {
      Class<? extends InputFormat> clazz = (Class<? extends InputFormat>) 
          getClassFromName(split.wrappedInputFormatName);
      try {
        wrappedInputFormat = clazz.newInstance();
      } catch (Exception e) {
        throw new TezUncheckedException(e);
      }
    }
  }
  
  static Class<?> getClassFromName(String name) {
    try {
      return Class.forName(name);
    } catch (ClassNotFoundException e1) {
      throw new TezUncheckedException(e1);
    }
  }
  
  public class TezGroupedSplitsRecordReader implements RecordReader<K, V> {

    TezGroupedSplit groupedSplit;
    JobConf job;
    Reporter reporter;
    int idx = 0;
    long progress;
    RecordReader<K, V> curReader;
    
    public TezGroupedSplitsRecordReader(TezGroupedSplit split, JobConf job,
        Reporter reporter) throws IOException {
      this.groupedSplit = split;
      this.job = job;
      this.reporter = reporter;
      initNextRecordReader();
    }
    
    @Override
    public boolean next(K key, V value) throws IOException {

      while ((curReader == null) || !curReader.next(key, value)) {
        if (!initNextRecordReader()) {
          return false;
        }
      }
      return true;
    }

    @Override
    public K createKey() {
      return curReader.createKey();
    }
    
    @Override
    public V createValue() {
      return curReader.createValue();
    }
    
    @Override
    public float getProgress() throws IOException {
      return Math.min(1.0f,  getPos()/(float)(groupedSplit.getLength()));
    }
    
    @Override
    public void close() throws IOException {
      if (curReader != null) {
        curReader.close();
        curReader = null;
      }
    }
    
    protected boolean initNextRecordReader() throws IOException {
      if (curReader != null) {
        curReader.close();
        curReader = null;
        if (idx > 0) {
          progress += groupedSplit.wrappedSplits.get(idx-1).getLength();
        }
      }

      // if all chunks have been processed, nothing more to do.
      if (idx == groupedSplit.wrappedSplits.size()) {
        return false;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Init record reader for index " + idx + " of " + 
                  groupedSplit.wrappedSplits.size());
      }

      // get a record reader for the idx-th chunk
      try {
        curReader = wrappedInputFormat.getRecordReader(
            groupedSplit.wrappedSplits.get(idx), job, reporter);
      } catch (Exception e) {
        throw new RuntimeException (e);
      }
      idx++;
      return true;
    }

    @Override
    public long getPos() throws IOException {
      long subprogress = 0;    // bytes processed in current split
      if (null != curReader) {
        // idx is always one past the current subsplit's true index.
        subprogress = curReader.getPos();
      }
      return (progress + subprogress);
    }
  }  

}
