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

package org.apache.hadoop.mapreduce.split;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;

public class TezGroupedSplitsInputFormat<K, V> extends InputFormat<K, V>
  implements Configurable{
  
  private static final Log LOG = LogFactory.getLog(TezGroupedSplitsInputFormat.class);

  InputFormat<K, V> wrappedInputFormat;
  int desiredNumSplits = 0;
  List<InputSplit> groupedSplits = null;
  Configuration conf;
  
  public TezGroupedSplitsInputFormat() {
    
  }
  
  public void setInputFormat(InputFormat<K, V> wrappedInputFormat) {
    this.wrappedInputFormat = wrappedInputFormat;
    if (LOG.isDebugEnabled()) {
      LOG.debug("wrappedInputFormat: " + wrappedInputFormat.getClass().getName());
    }
  }
  
  public void setDesiredNumberOfSPlits(int num) {
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
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    
    int configNumSplits = conf.getInt(TezConfiguration.TEZ_AM_GROUPING_SPLIT_COUNT, 0);
    if (configNumSplits > 0) {
      // always use config override if specified
      desiredNumSplits = configNumSplits;
      LOG.info("Desired numSplits overridden by config to: " + desiredNumSplits);
    }
    
    List<InputSplit> originalSplits = wrappedInputFormat.getSplits(context);
    String wrappedInputFormatName = wrappedInputFormat.getClass().getName();
    if (desiredNumSplits == 0 ||
        originalSplits.size() == 0 ||
        desiredNumSplits >= originalSplits.size()) {
      // nothing set. so return all the splits as is
      LOG.info("Using original number of splits: " + originalSplits.size() +
          " desired splits: " + desiredNumSplits);
      groupedSplits = new ArrayList<InputSplit>(originalSplits.size());
      for (InputSplit split : originalSplits) {
        TezGroupedSplit newSplit = 
            new TezGroupedSplit(1, wrappedInputFormatName, split.getLocations());
        newSplit.addSplit(split);
        groupedSplits.add(newSplit);
      }
      return groupedSplits;
    }
    
    String[] emptyLocations = {"EmptyLocation"};
    groupedSplits = new ArrayList<InputSplit>(desiredNumSplits);
    
    // sort the splits by length
    Collections.sort(originalSplits, new Comparator<InputSplit>() {
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
    int numSplitsGroupedPerLocation = originalSplits.size()/numLocations;
    int numSplitsInGroup = originalSplits.size()/desiredNumSplits;
    for (String location : distinctLocations.keySet()) {
      distinctLocations.put(location, new LocationHolder(numSplitsGroupedPerLocation));
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
    
    
    LOG.info("Desired lengthPerSplit: " + lengthPerSplit +
        " numLocations: " + numLocations +
        " numSplitsGroupedPerLocation: " + numSplitsGroupedPerLocation +
        " numSplitsInGroup: " + numSplitsInGroup + 
        " totalLength: " + totalLength +
        " numSplits: " + originalSplits.size());
    
    // go through locations and group splits
    int splitsProcessed = 0;
    List<SplitHolder> group = new ArrayList<SplitHolder>(numSplitsInGroup);
    boolean allowSmallGroups = false;
    int iterations = 0;
    while (splitsProcessed < originalSplits.size()) {
      iterations++;
      int numFullGroupsCreated = 0;
      for (Map.Entry<String, LocationHolder> entry : distinctLocations.entrySet()) {
        group.clear();
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
          LOG.debug("Grouped " + group.size()
              + " length: " + groupedSplit.getLength()
              + " split at: " + location);
        }
        groupedSplits.add(groupedSplit);
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
            " totalGroups: " + groupedSplits.size());
      }
    }
    LOG.info("Number of splits created: " + groupedSplits.size());
    return groupedSplits;
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    TezGroupedSplit groupedSplit = (TezGroupedSplit) split;
    initInputFormatFromSplit(groupedSplit);
    return new TezGroupedSplitsRecordReader(groupedSplit, context);
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  void initInputFormatFromSplit(TezGroupedSplit split) {
    if (wrappedInputFormat == null) {
      Class<? extends InputFormat> clazz = (Class<? extends InputFormat>) 
          getClassFromName(split.wrappedInputFormatName);
      try {
        wrappedInputFormat = ReflectionUtils.newInstance(clazz, conf);
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
  
  public class TezGroupedSplitsRecordReader  extends RecordReader<K, V> {

    TezGroupedSplit groupedSplit;
    TaskAttemptContext context;
    int idx = 0;
    long progress;
    RecordReader<K, V> curReader;
    
    public TezGroupedSplitsRecordReader(TezGroupedSplit split,
        TaskAttemptContext context) throws IOException {
      this.groupedSplit = split;
      this.context = context;
    }
    
    public void initialize(InputSplit split,
        TaskAttemptContext context) throws IOException, InterruptedException {
      if (this.groupedSplit != split) {
        throw new TezUncheckedException("Splits dont match");
      }
      if (this.context != context) {
        throw new TezUncheckedException("Contexts dont match");
      }
      initNextRecordReader();
    }
    
    public boolean nextKeyValue() throws IOException, InterruptedException {
      while ((curReader == null) || !curReader.nextKeyValue()) {
        // false return finishes. true return loops back for nextKeyValue()
        if (!initNextRecordReader()) {
          return false;
        }
      }
      return true;
    }

    public K getCurrentKey() throws IOException, InterruptedException {
      return curReader.getCurrentKey();
    }
    
    public V getCurrentValue() throws IOException, InterruptedException {
      return curReader.getCurrentValue();
    }
    
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
          try {
            progress += groupedSplit.wrappedSplits.get(idx-1).getLength();
          } catch (InterruptedException e) {
            throw new TezUncheckedException(e);
          }
        }
      }

      // if all chunks have been processed, nothing more to do.
      if (idx == groupedSplit.wrappedSplits.size()) {
        return false;
      }

      // get a record reader for the idx-th chunk
      try {
        curReader = wrappedInputFormat.createRecordReader(
            groupedSplit.wrappedSplits.get(idx), context);

        curReader.initialize(groupedSplit.wrappedSplits.get(idx), context);
      } catch (Exception e) {
        throw new RuntimeException (e);
      }
      idx++;
      return true;
    }
    
    /**
     * return progress based on the amount of data processed so far.
     */
    public float getProgress() throws IOException, InterruptedException {
      long subprogress = 0;    // bytes processed in current split
      if (null != curReader) {
        // idx is always one past the current subsplit's true index.
        subprogress = (long) (curReader.getProgress() * groupedSplit.wrappedSplits
            .get(idx - 1).getLength());
      }
      return Math.min(1.0f,  (progress + subprogress)/(float)(groupedSplit.getLength()));
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

}
