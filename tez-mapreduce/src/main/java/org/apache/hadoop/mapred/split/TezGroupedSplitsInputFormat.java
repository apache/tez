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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;

import com.google.common.base.Preconditions;

public class TezGroupedSplitsInputFormat<K, V> 
  implements InputFormat<K, V>, Configurable{
  
  private static final Log LOG = LogFactory.getLog(TezGroupedSplitsInputFormat.class);

  InputFormat<K, V> wrappedInputFormat;
  int desiredNumSplits = 0;
  Configuration conf;
  
  public TezGroupedSplitsInputFormat() {
    
  }
  
  public void setInputFormat(InputFormat<K, V> wrappedInputFormat) {
    this.wrappedInputFormat = wrappedInputFormat;
    if (LOG.isDebugEnabled()) {
      LOG.debug("wrappedInputFormat: " + wrappedInputFormat.getClass().getName());
    }
  }
  
  public void setDesiredNumberOfSplits(int num) {
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
    LOG.info("Grouping splits in Tez");

    int configNumSplits = conf.getInt(TezConfiguration.TEZ_AM_GROUPING_SPLIT_COUNT, 0);
    if (configNumSplits > 0) {
      // always use config override if specified
      desiredNumSplits = configNumSplits;
      LOG.info("Desired numSplits overridden by config to: " + desiredNumSplits);
    }
    
    if (desiredNumSplits > 0) {
      // get the desired num splits directly if possible
      numSplits = desiredNumSplits;
    }
    InputSplit[] originalSplits = wrappedInputFormat.getSplits(job, numSplits);
    
    if (! (configNumSplits > 0 || 
          desiredNumSplits == 0 ||
          originalSplits == null || 
          originalSplits.length == 0) ) {
      // numSplits has not been overridden by config
      // numSplits has been set at runtime
      // there are splits generated
      // Do sanity checks
      long totalLength = 0;
      for (InputSplit split : originalSplits) {
        totalLength += split.getLength();
      }
  
      long lengthPerGroup = totalLength/desiredNumSplits;
      
      long maxLengthPerGroup = job.getLong(
          TezConfiguration.TEZ_AM_GROUPING_SPLIT_MAX_SIZE,
          TezConfiguration.TEZ_AM_GROUPING_SPLIT_MAX_SIZE_DEFAULT);
      if (lengthPerGroup > maxLengthPerGroup) {
        // splits too big to work. Need to override with max size.
        int newDesiredNumSplits = (int)(totalLength/maxLengthPerGroup) + 1;
        LOG.info("Desired splits: " + desiredNumSplits + " too small. " + 
            " Desired splitLength: " + lengthPerGroup + 
            " Max splitLength: " + maxLengthPerGroup +
            " . New desired splits: " + newDesiredNumSplits);
        
        desiredNumSplits = newDesiredNumSplits;
        if (desiredNumSplits > originalSplits.length) {
          // too few splits were produced. See if we can produce more splits
          LOG.info("Recalculating splits. Original splits: " + originalSplits.length);
          originalSplits = wrappedInputFormat.getSplits(job, desiredNumSplits);
        }
      }
    }
    
    if (originalSplits == null) {
      LOG.info("Null original splits");
      return null;
    }
    
    String wrappedInputFormatName = wrappedInputFormat.getClass().getName();
    if (desiredNumSplits == 0 ||
        originalSplits.length == 0 ||
        desiredNumSplits >= originalSplits.length) {
      // nothing set. so return all the splits as is
      LOG.info("Using original number of splits: " + originalSplits.length +
          " desired splits: " + desiredNumSplits);
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
    
    long totalLength = 0;
    Map<String, LocationHolder> distinctLocations = new HashMap<String, LocationHolder>();
    // go through splits and add them to locations
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
    
    long lengthPerGroup = totalLength/desiredNumSplits;
    int numNodeLocations = distinctLocations.size();
    int numSplitsPerLocation = originalSplits.length/numNodeLocations;
    int numSplitsInGroup = originalSplits.length/desiredNumSplits;

    // allocation loop here so that we have a good initial size for the lists
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
        holder.splits.add(splitHolder);
      }
    }
    
    boolean groupByLength = conf.getBoolean(
        TezConfiguration.TEZ_AM_GROUPING_SPLIT_BY_LENGTH,
        TezConfiguration.TEZ_AM_GROUPING_SPLIT_BY_LENGTH_DEFAULT);
    boolean groupByCount = conf.getBoolean(
        TezConfiguration.TEZ_AM_GROUPING_SPLIT_BY_COUNT,
        TezConfiguration.TEZ_AM_GROUPING_SPLIT_BY_COUNT_DEFAULT);
    if (!(groupByLength || groupByCount)) {
      throw new TezUncheckedException(
          "None of the grouping parameters are true: "
              + TezConfiguration.TEZ_AM_GROUPING_SPLIT_BY_LENGTH + ", "
              + TezConfiguration.TEZ_AM_GROUPING_SPLIT_BY_COUNT);
    }
    LOG.info("Desired numSplits: " + desiredNumSplits +
        " lengthPerGroup: " + lengthPerGroup +
        " numLocations: " + numNodeLocations +
        " numSplitsPerLocation: " + numSplitsPerLocation +
        " numSplitsInGroup: " + numSplitsInGroup + 
        " totalLength: " + totalLength +
        " numOriginalSplits: " + originalSplits.length +
        " . Grouping by length: " + groupByLength + " count: " + groupByCount);
    
    // go through locations and group splits
    int splitsProcessed = 0;
    List<SplitHolder> group = new ArrayList<SplitHolder>(numSplitsInGroup+1);
    Set<String> groupLocationSet = new HashSet<String>(10);
    boolean allowSmallGroups = false;
    boolean doingRackLocal = false;
    int iterations = 0;
    while (splitsProcessed < originalSplits.length) {
      iterations++;
      int numFullGroupsCreated = 0;
      for (Map.Entry<String, LocationHolder> entry : distinctLocations.entrySet()) {
        group.clear();
        groupLocationSet.clear();
        String location = entry.getKey();
        LocationHolder holder = entry.getValue();
        SplitHolder splitHolder = holder.getUnprocessedHeadSplit();
        if (splitHolder == null) {
          // all splits on node processed
          continue;
        }
        int oldHeadIndex = holder.headIndex;
        long groupLength = 0;
        int groupNumSplits = 0;
        do {
          group.add(splitHolder);
          groupLength += splitHolder.split.getLength();
          groupNumSplits++;
          holder.incrementHeadIndex();
          splitHolder = holder.getUnprocessedHeadSplit();
        } while(splitHolder != null  
            && (!groupByLength || 
                (groupLength + splitHolder.split.getLength() <= lengthPerGroup))
            && (!groupByCount || 
                (groupNumSplits + 1 <= numSplitsInGroup)));

        if (holder.isEmpty() 
            && !allowSmallGroups
            && (!groupByLength || groupLength < lengthPerGroup/2)
            && (!groupByCount || groupNumSplits < numSplitsInGroup/2)) {
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
        if (doingRackLocal) {
          for (SplitHolder splitH : group) {
            String[] locations = splitH.split.getLocations();
            if (locations != null) {
              for (String loc : locations) {
                groupLocationSet.add(loc);
              }
            }
          }
          groupLocation = groupLocationSet.toArray(groupLocation);
        }
        TezGroupedSplit groupedSplit = 
            new TezGroupedSplit(group.size(), wrappedInputFormatName, 
                groupLocation, (doingRackLocal?location:null));
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
        groupedSplitsList.add(groupedSplit);
      }
      
      if (!doingRackLocal && numFullGroupsCreated < 1) {
        // no node could create a node-local group. go rack-local
        doingRackLocal = true;
        // re-create locations
        int numRemainingSplits = originalSplits.length - splitsProcessed;
        Set<InputSplit> remainingSplits = new HashSet<InputSplit>(numRemainingSplits);
        // gather remaining splits.
        for (Map.Entry<String, LocationHolder> entry : distinctLocations.entrySet()) {
          LocationHolder locHolder = entry.getValue();
          while (!locHolder.isEmpty()) {
            SplitHolder splitHolder = locHolder.getUnprocessedHeadSplit();
            if (splitHolder != null) {
              remainingSplits.add(splitHolder.split);
              locHolder.incrementHeadIndex();
            }
          }
        }
        if (remainingSplits.size() != numRemainingSplits) {
          throw new TezUncheckedException("Expected: " + numRemainingSplits 
              + " got: " + remainingSplits.size());
        }
        
        // doing all this now instead of up front because the number of remaining
        // splits is expected to be much smaller
        RackResolver.init(conf);
        Map<String, String> locToRackMap = new HashMap<String, String>(distinctLocations.size());
        Map<String, LocationHolder> rackLocations = new HashMap<String, LocationHolder>();
        for (String location : distinctLocations.keySet()) {
          // unknown locations will get resolved to default-rack
          String rack = RackResolver.resolve(location).getNetworkLocation();
          locToRackMap.put(location, rack);
          if (rackLocations.get(rack) == null) {
            // splits will probably be located in all racks
            rackLocations.put(rack, new LocationHolder(numRemainingSplits));
          }
        }
        HashSet<String> rackSet = new HashSet<String>(rackLocations.size());
        for (InputSplit split : remainingSplits) {
          rackSet.clear();
          SplitHolder splitHolder = new SplitHolder(split);
          String[] locations = split.getLocations();
          if (locations == null || locations.length == 0) {
            locations = emptyLocations;
          }
          for (String location : locations ) {
            rackSet.add(locToRackMap.get(location));
          }
          for (String rack : rackSet) {
            rackLocations.get(rack).splits.add(splitHolder);
          }
        }
        
        distinctLocations.clear();
        distinctLocations = rackLocations;
        // adjust split length to be smaller because the data is non local
        float rackSplitReduction = job.getFloat(
            TezConfiguration.TEZ_AM_GROUPING_RACK_SPLIT_SIZE_REDUCTION,
            TezConfiguration.TEZ_AM_GROUPING_RACK_SPLIT_SIZE_REDUCTION_DEFAULT);
        if (rackSplitReduction > 0) {
          long newLengthPerGroup = (long)(lengthPerGroup*rackSplitReduction);
          int newNumSplitsInGroup = (int) (numSplitsInGroup*rackSplitReduction);
          if (newLengthPerGroup > 0) {
            lengthPerGroup = newLengthPerGroup;
          }
          if (newNumSplitsInGroup > 0) {
            numSplitsInGroup = newNumSplitsInGroup;
          }
        }
        
        LOG.info("Doing rack local after iteration: " + iterations +
            " splitsProcessed: " + splitsProcessed + 
            " numFullGroupsInRound: " + numFullGroupsCreated +
            " totalGroups: " + groupedSplitsList.size() +
            " lengthPerGroup: " + lengthPerGroup +
            " numSplitsInGroup: " + numSplitsInGroup);
        
        // dont do smallGroups for the first pass
        continue;
      }
      
      if (!allowSmallGroups && numFullGroupsCreated < numNodeLocations/10) {
        // a few nodes have a lot of data or data is thinly spread across nodes
        // so allow small groups now        
        allowSmallGroups = true;
        LOG.info("Allowing small groups after iteration: " + iterations +
            " splitsProcessed: " + splitsProcessed + 
            " numFullGroupsInRound: " + numFullGroupsCreated +
            " totalGroups: " + groupedSplitsList.size());
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
    LOG.info("Number of splits desired: " + desiredNumSplits + 
        " created: " + groupedSplitsList.size() + 
        " splitsProcessed: " + splitsProcessed);
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

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

}
