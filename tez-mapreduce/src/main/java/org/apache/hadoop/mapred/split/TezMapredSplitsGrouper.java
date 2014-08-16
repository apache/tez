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
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.split.TezMapReduceSplitsGrouper;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tez.dag.api.TezUncheckedException;

import com.google.common.base.Preconditions;

/**
 * A Helper that provides grouping logic to group InputSplits
 * using various parameters. A {@link TezGroupedSplit} is used
 * to wrap the real InputSplits in a group.
 */
@Public
@Evolving
public class TezMapredSplitsGrouper {
  private static final Log LOG = LogFactory.getLog(TezMapredSplitsGrouper.class);

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
  
  public InputSplit[] getGroupedSplits(Configuration conf,
      InputSplit[] originalSplits, int desiredNumSplits,
      String wrappedInputFormatName) throws IOException {
    LOG.info("Grouping splits in Tez");

    int configNumSplits = conf.getInt(TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_COUNT, 0);
    if (configNumSplits > 0) {
      // always use config override if specified
      desiredNumSplits = configNumSplits;
      LOG.info("Desired numSplits overridden by config to: " + desiredNumSplits);
    }
    
    if (! (configNumSplits > 0 || 
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

      int splitCount = desiredNumSplits>0?desiredNumSplits:originalSplits.length;
      long lengthPerGroup = totalLength/splitCount;
      
      long maxLengthPerGroup = conf.getLong(
          TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_MAX_SIZE,
          TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_MAX_SIZE_DEFAULT);
      long minLengthPerGroup = conf.getLong(
          TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_MIN_SIZE,
          TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_MIN_SIZE_DEFAULT);
      if (maxLengthPerGroup < minLengthPerGroup || 
          minLengthPerGroup <=0) {
        throw new TezUncheckedException(
          "Invalid max/min group lengths. Required min>0, max>=min. " +
          " max: " + maxLengthPerGroup + " min: " + minLengthPerGroup);
      }
      if (lengthPerGroup > maxLengthPerGroup) {
        // splits too big to work. Need to override with max size.
        int newDesiredNumSplits = (int)(totalLength/maxLengthPerGroup) + 1;
        LOG.info("Desired splits: " + desiredNumSplits + " too small. " + 
            " Desired splitLength: " + lengthPerGroup + 
            " Max splitLength: " + maxLengthPerGroup +
            " New desired splits: " + newDesiredNumSplits + 
            " Total length: " + totalLength +
            " Original splits: " + originalSplits.length);
        
        desiredNumSplits = newDesiredNumSplits;
      } else if (lengthPerGroup < minLengthPerGroup) {
        // splits too small to work. Need to override with size.
        int newDesiredNumSplits = (int)(totalLength/minLengthPerGroup) + 1;
        LOG.info("Desired splits: " + desiredNumSplits + " too large. " + 
            " Desired splitLength: " + lengthPerGroup + 
            " Min splitLength: " + minLengthPerGroup +
            " New desired splits: " + newDesiredNumSplits + 
            " Total length: " + totalLength +
            " Original splits: " + originalSplits.length);
        
        desiredNumSplits = newDesiredNumSplits;
      }
    }
    
    if (originalSplits == null) {
      LOG.info("Null original splits");
      return null;
    }
    
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
    
    String emptyLocation = "EmptyLocation";
    String[] emptyLocations = {emptyLocation};
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
        if (location == null) {
          location = emptyLocation;
        }
        distinctLocations.put(location, null);
      }
    }
    
    long lengthPerGroup = totalLength/desiredNumSplits;
    int numNodeLocations = distinctLocations.size();
    int numSplitsPerLocation = originalSplits.length/numNodeLocations;
    int numSplitsInGroup = originalSplits.length/desiredNumSplits;

    // allocation loop here so that we have a good initial size for the lists
    for (String location : distinctLocations.keySet()) {
      distinctLocations.put(location, new LocationHolder(numSplitsPerLocation+1));
    }
    
    Set<String> locSet = new HashSet<String>();
    for (InputSplit split : originalSplits) {
      locSet.clear();
      SplitHolder splitHolder = new SplitHolder(split);
      String[] locations = split.getLocations();
      if (locations == null || locations.length == 0) {
        locations = emptyLocations;
      }
      for (String location : locations) {
        if (location == null) {
          location = emptyLocation;
        }
        locSet.add(location);
      }
      for (String location : locSet) {
        LocationHolder holder = distinctLocations.get(location);
        holder.splits.add(splitHolder);
      }
    }
    
    boolean groupByLength = conf.getBoolean(
        TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_BY_LENGTH,
        TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_BY_LENGTH_DEFAULT);
    boolean groupByCount = conf.getBoolean(
        TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_BY_COUNT,
        TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_BY_COUNT_DEFAULT);
    if (!(groupByLength || groupByCount)) {
      throw new TezUncheckedException(
          "None of the grouping parameters are true: "
              + TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_BY_LENGTH + ", "
              + TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_BY_COUNT);
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
        if (location == emptyLocation) {
          groupLocation = null;
        } else if (doingRackLocal) {
          for (SplitHolder splitH : group) {
            String[] locations = splitH.split.getLocations();
            if (locations != null) {
              for (String loc : locations) {
                if (loc != null) {
                  groupLocationSet.add(loc);
                }
              }
            }
          }
          groupLocation = groupLocationSet.toArray(groupLocation);
        }
        TezGroupedSplit groupedSplit = 
            new TezGroupedSplit(group.size(), wrappedInputFormatName, 
                groupLocation, 
                // pass rack local hint directly to AM
                ((doingRackLocal && location != emptyLocation)?location:null));
        for (SplitHolder groupedSplitHolder : group) {
          groupedSplit.addSplit(groupedSplitHolder.split);
          Preconditions.checkState(groupedSplitHolder.isProcessed == false, 
              "Duplicates in grouping at location: " + location);
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
          String rack = emptyLocation;
          if (location != emptyLocation) {
            rack = RackResolver.resolve(location).getNetworkLocation();
          }
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
            if ( location == null) {
              location = emptyLocation;
            }
            rackSet.add(locToRackMap.get(location));
          }
          for (String rack : rackSet) {
            rackLocations.get(rack).splits.add(splitHolder);
          }
        }
        
        distinctLocations.clear();
        distinctLocations = rackLocations;
        // adjust split length to be smaller because the data is non local
        float rackSplitReduction = conf.getFloat(
            TezMapReduceSplitsGrouper.TEZ_GROUPING_RACK_SPLIT_SIZE_REDUCTION,
            TezMapReduceSplitsGrouper.TEZ_GROUPING_RACK_SPLIT_SIZE_REDUCTION_DEFAULT);
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
      
      if (!allowSmallGroups && numFullGroupsCreated <= numNodeLocations/10) {
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

}
