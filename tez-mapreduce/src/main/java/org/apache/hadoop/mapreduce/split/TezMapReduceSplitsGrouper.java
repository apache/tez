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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tez.dag.api.TezUncheckedException;

import com.google.common.base.Preconditions;

/**
 * Helper that provides a grouping of input splits based 
 * on multiple parameters. It creates {@link TezGroupedSplit}
 * to wrap the each group of real InputSplits
 */
@Public
@Evolving
public class TezMapReduceSplitsGrouper {
  private static final Log LOG = LogFactory.getLog(TezMapReduceSplitsGrouper.class);

  /**
   * Specify the number of splits desired to be created
   */
  public static final String TEZ_GROUPING_SPLIT_COUNT = "tez.grouping.split-count";
  /**
   * Limit the number of splits in a group by the total length of the splits in the group
   */
  public static final String TEZ_GROUPING_SPLIT_BY_LENGTH = "tez.grouping.by-length";
  public static final boolean TEZ_GROUPING_SPLIT_BY_LENGTH_DEFAULT = true;
  /**
   * Limit the number of splits in a group by the number of splits in the group
   */
  public static final String TEZ_GROUPING_SPLIT_BY_COUNT = "tez.grouping.by-count";
  public static final boolean TEZ_GROUPING_SPLIT_BY_COUNT_DEFAULT = false;

  /**
   * The multiplier for available queue capacity when determining number of
   * tasks for a Vertex. 1.7 with 100% queue available implies generating a
   * number of tasks roughly equal to 170% of the available containers on the
   * queue. This enables multiple waves of mappers where the final wave is slightly smaller
   * than the remaining waves. The gap helps overlap the final wave with any slower tasks
   * from previous waves and tries to hide the delays from the slower tasks. Good values for 
   * this are 1.7, 2.7, 3.7 etc. Increase the number of waves to make the tasks smaller or
   * shorter.
   */
  public static final String TEZ_GROUPING_SPLIT_WAVES = "tez.grouping.split-waves";
  public static float TEZ_GROUPING_SPLIT_WAVES_DEFAULT = 1.7f;

  /**
   * Upper bound on the size (in bytes) of a grouped split, to avoid generating excessively large splits.
   */
  public static final String TEZ_GROUPING_SPLIT_MAX_SIZE = "tez.grouping.max-size";
  public static long TEZ_GROUPING_SPLIT_MAX_SIZE_DEFAULT = 1024*1024*1024L;

  /**
   * Lower bound on the size (in bytes) of a grouped split, to avoid generating too many small splits.
   */
  public static final String TEZ_GROUPING_SPLIT_MIN_SIZE = "tez.grouping.min-size";
  public static long TEZ_GROUPING_SPLIT_MIN_SIZE_DEFAULT = 50*1024*1024L;

  /**
   * This factor is used to decrease the per group desired (length and count) limits for groups
   * created by combining splits within a rack. Since reading this split involves reading data intra
   * rack, the group is made smaller to cover up for the increased latencies in doing intra rack 
   * reads. The value should be a fraction <= 1.
   */
  public static final String TEZ_GROUPING_RACK_SPLIT_SIZE_REDUCTION = 
                                              "tez.grouping.rack-split-reduction";
  public static final float TEZ_GROUPING_RACK_SPLIT_SIZE_REDUCTION_DEFAULT = 0.75f;

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
  
  public List<InputSplit> getGroupedSplits(Configuration conf,
      List<InputSplit> originalSplits, int desiredNumSplits,
      String wrappedInputFormatName) throws IOException, InterruptedException {
    LOG.info("Grouping splits in Tez");

    int configNumSplits = conf.getInt(TEZ_GROUPING_SPLIT_COUNT, 0);
    if (configNumSplits > 0) {
      // always use config override if specified
      desiredNumSplits = configNumSplits;
      LOG.info("Desired numSplits overridden by config to: " + desiredNumSplits);
    }
    
    if (! (configNumSplits > 0 || 
          originalSplits == null || 
          originalSplits.size() == 0)) {
      // numSplits has not been overridden by config
      // numSplits has been set at runtime
      // there are splits generated
      // desired splits is less than number of splits generated
      // Do sanity checks
      long totalLength = 0;
      for (InputSplit split : originalSplits) {
        totalLength += split.getLength();
      }
  
      int splitCount = desiredNumSplits>0?desiredNumSplits:originalSplits.size();
      long lengthPerGroup = totalLength/splitCount;

      long maxLengthPerGroup = conf.getLong(
          TEZ_GROUPING_SPLIT_MAX_SIZE,
          TEZ_GROUPING_SPLIT_MAX_SIZE_DEFAULT);
      long minLengthPerGroup = conf.getLong(
          TEZ_GROUPING_SPLIT_MIN_SIZE,
          TEZ_GROUPING_SPLIT_MIN_SIZE_DEFAULT);
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
            " Original splits: " + originalSplits.size());
        
        desiredNumSplits = newDesiredNumSplits;
      } else if (lengthPerGroup < minLengthPerGroup) {
        // splits too small to work. Need to override with size.
        int newDesiredNumSplits = (int)(totalLength/minLengthPerGroup) + 1;
        LOG.info("Desired splits: " + desiredNumSplits + " too large. " + 
            " Desired splitLength: " + lengthPerGroup + 
            " Min splitLength: " + minLengthPerGroup +
            " New desired splits: " + newDesiredNumSplits + 
            " Total length: " + totalLength +
            " Original splits: " + originalSplits.size());
        
        desiredNumSplits = newDesiredNumSplits;
      }
    }
     
    List<InputSplit> groupedSplits = null;
    
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
    
    String emptyLocation = "EmptyLocation";
    String[] emptyLocations = {emptyLocation};
    groupedSplits = new ArrayList<InputSplit>(desiredNumSplits);
    
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
    int numSplitsPerLocation = originalSplits.size()/numNodeLocations;
    int numSplitsInGroup = originalSplits.size()/desiredNumSplits;
    
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
        TEZ_GROUPING_SPLIT_BY_LENGTH,
        TEZ_GROUPING_SPLIT_BY_LENGTH_DEFAULT);
    boolean groupByCount = conf.getBoolean(
        TEZ_GROUPING_SPLIT_BY_COUNT,
        TEZ_GROUPING_SPLIT_BY_COUNT_DEFAULT);
    if (!(groupByLength || groupByCount)) {
      throw new TezUncheckedException(
          "None of the grouping parameters are true: "
              + TEZ_GROUPING_SPLIT_BY_LENGTH + ", "
              + TEZ_GROUPING_SPLIT_BY_COUNT);
    }
    LOG.info("Desired numSplits: " + desiredNumSplits +
        " lengthPerGroup: " + lengthPerGroup +
        " numLocations: " + numNodeLocations +
        " numSplitsPerLocation: " + numSplitsPerLocation +
        " numSplitsInGroup: " + numSplitsInGroup + 
        " totalLength: " + totalLength +
        " numOriginalSplits: " + originalSplits.size() +
        " . Grouping by length: " + groupByLength + " count: " + groupByCount);
    
    // go through locations and group splits
    int splitsProcessed = 0;
    List<SplitHolder> group = new ArrayList<SplitHolder>(numSplitsInGroup);
    Set<String> groupLocationSet = new HashSet<String>(10);
    boolean allowSmallGroups = false;
    boolean doingRackLocal = false;
    int iterations = 0;
    while (splitsProcessed < originalSplits.size()) {
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
        groupedSplits.add(groupedSplit);
      }
      
      if (!doingRackLocal && numFullGroupsCreated < 1) {
        // no node could create a node-local group. go rack-local
        doingRackLocal = true;
        // re-create locations
        int numRemainingSplits = originalSplits.size() - splitsProcessed;
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
        distinctLocations.clear();
        HashSet<String> rackSet = new HashSet<String>(rackLocations.size());
        int numRackSplitsToGroup = remainingSplits.size();
        for (InputSplit split : originalSplits) {
          if (numRackSplitsToGroup == 0) {
            break;
          }
          // Iterate through the original splits in their order and consider them for grouping. 
          // This maintains the original ordering in the list and thus subsequent grouping will 
          // maintain that order
          if (!remainingSplits.contains(split)) {
            continue;
          }
          numRackSplitsToGroup--;
          rackSet.clear();
          SplitHolder splitHolder = new SplitHolder(split);
          String[] locations = split.getLocations();
          if (locations == null || locations.length == 0) {
            locations = emptyLocations;
          }
          for (String location : locations ) {
            if (location == null) {
              location = emptyLocation;
            }
            rackSet.add(locToRackMap.get(location));
          }
          for (String rack : rackSet) {
            rackLocations.get(rack).splits.add(splitHolder);
          }
        }
        
        remainingSplits.clear();
        distinctLocations = rackLocations;
        // adjust split length to be smaller because the data is non local
        float rackSplitReduction = conf.getFloat(
            TEZ_GROUPING_RACK_SPLIT_SIZE_REDUCTION,
            TEZ_GROUPING_RACK_SPLIT_SIZE_REDUCTION_DEFAULT);
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
            " totalGroups: " + groupedSplits.size() +
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
            " totalGroups: " + groupedSplits.size());
      }
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("Iteration: " + iterations +
            " splitsProcessed: " + splitsProcessed + 
            " numFullGroupsInRound: " + numFullGroupsCreated +
            " totalGroups: " + groupedSplits.size());
      }
    }
    LOG.info("Number of splits desired: " + desiredNumSplits + 
        " created: " + groupedSplits.size() + 
        " splitsProcessed: " + splitsProcessed);
    return groupedSplits;
  }
  
  /**
   * Builder that can be used to configure grouping in Tez
   * 
   * @param conf
   *          {@link Configuration} This will be modified in place. If
   *          configuration values may be changed at runtime via a config file
   *          then pass in a {@link Configuration} that is initialized from a
   *          config file. The parameters that are not overridden in code will
   *          be derived from the Configuration object.
   * @return {@link org.apache.hadoop.mapreduce.split.TezMapReduceSplitsGrouper.TezMRSplitsGrouperConfigBuilder}
   */
  public static TezMRSplitsGrouperConfigBuilder createConfigBuilder(Configuration conf) {
    return new TezMRSplitsGrouperConfigBuilder(conf);
  }  

  public static final class TezMRSplitsGrouperConfigBuilder {
    private final Configuration conf;

    /**
     * This configuration will be modified in place
     */
    private TezMRSplitsGrouperConfigBuilder(Configuration conf) {
      if (conf == null) {
        conf = new Configuration(false);
      }
      this.conf = conf;
    }

    public TezMRSplitsGrouperConfigBuilder setGroupSplitCount(int count) {
      this.conf.setInt(TEZ_GROUPING_SPLIT_COUNT, count);
      return this;
    }

    public TezMRSplitsGrouperConfigBuilder setGroupSplitByCount(boolean enabled) {
      this.conf.setBoolean(TEZ_GROUPING_SPLIT_BY_COUNT, enabled);
      return this;
    }

    public TezMRSplitsGrouperConfigBuilder setGroupSplitByLength(boolean enabled) {
      this.conf.setBoolean(TEZ_GROUPING_SPLIT_BY_LENGTH, enabled);
      return this;
    }

    public TezMRSplitsGrouperConfigBuilder setGroupSplitWaves(float multiplier) {
      this.conf.setFloat(TEZ_GROUPING_SPLIT_WAVES, multiplier);
      return this;
    }

    public TezMRSplitsGrouperConfigBuilder setGroupingRackSplitSizeReduction(float rackSplitSizeReduction) {
      this.conf.setFloat(TEZ_GROUPING_RACK_SPLIT_SIZE_REDUCTION, rackSplitSizeReduction);
      return this;
    }

    /**
     * upper and lower bounds for the splits
     */
    public TezMRSplitsGrouperConfigBuilder setGroupingSplitSize(long lowerBound, long upperBound) {
      this.conf.setLong(TEZ_GROUPING_SPLIT_MIN_SIZE, lowerBound);
      this.conf.setLong(TEZ_GROUPING_SPLIT_MAX_SIZE, upperBound);
      return this;
    }

    public Configuration build() {
      return this.conf;
    }
  }

}
