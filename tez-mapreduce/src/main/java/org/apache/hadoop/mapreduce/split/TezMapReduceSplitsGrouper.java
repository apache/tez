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
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.tez.mapreduce.grouper.GroupedSplitContainer;
import org.apache.tez.mapreduce.grouper.MapReduceSplitContainer;
import org.apache.tez.mapreduce.grouper.SplitContainer;
import org.apache.tez.mapreduce.grouper.SplitSizeEstimatorWrapperMapReduce;
import org.apache.tez.mapreduce.grouper.TezSplitGrouper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * Helper that provides a grouping of input splits based 
 * on multiple parameters. It creates {@link TezGroupedSplit}
 * to wrap the each group of real InputSplits
 */
@Public
@Evolving
public class TezMapReduceSplitsGrouper extends TezSplitGrouper {
  private static final Logger LOG = LoggerFactory.getLogger(TezMapReduceSplitsGrouper.class);

  /**
   * @deprecated See equivalent in {@link TezSplitGrouper}
   */
  @Deprecated
  public static final String TEZ_GROUPING_SPLIT_COUNT = TezSplitGrouper.TEZ_GROUPING_SPLIT_COUNT;

  /**
   * @deprecated See equivalent in {@link TezSplitGrouper}
   */
  @Deprecated
  public static final String TEZ_GROUPING_SPLIT_BY_LENGTH = TezSplitGrouper.TEZ_GROUPING_SPLIT_BY_LENGTH;
  /**
   * @deprecated See equivalent in {@link TezSplitGrouper}
   */
  @Deprecated
  public static final boolean TEZ_GROUPING_SPLIT_BY_LENGTH_DEFAULT = TezSplitGrouper.TEZ_GROUPING_SPLIT_BY_LENGTH_DEFAULT;

  /**
   * @deprecated See equivalent in {@link TezSplitGrouper}
   */
  @Deprecated
  public static final String TEZ_GROUPING_SPLIT_BY_COUNT = TezSplitGrouper.TEZ_GROUPING_SPLIT_BY_COUNT;
  /**
   * @deprecated See equivalent in {@link TezSplitGrouper}
   */
  @Deprecated
  public static final boolean TEZ_GROUPING_SPLIT_BY_COUNT_DEFAULT = TezSplitGrouper.TEZ_GROUPING_SPLIT_BY_COUNT_DEFAULT;

  /**
   * @deprecated See equivalent in {@link TezSplitGrouper}
   */
  @Deprecated
  public static final String TEZ_GROUPING_SPLIT_WAVES = TezSplitGrouper.TEZ_GROUPING_SPLIT_WAVES;
  /**
   * @deprecated See equivalent in {@link TezSplitGrouper}
   */
  @Deprecated
  public static final float TEZ_GROUPING_SPLIT_WAVES_DEFAULT = TezSplitGrouper.TEZ_GROUPING_SPLIT_WAVES_DEFAULT;

  /**
   * @deprecated See equivalent in {@link TezSplitGrouper}
   */
  @Deprecated
  public static final String TEZ_GROUPING_SPLIT_MAX_SIZE = TezSplitGrouper.TEZ_GROUPING_SPLIT_MAX_SIZE;
  /**
   * @deprecated See equivalent in {@link TezSplitGrouper}
   */
  @Deprecated
  public static final long TEZ_GROUPING_SPLIT_MAX_SIZE_DEFAULT = TezSplitGrouper.TEZ_GROUPING_SPLIT_MAX_SIZE_DEFAULT;

  /**
   * @deprecated See equivalent in {@link TezSplitGrouper}
   */
  @Deprecated
  public static final String TEZ_GROUPING_SPLIT_MIN_SIZE = TezSplitGrouper.TEZ_GROUPING_SPLIT_MIN_SIZE;
  /**
   * @deprecated See equivalent in {@link TezSplitGrouper}
   */
  @Deprecated
  public static final long TEZ_GROUPING_SPLIT_MIN_SIZE_DEFAULT = TezSplitGrouper.TEZ_GROUPING_SPLIT_MIN_SIZE_DEFAULT;

  /**
   * @deprecated See equivalent in {@link TezSplitGrouper}
   */
  @Deprecated
  public static final String TEZ_GROUPING_RACK_SPLIT_SIZE_REDUCTION = 
                                              TezSplitGrouper.TEZ_GROUPING_RACK_SPLIT_SIZE_REDUCTION;
  /**
   * @deprecated See equivalent in {@link TezSplitGrouper}
   */
  @Deprecated
  public static final float TEZ_GROUPING_RACK_SPLIT_SIZE_REDUCTION_DEFAULT = TezSplitGrouper.TEZ_GROUPING_RACK_SPLIT_SIZE_REDUCTION_DEFAULT;

  /**
   * @deprecated See equivalent in {@link TezSplitGrouper}
   */
  @Deprecated
  public static final String TEZ_GROUPING_REPEATABLE = TezSplitGrouper.TEZ_GROUPING_REPEATABLE;
  /**
   * @deprecated See equivalent in {@link TezSplitGrouper}
   */
  @Deprecated
  public static final boolean TEZ_GROUPING_REPEATABLE_DEFAULT = TezSplitGrouper.TEZ_GROUPING_REPEATABLE_DEFAULT;


  public List<InputSplit> getGroupedSplits(Configuration conf,
      List<InputSplit> originalSplits, int desiredNumSplits,
      String wrappedInputFormatName) throws IOException, InterruptedException {
    return getGroupedSplits(conf, originalSplits, desiredNumSplits,
        wrappedInputFormatName, null);
  }

  public List<InputSplit> getGroupedSplits(Configuration conf,
                                           List<InputSplit> originalSplits, int desiredNumSplits,
                                           String wrappedInputFormatName,
                                           SplitSizeEstimator estimator) throws IOException,
      InterruptedException {
    return getGroupedSplits(conf, originalSplits, desiredNumSplits, wrappedInputFormatName, estimator, null);
  }

  public List<InputSplit> getGroupedSplits(Configuration conf,
                                           List<InputSplit> originalSplits, int desiredNumSplits,
                                           String wrappedInputFormatName,
                                           SplitSizeEstimator estimator,
                                           SplitLocationProvider locationProvider) throws IOException,
      InterruptedException {
    Objects.requireNonNull(originalSplits, "Splits must be specified");
    List<SplitContainer> originalSplitContainers = Lists.transform(originalSplits,
        new Function<InputSplit, SplitContainer>() {
          @Override
          public SplitContainer apply(InputSplit input) {
            return new MapReduceSplitContainer(input);
          }
        });


    return Lists.transform(super
            .getGroupedSplits(conf, originalSplitContainers, desiredNumSplits,
                wrappedInputFormatName, estimator == null ? null :
                    new SplitSizeEstimatorWrapperMapReduce(estimator),
                locationProvider == null ? null :
                    new SplitLocationProviderMapReduce(locationProvider)),
        new Function<GroupedSplitContainer, InputSplit>() {
          @Override
          public InputSplit apply(GroupedSplitContainer input) {

            List<InputSplit> underlyingSplits = Lists.transform(input.getWrappedSplitContainers(),
                new Function<SplitContainer, InputSplit>() {
                  @Override
                  public InputSplit apply(SplitContainer input) {
                    return ((MapReduceSplitContainer) input).getRawSplit();
                  }
                });

            return new TezGroupedSplit(underlyingSplits, input.getWrappedInputFormatName(),
                input.getLocations(), input.getRack(), input.getLength());

          }
        });
  }

  /**
   * Builder that can be used to configure grouping in Tez
   *
   * @deprecated See {@link org.apache.tez.mapreduce.grouper.TezSplitGrouper.TezMRSplitsGrouperConfigBuilder#newConfigBuilder(Configuration)}
   *
   * @param conf
   *          {@link Configuration} This will be modified in place. If
   *          configuration values may be changed at runtime via a config file
   *          then pass in a {@link Configuration} that is initialized from a
   *          config file. The parameters that are not overridden in code will
   *          be derived from the Configuration object.
   * @return {@link org.apache.hadoop.mapreduce.split.TezMapReduceSplitsGrouper.TezMRSplitsGrouperConfigBuilder}
   */
  @Deprecated
  public static TezMRSplitsGrouperConfigBuilder createConfigBuilder(Configuration conf) {
    return new TezMRSplitsGrouperConfigBuilder(conf);
  }

  /**
   * @deprecated See {@link org.apache.tez.mapreduce.grouper.TezSplitGrouper.TezMRSplitsGrouperConfigBuilder}
   */
  @Deprecated
  public static final class TezMRSplitsGrouperConfigBuilder {
    private final Configuration conf;

    /**
     * This configuration will be modified in place
     */
    private TezMRSplitsGrouperConfigBuilder(@Nullable Configuration conf) {
      if (conf == null) {
        conf = new Configuration(false);
      }
      this.conf = conf;
    }

    public TezMRSplitsGrouperConfigBuilder setGroupSplitCount(int count) {
      this.conf.setInt(TezSplitGrouper.TEZ_GROUPING_SPLIT_COUNT, count);
      return this;
    }

    public TezMRSplitsGrouperConfigBuilder setGroupSplitByCount(boolean enabled) {
      this.conf.setBoolean(TezSplitGrouper.TEZ_GROUPING_SPLIT_BY_COUNT, enabled);
      return this;
    }

    public TezMRSplitsGrouperConfigBuilder setGroupSplitByLength(boolean enabled) {
      this.conf.setBoolean(TezSplitGrouper.TEZ_GROUPING_SPLIT_BY_LENGTH, enabled);
      return this;
    }

    public TezMRSplitsGrouperConfigBuilder setGroupSplitWaves(float multiplier) {
      this.conf.setFloat(TezSplitGrouper.TEZ_GROUPING_SPLIT_WAVES, multiplier);
      return this;
    }

    public TezMRSplitsGrouperConfigBuilder setGroupingRackSplitSizeReduction(float rackSplitSizeReduction) {
      this.conf.setFloat(TezSplitGrouper.TEZ_GROUPING_RACK_SPLIT_SIZE_REDUCTION, rackSplitSizeReduction);
      return this;
    }

    /**
     * upper and lower bounds for the splits
     */
    public TezMRSplitsGrouperConfigBuilder setGroupingSplitSize(long lowerBound, long upperBound) {
      this.conf.setLong(TezSplitGrouper.TEZ_GROUPING_SPLIT_MIN_SIZE, lowerBound);
      this.conf.setLong(TezSplitGrouper.TEZ_GROUPING_SPLIT_MAX_SIZE, upperBound);
      return this;
    }

    public Configuration build() {
      return this.conf;
    }
  }
}
