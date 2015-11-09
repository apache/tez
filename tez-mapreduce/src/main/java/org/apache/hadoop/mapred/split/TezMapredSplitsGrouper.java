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
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.tez.mapreduce.grouper.GroupedSplitContainer;
import org.apache.tez.mapreduce.grouper.MapredSplitContainer;
import org.apache.tez.mapreduce.grouper.SplitContainer;
import org.apache.tez.mapreduce.grouper.SplitLocationProviderWrapperMapred;
import org.apache.tez.mapreduce.grouper.SplitSizeEstimatorWrapperMapred;
import org.apache.tez.mapreduce.grouper.TezSplitGrouper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;

/**
 * A Helper that provides grouping logic to group InputSplits
 * using various parameters. A {@link TezGroupedSplit} is used
 * to wrap the real InputSplits in a group.
 */
@Public
@Evolving
public class TezMapredSplitsGrouper extends TezSplitGrouper {
  private static final Logger LOG = LoggerFactory.getLogger(TezMapredSplitsGrouper.class);

  public InputSplit[] getGroupedSplits(Configuration conf,
      InputSplit[] originalSplits, int desiredNumSplits,
      String wrappedInputFormatName) throws IOException {
    return getGroupedSplits(conf, originalSplits, desiredNumSplits, wrappedInputFormatName, null);
  }

  public InputSplit[] getGroupedSplits(Configuration conf,
                                       InputSplit[] originalSplits, int desiredNumSplits,
                                       String wrappedInputFormatName,
                                       SplitSizeEstimator estimator) throws IOException {
    return getGroupedSplits(conf, originalSplits, desiredNumSplits, wrappedInputFormatName,
        estimator, null);
  }


  public InputSplit[] getGroupedSplits(Configuration conf,
      InputSplit[] originalSplits, int desiredNumSplits,
      String wrappedInputFormatName, SplitSizeEstimator estimator, SplitLocationProvider locationProvider) throws IOException {
    Preconditions.checkArgument(originalSplits != null, "Splits must be specified");

    List<SplitContainer> originalSplitContainers = Lists.transform(Arrays.asList(originalSplits),
        new Function<InputSplit, SplitContainer>() {
          @Override
          public SplitContainer apply(InputSplit input) {
            return new MapredSplitContainer(input);
          }
        });

    try {
      List<InputSplit> resultList = Lists.transform(super
              .getGroupedSplits(conf, originalSplitContainers, desiredNumSplits,
                  wrappedInputFormatName, estimator == null ? null :
                      new SplitSizeEstimatorWrapperMapred(estimator),
                  locationProvider == null ? null :
                      new SplitLocationProviderWrapperMapred(locationProvider)),
          new Function<GroupedSplitContainer, InputSplit>() {
            @Override
            public InputSplit apply(GroupedSplitContainer input) {
              List<InputSplit> underlyingSplits = Lists.transform(input.getWrappedSplitContainers(),
                  new Function<SplitContainer, InputSplit>() {
                    @Override
                    public InputSplit apply(SplitContainer input) {
                      return ((MapredSplitContainer) input).getRawSplit();
                    }
                  });


              return new TezGroupedSplit(underlyingSplits, input.getWrappedInputFormatName(),
                  input.getLocations(), input.getRack(), input.getLength());
            }
          });
      InputSplit[] resultArr = resultList.toArray(new InputSplit[resultList.size()]);
      return resultArr;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
