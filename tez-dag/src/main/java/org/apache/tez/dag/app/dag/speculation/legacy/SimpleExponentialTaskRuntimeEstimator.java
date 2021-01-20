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
package org.apache.tez.dag.app.dag.speculation.legacy;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.dag.impl.Task;
import org.apache.tez.dag.app.dag.impl.TaskAttempt;
import org.apache.tez.dag.app.dag.impl.Vertex;
import org.apache.tez.dag.app.dag.speculation.legacy.forecast.SimpleExponentialSmoothing;
import org.apache.tez.dag.records.TezTaskAttemptID;

/**
 * A task Runtime Estimator based on exponential smoothing.
 */
public class SimpleExponentialTaskRuntimeEstimator extends StartEndTimesBase {
  /**
   * The default value returned by the estimator when no records exist.
   */
  private static final long DEFAULT_ESTIMATE_RUNTIME = -1L;

  /**
   * Given a forecast of value 0.0, it is getting replaced by the default value
   * to avoid division by 0.
   */
  private static final double DEFAULT_PROGRESS_VALUE = 1E-10;

  /**
   * Factor used to calculate the confidence interval.
   */
  private static final double CONFIDENCE_INTERVAL_FACTOR = 0.25;
  /**
   * Constant time used to calculate the smoothing exponential factor.
   */
  private long constTime;

  /**
   * Number of readings before we consider the estimate stable.
   * Otherwise, the estimate will be skewed due to the initial estimate
   */
  private int skipCount;

  /**
   * Time window to automatically update the count of the skipCount. This is
   * needed when a task stalls without any progress, causing the estimator to
   * return -1 as an estimatedRuntime.
   */
  private long stagnatedWindow;

  /**
   * A map of TA Id to the statistic model of smooth exponential.
   */
  private final ConcurrentMap<TezTaskAttemptID,
      AtomicReference<SimpleExponentialSmoothing>>
      estimates = new ConcurrentHashMap<>();

  private SimpleExponentialSmoothing getForecastEntry(
      final TezTaskAttemptID attemptID) {
    AtomicReference<SimpleExponentialSmoothing> entryRef = estimates
        .get(attemptID);
    if (entryRef == null) {
      return null;
    }
    return entryRef.get();
  }

  private void incorporateReading(final TezTaskAttemptID attemptID,
      final float newRawData, final long newTimeStamp) {
    SimpleExponentialSmoothing foreCastEntry = getForecastEntry(attemptID);
    if (foreCastEntry == null) {
      Long tStartTime = startTimes.get(attemptID);
      // skip if the startTime is not set yet
      if (tStartTime == null) {
        return;
      }
      estimates.putIfAbsent(attemptID,
          new AtomicReference<>(SimpleExponentialSmoothing.createForecast(
              constTime, skipCount, stagnatedWindow,
              tStartTime - 1)));
      incorporateReading(attemptID, newRawData, newTimeStamp);
      return;
    }
    foreCastEntry.incorporateReading(newTimeStamp, newRawData);
  }

  @Override
  public void contextualize(final Configuration conf, final Vertex vertex) {
    super.contextualize(conf, vertex);

    constTime
        = conf.getLong(TezConfiguration.TEZ_AM_ESTIMATOR_EXPONENTIAL_LAMBDA_MS,
        TezConfiguration.TEZ_AM_ESTIMATOR_EXPONENTIAL_LAMBDA_MS_DEFAULT);

    stagnatedWindow = Math.max(2 * constTime, conf.getLong(
        TezConfiguration.TEZ_AM_ESTIMATOR_EXPONENTIAL_STAGNATED_MS,
        TezConfiguration.TEZ_AM_ESTIMATOR_EXPONENTIAL_STAGNATED_MS_DEFAULT));

    skipCount = conf
        .getInt(TezConfiguration.TEZ_AM_ESTIMATOR_EXPONENTIAL_SKIP_INITIALS,
            TezConfiguration
                .TEZ_AM_ESTIMATOR_EXPONENTIAL_SKIP_INITIALS_DEFAULT);
  }

  @Override
  public long estimatedRuntime(final TezTaskAttemptID id) {
    SimpleExponentialSmoothing foreCastEntry = getForecastEntry(id);
    if (foreCastEntry == null) {
      return DEFAULT_ESTIMATE_RUNTIME;
    }
    double remainingWork =
        Math.max(0.0, Math.min(1.0, 1.0 - foreCastEntry.getRawData()));
    double forecast =
        Math.max(DEFAULT_PROGRESS_VALUE, foreCastEntry.getForecast());
    long remainingTime = (long) (remainingWork / forecast);
    long estimatedRuntime =
        remainingTime + foreCastEntry.getTimeStamp() - foreCastEntry.getStartTime();
    return estimatedRuntime;
  }

  @Override
  public long newAttemptEstimatedRuntime() {
    if (taskStatistics == null) {
      return DEFAULT_ESTIMATE_RUNTIME;
    }

    double statsMeanCI = taskStatistics.meanCI();
    double expectedVal =
        statsMeanCI + Math.min(statsMeanCI * CONFIDENCE_INTERVAL_FACTOR,
            taskStatistics.std() / 2);
    return (long) (expectedVal);
  }

  @Override
  public boolean hasStagnatedProgress(final TezTaskAttemptID id,
      final long timeStamp) {
    SimpleExponentialSmoothing foreCastEntry = getForecastEntry(id);
    if (foreCastEntry == null) {
      return false;
    }
    return foreCastEntry.isDataStagnated(timeStamp);
  }

  @Override
  public long runtimeEstimateVariance(final TezTaskAttemptID id) {
    SimpleExponentialSmoothing forecastEntry = getForecastEntry(id);
    if (forecastEntry == null) {
      return DEFAULT_ESTIMATE_RUNTIME;
    }
    double forecast = forecastEntry.getForecast();
    if (forecastEntry.isDefaultForecast(forecast)) {
      return DEFAULT_ESTIMATE_RUNTIME;
    }
    //TODO What is the best way to measure variance in runtime
    return 0L;
  }


  @Override
  public void updateAttempt(final TezTaskAttemptID attemptID,
      final TaskAttemptState state,
      final long timestamp) {
    super.updateAttempt(attemptID, state, timestamp);
    Task task = vertex.getTask(attemptID.getTaskID());
    if (task == null) {
      return;
    }
    TaskAttempt taskAttempt = task.getAttempt(attemptID);
    if (taskAttempt == null) {
      return;
    }
    float progress = taskAttempt.getProgress();
    incorporateReading(attemptID, progress, timestamp);
  }
}

