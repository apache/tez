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

import com.google.common.annotations.VisibleForTesting;

public class DataStatistics {
  /**
   * factor used to calculate confidence interval within 95%.
   */
  private static final double DEFAULT_CI_FACTOR = 1.96;

  private int count = 0;
  private double sum = 0;
  private double sumSquares = 0;

  public DataStatistics() {
  }

  public DataStatistics(double initNum) {
    this.count = 1;
    this.sum = initNum;
    this.sumSquares = initNum * initNum;
  }

  public synchronized void add(double newNum) {
    this.count++;
    this.sum += newNum;
    this.sumSquares += newNum * newNum;
  }

  @VisibleForTesting
  synchronized void updateStatistics(double old, double update) {
    this.sum += update - old;
    this.sumSquares += (update * update) - (old * old);
  }

  public synchronized double mean() {
    // when no data then mean estimate should be large
    //return count == 0 ? 0.0 : sum/count;
    return count == 0 ? Long.MAX_VALUE : sum/count;
  }

  public synchronized double var() {
    // E(X^2) - E(X)^2
    if (count <= 1) {
      return 0.0;
    }
    double mean = mean();
    return Math.max((sumSquares/count) - mean * mean, 0.0d);
  }

  public synchronized double std() {
    return Math.sqrt(this.var());
  }

  public synchronized double outlier(float sigma) {
    if (count != 0.0) {
      return mean() + std() * sigma;
    }

    // when no data available then outlier estimate should be large
    //return 0.0;
    return Long.MAX_VALUE;
  }

  public synchronized double count() {
    return count;
  }

  /**
   * calculates the mean value within 95% ConfidenceInterval. 1.96 is standard
   * for 95%.
   *
   * @return the mean value adding 95% confidence interval.
   */
  public synchronized double meanCI() {
    if (count <= 1) {
      return 0.0;
    }
    double currMean = mean();
    double currStd = std();
    return currMean + (DEFAULT_CI_FACTOR * currStd / Math.sqrt(count));
  }

  public String toString() {
    return "DataStatistics: count is " + count + ", sum is " + sum
        + ", sumSquares is " + sumSquares + " mean is " + mean()
        + " std() is " + std() + ", meanCI() is " + meanCI();
  }
}