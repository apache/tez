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
package org.apache.tez.runtime.library.common.shuffle;

import java.util.Arrays;
import java.util.Collection;

import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;

/**
 * InputAttemptFetchFailure is supposed to wrap an InputAttemptIdentifier with any kind of failure
 * information during fetch. It can be useful for propagating as a single object instead of multiple
 * parameters (local fetch error, remote fetch error, connect failed, read failed, etc.).
 */
public class InputAttemptFetchFailure {

  private final InputAttemptIdentifier inputAttemptIdentifier;
  private final boolean isLocalFetch;
  private final boolean isDiskErrorAtSource;

  public InputAttemptFetchFailure(InputAttemptIdentifier inputAttemptIdentifier) {
    this(inputAttemptIdentifier, false, false);
  }

  public InputAttemptFetchFailure(InputAttemptIdentifier inputAttemptIdentifier,
      boolean isLocalFetch, boolean isDiskErrorAtSource) {
    this.inputAttemptIdentifier = inputAttemptIdentifier;
    this.isLocalFetch = isLocalFetch;
    this.isDiskErrorAtSource = isDiskErrorAtSource;
  }

  public InputAttemptIdentifier getInputAttemptIdentifier() {
    return inputAttemptIdentifier;
  }

  public boolean isLocalFetch() {
    return isLocalFetch;
  }

  public boolean isDiskErrorAtSource() {
    return isDiskErrorAtSource;
  }

  public static InputAttemptFetchFailure fromAttempt(InputAttemptIdentifier attempt) {
    return new InputAttemptFetchFailure(attempt, false, false);
  }

  public static InputAttemptFetchFailure fromLocalFetchFailure(InputAttemptIdentifier attempt) {
    return new InputAttemptFetchFailure(attempt, true, false);
  }

  public static InputAttemptFetchFailure fromDiskErrorAtSource(InputAttemptIdentifier attempt) {
    return new InputAttemptFetchFailure(attempt, false, true);
  }

  public static InputAttemptFetchFailure[] fromAttempts(Collection<InputAttemptIdentifier> values) {
    return values.stream().map(identifier -> new InputAttemptFetchFailure(identifier, false, false))
        .toArray(InputAttemptFetchFailure[]::new);
  }

  public static InputAttemptFetchFailure[] fromAttempts(InputAttemptIdentifier[] values) {
    return Arrays.asList(values).stream()
        .map(identifier -> new InputAttemptFetchFailure(identifier, false, false))
        .toArray(InputAttemptFetchFailure[]::new);
  }

  public static InputAttemptFetchFailure[] fromAttemptsLocalFetchFailure(
      Collection<InputAttemptIdentifier> values) {
    return values.stream().map(identifier -> new InputAttemptFetchFailure(identifier, true, false))
        .toArray(InputAttemptFetchFailure[]::new);
  }

  public static InputAttemptFetchFailure fromCompositeAttemptLocalFetchFailure(
      CompositeInputAttemptIdentifier compositeInputAttemptIdentifier) {
    return new InputAttemptFetchFailure(compositeInputAttemptIdentifier, true, false);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || (obj.getClass() != this.getClass())) {
      return false;
    }
    return inputAttemptIdentifier.equals(((InputAttemptFetchFailure) obj).inputAttemptIdentifier)
        && isLocalFetch == ((InputAttemptFetchFailure) obj).isLocalFetch
        && isDiskErrorAtSource == ((InputAttemptFetchFailure) obj).isDiskErrorAtSource;
  }

  @Override
  public int hashCode() {
    return 31 * inputAttemptIdentifier.hashCode() + 31 * (isLocalFetch ? 0 : 1)
        + 31 * (isDiskErrorAtSource ? 0 : 1);
  }

  @Override
  public String toString() {
    return String.format("%s, isLocalFetch: %s, isDiskErrorAtSource: %s",
        inputAttemptIdentifier.toString(), isLocalFetch, isDiskErrorAtSource);
  }
}
