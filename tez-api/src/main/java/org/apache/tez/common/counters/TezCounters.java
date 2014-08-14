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
package org.apache.tez.common.counters;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * <p><code>Counters</code> holds per job/task counters, defined either by the
 * framework or applications. Each <code>Counter</code> can be of
 * any {@link Enum} type.</p>
 *
 * <p><code>Counters</code> are bunched into {@link CounterGroup}s, each
 * comprising of counters from a particular <code>Enum</code> class.
 */
@Public
@Unstable
public class TezCounters extends AbstractCounters<TezCounter, CounterGroup> {

  // Mix framework group implementation into CounterGroup interface
  private static class FrameworkGroupImpl<T extends Enum<T>>
      extends FrameworkCounterGroup<T, TezCounter> implements CounterGroup {

    FrameworkGroupImpl(Class<T> cls) {
      super(cls);
    }

    @Override
    protected FrameworkCounter<T> newCounter(T key) {
      return new FrameworkCounter<T>(key, getName());
    }

    @Override
    public CounterGroupBase<TezCounter> getUnderlyingGroup() {
      return this;
    }
  }

  // Mix generic group implementation into CounterGroup interface
  // and provide some mandatory group factory methods.
  private static class GenericGroup extends AbstractCounterGroup<TezCounter>
      implements CounterGroup {

    GenericGroup(String name, String displayName, Limits limits) {
      super(name, displayName, limits);
    }

    @Override
    protected TezCounter newCounter(String name, String displayName, long value) {
      return new GenericCounter(name, displayName, value);
    }

    @Override
    protected TezCounter newCounter() {
      return new GenericCounter();
    }

    @Override
    public CounterGroupBase<TezCounter> getUnderlyingGroup() {
      return this;
    }
  }

  // Mix file system group implementation into the CounterGroup interface
  private static class FileSystemGroup extends FileSystemCounterGroup<TezCounter>
      implements CounterGroup {

    @Override
    protected TezCounter newCounter(String scheme, FileSystemCounter key) {
      return new FSCounter(scheme, key);
    }

    @Override
    public CounterGroupBase<TezCounter> getUnderlyingGroup() {
      return this;
    }
  }

  /**
   * Provide factory methods for counter group factory implementation.
   * See also the GroupFactory in
   *  {@link org.apache.hadoop.TezCounters.Counters mapred.Counters}
   */
  private static class GroupFactory
      extends CounterGroupFactory<TezCounter, CounterGroup> {

    @Override
    protected <T extends Enum<T>>
    FrameworkGroupFactory<CounterGroup>
        newFrameworkGroupFactory(final Class<T> cls) {
      return new FrameworkGroupFactory<CounterGroup>() {
        @Override public CounterGroup newGroup(String name) {
          return new FrameworkGroupImpl<T>(cls); // impl in this package
        }
      };
    }

    @Override
    protected CounterGroup newGenericGroup(String name, String displayName,
                                           Limits limits) {
      return new GenericGroup(name, displayName, limits);
    }

    @Override
    protected CounterGroup newFileSystemGroup() {
      return new FileSystemGroup();
    }
  }

  private static final GroupFactory groupFactory = new GroupFactory();

  /**
   * Default constructor
   */
  public TezCounters() {
    super(groupFactory);
  }

  /**
   * Construct the Counters object from the another counters object
   * @param <C> the type of counter
   * @param <G> the type of counter group
   * @param counters the old counters object
   */
  public <C extends TezCounter, G extends CounterGroupBase<C>>
  TezCounters(AbstractCounters<C, G> counters) {
    super(counters, groupFactory);
  }
}
