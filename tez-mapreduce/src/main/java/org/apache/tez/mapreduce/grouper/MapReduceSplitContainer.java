/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.mapreduce.grouper;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.mapreduce.InputSplit;

public class MapReduceSplitContainer extends SplitContainer {

  private final InputSplit inputSplit;

  public MapReduceSplitContainer(InputSplit inputSplit) {
    Objects.requireNonNull(inputSplit);
    this.inputSplit = inputSplit;
  }

  @Override
  public String[] getPreferredLocations() throws IOException, InterruptedException {
    return inputSplit.getLocations();
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return inputSplit.getLength();
  }

  public InputSplit getRawSplit() {
    return this.inputSplit;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MapReduceSplitContainer that = (MapReduceSplitContainer) o;

    return !(inputSplit != null ? !inputSplit.equals(that.inputSplit) : that.inputSplit != null);

  }

  @Override
  public int hashCode() {
    return inputSplit != null ? inputSplit.hashCode() : 0;
  }
}
