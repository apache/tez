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

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
/**
 * Interface to represent both mapred and mapreduce splits
 */
public abstract class SplitContainer {

  private boolean isProcessed = false;


  public abstract String[] getPreferredLocations() throws IOException, InterruptedException;

  public abstract long getLength() throws IOException, InterruptedException;

  public boolean isProcessed() {
    return isProcessed;
  }

  public void setIsProcessed(boolean val) {
    this.isProcessed = val;
  }
}
