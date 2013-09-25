/**
 * <code>ShuffleMergedInput</code> in a {@link LogicalInput} which shuffles
 * intermediate sorted data, merges them and provides key/<values> to the
 * consumer.
 * 
 * The Copy and Merge will be triggered by the initialization - which is handled
 * by the Tez framework. Input is not consumable until the Copy and Merge are
 * complete. Methods are provided to check for this, as well as to wait for
 * completion. Attempting to get a reader on a non-complete input will block.
 * 
 */

package org.apache.tez.runtime.library.input;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;

@LimitedPrivate("mapreduce")
public class ShuffledMergedInputLegacy extends ShuffledMergedInput {

  @Private
  public TezRawKeyValueIterator getIterator() throws IOException, InterruptedException {
    // wait for input so that iterator is available
    waitForInputReady();
    return rawIter;
  }
}
