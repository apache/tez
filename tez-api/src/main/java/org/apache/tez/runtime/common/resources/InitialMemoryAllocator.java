package org.apache.tez.runtime.common.resources;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configurable;



/**
 * Used to balance memory requests before a task starts executing.
 */
@Private
public interface InitialMemoryAllocator extends Configurable {

  /**
   * @param availableForAllocation
   *          memory available for allocation
   * @param numTotalInputs
   *          number of inputs for the task
   * @param numTotalOutputs
   *          number of outputs for the tasks
   * @param requests
   *          Iterable view of requests received
   * @return list of allocations, one per request. This must be ordered in the
   *         same order of the requests.
   */
  public abstract Iterable<Long> assignMemory(long availableForAllocation, int numTotalInputs,
      int numTotalOutputs, Iterable<InitialMemoryRequestContext> requests);

}