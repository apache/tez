package org.apache.tez.runtime.hook;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.records.TezTaskAttemptID;

/**
 * A hook which is instantiated and triggered before and after a task attempt is executed.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TezTaskAttemptHook {
  /**
   * Invoked before the task attempt starts.
   *
   * @param id the task attempt id
   * @param conf the conf
   */
  void start(TezTaskAttemptID id, Configuration conf);

  /**
   * Invoked after the task attempt finishes.
   */
  void stop();
}
