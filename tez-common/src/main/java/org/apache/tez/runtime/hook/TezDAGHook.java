package org.apache.tez.runtime.hook;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.records.TezDAGID;

/**
 * A hook which is instantiated and triggered before and after a DAG is exeucted.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TezDAGHook {
  /**
   * Invoked before the DAG starts.
   *
   * @param id the DAG id
   * @param conf the conf
   */
  void start(TezDAGID id, Configuration conf);

  /**
   * Invoked after the DAG finishes.
   */
  void stop();
}
