package org.apache.tez.dag.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.runtime.TezThreadDumpHelper;
import org.apache.tez.runtime.hook.TezDAGHook;

/**
 * A DAG hook which dumps thread information periodically.
 */
public class ThreadDumpDAGHook implements TezDAGHook {
  private TezThreadDumpHelper helper;

  @Override
  public void start(TezDAGID id, Configuration conf) {
    helper = TezThreadDumpHelper.getInstance(conf).start(id.toString());
  }

  @Override
  public void stop() {
    helper.stop();
  }
}
