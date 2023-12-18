package org.apache.tez.runtime.task;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.TezThreadDumpHelper;
import org.apache.tez.runtime.hook.TezTaskAttemptHook;

/**
 * A task attempt hook which dumps thread information periodically.
 */
public class ThreadDumpTaskAttemptHook implements TezTaskAttemptHook {
  private TezThreadDumpHelper helper;

  @Override
  public void start(TezTaskAttemptID id, Configuration conf) {
    helper = TezThreadDumpHelper.getInstance(conf).start(id.toString());
  }

  @Override
  public void stop() {
    helper.stop();
  }
}
