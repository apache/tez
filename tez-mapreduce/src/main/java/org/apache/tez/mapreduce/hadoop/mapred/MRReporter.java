package org.apache.tez.mapreduce.hadoop.mapred;

import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tez.engine.api.TezProcessorContext;
import org.apache.tez.engine.api.TezTaskContext;
import org.apache.tez.mapreduce.common.Utils;

public class MRReporter implements Reporter {

  private TezTaskContext tezTaskContext;
  private InputSplit split;
  private boolean isProcessorContext = false;
  
  public MRReporter(TezProcessorContext tezProcContext) {
    this(tezProcContext, null);
    isProcessorContext = true;
  }
  public MRReporter(TezTaskContext tezTaskContext) {
    this(tezTaskContext, null);
  }

  public MRReporter(TezTaskContext tezTaskContext, InputSplit split) {
    this.tezTaskContext = tezTaskContext;
    this.split = split;
  }
  
  @Override
  public void progress() {
    //TODO NEWTEZ
  }

  @Override
  public void setStatus(String status) {
    // Not setting status string in Tez.

  }

  @Override
  public Counter getCounter(Enum<?> name) {
    return Utils.getMRCounter(tezTaskContext.getCounters().findCounter(name));
  }

  @Override
  public Counter getCounter(String group, String name) {
    return Utils.getMRCounter(tezTaskContext.getCounters().findCounter(group,
        name));
  }

  @Override
  public void incrCounter(Enum<?> key, long amount) {
    getCounter(key).increment(amount);
  }

  @Override
  public void incrCounter(String group, String counter, long amount) {
    getCounter(group, counter).increment(amount);
  }

  @Override
  public InputSplit getInputSplit() throws UnsupportedOperationException {
    if (split == null) {
      throw new UnsupportedOperationException("Input only available on map");
    } else {
      return split;
    }
  }

  @Override
  public float getProgress() {
    // TOOD NEWTEZ Does this make a difference to anything ?
    return 0.0f;
  }

}
