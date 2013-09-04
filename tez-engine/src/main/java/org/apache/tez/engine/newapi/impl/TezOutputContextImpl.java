package org.apache.tez.engine.newapi.impl;

import java.util.List;

import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.engine.newapi.Event;

public class TezOutputContextImpl extends TezTaskContextImpl {

  private final byte[] userPayload;

  public TezOutputContextImpl(TezConfiguration tezConf, String vertexName,
      TezTaskAttemptID taskAttemptID, TezCounters counters,
      byte[] userPayload) {
    super(tezConf, vertexName, taskAttemptID, counters);
    this.userPayload = userPayload;
  }

  @Override
  public void sendEvents(List<Event> events) {
    // TODO Auto-generated method stub

  }

  @Override
  public byte[] getUserPayload() {
    return userPayload;
  }

}
