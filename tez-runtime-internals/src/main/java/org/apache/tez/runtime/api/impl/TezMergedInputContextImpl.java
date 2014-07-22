package org.apache.tez.runtime.api.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import org.apache.tez.common.TezUserPayload;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.runtime.InputReadyTracker;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.TezMergedInputContext;


public class TezMergedInputContextImpl implements TezMergedInputContext {

  private final TezUserPayload userPayload;
  private final Input input;
  private final InputReadyTracker inputReadyTracker;
  private final String[] workDirs;

  public TezMergedInputContextImpl(@Nullable byte[] userPayload,
      Input input, InputReadyTracker inputReadyTracker, String[] workDirs) {
    checkNotNull(input, "input is null");
    checkNotNull(inputReadyTracker, "inputReadyTracker is null");
    this.userPayload = DagTypeConverters.convertToTezUserPayload(userPayload);
    this.input = input;
    this.inputReadyTracker = inputReadyTracker;
    this.workDirs = workDirs;
  }

  @Nullable
  @Override
  public byte[] getUserPayload() {
    return userPayload.getPayload();
  }
  
  @Override
  public void inputIsReady() {
    inputReadyTracker.setInputIsReady(input);
  }

  @Override
  public String[] getWorkDirs() {
    return workDirs;
  }

}
