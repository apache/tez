package org.apache.tez.runtime.api.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tez.common.TezUserPayload;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.runtime.InputReadyTracker;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.MergedLogicalInput;
import org.apache.tez.runtime.api.TezMergedInputContext;


public class TezMergedInputContextImpl implements TezMergedInputContext {

  private final TezUserPayload userPayload;
  private final String groupInputName;
  private final Map<String, MergedLogicalInput> groupInputsMap;
  private final InputReadyTracker inputReadyTracker;
  private final String[] workDirs;

  public TezMergedInputContextImpl(@Nullable byte[] userPayload, String groupInputName,
                                   Map<String, MergedLogicalInput> groupInputsMap,
                                   InputReadyTracker inputReadyTracker, String[] workDirs) {
    checkNotNull(groupInputName, "groupInputName is null");
    checkNotNull(groupInputsMap, "input-group map is null");
    checkNotNull(inputReadyTracker, "inputReadyTracker is null");
    this.groupInputName = groupInputName;
    this.groupInputsMap = groupInputsMap;
    this.userPayload = DagTypeConverters.convertToTezUserPayload(userPayload);
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
    inputReadyTracker.setInputIsReady(groupInputsMap.get(groupInputName));
  }

  @Override
  public String[] getWorkDirs() {
    return workDirs;
  }

}
