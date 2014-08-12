package org.apache.tez.runtime.api.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import java.util.Map;

import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.InputReadyTracker;
import org.apache.tez.runtime.api.MergedLogicalInput;
import org.apache.tez.runtime.api.MergedInputContext;


public class TezMergedInputContextImpl implements MergedInputContext {

  private final UserPayload userPayload;
  private final String groupInputName;
  private final Map<String, MergedLogicalInput> groupInputsMap;
  private final InputReadyTracker inputReadyTracker;
  private final String[] workDirs;

  public TezMergedInputContextImpl(@Nullable UserPayload userPayload, String groupInputName,
                                   Map<String, MergedLogicalInput> groupInputsMap,
                                   InputReadyTracker inputReadyTracker, String[] workDirs) {
    checkNotNull(groupInputName, "groupInputName is null");
    checkNotNull(groupInputsMap, "input-group map is null");
    checkNotNull(inputReadyTracker, "inputReadyTracker is null");
    this.groupInputName = groupInputName;
    this.groupInputsMap = groupInputsMap;
    this.userPayload = userPayload == null ? new UserPayload(null) : userPayload;
    this.inputReadyTracker = inputReadyTracker;
    this.workDirs = workDirs;
  }

  @Override
  public UserPayload getUserPayload() {
    return userPayload;
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
