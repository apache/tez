package org.apache.tez.runtime.library.utils;

import org.apache.hadoop.util.Progress;

/*
 * thread unsafe version of hadoop's progress impl
 */
public final class LocalProgress extends Progress {
  private float currentProgress = Float.NaN;

  @Override
  public void set(float progress) {
    if (progress != currentProgress) {
      currentProgress = progress;
      // enter lock section
      super.set(progress);
    }
  }
}
