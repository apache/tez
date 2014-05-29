package org.apache.tez.runtime.task;

public interface ErrorReporter {

  void reportError(Throwable t);
  
  void shutdownRequested();
}
