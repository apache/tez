package org.apache.tez.runtime.library;

import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.OutputCommitterContext;

public class FakeOutputCommitter extends OutputCommitter {

  /**
   * Constructor an instance of the OutputCommitter. Classes extending this to create a
   * OutputCommitter, must provide the same constructor so that Tez can create an instance of
   * the class at runtime.
   *
   * @param committerContext committer context which can be used to access the payload, vertex
   *                         properties, etc
   */
  public FakeOutputCommitter(OutputCommitterContext committerContext) {
    super(committerContext);
  }

  @Override
  public void initialize() throws Exception {

  }

  @Override
  public void setupOutput() throws Exception {

  }

  @Override
  public void commitOutput() throws Exception {

  }

  @Override
  public void abortOutput(VertexStatus.State finalState) throws Exception {

  }
}
