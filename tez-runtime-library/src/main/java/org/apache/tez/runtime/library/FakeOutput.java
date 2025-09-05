package org.apache.tez.runtime.library;

import java.io.IOException;
import java.util.List;

import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.library.api.KeyValueWriter;

public class FakeOutput extends AbstractLogicalOutput {

  /**
   * Constructor an instance of the LogicalOutput. Classes extending this one to create a
   * LogicalOutput, must provide the same constructor so that Tez can create an instance of the
   * class at runtime.
   *
   * @param outputContext      the {@link OutputContext} which
   *                           provides
   *                           the Output with context information within the running task.
   * @param numPhysicalOutputs the number of physical outputs that the logical output will
   */
  public FakeOutput(OutputContext outputContext, int numPhysicalOutputs) {
    super(outputContext, numPhysicalOutputs);
  }

  @Override
  public List<Event> initialize() throws Exception {
    getContext().requestInitialMemory(0, null);
    return null;
  }

  @Override
  public void handleEvents(List<org.apache.tez.runtime.api.Event> outputEvents) {

  }

  @Override
  public List<org.apache.tez.runtime.api.Event> close() throws Exception {
    return null;
  }

  @Override
  public void start() throws Exception {

  }

  @Override
  public Writer getWriter() throws Exception {
    return new KeyValueWriter() {
      @Override
      public void write(Object key, Object value) throws IOException {
        System.out.println(key + " XXX " + value);
      }
    };
  }
}
