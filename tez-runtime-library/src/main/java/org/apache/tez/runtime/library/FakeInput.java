package org.apache.tez.runtime.library;

import java.io.IOException;
import java.util.List;

import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.api.KeyValueReader;

public class FakeInput extends AbstractLogicalInput {

  private static final int numRecordPerSrc = 10;

  /**
   * Constructor an instance of the LogicalInput. Classes extending this one to create a
   * LogicalInput, must provide the same constructor so that Tez can create an instance of the
   * class at runtime.
   *
   * @param inputContext      the {@link InputContext} which provides
   *                          the Input with context information within the running task.
   * @param numPhysicalInputs the number of physical inputs that the logical input will
   */
  public FakeInput(InputContext inputContext, int numPhysicalInputs) {
    super(inputContext, numPhysicalInputs);
  }

  @Override
  public List<Event> initialize() throws Exception {
    getContext().requestInitialMemory(0, null);
    getContext().inputIsReady();
    return null;
  }

  @Override
  public void handleEvents(List<org.apache.tez.runtime.api.Event> inputEvents) throws Exception {

  }

  @Override
  public List<org.apache.tez.runtime.api.Event> close() throws Exception {
    return null;
  }

  @Override
  public void start() throws Exception {

  }

  @Override
  public Reader getReader() throws Exception {
    return new KeyValueReader() {
      String[] keys = new String[numRecordPerSrc];

      int i = -1;

      @Override
      public boolean next() throws IOException {
        if (i == -1) {
          for (int j = 0; j < numRecordPerSrc; j++) {
            keys[j] = ""+j;
          }
        }
        i++;
        return i < keys.length;
      }

      @Override
      public Object getCurrentKey() throws IOException {
        return keys[i];
      }

      @Override
      public Object getCurrentValue() throws IOException {
        return keys[i];
      }
    };
  }
}
