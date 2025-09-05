package org.apache.tez.runtime.library;

import java.util.ArrayList;
import java.util.List;

import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;

public class FakeInputInitializer extends InputInitializer {

  private static final int srcParallelism = 1;

  /**
   * Constructor an instance of the InputInitializer. Classes extending this to create a
   * InputInitializer, must provide the same constructor so that Tez can create an instance of
   * the class at runtime.
   *
   * @param initializerContext initializer context which can be used to access the payload, vertex
   *                           properties, etc
   */
  public FakeInputInitializer(InputInitializerContext initializerContext) {
    super(initializerContext);
  }

  @Override
  public List<Event> initialize() throws Exception {
    List<org.apache.tez.runtime.api.Event> list = new ArrayList<>();
    list.add(InputConfigureVertexTasksEvent.create(srcParallelism, null, null));
    for (int i = 0; i < srcParallelism; i++) {
      list.add(InputDataInformationEvent.createWithObjectPayload(i, null));
    }
    return list;
  }

  @Override
  public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws Exception {

  }
}
