package org.apache.tez;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.events.UpdateCredentialsEvent;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemEventHandler {

  private static final Logger LOG = LoggerFactory
      .getLogger(LogicalIOProcessorRuntimeTask.class);

  private final UserGroupInformation umbilicalUGI;
  private UserGroupInformation taskUGI;
  private final boolean ownUmbilical;

  public SystemEventHandler(UserGroupInformation umbilicalUGI, boolean ownUmbilical,
      UserGroupInformation taskUGI) {
    this.umbilicalUGI = umbilicalUGI;
    this.taskUGI = taskUGI;
    this.ownUmbilical = ownUmbilical;
  }

  public void handleEvents(TezEvent event) {
    if (event.getEvent() instanceof UpdateCredentialsEvent) {
      taskUGI.addCredentials(((UpdateCredentialsEvent)event.getEvent()).getCredentials());

      LOG.info("Task credentials updated");
      if (ownUmbilical) {
        umbilicalUGI.addCredentials(((UpdateCredentialsEvent)event.getEvent()).getCredentials());
      }
    } else {
      LOG.warn("Unrecognized system event with class " + event.getClass().getName());
    }
  }

  public void setTaskUGI(UserGroupInformation taskUGI) {
    this.taskUGI = taskUGI;
  }
}
