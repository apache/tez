/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app.launcher;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.rm.NMCommunicatorEvent;

public class ContainerLauncherRouter extends AbstractService
    implements EventHandler<NMCommunicatorEvent> {

  static final Log LOG = LogFactory.getLog(ContainerLauncherImpl.class);

  private final ContainerLauncher containerLaunchers[];

  @VisibleForTesting
  public ContainerLauncherRouter(ContainerLauncher containerLauncher) {
    super(ContainerLauncherRouter.class.getName());
    containerLaunchers = new ContainerLauncher[] {containerLauncher};
  }

  // Accepting conf to setup final parameters, if required.
  public ContainerLauncherRouter(Configuration conf, AppContext context,
                                 TaskAttemptListener taskAttemptListener,
                                 String workingDirectory,
                                 String[] containerLauncherClassIdentifiers,
                                 boolean isPureLocalMode) throws UnknownHostException {
    super(ContainerLauncherRouter.class.getName());

    if (containerLauncherClassIdentifiers == null || containerLauncherClassIdentifiers.length == 0) {
      if (isPureLocalMode) {
        containerLauncherClassIdentifiers =
            new String[]{TezConstants.TEZ_AM_SERVICE_PLUGINS_LOCAL_MODE_NAME_DEFAULT};
      } else {
        containerLauncherClassIdentifiers =
            new String[]{TezConstants.TEZ_AM_SERVICE_PLUGINS_NAME_DEFAULT};
      }
    }
    containerLaunchers = new ContainerLauncher[containerLauncherClassIdentifiers.length];

    for (int i = 0; i < containerLauncherClassIdentifiers.length; i++) {
      containerLaunchers[i] = createContainerLauncher(containerLauncherClassIdentifiers[i], context,
          taskAttemptListener, workingDirectory, isPureLocalMode, conf);
    }
  }

  private ContainerLauncher createContainerLauncher(String containerLauncherClassIdentifier,
                                                    AppContext context,
                                                    TaskAttemptListener taskAttemptListener,
                                                    String workingDirectory,
                                                    boolean isPureLocalMode,
                                                    Configuration conf) throws
      UnknownHostException {
    if (containerLauncherClassIdentifier.equals(TezConstants.TEZ_AM_SERVICE_PLUGINS_NAME_DEFAULT)) {
      LOG.info("Creating DefaultContainerLauncher");
      return new ContainerLauncherImpl(context);
    } else if (containerLauncherClassIdentifier
        .equals(TezConstants.TEZ_AM_SERVICE_PLUGINS_LOCAL_MODE_NAME_DEFAULT)) {
      LOG.info("Creating LocalContainerLauncher");
      return
          new LocalContainerLauncher(context, taskAttemptListener, workingDirectory, isPureLocalMode);
    } else {
      LOG.info("Creating container launcher : " + containerLauncherClassIdentifier);
      Class<? extends ContainerLauncher> containerLauncherClazz =
          (Class<? extends ContainerLauncher>) ReflectionUtils.getClazz(
              containerLauncherClassIdentifier);
      try {
        Constructor<? extends ContainerLauncher> ctor = containerLauncherClazz
            .getConstructor(AppContext.class, Configuration.class, TaskAttemptListener.class);
        ctor.setAccessible(true);
        return ctor.newInstance(context, conf, taskAttemptListener);
      } catch (NoSuchMethodException e) {
        throw new TezUncheckedException(e);
      } catch (InvocationTargetException e) {
        throw new TezUncheckedException(e);
      } catch (InstantiationException e) {
        throw new TezUncheckedException(e);
      } catch (IllegalAccessException e) {
        throw new TezUncheckedException(e);
      }
    }
    // TODO TEZ-2118 Handle routing to multiple launchers
  }

  @Override
  public void serviceInit(Configuration conf) {
    for (int i = 0 ; i < containerLaunchers.length ; i++) {
      ((AbstractService) containerLaunchers[i]).init(conf);
    }
  }

  @Override
  public void serviceStart() {
    for (int i = 0 ; i < containerLaunchers.length ; i++) {
      ((AbstractService) containerLaunchers[i]).start();
    }
  }

  @Override
  public void serviceStop() {
    for (int i = 0 ; i < containerLaunchers.length ; i++) {
      ((AbstractService) containerLaunchers[i]).stop();
    }
  }


  @Override
  public void handle(NMCommunicatorEvent event) {
    containerLaunchers[event.getLauncherId()].handle(event);
  }
}
