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
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.rm.NMCommunicatorEvent;

public class ContainerLauncherRouter extends AbstractService
    implements EventHandler<NMCommunicatorEvent> {

  static final Log LOG = LogFactory.getLog(ContainerLauncherImpl.class);

  private final ContainerLauncher containerLauncher;

  @VisibleForTesting
  public ContainerLauncherRouter(ContainerLauncher containerLauncher) {
    super(ContainerLauncherRouter.class.getName());
    this.containerLauncher = containerLauncher;
  }

  // Accepting conf to setup final parameters, if required.
  public ContainerLauncherRouter(Configuration conf, boolean isLocal, AppContext context,
                                 TaskAttemptListener taskAttemptListener,
                                 String workingDirectory) throws UnknownHostException {
    super(ContainerLauncherRouter.class.getName());

    if (isLocal) {
      LOG.info("Creating LocalContainerLauncher");
      containerLauncher =
          new LocalContainerLauncher(context, taskAttemptListener, workingDirectory);
    } else {
      // TODO: Temporary reflection with specific parameters until a clean interface is defined.
      String containerLauncherClassName =
          conf.get(TezConfiguration.TEZ_AM_CONTAINER_LAUNCHER_CLASS);
      if (containerLauncherClassName == null) {
        LOG.info("Creating Default Container Launcher");
        containerLauncher = new ContainerLauncherImpl(context);
      } else {
        LOG.info("Creating container launcher : " + containerLauncherClassName);
        Class<? extends ContainerLauncher> containerLauncherClazz =
            (Class<? extends ContainerLauncher>) ReflectionUtils.getClazz(
                containerLauncherClassName);
        try {
          Constructor<? extends ContainerLauncher> ctor = containerLauncherClazz
              .getConstructor(AppContext.class, Configuration.class, TaskAttemptListener.class);
          ctor.setAccessible(true);
          containerLauncher = ctor.newInstance(context, conf, taskAttemptListener);
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

    }
  }

  @Override
  public void serviceInit(Configuration conf) {
    ((AbstractService)containerLauncher).init(conf);
  }

  @Override
  public void serviceStart() {
    ((AbstractService)containerLauncher).start();
  }

  @Override
  public void serviceStop() {
    ((AbstractService)containerLauncher).stop();
  }


  @Override
  public void handle(NMCommunicatorEvent event) {
    containerLauncher.handle(event);
  }
}
