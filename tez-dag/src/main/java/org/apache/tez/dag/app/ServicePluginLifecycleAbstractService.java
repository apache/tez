/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.common.ServicePluginLifecycle;

/**
 * Provides service lifecycle management over ServicePlugins using {@link AbstractService}
 * @param <T>
 */
public class ServicePluginLifecycleAbstractService<T extends ServicePluginLifecycle> extends AbstractService {

  private final T service;

  public ServicePluginLifecycleAbstractService(T service) {
    super(service.getClass().getName());
    this.service = service;
  }

  @Override
  public void serviceInit(Configuration unused) throws Exception {
    service.initialize();
  }

  @Override
  public void serviceStart() throws Exception {
    service.start();
  }

  @Override
  public void serviceStop() throws Exception {
    service.shutdown();
  }

  public T getService() {
    return service;
  }
}
