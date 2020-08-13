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
package org.apache.tez.common;

import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * ClassLoader to allow addition of new paths to classpath in the runtime.
 *
 * It uses URLClassLoader with this class' classloader as parent classloader.
 * And hence first delegates the resource loading to parent and then to the URLs
 * added. The process must be setup to use by invoking setupTezClassLoader() which sets
 * the global TezClassLoader as current thread context class loader. All threads
 * created will inherit the classloader and hence will resolve the class/resource
 * from TezClassLoader.
 */
public class TezClassLoader extends URLClassLoader {
  private static final TezClassLoader INSTANCE;

  static {
    INSTANCE = AccessController.doPrivileged(new PrivilegedAction<TezClassLoader>() {
      public TezClassLoader run() {
        return new TezClassLoader();
      }
    });
  }

  private TezClassLoader() {
    super(new URL[] {}, TezClassLoader.class.getClassLoader());
  }

  public void addURL(URL url) {
    super.addURL(url);
  }

  public static TezClassLoader getInstance() {
    return INSTANCE;
  }

  public static void setupTezClassLoader() {
    Thread.currentThread().setContextClassLoader(INSTANCE);
  }
}
