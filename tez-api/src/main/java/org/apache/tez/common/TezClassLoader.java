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

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;

public class TezClassLoader extends URLClassLoader {
  private static TezClassLoader INSTANCE;

  static {
    INSTANCE = AccessController.doPrivileged(new PrivilegedAction<TezClassLoader>() {
      ClassLoader sysLoader = TezClassLoader.class.getClassLoader();

      public TezClassLoader run() {
        return new TezClassLoader(
            sysLoader instanceof URLClassLoader ? ((URLClassLoader) sysLoader).getURLs() : extractClassPathEntries(),
            sysLoader);
      }
    });
  }

  public TezClassLoader(URL[] urls, ClassLoader classLoader) {
    super(urls, classLoader);
  }

  public void addURL(URL url) {
    super.addURL(url);
  }

  public static TezClassLoader getInstance() {
    return INSTANCE;
  }

  private static URL[] extractClassPathEntries() {
    String pathSeparator = System.getProperty("path.separator");
    String[] classPathEntries = System.getProperty("java.class.path").split(pathSeparator);
    URL[] cp = Arrays.asList(classPathEntries).stream().map(s -> {
      try {
        return new URL("file://" + s);
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
    }).toArray(URL[]::new);
    return cp;
  }
}
