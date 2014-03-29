/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.common.RuntimeUtils;
import org.apache.tez.dag.api.TezException;
import org.junit.Test;

public class TestRuntimeUtils {

  @Test
  public void testAddResourceToClasspath() throws IOException, TezException {

    String rsrcName = "dummyfile.xml";
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    Path p = new Path(rsrcName);
    p = localFs.makeQualified(p);

    localFs.delete(p, false);

    try {
      URL loadedUrl = null;

      loadedUrl = Thread.currentThread().getContextClassLoader().getResource(rsrcName);
      assertNull(loadedUrl);

      // Add parent to classpath since we're not adding a jar
      assertTrue(localFs.createNewFile(p));
      String urlForm = p.toUri().toURL().toString();
      urlForm = urlForm.substring(0, urlForm.lastIndexOf('/') + 1);
      URL url = new URL(urlForm);

      RuntimeUtils.addResourcesToClasspath(Collections.singletonList(url));

      loadedUrl = Thread.currentThread().getContextClassLoader().getResource(rsrcName);

      assertNotNull(loadedUrl);
    } finally {
      localFs.delete(p, false);
    }
  }
}
