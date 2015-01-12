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

package org.apache.tez.dag.utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.TezException;

import com.google.common.collect.Lists;

@InterfaceAudience.Private
public class RelocalizationUtils {
  
  public static List<URL> processAdditionalResources(Map<String, URI> additionalResources,
      Configuration conf, String destDir) throws IOException, TezException {
    if (additionalResources == null || additionalResources.isEmpty()) {
      return Collections.emptyList();
    }

    List<URL> urls = Lists.newArrayListWithCapacity(additionalResources.size());

    for (Entry<String, URI> lrEntry : additionalResources.entrySet()) {
      Path dFile = downloadResource(lrEntry.getKey(), lrEntry.getValue(), conf, destDir);
      urls.add(dFile.toUri().toURL());
    }
    return urls;
  }

  public static void addUrlsToClassPath(List<URL> urls) {
    ReflectionUtils.addResourcesToSystemClassLoader(urls);
  }

  private static Path downloadResource(String destName, URI uri, Configuration conf, String destDir)
      throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);
    Path cwd = new Path(destDir);
    Path dFile = new Path(cwd, destName);
    Path srcPath = new Path(uri);
    fs.copyToLocalFile(srcPath, dFile);
    return dFile.makeQualified(FileSystem.getLocal(conf).getUri(), cwd);
  }

  public static byte[] getLocalSha(Path path, Configuration conf) throws IOException {
    InputStream is = null;
    try {
      is = FileSystem.getLocal(conf).open(path);
      return DigestUtils.sha256(is);
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }

  public static byte[] getResourceSha(URI uri, Configuration conf) throws IOException {
    InputStream is = null;
    try {
      is = FileSystem.get(uri, conf).open(new Path(uri));
      return DigestUtils.sha256(is);
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }
}
