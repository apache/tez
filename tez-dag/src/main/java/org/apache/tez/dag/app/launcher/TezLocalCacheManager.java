/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app.launcher;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.util.FSDownload;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for localizing files from the distributed cache for Tez local mode.
 */
public class TezLocalCacheManager {

  private static final Logger LOG = LoggerFactory.getLogger(TezLocalCacheManager.class);

  final private Map<String, LocalResource> resources;
  final private Configuration conf;
  final private UserGroupInformation ugi;
  final private FileContext fileContext;
  final private java.nio.file.Path tempDir;

  final private Map<LocalResource, ResourceInfo> resourceInfo = new HashMap<>();

  public TezLocalCacheManager(Map<String, LocalResource> resources, Configuration conf) throws IOException {
    this.ugi = UserGroupInformation.getCurrentUser();
    this.fileContext = FileContext.getLocalFSFileContext();
    this.resources = resources;
    this.conf = conf;
    this.tempDir = Files.createTempDirectory(Paths.get("."), "tez-local-cache");
  }

  /**
   * Localize this instance's resources by downloading and symlinking them.
   *
   * @throws IOException when an error occurs in download or link
   */
  public void localize() throws IOException {
    String absPath = Paths.get(".").toAbsolutePath().normalize().toString();
    Path cwd = fileContext.makeQualified(new Path(absPath));
    ExecutorService threadPool = null;

    try {
      // construct new threads with helpful names
      ThreadFactory threadFactory = new ThreadFactoryBuilder()
          .setNameFormat("TezLocalCacheManager Downloader #%d")
          .build();
      threadPool = Executors.newCachedThreadPool(threadFactory);

      // start all fetches
      for (Map.Entry<String, LocalResource> entry : resources.entrySet()) {
        String resourceName = entry.getKey();
        LocalResource resource = entry.getValue();

        if (resource.getType() == LocalResourceType.PATTERN) {
          throw new IllegalArgumentException("Resource type PATTERN not supported.");
        }

        // linkPath is the path we want to symlink the file/directory into
        Path linkPath = new Path(cwd, entry.getKey());

        if (resourceInfo.containsKey(resource)) {
            // We've already downloaded this resource and just need to add another link.
            resourceInfo.get(resource).linkPaths.add(linkPath);
        } else {
          // submit task to download the object
          java.nio.file.Path downloadDir = Files.createTempDirectory(tempDir, resourceName);
          Path dest = new Path(downloadDir.toAbsolutePath().toString());
          FSDownload downloader = new FSDownload(fileContext, ugi, conf, dest, resource);
          Future<Path> downloadedPath = threadPool.submit(downloader);
          resourceInfo.put(resource, new ResourceInfo(downloadedPath, linkPath));
        }
      }

      // Link each file
      for (Map.Entry<LocalResource, ResourceInfo> entry : resourceInfo.entrySet()) {
        LocalResource resource = entry.getKey();
        ResourceInfo resourceMeta = entry.getValue();

        for (Path linkPath : resourceMeta.linkPaths) {
          Path targetPath;

          try {
            // this blocks on the download completing
            targetPath = resourceMeta.downloadPath.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
          }

          if (createSymlink(targetPath, linkPath)) {
            LOG.info("Localized file: {} as {}", resource, linkPath);
          } else {
            LOG.warn("Failed to create symlink: {} <- {}", targetPath, linkPath);
          }
        }
      }
    } finally {
      if (threadPool != null) {
        threadPool.shutdownNow();
      }
    }
  }

  /**
   * Clean up any symlinks and temp files that were created.
   *
   * @throws IOException when an error occurs in cleanup
   */
  public void cleanup() throws IOException {
    for (ResourceInfo info : resourceInfo.values()) {
      for (Path linkPath : info.linkPaths) {
        if (fileContext.util().exists(linkPath)) {
          fileContext.delete(linkPath, true);
        }
      }
    }

    Path temp = new Path(tempDir.toString());
    if (fileContext.util().exists(temp)) {
      fileContext.delete(temp, true);
    }
  }

  /**
   * Create a symlink.
   */
  private boolean createSymlink(Path target, Path link) throws IOException {
    LOG.info("Creating symlink: {} <- {}", target, link);
    String targetPath = target.toUri().getPath();
    String linkPath = link.toUri().getPath();

    if (fileContext.util().exists(link)) {
      LOG.warn("File already exists at symlink path: {}", link);
      return false;
    } else {
      try {
        Files.createSymbolicLink(Paths.get(linkPath), Paths.get(targetPath));
        return true;
      } catch (UnsupportedOperationException e) {
        LOG.warn("Unable to create symlink {} <- {}: UnsupportedOperationException", target, link);
        return false;
      }
    }
  }

  /**
   * Wrapper to keep track of download path and link path
   */
  private static class ResourceInfo {
    final Future<Path> downloadPath;
    final Set<Path> linkPaths = new HashSet<>();

    public ResourceInfo(Future<Path> downloadPath, Path linkPath) {
      this.downloadPath = downloadPath;
      this.linkPaths.add(linkPath);
    }
  }
}
