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
package org.apache.tez.runtime.library.common.shuffle;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetcherWithInjectableErrors extends Fetcher {
  private static final Logger LOG = LoggerFactory.getLogger(FetcherWithInjectableErrors.class);

  private FetcherErrorTestingConfig fetcherErrorTestingConfig;
  private String srcNameTrimmed;

  protected FetcherWithInjectableErrors(FetcherCallback fetcherCallback, HttpConnectionParams params,
      FetchedInputAllocator inputManager, InputContext inputContext,
      JobTokenSecretManager jobTokenSecretManager, Configuration conf,
      RawLocalFileSystem localFs, LocalDirAllocator localDirAllocator, Path lockPath, boolean localDiskFetchEnabled,
      boolean sharedFetchEnabled, String localHostname, int shufflePort, boolean asyncHttp, boolean verifyDiskChecksum,
      boolean compositeFetch) {
    super(fetcherCallback, params, inputManager, inputContext, jobTokenSecretManager, conf,
        localFs, localDirAllocator, lockPath, localDiskFetchEnabled, sharedFetchEnabled, localHostname, shufflePort,
        asyncHttp, verifyDiskChecksum, compositeFetch);
    this.fetcherErrorTestingConfig = new FetcherErrorTestingConfig(conf, inputContext.getObjectRegistry());
    this.srcNameTrimmed = TezUtilsInternal.cleanVertexName(inputContext.getSourceVertexName());
    LOG.info("Initialized FetcherWithInjectableErrors with config: {}", fetcherErrorTestingConfig);
  }

  @Override
  protected void setupConnectionInternal(String host, Collection<InputAttemptIdentifier> attempts)
      throws IOException, InterruptedException {
    LOG.info("Checking if fetcher should fail for host: {} ...", host);
    for (InputAttemptIdentifier inputAttemptIdentifier : attempts) {
      if (fetcherErrorTestingConfig.shouldFail(host, srcNameTrimmed, inputAttemptIdentifier)) {
        throw new IOException(String.format(
            "FetcherWithInjectableErrors tester made failure for host: %s, input attempt: %s", host,
            inputAttemptIdentifier.getAttemptNumber()));
      }
    }
    super.setupConnectionInternal(host, attempts);
  }

  @Override
  public int hashCode() {
    return fetcherIdentifier;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    FetcherWithInjectableErrors other = (FetcherWithInjectableErrors) obj;
    if (fetcherIdentifier != other.fetcherIdentifier) {
      return false;
    }
    return true;
  }
}
