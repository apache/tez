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
package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetcherErrorTestingConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetcherOrderedGroupedWithInjectableErrors extends FetcherOrderedGrouped {
  private static final Logger LOG = LoggerFactory.getLogger(FetcherOrderedGroupedWithInjectableErrors.class);

  private FetcherErrorTestingConfig fetcherErrorTestingConfig;
  private String srcNameTrimmed;

  public FetcherOrderedGroupedWithInjectableErrors(HttpConnectionParams httpConnectionParams,
      ShuffleScheduler scheduler, FetchedInputAllocatorOrderedGrouped allocator, ExceptionReporter exceptionReporter,
      JobTokenSecretManager jobTokenSecretMgr, boolean ifileReadAhead, int ifileReadAheadLength, CompressionCodec codec,
      Configuration conf, RawLocalFileSystem localFs, boolean localDiskFetchEnabled, String localHostname,
      int shufflePort, String srcNameTrimmed, MapHost mapHost, TezCounter ioErrsCounter,
      TezCounter wrongLengthErrsCounter, TezCounter badIdErrsCounter, TezCounter wrongMapErrsCounter,
      TezCounter connectionErrsCounter, TezCounter wrongReduceErrsCounter, String applicationId, int dagId,
      boolean asyncHttp, boolean sslShuffle, boolean verifyDiskChecksum, boolean compositeFetch,
      ObjectRegistry objectRegistry) {
    super(httpConnectionParams, scheduler, allocator, exceptionReporter, jobTokenSecretMgr, ifileReadAhead,
        ifileReadAheadLength, codec, conf, localFs, localDiskFetchEnabled, localHostname, shufflePort, srcNameTrimmed,
        mapHost, ioErrsCounter, wrongLengthErrsCounter, badIdErrsCounter, wrongMapErrsCounter, connectionErrsCounter,
        wrongReduceErrsCounter, applicationId, dagId, asyncHttp, sslShuffle, verifyDiskChecksum, compositeFetch);
    this.fetcherErrorTestingConfig = new FetcherErrorTestingConfig(conf, objectRegistry);
    this.srcNameTrimmed = srcNameTrimmed;
    LOG.info("Initialized FetcherOrderedGroupedWithInjectableErrors with config: {}", fetcherErrorTestingConfig);
  }

  @Override
  protected void setupConnectionInternal(MapHost host, Collection<InputAttemptIdentifier> attempts)
      throws IOException, InterruptedException {
    LOG.info("Checking if fetcher should fail for host: {} ...", mapHost.getHost());
    for (InputAttemptIdentifier inputAttemptIdentifier : attempts) {
      if (fetcherErrorTestingConfig.shouldFail(mapHost.getHost(), srcNameTrimmed, inputAttemptIdentifier)) {
        throw new IOException(String.format(
            "FetcherOrderedGroupedWithInjectableErrors tester made failure for host: %s, input attempt: %s",
            mapHost.getHost(), inputAttemptIdentifier.getAttemptNumber()));
      }
    }
    super.setupConnectionInternal(host, attempts);
  }
}
