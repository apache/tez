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

package org.apache.tez.common;

import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class StreamHelper {

  private static final Logger LOG = LoggerFactory.getLogger(StreamHelper.class);

  private StreamHelper() {
  }

  public static void hflushIfSupported(Syncable syncable) throws IOException {
    if (syncable instanceof StreamCapabilities) {
      if (((StreamCapabilities) syncable).hasCapability(StreamCapabilities.HFLUSH)) {
        syncable.hflush();
      } else {
        // it would be no-op, if hflush is not supported by a given writer.
        LOG.debug("skipping hflush, since the writer doesn't support it");
      }
    } else {
      // this is done for backward compatibility in order to make it work with
      // older versions of Hadoop.
      syncable.hflush();
    }
  }
}
