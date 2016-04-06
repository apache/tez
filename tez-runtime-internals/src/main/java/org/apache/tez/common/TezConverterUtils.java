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

package org.apache.tez.common;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.internals.api.events.SystemEventProtos.TaskFailureTypeProto;

public class TezConverterUtils {

  /**
   * return a {@link URI} from a given url
   *
   * @param url
   *          url to convert
   * @return path from {@link URL}
   * @throws URISyntaxException
   */
  @Private
  public static URI getURIFromYarnURL(URL url) throws URISyntaxException {
    String scheme = url.getScheme() == null ? "" : url.getScheme();

    String authority = "";
    if (url.getHost() != null) {
      authority = url.getHost();
      if (url.getUserInfo() != null) {
        authority = url.getUserInfo() + "@" + authority;
      }
      if (url.getPort() > 0) {
        authority += ":" + url.getPort();
      }
    }

    return new URI(scheme, authority, url.getFile(), null, null).normalize();
  }

  @Private
  public static TezLocalResource convertYarnLocalResourceToTez(LocalResource lr)
      throws URISyntaxException {
    return new TezLocalResource(getURIFromYarnURL(lr.getResource()), lr.getSize(),
        lr.getTimestamp());
  }

  public static TaskFailureType failureTypeFromProto(TaskFailureTypeProto proto) {
    switch (proto) {
      case FT_NON_FATAL:
        return TaskFailureType.NON_FATAL;
      case FT_FATAL:
        return TaskFailureType.FATAL;
      default:
        throw new TezUncheckedException("Unknown FailureTypeProto: " + proto);
    }
  }

  public static TaskFailureTypeProto failureTypeToProto(TaskFailureType taskFailureType) {
    switch (taskFailureType) {

      case NON_FATAL:
        return TaskFailureTypeProto.FT_NON_FATAL;
      case FATAL:
        return TaskFailureTypeProto.FT_FATAL;
      default:
        throw new TezUncheckedException("Unknown FailureType: " + taskFailureType);
    }
  }

  // @Private
  // public static void writeLocalResource(LocalResource lr, DataOutput out)
  // throws IOException {
  // // Not bothering with type, visibility and pattern. Pattern may be needed
  // at
  // // some point.
  // // Size and length will not be required once Localization happens via YARN
  // -
  // // yarn takes care of validation, original file as well for that matter.
  // writeYARNURL(lr.getResource(), out);
  // out.writeLong(lr.getSize());
  // out.writeLong(lr.getTimestamp());
  // }
  //
  // @Private
  // public static LocalResource readLocalResource(DataInput in) throws
  // IOException {
  // return LocalResource.newInstance(readYARNURL(in), null, null,
  // in.readLong(), in.readLong());
  // }
  //
  // @Private
  // public static void writeYARNURL(URL url, DataOutput out) throws IOException
  // {
  // // Assuming all fields have to be present. Otherwise YARN itself would
  // fail.
  // out.writeUTF(url.getScheme());
  // out.writeUTF(url.getHost());
  // out.writeInt(url.getPort());
  // out.writeUTF(url.getFile());
  // }
  //
  // @Private
  // public static URL readYARNURL(DataInput in) throws IOException {
  // URL url = URL.newInstance(in.readUTF(), in.readUTF(), in.readInt(),
  // in.readUTF());
  // return url;
  // }

}
