/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.runtime.internals.protocolPB;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.tez.dag.api.TezException;

@Private
public final class TezTaskUmbilicalProtocolUtils {

  private TezTaskUmbilicalProtocolUtils() {}

  /** Serialize a Writable into a protobuf ByteString. */
  public static ByteString serializeToByteString(Writable writable) throws IOException {
    if (writable == null) {
      return null;
    }
    ByteString.Output os = ByteString.newOutput();
    try (DataOutputStream dos = new DataOutputStream(os)) {
      writable.write(dos);
    }
    return os.toByteString();
  }

  /** Deserialize specific writable from bytes in a protobuf string. */
  public static void deserializeFromByteString(ByteString byteString, Writable writable)
      throws IOException {
    if (byteString == null || byteString.isEmpty()) {
      return;
    }
    try (InputStream is = byteString.newInput();
        DataInputStream dis = new DataInputStream(is)) {
      writable.readFields(dis);
    }
  }

  /*
   * Service invocation with exception translation.
   */

  @FunctionalInterface
  public interface ServiceCallable<T> {
    T call() throws ServiceException;
  }

  /** Invoke a service. Translates ServiceException to IOException. */
  public static <T> T service(ServiceCallable<T> callable) throws IOException {
    try {
      return callable.call();
    } catch (ServiceException e) {
      throw new IOException(e);
    }
  }

  @FunctionalInterface
  public interface ClientCallable<T> {
    T call() throws IOException, TezException;
  }

  /** Translates application exceptions (IOException, TezException) to RPC ServiceException. */
  public static <T> T translate(ClientCallable<T> callable) throws ServiceException {
    try {
      return callable.call();
    } catch (IOException | TezException e) {
      throw new ServiceException(e);
    }
  }
}
