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
package org.apache.tez.runtime.library.testutils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleHeader;

public final class RuntimeTestUtils {

  private RuntimeTestUtils() {
  }

  public static DataInputStream shuffleHeaderToDataInput(ShuffleHeader header) throws IOException {
    ByteArrayOutputStream byteOutput = new ByteArrayOutputStream(1000);
    DataOutputStream output = new DataOutputStream(byteOutput);
    header.write(output);

    InputStream inputStream = new ByteArrayInputStream(byteOutput.toByteArray());
    DataInputStream input = new DataInputStream(inputStream);

    return input;
  }
}
