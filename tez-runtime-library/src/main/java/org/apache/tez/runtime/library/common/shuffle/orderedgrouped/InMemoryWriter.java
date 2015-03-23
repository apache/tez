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
package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.DataOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.IFileOutputStream;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InMemoryWriter extends Writer {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryWriter.class);

  // TODO Verify and fix counters if required.

  public InMemoryWriter(BoundedByteArrayOutputStream arrayStream) {
    super(null, null);
    this.out =
      new DataOutputStream(new IFileOutputStream(arrayStream));
  }

  public void append(Object key, Object value) throws IOException {
    throw new UnsupportedOperationException
    ("InMemoryWriter.append(K key, V value");
  }

  public void close() throws IOException {

    // write V_END_MARKER as needed
    writeValueMarker(out);

    // Write EOF_MARKER for key/value length
    WritableUtils.writeVInt(out, IFile.EOF_MARKER);
    WritableUtils.writeVInt(out, IFile.EOF_MARKER);

    // Close the stream
    out.close();
    out = null;
  }

}
