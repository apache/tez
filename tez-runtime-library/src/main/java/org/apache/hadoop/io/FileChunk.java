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

package org.apache.hadoop.io;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.Path;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;

@Private
public class FileChunk implements Comparable<FileChunk> {

  private final long offset;
  private final long length;
  private final boolean isLocalFile;
  private final Path path;
  private final InputAttemptIdentifier identifier;

  public FileChunk(Path path, long offset, long length, boolean isLocalFile,
                   InputAttemptIdentifier identifier) {
    this.path = path;
    this.offset = offset;
    this.length = length;
    this.isLocalFile = isLocalFile;
    this.identifier = identifier;
    if (isLocalFile) {
      Preconditions.checkNotNull(identifier);
    }
  }

  public FileChunk(Path path, long offset, long length) {
    this(path, offset, length, false, null);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }

    FileChunk that = (FileChunk)o;
    return path.equals(that.path) && (offset == that.offset) && (length == that.length);
  }

  @Override
  public int hashCode() {
    int result = path.hashCode();
    result = 31 * result + (int) (offset ^ (offset >>> 32));
    result = 31 * result + (int) (length ^ (length >>> 32));
    return result;
  }

  @Override
  public int compareTo(FileChunk that) {
    int c = path.compareTo(that.path);
    if (c != 0) {
      return c;
    }

    long lc;
    lc = offset - that.offset;
    if (lc != 0) {
      return lc < 0 ? -1 : 1;
    }

    lc = length - that.length;
    if (lc != 0) {
      return lc < 0 ? -1 : 1;
    }

    return 0;
  }

  public long getOffset() {
    return offset;
  }

  public long getLength() {
    return length;
  }

  public Path getPath() {
    return path;
  }

  public boolean isLocalFile() {
    return this.isLocalFile;
  }

  public InputAttemptIdentifier getInputAttemptIdentifier() {
    return this.identifier;
  }
}
