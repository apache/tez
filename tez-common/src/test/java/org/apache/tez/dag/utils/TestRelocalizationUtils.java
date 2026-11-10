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
package org.apache.tez.dag.utils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Test;

public class TestRelocalizationUtils {

  @Test
  public void plainFileNameIsAccepted() {
    assertDoesNotThrow(() -> RelocalizationUtils.validateDestName("lib.jar"));
    assertDoesNotThrow(() -> RelocalizationUtils.validateDestName("some-name_1.tar.gz"));
  }

  @Test
  public void traversalIsRejected() {
    // Any path separator or parent-directory reference must be refused: those
    // are the shapes that let a submitter's key redirect the AM download to
    // a location outside its working directory.
    assertThrows(IllegalArgumentException.class,
        () -> RelocalizationUtils.validateDestName("../evil.jar"));
    assertThrows(IllegalArgumentException.class,
        () -> RelocalizationUtils.validateDestName("dir/child.jar"));
    assertThrows(IllegalArgumentException.class,
        () -> RelocalizationUtils.validateDestName("dir\\child.jar"));
    assertThrows(IllegalArgumentException.class,
        () -> RelocalizationUtils.validateDestName("/absolute/path.jar"));
    assertThrows(IllegalArgumentException.class,
        () -> RelocalizationUtils.validateDestName("."));
    assertThrows(IllegalArgumentException.class,
        () -> RelocalizationUtils.validateDestName(".."));
    assertThrows(IllegalArgumentException.class,
        () -> RelocalizationUtils.validateDestName(""));
    assertThrows(IllegalArgumentException.class,
        () -> RelocalizationUtils.validateDestName(null));
  }

  @Test
  public void validateDestNamesRejectsAnyBadNameInBatch() {
    // One bad name must fail the whole batch, before any download starts.
    assertDoesNotThrow(() -> RelocalizationUtils.validateDestNames(
        Arrays.asList("a.jar", "b.jar")));
    assertDoesNotThrow(() -> RelocalizationUtils.validateDestNames(null));
    assertDoesNotThrow(
        () -> RelocalizationUtils.validateDestNames(Collections.emptyList()));
    assertThrows(IllegalArgumentException.class,
        () -> RelocalizationUtils.validateDestNames(
            Arrays.asList("ok.jar", "../evil.jar")));
    assertThrows(IllegalArgumentException.class,
        () -> RelocalizationUtils.validateDestNames(
            Arrays.asList("a.jar", "/absolute.jar", "b.jar")));
  }

  /**
   * A URI-scheme opaque form such as {@code "file:.."} is rejected today: the
   * {@code new Path(destName)} call inside {@link RelocalizationUtils#validateDestName}
   * throws {@link IllegalArgumentException} from Hadoop's URI parser
   * ("Relative path in absolute URI"). And {@code new Path(cwd, "file:..")}
   * — the very next construction in {@code downloadResource} — throws
   * identically, so the value cannot reach {@code copyToLocalFile} and
   * cannot resolve outside {@code cwd}.
   */
  @Test
  public void uriSchemeOpaqueFormIsRejected() {
    for (String s : new String[]{"file:..", "file:../evil.jar", "file:evil.jar",
        "mailto:x", "C:evil.jar"}) {
      assertThrows(IllegalArgumentException.class,
          () -> RelocalizationUtils.validateDestName(s),
          "validateDestName should reject " + s);
      assertThrows(IllegalArgumentException.class,
          () -> new Path(new Path("/tmp/work"), s),
          "new Path(cwd, s) should also throw for " + s);
    }
  }
}
