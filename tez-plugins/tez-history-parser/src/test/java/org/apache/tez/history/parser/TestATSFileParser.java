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
package org.apache.tez.history.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.datamodel.DagInfo;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestATSFileParser {

  private static final String DAG_ID = "dag_1234567890_0001_1";

  private static JSONObject minimalDagJson() throws JSONException {
    JSONObject dag = new JSONObject();
    dag.put("entityId", DAG_ID);
    dag.put("entityType", "TEZ_DAG_ID");
    JSONObject otherInfo = new JSONObject();
    otherInfo.put("startTime", 1L);
    otherInfo.put("endTime", 2L);
    otherInfo.put("status", "SUCCEEDED");
    otherInfo.put("counters", new JSONObject().put("counterGroups", new JSONArray()));
    dag.put("otherInfo", otherInfo);
    return dag;
  }

  private static File writeZip(Path dir, String name, ZipContent... entries) throws Exception {
    File zip = dir.resolve(name).toFile();
    try (FileOutputStream fos = new FileOutputStream(zip);
         ZipOutputStream zos = new ZipOutputStream(fos)) {
      for (ZipContent entry : entries) {
        zos.putNextEntry(new ZipEntry(entry.name()));
        zos.write(entry.payload().getBytes(StandardCharsets.UTF_8));
        zos.closeEntry();
      }
    }
    return zip;
  }

  @Test
  public void parserSkipsEmptyZipEntryAndParsesRemaining(@TempDir Path tmp) throws Exception {
    JSONObject dagRoot = new JSONObject().put("dag", minimalDagJson());

    File zip = writeZip(tmp, "empty-then-good.zip",
        new ZipContent("empty-part.json", ""),
        new ZipContent("whitespace-part.json", "   \n\t  "),
        new ZipContent(DAG_ID, dagRoot.toString()));

    ATSFileParser parser = new ATSFileParser(Collections.singletonList(zip));
    DagInfo info = parser.getDAGData(DAG_ID);

    assertNotNull(info, "Parser should return DagInfo even when some entries are empty");
    assertEquals(DAG_ID, info.getDagId());
    assertEquals("SUCCEEDED", info.getStatus());
  }

  @Test
  public void parserReportsOffendingEntryOnMalformedJson(@TempDir Path tmp) throws Exception {
    // Simulates the timeline server returning an HTML error page instead of JSON.
    File zip = writeZip(tmp, "malformed.zip",
        new ZipContent(DAG_ID, "<html><body>internal error</body></html>"));

    ATSFileParser parser = new ATSFileParser(Collections.singletonList(zip));
    TezException thrown = assertThrows(TezException.class, () -> parser.getDAGData(DAG_ID));

    Throwable cause = thrown.getCause();
    assertNotNull(cause);
    String msg = cause.getMessage();
    assertNotNull(msg);
    assertTrue(msg.contains(DAG_ID),
        "Error should name the offending zip entry, got: " + msg);
    assertTrue(msg.contains("<html>"),
        "Error should include a snippet of the offending payload, got: " + msg);
  }

  private record ZipContent(String name, String payload) {
  }
}
