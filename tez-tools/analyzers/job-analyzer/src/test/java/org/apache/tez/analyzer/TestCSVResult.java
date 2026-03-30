/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.analyzer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCSVResult {

  private Path testDir;

  @Before
  public void setUp() throws IOException {
    testDir = Paths.get(System.getProperty("user.dir") + "/test");
    Files.createDirectories(testDir);
  }

  @After
  public void tearDown() throws IOException {
    if (Files.exists(testDir)) {
      Files.walk(testDir)
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(path -> path.toFile().delete());
    }
  }

  @Test
  public void testDumpToFileWritesContent() throws Exception {
    Path dir = Files.createDirectories(testDir.resolve("test1"));
    Path out = dir.resolve("out.csv");

    CSVResult result = new CSVResult(new String[]{"h1", "h2"});
    result.addRecord(new String[]{"a", "b"});
    result.dumpToFile(out.toString());

    String content = Files.readString(out, StandardCharsets.UTF_8);
    Assert.assertEquals("h1,h2\na,b\n", content);
  }

  @Test
  public void testDumpToFileRejectsExistingFile() throws Exception {
    Path out = testDir.resolve("existing.csv");
    Files.createFile(out);

    CSVResult result = new CSVResult(new String[]{"x"});

    try {
      result.dumpToFile(out.toString());
      Assert.fail("Expected FileAlreadyExistsException when output file already exists");
    } catch (FileAlreadyExistsException e) {
      Assert.assertTrue(e.getMessage().contains(out.toString()));
    }
  }

  @Test
  public void testDumpToFileRejectsMissingParentDir() throws Exception {
    Path missingParent = testDir.resolve("no-such-dir");
    Path out = missingParent.resolve("out.csv");

    CSVResult result = new CSVResult(new String[]{"x"});

    try {
      result.dumpToFile(out.toString());
      Assert.fail("Expected IOException when parent directory does not exist");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("Parent directory does not exist"));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDumpToFileRejectsNullFileName() throws Exception {
    new CSVResult(new String[]{"x"}).dumpToFile(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDumpToFileRejectsEmptyFileName() throws Exception {
    new CSVResult(new String[]{"x"}).dumpToFile("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDumpToFileRejectsBlankFileName() throws Exception {
    new CSVResult(new String[]{"x"}).dumpToFile("   ");
  }

  @Test
  public void testDumpToFileNestedDirectory() throws Exception {
    Path nested = testDir.resolve("a").resolve("b");
    Files.createDirectories(nested);

    Path out = nested.resolve("nested.csv");

    CSVResult result = new CSVResult(new String[]{"h"});
    result.addRecord(new String[]{"r"});
    result.dumpToFile(out.toString());

    Assert.assertEquals("h\nr\n", Files.readString(out, StandardCharsets.UTF_8));
  }

  @Test
  public void testDumpToFileNoWriteUpwardDirPath() throws Exception {
    String relativePath = testDir + "/../../out.csv";
    CSVResult result = new CSVResult(new String[]{"h"});

    try {
      result.dumpToFile(relativePath);
      Assert.fail("Expected IOException due to moving upwards from pwd");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains(Paths.get(relativePath).toAbsolutePath().normalize().toString()));
    }
  }
}
