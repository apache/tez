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
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestCSVResult {

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder();
  private static final Logger LOG = LoggerFactory.getLogger(TestCSVResult.class);

  @Test
  public void testDumpToFileWritesContent() throws Exception {
    Path out = tmpDir.newFolder().toPath().resolve("out.csv");
    CSVResult result = new CSVResult(new String[] { "h1", "h2" });
    result.addRecord(new String[] { "a", "b" });
    result.dumpToFile(out.toString());
    LOG.info("output file path is : {}", out.getFileName() );
    String content = new String(Files.readAllBytes(out), StandardCharsets.UTF_8);
    Assert.assertEquals("h1,h2\na,b\n", content);
  }

   @Test
   public void testDumpToFileRejectsExistingFile() throws Exception {
     Path out = tmpDir.newFile("existing.csv").toPath();
     CSVResult result = new CSVResult(new String[] { "x" });
     try {
       result.dumpToFile(out.toString());
       Assert.fail("Expected FileAlreadyExistsException when output file already exists");
     } catch (FileAlreadyExistsException e) {
       Assert.assertTrue("expected File Already Exist Exception",!e.getMessage().equals(out.toString()));
     }
   }

   @Test
   public void testDumpToFileRejectsMissingParentDir() throws Exception {
     Path missingParent = tmpDir.getRoot().toPath().resolve("no-such-dir");
     Path out = missingParent.resolve("out.csv");
     CSVResult result = new CSVResult(new String[] { "x" });
     try {
       result.dumpToFile(out.toString());
       Assert.fail("Expected IOException when parent directory does not exist");
     } catch (IOException e) {
       Assert.assertTrue(e.getMessage().contains("Parent directory does not exist"));
     }
   }

   @Test(expected = IllegalArgumentException.class)
   public void testDumpToFileRejectsNullFileName() throws Exception {
     new CSVResult(new String[] { "x" }).dumpToFile(null);
   }

   @Test(expected = IllegalArgumentException.class)
   public void testDumpToFileRejectsEmptyFileName() throws Exception {
     new CSVResult(new String[] { "x" }).dumpToFile("");
   }

   @Test(expected = IllegalArgumentException.class)
   public void testDumpToFileRejectsBlankFileName() throws Exception {
     new CSVResult(new String[] { "x" }).dumpToFile("   ");
   }

   @Test
   public void testDumpToFileNestedDirectory() throws Exception {
     Path base = tmpDir.newFolder().toPath();
     Path nested = base.resolve("a").resolve("b");
     Files.createDirectories(nested);
     Path out = nested.resolve("nested.csv");
     CSVResult result = new CSVResult(new String[] { "h" });
     result.addRecord(new String[] { "r" });
     result.dumpToFile(out.toString());
     Assert.assertEquals("h\nr\n", new String(Files.readAllBytes(out), StandardCharsets.UTF_8));
   }

  @Test
  public void testDumpToFileNoWritePermissionRelativePath() throws Exception {
    Path dir = tmpDir.newFolder("noWriteDir").toPath();
    dir.toFile().setWritable(false);
    Path out = dir.resolve("out.csv");
    String relativePath = dir + "/../noWriteDir/out.csv";
    CSVResult result = new CSVResult(new String[] { "h" });

    try {
      result.dumpToFile(relativePath);
      Assert.fail("Expected AccessDeniedException due to no write permission");
    } catch (AccessDeniedException e) {
      Assert.assertTrue(
          "Expected permission related error",
          e.getMessage() == null || !e.getMessage().equals(out.toString())
      );
    } finally {
      dir.toFile().setWritable(true);
    }
  }
}
