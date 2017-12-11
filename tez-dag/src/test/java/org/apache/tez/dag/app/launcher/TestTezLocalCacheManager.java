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

package org.apache.tez.dag.app.launcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class TestTezLocalCacheManager {

    @Test
    public void testManager() throws URISyntaxException, IOException {
        Map<String, LocalResource> resources = new HashMap<>();

        // Test that localization works for regular files and verify that if multiple symlinks are created,
        // they all work
        LocalResource resourceOne = createFile("content-one");
        LocalResource resourceTwo = createFile("content-two");

        resources.put("file-one", resourceOne);
        resources.put("file-two", resourceTwo);
        resources.put("file-three", resourceTwo);

        TezLocalCacheManager manager = new TezLocalCacheManager(resources, new Configuration());

        try {
            manager.localize();

            Assert.assertEquals(
                    "content-one",
                    new String(Files.readAllBytes(Paths.get("./file-one")))
            );

            Assert.assertEquals(
                    "content-two",
                    new String(Files.readAllBytes(Paths.get("./file-two")))
            );

            Assert.assertEquals(
                    "content-two",
                    new String(Files.readAllBytes(Paths.get("./file-three")))
            );
        } finally {
            manager.cleanup();
        }

        // verify that symlinks were removed
        Assert.assertFalse(Files.exists(Paths.get("./file-one")));
        Assert.assertFalse(Files.exists(Paths.get("./file-two")));
        Assert.assertFalse(Files.exists(Paths.get("./file-three")));
    }

    // create a temporary file with the given content and return a LocalResource
    private static LocalResource createFile(String content) throws IOException {
        FileContext fs = FileContext.getLocalFSFileContext();

        java.nio.file.Path tempFile = Files.createTempFile("test-cache-manager", ".txt");
        File temp = tempFile.toFile();
        temp.deleteOnExit();
        Path p = new Path("file:///" + tempFile.toAbsolutePath().toString());

        Files.write(tempFile, content.getBytes());

        RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
        LocalResource ret = recordFactory.newRecordInstance(LocalResource.class);
        URL yarnUrlFromPath = ConverterUtils.getYarnUrlFromPath(p);
        ret.setResource(yarnUrlFromPath);
        ret.setSize(content.getBytes().length);
        ret.setType(LocalResourceType.FILE);
        ret.setVisibility(LocalResourceVisibility.PRIVATE);
        ret.setTimestamp(fs.getFileStatus(p).getModificationTime());
        return ret;
    }
}
