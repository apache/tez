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
package org.apache.tez.tools.webapp.service;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.ATSFileParser;
import org.apache.tez.history.parser.ProtoHistoryParser;
import org.apache.tez.history.parser.SimpleHistoryParser;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.common.io.Files;

@Service
public class EventDumpService {
  private final Logger LOG = LoggerFactory.getLogger(getClass());

  public List<JSONObject> getArrayOfAllEvents(String dagId, List<File> files) throws Exception {
    // .zip file is typically an ATS history file, let's try
    if ("zip".equals(Files.getFileExtension(files.get(0).getName()))) {
      LOG.info("Assuming ATS history file: {}", files.get(0));
      return new ATSFileParser(files).dumpAllEvents();
    } else {
      try {
        LOG.info("Trying as proto history files: {}", files);
        return new ProtoHistoryParser(files).dumpAllEvents();
      } catch (TezException e) {
        if (e.getCause() instanceof IOException && e.getCause().getMessage().contains("not a SequenceFile")) {
          LOG.info("Trying as simple history files: {}", files);
          return new SimpleHistoryParser(files).dumpAllEvents();
        } else {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
