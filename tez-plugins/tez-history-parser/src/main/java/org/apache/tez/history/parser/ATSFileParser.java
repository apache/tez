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

package org.apache.tez.history.parser;

import org.apache.tez.common.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.common.io.NonSyncByteArrayOutputStream;
import org.apache.tez.history.parser.datamodel.BaseParser;
import org.apache.tez.history.parser.datamodel.Constants;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.TaskInfo;
import org.apache.tez.history.parser.datamodel.VersionInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.apache.hadoop.classification.InterfaceAudience.Public;
import static org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Simple class to parse ATS zip file of a DAG and generate the relevant in-memory structure
 * (DagInfo) necessary for processing later.
 */

@Public
@Evolving
public class ATSFileParser extends BaseParser implements ATSData {

  private static final Logger LOG = LoggerFactory.getLogger(ATSFileParser.class);

  private final File atsZipFile;

  public ATSFileParser(List<File> files) throws TezException {
    super();
    Preconditions.checkArgument(checkFiles(files), "Zipfile " + files + " are empty or they don't exist");
    this.atsZipFile = files.get(0); //doesn't support multiple files at the moment
  }

  @Override
  public DagInfo getDAGData(String dagId) throws TezException {
    try {
      parseATSZipFile(atsZipFile);

      linkParsedContents();
      addRawDataToDagInfo();

      return dagInfo;
    } catch (IOException | JSONException e) {
      LOG.error("Error in reading DAG ", e);
      throw new TezException(e);
    } catch (InterruptedException e) {
      throw new TezException(e);
    }
  }

  /**
   * Parse vertices json
   *
   * @param verticesJson
   * @throws JSONException
   * @throws NullPointerException if {@code verticesJson} is {@code null}
   */
  private void processVertices(JSONArray verticesJson) throws JSONException {
    //Process vertex information
    Objects.requireNonNull(verticesJson, "Vertex json cannot be null");
    LOG.debug("Started parsing vertex");
    for (int i = 0; i < verticesJson.length(); i++) {
      VertexInfo vertexInfo = VertexInfo.create(verticesJson.getJSONObject(i));
      vertexList.add(vertexInfo);
    }
    LOG.debug("Finished parsing vertex");
  }

  /**
   * Parse Tasks json
   *
   * @param tasksJson
   * @throws JSONException
   * @throws NullPointerException if {@code verticesJson} is {@code null}
   */
  private void processTasks(JSONArray tasksJson) throws JSONException {
    //Process Task information
    Objects.requireNonNull(tasksJson, "Task json can not be null");
    LOG.debug("Started parsing task");
    for (int i = 0; i < tasksJson.length(); i++) {
      TaskInfo taskInfo = TaskInfo.create(tasksJson.getJSONObject(i));
      taskList.add(taskInfo);
    }
    LOG.debug("Finished parsing task");
  }

  /**
   * Parse TaskAttempt json
   *
   * @param taskAttemptsJson
   * @throws JSONException
   * @throws NullPointerException if {@code taskAttemptsJson} is {@code null}
   */
  private void processAttempts(JSONArray taskAttemptsJson) throws JSONException {
    //Process TaskAttempt information
    Objects.requireNonNull(taskAttemptsJson, "Attempts json can not be null");
    LOG.debug("Started parsing task attempts");
    for (int i = 0; i < taskAttemptsJson.length(); i++) {
      TaskAttemptInfo attemptInfo =
          TaskAttemptInfo.create(taskAttemptsJson.getJSONObject(i));
      attemptList.add(attemptInfo);
    }
    LOG.debug("Finished parsing task attempts");
  }

  /**
   * Parse TezApplication json
   *
   * @param tezApplicationJson
   * @throws JSONException
   */
  private void processApplication(JSONObject tezApplicationJson) throws JSONException {
    if (tezApplicationJson != null) {
      LOG.debug("Started parsing tez application");
      JSONObject otherInfoNode = tezApplicationJson.optJSONObject(Constants.OTHER_INFO);
      if (otherInfoNode != null) {
        JSONObject tezVersion = otherInfoNode.optJSONObject(Constants.TEZ_VERSION);
        if (tezVersion != null) {
          String version = tezVersion.optString(Constants.VERSION);
          String buildTime = tezVersion.optString(Constants.BUILD_TIME);
          String revision = tezVersion.optString(Constants.REVISION);
          this.versionInfo = new VersionInfo(version, revision, buildTime);
        }
        //Parse Config info
        this.config = Maps.newHashMap();
        JSONObject configNode = otherInfoNode.getJSONObject(Constants.CONFIG);
        Iterator it = configNode.keys();
        while(it.hasNext()) {
          String key = (String) it.next();
          String value = configNode.getString(key);
          config.put(key, value);
        }
      }
      LOG.debug("Finished parsing tez application");
    }
  }

  private JSONObject readJson(InputStream in) throws IOException, JSONException {
    //Read entire content to memory
    final NonSyncByteArrayOutputStream bout = new NonSyncByteArrayOutputStream();
    IOUtils.copy(in, bout);
    return new JSONObject(new String(bout.toByteArray(), "UTF-8"));
  }

  /**
   * Read zip file contents. Every file can contain "dag", "vertices", "tasks", "task_attempts"
   *
   * @param atsFile
   * @throws IOException
   * @throws JSONException
   */
  private void parseATSZipFile(File atsFile)
      throws IOException, JSONException, TezException, InterruptedException {
    final ZipFile atsZipFile = new ZipFile(atsFile);
    try {
      Enumeration<? extends ZipEntry> zipEntries = atsZipFile.entries();
      while (zipEntries.hasMoreElements()) {
        ZipEntry zipEntry = zipEntries.nextElement();
        LOG.debug("Processing " + zipEntry.getName());
        InputStream inputStream = atsZipFile.getInputStream(zipEntry);
        JSONObject jsonObject = readJson(inputStream);

        //This json can contain dag, vertices, tasks, task_attempts
        JSONObject dagJson = jsonObject.optJSONObject(Constants.DAG);
        if (dagJson != null) {
          //TODO: support for multiple dags per ATS file later.
          dagInfo = DagInfo.create(dagJson);
        }

        //Process vertex
        JSONArray vertexJson = jsonObject.optJSONArray(Constants.VERTICES);
        if (vertexJson != null) {
          processVertices(vertexJson);
        }

        //Process task
        JSONArray taskJson = jsonObject.optJSONArray(Constants.TASKS);
        if (taskJson != null) {
          processTasks(taskJson);
        }

        //Process task attempts
        JSONArray attemptsJson = jsonObject.optJSONArray(Constants.TASK_ATTEMPTS);
        if (attemptsJson != null) {
          processAttempts(attemptsJson);
        }

        //Process application (mainly versionInfo)
        JSONObject tezAppJson = jsonObject.optJSONObject(Constants.APPLICATION);
        if (tezAppJson != null) {
          processApplication(tezAppJson);
        }
      }
    } finally {
      atsZipFile.close();
    }
  }
}
