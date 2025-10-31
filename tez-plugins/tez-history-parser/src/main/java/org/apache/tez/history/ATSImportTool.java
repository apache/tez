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

package org.apache.tez.history;

import static org.apache.hadoop.classification.InterfaceStability.Evolving;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.common.Preconditions;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.history.parser.datamodel.Constants;
import org.apache.tez.history.parser.utils.Utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 * Simple tool which imports ATS data pertaining to a DAG (Dag, Vertex, Task, Attempt)
 * and creates a zip file out of it.
 *
 * usage:
 *
 * java -cp tez-history-parser-x.y.z-jar-with-dependencies.jar org.apache.tez.history.ATSImportTool
 *
 * OR
 *
 * HADOOP_CLASSPATH=$TEZ_HOME/*:$TEZ_HOME/lib/*:$HADOOP_CLASSPATH hadoop jar tez-history-parser-x.y.z.jar org.apache.tez.history.ATSImportTool
 *
 *
 * --yarnTimelineAddress <yarnTimelineAddress>  Optional. Yarn Timeline Address(e.g http://clusterATSNode:8188)
 * --batchSize <batchSize>       Optional. batch size for downloading data
 * --dagId <dagId>               DagId that needs to be downloaded
 * --downloadDir <downloadDir>   download directory where data needs to be downloaded
 * --help                        print help
 *
 * </pre>
 */
@Evolving
public class ATSImportTool extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(ATSImportTool.class);

  private static final String BATCH_SIZE = "batchSize";
  private static final int BATCH_SIZE_DEFAULT = 100;

  private static final String YARN_TIMELINE_SERVICE_ADDRESS = "yarnTimelineAddress";
  private static final String DAG_ID = "dagId";
  private static final String BASE_DOWNLOAD_DIR = "downloadDir";

  private static final String HTTPS_SCHEME = "https://";
  private static final String HTTP_SCHEME = "http://";

  private static final String VERTEX_QUERY_STRING = "%s/%s?limit=%s&primaryFilter=%s:%s";
  private static final String TASK_QUERY_STRING = "%s/%s?limit=%s&primaryFilter=%s:%s";
  private static final String TASK_ATTEMPT_QUERY_STRING = "%s/%s?limit=%s&primaryFilter=%s:%s";
  private static final String UTF8 = "UTF-8";

  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  private final int batchSize;
  private final String baseUri;
  private final String dagId;

  private final File zipFile;
  private final Client httpClient;
  private final TezDAGID tezDAGID;

  public ATSImportTool(String baseUri, String dagId, File downloadDir, int batchSize) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dagId), "dagId can not be null or empty");
    Preconditions.checkArgument(downloadDir != null, "downloadDir can not be null");
    tezDAGID = TezDAGID.fromString(dagId);

    this.baseUri = baseUri;
    this.batchSize = batchSize;
    this.dagId = dagId;

    this.httpClient = getHttpClient();

    this.zipFile = new File(downloadDir, this.dagId + ".zip");

    boolean result = downloadDir.mkdirs();
    LOG.trace("Result of creating dir {}={}", downloadDir, result);
    if (!downloadDir.exists()) {
      throw new IllegalArgumentException("dir=" + downloadDir + " does not exist");
    }

    LOG.info("Using baseURL={}, dagId={}, batchSize={}, downloadDir={}", baseUri, dagId,
        batchSize, downloadDir);
  }

  /**
   * Download data from ATS for specific DAG
   *
   * @throws Exception
   */
  private void download() throws Exception {
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(zipFile, false);
      ZipOutputStream zos = new ZipOutputStream(fos);
      downloadData(zos);
      IOUtils.closeQuietly(zos);
    } catch (Exception e) {
      LOG.error("Exception in download", e);
      throw e;
    } finally {
      if (httpClient != null) {
        httpClient.close();
      }
      IOUtils.closeQuietly(fos);
    }
  }

  /**
   * Download DAG data (DAG, Vertex, Task, TaskAttempts) from ATS and write to zip file
   *
   * @param zos
   * @throws TezException
   * @throws JSONException
   * @throws IOException
   */
  private void downloadData(ZipOutputStream zos) throws TezException, JSONException, IOException {
    JSONObject finalJson = new JSONObject();

    //Download application details (TEZ_VERSION etc)
    String tezAppId = "tez_" + tezDAGID.getApplicationId().toString();
    String tezAppUrl = String.format("%s/%s/%s", baseUri, Constants.TEZ_APPLICATION, tezAppId);
    JSONObject tezAppJson = getJsonRootEntity(tezAppUrl);
    finalJson.put(Constants.APPLICATION, tezAppJson);

    //Download dag
    String dagUrl = String.format("%s/%s/%s", baseUri, Constants.TEZ_DAG_ID, dagId);
    JSONObject dagRoot = getJsonRootEntity(dagUrl);

    // We have added dag extra info, if we find any from ATS we copy the info into dag object
    // extra info.
    String dagExtraInfoUrl = String.format("%s/%s/%s", baseUri, EntityTypes.TEZ_DAG_EXTRA_INFO,
        dagId);
    JSONObject dagExtraInfo = getJsonRootEntity(dagExtraInfoUrl);
    if (dagExtraInfo.has(Constants.OTHER_INFO)) {
      JSONObject dagOtherInfo = dagRoot.getJSONObject(Constants.OTHER_INFO);
      JSONObject extraOtherInfo = dagExtraInfo.getJSONObject(Constants.OTHER_INFO);
      @SuppressWarnings("unchecked")
      Iterator<String> iter = extraOtherInfo.keys();
      while (iter.hasNext()) {
        String key = iter.next();
        dagOtherInfo.put(key, extraOtherInfo.get(key));
      }
    }
    finalJson.put(Constants.DAG, dagRoot);

    //Create a zip entry with dagId as its name.
    ZipEntry zipEntry = new ZipEntry(dagId);
    zos.putNextEntry(zipEntry);
    //Write in formatted way
    IOUtils.write(finalJson.toString(4), zos, UTF8);

    //Download vertex
    String vertexURL =
        String.format(VERTEX_QUERY_STRING, baseUri,
            Constants.TEZ_VERTEX_ID, batchSize, Constants.TEZ_DAG_ID, dagId);
    downloadJSONArrayFromATS(vertexURL, zos, Constants.VERTICES);

    //Download task
    String taskURL = String.format(TASK_QUERY_STRING, baseUri,
        Constants.TEZ_TASK_ID, batchSize, Constants.TEZ_DAG_ID, dagId);
    downloadJSONArrayFromATS(taskURL, zos, Constants.TASKS);

    //Download task attempts
    String taskAttemptURL = String.format(TASK_ATTEMPT_QUERY_STRING, baseUri,
        Constants.TEZ_TASK_ATTEMPT_ID, batchSize, Constants.TEZ_DAG_ID, dagId);
    downloadJSONArrayFromATS(taskAttemptURL, zos, Constants.TASK_ATTEMPTS);
  }

  /**
   * Download data from ATS in batches
   *
   * @param url
   * @param zos
   * @param tag
   * @throws IOException
   * @throws TezException
   * @throws JSONException
   * @throws NullPointerException if {@code zos} is {@code null}
   */
  private void downloadJSONArrayFromATS(String url, ZipOutputStream zos, String tag)
      throws IOException, TezException, JSONException {

    Objects.requireNonNull(zos, "ZipOutputStream can not be null");

    String baseUrl = url;
    JSONArray entities;

    long downloadedCount = 0;
    while ((entities = getJsonRootEntity(url).optJSONArray(Constants.ENTITIES)) != null
        && entities.length() > 0) {

      int limit = (entities.length() >= batchSize) ? (entities.length() - 1) : entities.length();
      LOG.debug("Limit={}, downloaded entities len={}", limit, entities.length());

      //write downloaded part to zipfile.  This is done to avoid any memory pressure when
      // downloading and writing 1000s of tasks.
      String zipEntryName = "part-" + System.nanoTime() + ".json";
      ZipEntry zipEntry = new ZipEntry(zipEntryName);
      LOG.debug("Putting {} entities to a zip entry: {}", entities.length(), zipEntryName);
      zos.putNextEntry(zipEntry);
      JSONObject finalJson = new JSONObject();
      finalJson.put(tag, entities);
      IOUtils.write(finalJson.toString(4), zos, "UTF-8");
      downloadedCount += entities.length();

      if (entities.length() < batchSize) {
        break;
      }

      //Set the last item in entities as the fromId
      url = baseUrl + "&fromId="
          + entities.getJSONObject(entities.length() - 1).getString(Constants.ENTITY);

      String firstItem = entities.getJSONObject(0).getString(Constants.ENTITY);
      String lastItem = entities.getJSONObject(entities.length() - 1).getString(Constants.ENTITY);
      LOG.info("Downloaded={}, First item={}, LastItem={}, new url={}", downloadedCount,
          firstItem, lastItem, url);
    }
  }

  private void logErrorMessage(Response response) {
    LOG.error("Response status={}", Integer.toString(response.getStatus()));
    try {
      String entity = response.readEntity(String.class);
      if (entity != null) {
        LOG.error(entity);
      }
    } catch (Exception ignore) {
      // ignore
    }
  }

  //For secure cluster, this should work as long as valid ticket is available in the node.
  private JSONObject getJsonRootEntity(String url) throws TezException, IOException {
    try {
      WebTarget target = getHttpClient().target(url);
      Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
          .accept(MediaType.APPLICATION_JSON_TYPE)
          .get();

      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        // In the case of secure cluster, if there is any auth exception it sends the data back as
        // a html page and JSON parsing could throw exceptions. Instead, get the stream contents
        // completely and log it in case of error.
        logErrorMessage(response);
        throw new TezException("Failed to get response from YARN Timeline: url: " + url);
      }
      String json = response.readEntity(String.class);
      return new JSONObject(json);
    } catch (Exception e) {
      throw new TezException("Error processing response from YARN Timeline. URL=" + url, e);
    }
  }

  private Client getHttpClient() {
    if (httpClient == null) {
      return ClientBuilder.newClient();
    }
    return httpClient;
  }

  @Override
  public int run(String[] args) throws Exception {
    try {
      download();
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("Error occurred when downloading data ", e);
      return -1;
    }
  }

  private static Options buildOptions() {
    Option dagIdOption = Option.builder().argName(DAG_ID).longOpt(DAG_ID)
        .desc("DagId that needs to be downloaded").hasArg().required(true).build();

    Option downloadDirOption = Option.builder().argName(BASE_DOWNLOAD_DIR).longOpt
        (BASE_DOWNLOAD_DIR)
        .desc("Download directory where data needs to be downloaded").hasArg()
        .required(true).build();

    Option atsAddressOption = Option.builder().argName(YARN_TIMELINE_SERVICE_ADDRESS).longOpt(
        YARN_TIMELINE_SERVICE_ADDRESS)
        .desc("Optional. ATS address (e.g http://clusterATSNode:8188)").hasArg()
        .required(false)
        .build();

    Option batchSizeOption = Option.builder().argName(BATCH_SIZE).longOpt(BATCH_SIZE)
        .desc("Optional. batch size for downloading data").hasArg()
        .required(false)
        .build();

    Option help = Option.builder().argName("help").longOpt("help")
        .desc("print help").required(false).build();

    Options opts = new Options();
    opts.addOption(dagIdOption);
    opts.addOption(downloadDirOption);
    opts.addOption(atsAddressOption);
    opts.addOption(batchSizeOption);
    opts.addOption(help);
    return opts;
  }

  static void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setWidth(240);
    String help = LINE_SEPARATOR
        + "java -cp tez-history-parser-x.y.z-jar-with-dependencies.jar org.apache.tez.history.ATSImportTool"
        + LINE_SEPARATOR
        + "OR"
        + LINE_SEPARATOR
        + "HADOOP_CLASSPATH=$TEZ_HOME/*:$TEZ_HOME/lib/*:$HADOOP_CLASSPATH hadoop jar "
        + "tez-history-parser-x.y.z.jar " + ATSImportTool.class.getName()
        + LINE_SEPARATOR;
    formatter.printHelp(240, help, "Options", options, "", true);
  }

  static boolean hasHttpsPolicy(Configuration conf) {
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    return (HttpConfig.Policy.HTTPS_ONLY == HttpConfig.Policy.fromString(yarnConf
        .get(YarnConfiguration.YARN_HTTP_POLICY_KEY, YarnConfiguration.YARN_HTTP_POLICY_DEFAULT)));
  }

  static String getBaseTimelineURL(String yarnTimelineAddress, Configuration conf)
      throws TezException {
    boolean isHttps = hasHttpsPolicy(conf);

    if (yarnTimelineAddress == null) {
      if (isHttps) {
        yarnTimelineAddress = conf.get(Constants.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS_CONF_NAME);
      } else {
        yarnTimelineAddress = conf.get(Constants.TIMELINE_SERVICE_WEBAPP_HTTP_ADDRESS_CONF_NAME);
      }
      Preconditions.checkArgument(!Strings.isNullOrEmpty(yarnTimelineAddress), "Yarn timeline address can"
          + " not be empty. Please check configurations.");
    } else {
      yarnTimelineAddress = yarnTimelineAddress.trim();
      Preconditions.checkArgument(!Strings.isNullOrEmpty(yarnTimelineAddress), "Yarn timeline address can"
          + " not be empty. Please provide valid url with --" +
          YARN_TIMELINE_SERVICE_ADDRESS + " option");
    }

    yarnTimelineAddress = yarnTimelineAddress.toLowerCase();
    if (!yarnTimelineAddress.startsWith(HTTP_SCHEME)
        && !yarnTimelineAddress.startsWith(HTTPS_SCHEME)) {
      yarnTimelineAddress = ((isHttps) ? HTTPS_SCHEME : HTTP_SCHEME) + yarnTimelineAddress;
    }

    try {
      yarnTimelineAddress = new URI(yarnTimelineAddress).normalize().toString().trim();
      yarnTimelineAddress = (yarnTimelineAddress.endsWith("/")) ?
          yarnTimelineAddress.substring(0, yarnTimelineAddress.length() - 1) :
          yarnTimelineAddress;
    } catch (URISyntaxException e) {
      throw new TezException("Please provide a valid URL. url=" + yarnTimelineAddress, e);
    }

    return Joiner.on("").join(yarnTimelineAddress, Constants.RESOURCE_URI_BASE);
  }

  @VisibleForTesting
  public static int process(String[] args) throws Exception {
    Options options = buildOptions();
    try {
      Configuration conf = new Configuration();
      CommandLine cmdLine = new DefaultParser().parse(options, args);
      String dagId = cmdLine.getOptionValue(DAG_ID);

      File downloadDir = new File(cmdLine.getOptionValue(BASE_DOWNLOAD_DIR));

      String yarnTimelineAddress = cmdLine.getOptionValue(YARN_TIMELINE_SERVICE_ADDRESS);
      String baseTimelineURL = getBaseTimelineURL(yarnTimelineAddress, conf);

      int batchSize = (cmdLine.hasOption(BATCH_SIZE)) ?
          (Integer.parseInt(cmdLine.getOptionValue(BATCH_SIZE))) : BATCH_SIZE_DEFAULT;

      return ToolRunner.run(conf, new ATSImportTool(baseTimelineURL, dagId,
          downloadDir, batchSize), args);
    } catch (ParseException e) {
      LOG.error("Error in parsing options ", e);
      printHelp(options);
      throw e;
    } catch (Exception e) {
      LOG.error("Error in processing ", e);
      throw e;
    }
  }

  public static void main(String[] args) throws Exception {
    Utils.setupRootLogger();
    int res = process(args);
    System.exit(res);
  }
}
