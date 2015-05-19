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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import com.sun.jersey.json.impl.provider.entity.JSONRootElementProvider;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.history.parser.datamodel.Constants;
import org.apache.tez.history.parser.utils.Utils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.apache.hadoop.classification.InterfaceStability.Evolving;

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

  private final File downloadDir;
  private final File zipFile;
  private final Client httpClient;

  public ATSImportTool(String baseUri, String dagId, File baseDownloadDir, int batchSize)
      throws TezException {
    Preconditions.checkArgument(!Strings.isEmpty(dagId), "dagId can not be null or empty");
    Preconditions.checkArgument(baseDownloadDir != null, "downloadDir can not be null");
    Preconditions.checkArgument(TezDAGID.fromString(dagId) != null, "Not a valid DAG ID " + dagId);

    this.baseUri = baseUri;
    this.batchSize = batchSize;
    this.dagId = dagId;

    this.httpClient = getHttpClient();

    this.downloadDir = new File(baseDownloadDir, dagId);
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
        httpClient.destroy();
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
    String tezAppId = "tez_" + (TezDAGID.fromString(dagId)).getApplicationId().toString();
    String tezAppUrl = String.format("%s/%s/%s", baseUri, Constants.TEZ_APPLICATION, tezAppId);
    JSONObject tezAppJson = getJsonRootEntity(tezAppUrl);
    finalJson.put(Constants.APPLICATION, tezAppJson);

    //Download dag
    String dagUrl = String.format("%s/%s/%s", baseUri, Constants.TEZ_DAG_ID, dagId);
    JSONObject dagRoot = getJsonRootEntity(dagUrl);
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
   */
  private void downloadJSONArrayFromATS(String url, ZipOutputStream zos, String tag)
      throws IOException, TezException, JSONException {

    Preconditions.checkArgument(zos != null, "ZipOutputStream can not be null");

    String baseUrl = url;
    JSONArray entities;

    long downloadedCount = 0;
    while ((entities = getJsonRootEntity(url).optJSONArray(Constants.ENTITIES)) != null
        && entities.length() > 0) {

      int limit = (entities.length() >= batchSize) ? (entities.length() - 1) : entities.length();
      LOG.debug("Limit={}, downloaded entities len={}", limit, entities.length());

      //write downloaded part to zipfile.  This is done to avoid any memory pressure when
      // downloading and writing 1000s of tasks.
      ZipEntry zipEntry = new ZipEntry("part-" + System.currentTimeMillis() + ".json");
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

  private String logErrorMessage(ClientResponse response) throws IOException {
    StringBuilder sb = new StringBuilder();
    LOG.error("Response status={}", response.getClientResponseStatus().toString());
    LineIterator it = null;
    try {
      it = IOUtils.lineIterator(response.getEntityInputStream(), UTF8);
      while (it.hasNext()) {
        String line = it.nextLine();
        LOG.error(line);
      }
    } finally {
      if (it != null) {
        it.close();
      }
    }
    return sb.toString();
  }

  //For secure cluster, this should work as long as valid ticket is available in the node.
  private JSONObject getJsonRootEntity(String url) throws TezException, IOException {
    try {
      WebResource wr = getHttpClient().resource(url);
      ClientResponse response = wr.accept(MediaType.APPLICATION_JSON_TYPE)
          .type(MediaType.APPLICATION_JSON_TYPE)
          .get(ClientResponse.class);

      if (response.getClientResponseStatus() != ClientResponse.Status.OK) {
        // In the case of secure cluster, if there is any auth exception it sends the data back as
        // a html page and JSON parsing could throw exceptions. Instead, get the stream contents
        // completely and log it in case of error.
        logErrorMessage(response);
        throw new TezException("Failed to get response from YARN Timeline: url: " + url);
      }
      return response.getEntity(JSONObject.class);
    } catch (ClientHandlerException e) {
      throw new TezException("Error processing response from YARN Timeline. URL=" + url, e);
    } catch (UniformInterfaceException e) {
      throw new TezException("Error accessing content from YARN Timeline - unexpected response. "
          + "URL=" + url, e);
    } catch (IllegalArgumentException e) {
      throw new TezException("Error accessing content from YARN Timeline - invalid url. URL=" + url,
          e);
    }
  }

  private Client getHttpClient() {
    if (httpClient == null) {
      ClientConfig config = new DefaultClientConfig(JSONRootElementProvider.App.class);
      HttpURLConnectionFactory urlFactory = new PseudoAuthenticatedURLConnectionFactory();
      return new Client(new URLConnectionClientHandler(urlFactory), config);
    }
    return httpClient;
  }

  static class PseudoAuthenticatedURLConnectionFactory implements HttpURLConnectionFactory {
    @Override
    public HttpURLConnection getHttpURLConnection(URL url) throws IOException {
      String tokenString = (url.getQuery() == null ? "?" : "&") + "user.name=" +
          URLEncoder.encode(UserGroupInformation.getCurrentUser().getShortUserName(), "UTF8");
      return (HttpURLConnection) (new URL(url.toString() + tokenString)).openConnection();
    }
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
    Option dagIdOption = OptionBuilder.withArgName(DAG_ID).withLongOpt(DAG_ID)
        .withDescription("DagId that needs to be downloaded").hasArg().isRequired(true).create();

    Option downloadDirOption = OptionBuilder.withArgName(BASE_DOWNLOAD_DIR).withLongOpt
        (BASE_DOWNLOAD_DIR)
        .withDescription("Download directory where data needs to be downloaded").hasArg()
        .isRequired(true).create();

    Option atsAddressOption = OptionBuilder.withArgName(YARN_TIMELINE_SERVICE_ADDRESS).withLongOpt(
        YARN_TIMELINE_SERVICE_ADDRESS)
        .withDescription("Optional. ATS address (e.g http://clusterATSNode:8188)").hasArg()
        .isRequired(false)
        .create();

    Option batchSizeOption = OptionBuilder.withArgName(BATCH_SIZE).withLongOpt(BATCH_SIZE)
        .withDescription("Optional. batch size for downloading data").hasArg()
        .isRequired(false)
        .create();

    Option help = OptionBuilder.withArgName("help").withLongOpt("help")
        .withDescription("print help").isRequired(false).create();

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
      Preconditions.checkArgument(!Strings.isEmpty(yarnTimelineAddress), "Yarn timeline address can"
          + " not be empty. Please check configurations.");
    } else {
      yarnTimelineAddress = yarnTimelineAddress.trim();
      Preconditions.checkArgument(!Strings.isEmpty(yarnTimelineAddress), "Yarn timeline address can"
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
  static int process(String[] args) {
    Options options = buildOptions();
    int result = -1;
    try {
      Configuration conf = new Configuration();
      CommandLine cmdLine = new GnuParser().parse(options, args);
      String dagId = cmdLine.getOptionValue(DAG_ID);

      File downloadDir = new File(cmdLine.getOptionValue(BASE_DOWNLOAD_DIR));

      String yarnTimelineAddress = cmdLine.getOptionValue(YARN_TIMELINE_SERVICE_ADDRESS);
      String baseTimelineURL = getBaseTimelineURL(yarnTimelineAddress, conf);

      int batchSize = (cmdLine.hasOption(BATCH_SIZE)) ?
          (Integer.parseInt(cmdLine.getOptionValue(BATCH_SIZE))) : BATCH_SIZE_DEFAULT;

      result = ToolRunner.run(conf, new ATSImportTool(baseTimelineURL, dagId,
          downloadDir, batchSize), args);

      return result;
    } catch (MissingOptionException missingOptionException) {
      LOG.error("Error in parsing options ", missingOptionException);
      printHelp(options);
    } catch (ParseException e) {
      LOG.error("Error in parsing options ", e);
      printHelp(options);
    } catch (Exception e) {
      LOG.error("Error in processing ", e);
      throw e;
    } finally {
      return result;
    }
  }

  public static void main(String[] args) throws Exception {
    Utils.setupRootLogger();
    int res = process(args);
    System.exit(res);
  }
}
