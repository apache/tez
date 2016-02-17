/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.serviceplugins.api.ServicePluginContextBase;
import org.apache.tez.serviceplugins.api.ServicePluginErrorDefaults;

public class ErrorPluginConfiguration {

  public static final String REPORT_FATAL_ERROR_MESSAGE = "ReportedFatalError";
  public static final String REPORT_NONFATAL_ERROR_MESSAGE = "ReportedError";
  public static final String THROW_ERROR_EXCEPTION_STRING = "Simulated Error";

  private static final String CONF_THROW_ERROR = "throw.error";
  private static final String CONF_REPORT_ERROR = "report.error";
  private static final String CONF_REPORT_ERROR_FATAL = "report.error.fatal";
  private static final String CONF_REPORT_ERROR_DAG_NAME = "report.error.dag.name";

  private final HashMap<String, String> kv;

  private ErrorPluginConfiguration() {
    this.kv = new HashMap<>();
  }

  private ErrorPluginConfiguration(HashMap<String, String> map) {
    this.kv = map;
  }

  public static ErrorPluginConfiguration createThrowErrorConf() {
    ErrorPluginConfiguration conf = new ErrorPluginConfiguration();
    conf.kv.put(CONF_THROW_ERROR, String.valueOf(true));
    return conf;
  }

  public static ErrorPluginConfiguration createReportFatalErrorConf(String dagName) {
    ErrorPluginConfiguration conf = new ErrorPluginConfiguration();
    conf.kv.put(CONF_REPORT_ERROR, String.valueOf(true));
    conf.kv.put(CONF_REPORT_ERROR_FATAL, String.valueOf(true));
    conf.kv.put(CONF_REPORT_ERROR_DAG_NAME, dagName);
    return conf;
  }

  public static ErrorPluginConfiguration createReportNonFatalErrorConf(String dagName) {
    ErrorPluginConfiguration conf = new ErrorPluginConfiguration();
    conf.kv.put(CONF_REPORT_ERROR, String.valueOf(true));
    conf.kv.put(CONF_REPORT_ERROR_FATAL, String.valueOf(false));
    conf.kv.put(CONF_REPORT_ERROR_DAG_NAME, dagName);
    return conf;
  }

  public static UserPayload toUserPayload(ErrorPluginConfiguration conf) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(conf.kv);
    oos.close();
    UserPayload userPayload = UserPayload.create(ByteBuffer.wrap(baos.toByteArray()));
    return userPayload;
  }

  @SuppressWarnings("unchecked")
  public static ErrorPluginConfiguration toErrorPluginConfiguration(UserPayload userPayload) throws
      IOException, ClassNotFoundException {

    byte[] b = new byte[userPayload.getPayload().remaining()];
    userPayload.getPayload().get(b);
    ByteArrayInputStream bais = new ByteArrayInputStream(b);
    ObjectInputStream ois = new ObjectInputStream(bais);

    HashMap<String, String> map = (HashMap) ois.readObject();
    ErrorPluginConfiguration conf = new ErrorPluginConfiguration(map);
    return conf;
  }

  public boolean shouldThrowError() {
    return (kv.containsKey(CONF_THROW_ERROR) && Boolean.parseBoolean(kv.get(CONF_THROW_ERROR)));
  }

  public boolean shouldReportFatalError(String dagName) {
    if (kv.containsKey(CONF_REPORT_ERROR) && Boolean.parseBoolean(kv.get(CONF_REPORT_ERROR)) &&
        Boolean.parseBoolean(kv.get(CONF_REPORT_ERROR_FATAL))) {
      if (dagName == null || dagName.isEmpty() || kv.get(CONF_REPORT_ERROR_DAG_NAME).equals("*") ||
          kv.get(CONF_REPORT_ERROR_DAG_NAME).equals(dagName)) {
        return true;
      }
    }
    return false;
  }

  public boolean shouldReportNonFatalError(String dagName) {
    if (kv.containsKey(CONF_REPORT_ERROR) && Boolean.parseBoolean(kv.get(CONF_REPORT_ERROR)) &&
        Boolean.parseBoolean(kv.get(CONF_REPORT_ERROR_FATAL)) == false) {
      if (dagName == null || dagName.isEmpty() || kv.get(CONF_REPORT_ERROR_DAG_NAME).equals("*") ||
          kv.get(CONF_REPORT_ERROR_DAG_NAME).equals(dagName)) {
        return true;
      }
    }
    return false;
  }

  public static void processError(ErrorPluginConfiguration conf, ServicePluginContextBase context) {
    if (conf.shouldThrowError()) {
      throw new RuntimeException(ErrorPluginConfiguration.THROW_ERROR_EXCEPTION_STRING);
    } else if (conf.shouldReportFatalError(null)) {
      context.reportError(ServicePluginErrorDefaults.INCONSISTENT_STATE,
          ErrorPluginConfiguration.REPORT_FATAL_ERROR_MESSAGE,
          context.getCurrentDagInfo());
    } else if (conf.shouldReportNonFatalError(null)) {
      context.reportError(ServicePluginErrorDefaults.SERVICE_UNAVAILABLE,
          ErrorPluginConfiguration.REPORT_NONFATAL_ERROR_MESSAGE,
          context.getCurrentDagInfo());
    }
  }
}
