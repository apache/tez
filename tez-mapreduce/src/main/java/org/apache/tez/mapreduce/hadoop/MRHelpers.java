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

package org.apache.tez.mapreduce.hadoop;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.yarn.ContainerLogAppender;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.TezYARNUtils;
import org.apache.tez.mapreduce.combine.MRCombiner;
import org.apache.tez.mapreduce.partition.MRPartitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

import com.google.protobuf.ByteString;


public class MRHelpers {

  private static final Log LOG = LogFactory.getLog(MRHelpers.class);




  /**
   * Translates MR keys to Tez for the provided conf. The conversion is
   * done in place.
   *
   * @param conf
   *          mr based configuration to be translated to tez
   */
  @LimitedPrivate("Hive, Pig")
  @Unstable
  public static void translateVertexConfToTez(Configuration conf) {
    convertVertexConfToTez(conf);
  }

  private static void convertVertexConfToTez(Configuration vertexConf) {
    setStageKeysFromBaseConf(vertexConf, vertexConf, "unknown");
    processDirectConversion(vertexConf);
  }

  /**
   * Pulls in specific keys from the base configuration, if they are not set at
   * the stage level. An explicit list of keys is copied over (not all), which
   * require translation to tez keys.
   */
  private static void setStageKeysFromBaseConf(Configuration conf,
                                               Configuration baseConf, String stage) {
    // Don't clobber explicit tez config.
    JobConf jobConf = null;
    if (conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS) == null) {
      // If this is set, but the comparator is not set, and their types differ -
      // the job will break.
      if (conf.get(MRJobConfig.MAP_OUTPUT_KEY_CLASS) == null) {
        // Pull this in from the baseConf
        // Create jobConf only if required.
        jobConf = new JobConf(baseConf);
        conf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS, jobConf
            .getMapOutputKeyClass().getName());

        if (LOG.isDebugEnabled()) {
          LOG.debug("Setting " + MRJobConfig.MAP_OUTPUT_KEY_CLASS
              + " for stage: " + stage
              + " based on job level configuration. Value: "
              + conf.get(MRJobConfig.MAP_OUTPUT_KEY_CLASS));
        }
      }
    }

    if (conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS) == null) {
      if (conf.get(MRJobConfig.MAP_OUTPUT_VALUE_CLASS) == null) {
        if (jobConf == null) {
          // Create jobConf if not already created
          jobConf = new JobConf(baseConf);
        }
        conf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS, jobConf
            .getMapOutputValueClass().getName());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Setting " + MRJobConfig.MAP_OUTPUT_VALUE_CLASS
              + " for stage: " + stage
              + " based on job level configuration. Value: "
              + conf.get(MRJobConfig.MAP_OUTPUT_VALUE_CLASS));
        }
      }
    }
  }

  private static void processDirectConversion(Configuration conf) {
    for (Map.Entry<String, String> dep : DeprecatedKeys.getMRToTezRuntimeParamMap().entrySet()) {
      if (conf.get(dep.getKey()) != null) {
        // TODO Deprecation reason does not seem to reflect in the config ?
        // The ordering is important in case of keys which are also deprecated.
        // Unset will unset the deprecated keys and all it's variants.
        final String mrValue = conf.get(dep.getKey());
        final String tezValue = conf.get(dep.getValue());
        conf.unset(dep.getKey());
        if (tezValue == null) {
          conf.set(dep.getValue(), mrValue, "TRANSLATED_TO_TEZ");
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Config: mr(unset):" + dep.getKey() + ", mr initial value="
              + mrValue + ", tez:" + dep.getValue() + "=" + conf.get(dep.getValue()));
        }
      }
    }
  }

  private static String getChildLogLevel(Configuration conf, boolean isMap) {
    if (isMap) {
      return conf.get(
          MRJobConfig.MAP_LOG_LEVEL,
          JobConf.DEFAULT_LOG_LEVEL.toString()
          );
    } else {
      return conf.get(
          MRJobConfig.REDUCE_LOG_LEVEL,
          JobConf.DEFAULT_LOG_LEVEL.toString()
          );
    }
  }

  private static String getLog4jCmdLineProperties(Configuration conf,
      boolean isMap) {
    Vector<String> logProps = new Vector<String>(4);
    addLog4jSystemProperties(getChildLogLevel(conf, isMap), logProps);
    StringBuilder sb = new StringBuilder();
    for (String str : logProps) {
      sb.append(str).append(" ");
    }
    return sb.toString();
  }

  /**
   * Add the JVM system properties necessary to configure
   * {@link ContainerLogAppender}.
   *
   * @param logLevel
   *          the desired log level (eg INFO/WARN/DEBUG)
   * @param vargs
   *          the argument list to append to
   */
  public static void addLog4jSystemProperties(String logLevel,
      List<String> vargs) {
    TezClientUtils.addLog4jSystemProperties(logLevel, vargs);
  }

  /**
   * Generate JVM options to be used to launch map tasks
   *
   * Uses mapreduce.admin.map.child.java.opts, mapreduce.map.java.opts and
   * mapreduce.map.log.level from config to generate the opts.
   *
   * @param conf Configuration to be used to extract JVM opts specific info
   * @return JAVA_OPTS string to be used in launching the JVM
   */
  @SuppressWarnings("deprecation")
  public static String getMapJavaOpts(Configuration conf) {
    String adminOpts = conf.get(
        MRJobConfig.MAPRED_MAP_ADMIN_JAVA_OPTS,
        MRJobConfig.DEFAULT_MAPRED_ADMIN_JAVA_OPTS);

    String userOpts = conf.get(
        MRJobConfig.MAP_JAVA_OPTS,
        conf.get(
            JobConf.MAPRED_TASK_JAVA_OPTS,
            JobConf.DEFAULT_MAPRED_TASK_JAVA_OPTS));

    return adminOpts.trim() + " " + userOpts.trim() + " "
        + getLog4jCmdLineProperties(conf, true);
  }

  /**
   * Generate JVM options to be used to launch reduce tasks
   *
   * Uses mapreduce.admin.reduce.child.java.opts, mapreduce.reduce.java.opts
   * and mapreduce.reduce.log.level from config to generate the opts.
   *
   * @param conf Configuration to be used to extract JVM opts specific info
   * @return JAVA_OPTS string to be used in launching the JVM
   */
  @SuppressWarnings("deprecation")
  public static String getReduceJavaOpts(Configuration conf) {
    String adminOpts = conf.get(
        MRJobConfig.MAPRED_REDUCE_ADMIN_JAVA_OPTS,
        MRJobConfig.DEFAULT_MAPRED_ADMIN_JAVA_OPTS);

    String userOpts = conf.get(
        MRJobConfig.REDUCE_JAVA_OPTS,
        conf.get(
            JobConf.MAPRED_TASK_JAVA_OPTS,
            JobConf.DEFAULT_MAPRED_TASK_JAVA_OPTS));

    return adminOpts.trim() + " " + userOpts.trim() + " "
        + getLog4jCmdLineProperties(conf, false);
  }

  /**
   * Sets up parameters which used to be set by the MR JobClient. Includes
   * setting whether to use the new api or the old api. Note: Must be called
   * before generating InputSplits
   *
   * @param conf
   *          configuration for the vertex.
   */
  public static void doJobClientMagic(Configuration conf) throws IOException {
    setUseNewAPI(conf);
    // TODO Maybe add functionality to check output specifications - e.g. fail
    // early if the output directory exists.
    InetAddress ip = InetAddress.getLocalHost();
    if (ip != null) {
      String submitHostAddress = ip.getHostAddress();
      String submitHostName = ip.getHostName();
      conf.set(MRJobConfig.JOB_SUBMITHOST, submitHostName);
      conf.set(MRJobConfig.JOB_SUBMITHOSTADDR, submitHostAddress);
    }
    // conf.set("hadoop.http.filter.initializers",
    // "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer");
    // Skipping setting JOB_DIR - not used by AM.

    // Maybe generate SHUFFLE secret. The AM uses the job token generated in
    // the AM anyway.

    // TODO eventually ACLs
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, MRPartitioner.class.getName());
    
    boolean useNewApi = conf.getBoolean("mapred.mapper.new-api", false);
    if (useNewApi) {
      if (conf.get(MRJobConfig.COMBINE_CLASS_ATTR) != null) {
        conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS, MRCombiner.class.getName());
      }
    } else {
      if (conf.get("mapred.combiner.class") != null) {
        conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS, MRCombiner.class.getName());
      }
    }
    
    setWorkingDirectory(conf);
  }

  private static void setWorkingDirectory(Configuration conf) {
    String name = conf.get(JobContext.WORKING_DIR);
    if (name == null) {
      try {
        Path dir = FileSystem.get(conf).getWorkingDirectory();
        conf.set(JobContext.WORKING_DIR, dir.toString());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Default to the new APIs unless they are explicitly set or the old mapper or
   * reduce attributes are used.
   *
   * @throws IOException
   *           if the configuration is inconsistant
   */
  private static void setUseNewAPI(Configuration conf) throws IOException {
    int numReduces = conf.getInt(MRJobConfig.NUM_REDUCES, 1);
    String oldMapperClass = "mapred.mapper.class";
    String oldReduceClass = "mapred.reducer.class";
    conf.setBooleanIfUnset("mapred.mapper.new-api",
        conf.get(oldMapperClass) == null);
    if (conf.getBoolean("mapred.mapper.new-api", false)) {
      String mode = "new map API";
      ensureNotSet(conf, "mapred.input.format.class", mode);
      ensureNotSet(conf, oldMapperClass, mode);
      if (numReduces != 0) {
        ensureNotSet(conf, "mapred.partitioner.class", mode);
      } else {
        ensureNotSet(conf, "mapred.output.format.class", mode);
      }
    } else {
      String mode = "map compatability";
      ensureNotSet(conf, MRJobConfig.INPUT_FORMAT_CLASS_ATTR, mode);
      ensureNotSet(conf, MRJobConfig.MAP_CLASS_ATTR, mode);
      if (numReduces != 0) {
        ensureNotSet(conf, MRJobConfig.PARTITIONER_CLASS_ATTR, mode);
      } else {
        ensureNotSet(conf, MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, mode);
      }
    }
    if (numReduces != 0) {
      conf.setBooleanIfUnset("mapred.reducer.new-api",
          conf.get(oldReduceClass) == null);
      if (conf.getBoolean("mapred.reducer.new-api", false)) {
        String mode = "new reduce API";
        ensureNotSet(conf, "mapred.output.format.class", mode);
        ensureNotSet(conf, oldReduceClass, mode);
      } else {
        String mode = "reduce compatability";
        ensureNotSet(conf, MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, mode);
        ensureNotSet(conf, MRJobConfig.REDUCE_CLASS_ATTR, mode);
      }
    }
  }

  private static void ensureNotSet(Configuration conf, String attr, String msg)
      throws IOException {
    if (conf.get(attr) != null) {
      throw new IOException(attr + " is incompatible with " + msg + " mode.");
    }
  }

  @LimitedPrivate("Hive, Pig")
  @Unstable
  public static byte[] createUserPayloadFromConf(Configuration conf)
      throws IOException {
    return TezUtils.createUserPayloadFromConf(conf);
  }
  
  @LimitedPrivate("Hive, Pig")
  public static ByteString createByteStringFromConf(Configuration conf)
      throws IOException {
    return TezUtils.createByteStringFromConf(conf);
  }

  @LimitedPrivate("Hive, Pig")
  @Unstable
  public static Configuration createConfFromUserPayload(byte[] bb)
      throws IOException {
    return TezUtils.createConfFromUserPayload(bb);
  }

  @LimitedPrivate("Hive, Pig")
  public static Configuration createConfFromByteString(ByteString bs)
      throws IOException {
    return TezUtils.createConfFromByteString(bs);
  }


  /**
   * Extract the map task's container resource requirements from the
   * provided configuration.
   *
   * Uses mapreduce.map.memory.mb and mapreduce.map.cpu.vcores from the
   * provided configuration.
   *
   * @param conf Configuration with MR specific settings used to extract
   * information from
   *
   * @return Resource object used to define requirements for containers
   * running Map tasks
   */
  public static Resource getMapResource(Configuration conf) {
    return Resource.newInstance(conf.getInt(
        MRJobConfig.MAP_MEMORY_MB, MRJobConfig.DEFAULT_MAP_MEMORY_MB),
        conf.getInt(MRJobConfig.MAP_CPU_VCORES,
            MRJobConfig.DEFAULT_MAP_CPU_VCORES));
  }

  /**
   * Extract the reduce task's container resource requirements from the
   * provided configuration.
   *
   * Uses mapreduce.reduce.memory.mb and mapreduce.reduce.cpu.vcores from the
   * provided configuration.
   *
   * @param conf Configuration with MR specific settings used to extract
   * information from
   *
   * @return Resource object used to define requirements for containers
   * running Reduce tasks
   */
  public static Resource getReduceResource(Configuration conf) {
    return Resource.newInstance(conf.getInt(
        MRJobConfig.REDUCE_MEMORY_MB, MRJobConfig.DEFAULT_REDUCE_MEMORY_MB),
        conf.getInt(MRJobConfig.REDUCE_CPU_VCORES,
            MRJobConfig.DEFAULT_REDUCE_CPU_VCORES));
  }

  /**
   * Setup classpath and other environment variables
   * @param conf Configuration to retrieve settings from
   * @param environment Environment to update
   * @param isMap Whether task is a map or reduce task
   */
  public static void updateEnvironmentForMRTasks(Configuration conf,
      Map<String, String> environment, boolean isMap) {
    // Shell
    environment.put(Environment.SHELL.name(), conf.get(
        MRJobConfig.MAPRED_ADMIN_USER_SHELL, MRJobConfig.DEFAULT_SHELL));

    // Add pwd to LD_LIBRARY_PATH, add this before adding anything else
    TezYARNUtils.addToEnvironment(environment, Environment.LD_LIBRARY_PATH.name(),
        Environment.PWD.$(), File.pathSeparator);

    // Add the env variables passed by the admin
    TezYARNUtils.appendToEnvFromInputString(environment, conf.get(
        MRJobConfig.MAPRED_ADMIN_USER_ENV,
        MRJobConfig.DEFAULT_MAPRED_ADMIN_USER_ENV),
        File.pathSeparator);

    // Add the env variables passed by the user
    String mapredChildEnv = (isMap ?
        conf.get(MRJobConfig.MAP_ENV, "")
        : conf.get(MRJobConfig.REDUCE_ENV, ""));
    TezYARNUtils.appendToEnvFromInputString(environment, mapredChildEnv, File.pathSeparator);

    // Set logging level in the environment.
    environment.put(
        "HADOOP_ROOT_LOGGER",
        getChildLogLevel(conf, isMap) + ",CLA");
  }

  private static Configuration getBaseJobConf(Configuration conf) {
    if (conf != null) {
      return new JobConf(conf);
    } else {
      return new JobConf();
    }
  }

  /**
   * Get default initialize JobConf-based configuration
   * @param conf Conf to initialize JobConf with.
   * @return Base configuration for MR-based jobs
   */
  public static Configuration getBaseMRConfiguration(Configuration conf) {
    return getBaseJobConf(conf);
  }

  /**
   * Get default initialize JobConf-based configuration
   * @return Base configuration for MR-based jobs
   */
  static Configuration getBaseMRConfiguration() {
    return getBaseJobConf(null);
  }

  /**
   * Setup environment for the AM based on MR-based configuration
   * @param conf Configuration from which to extract information
   * @param environment Environment map to update
   */
  public static void updateEnvironmentForMRAM(Configuration conf, Map<String, String> environment) {
    TezYARNUtils.appendToEnvFromInputString(environment, conf.get(MRJobConfig.MR_AM_ADMIN_USER_ENV),
        File.pathSeparator);
    TezYARNUtils.appendToEnvFromInputString(environment, conf.get(MRJobConfig.MR_AM_ENV),
        File.pathSeparator);
  }

  /**
   * Extract Java Opts for the AM based on MR-based configuration
   * @param conf Configuration from which to extract information
   * @return Java opts for the AM
   */
  public static String getMRAMJavaOpts(Configuration conf) {
    // Admin opts
    String mrAppMasterAdminOptions = conf.get(
        MRJobConfig.MR_AM_ADMIN_COMMAND_OPTS,
        MRJobConfig.DEFAULT_MR_AM_ADMIN_COMMAND_OPTS);
    // Add AM user command opts
    String mrAppMasterUserOptions = conf.get(MRJobConfig.MR_AM_COMMAND_OPTS,
        MRJobConfig.DEFAULT_MR_AM_COMMAND_OPTS);

    return mrAppMasterAdminOptions.trim()
        + " " + mrAppMasterUserOptions.trim();
  }
}
