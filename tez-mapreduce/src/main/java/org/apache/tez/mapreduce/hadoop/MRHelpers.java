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
import java.util.Map;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.TezYARNUtils;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.mapreduce.combine.MRCombiner;
import org.apache.tez.mapreduce.partition.MRPartitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;


/**
 * This class contains helper methods for frameworks which migrate from MapReduce to Tez, and need
 * to continue to work with existing MapReduce configurations.
 */
@Public
@Evolving
public class MRHelpers {

  private static final Logger LOG = LoggerFactory.getLogger(MRHelpers.class);


  /**
   * Translate MapReduce configuration keys to the equivalent Tez keys in the provided
   * configuration. The translation is done in place. </p>
   * This method is meant to be used by frameworks which rely upon existing MapReduce configuration
   * instead of setting up their own.
   *
   * @param conf mr based configuration to be translated to tez
   */
  public static void translateMRConfToTez(Configuration conf) {
    translateMRConfToTez(conf, true);
  }

  /**
   * Translate MapReduce configuration keys to the equivalent Tez keys in the provided
   * configuration. The translation is done in place. </p>
   * This method is meant to be used by frameworks which rely upon existing MapReduce configuration
   * instead of setting up their own.
   *
   * @param conf mr based configuration to be translated to tez
   * @param preferTez If the tez setting already exists and is set, use the Tez setting
   */
  public static void translateMRConfToTez(Configuration conf, boolean preferTez) {
    convertVertexConfToTez(conf, preferTez);
  }


  /**
   * Update the provided configuration to use the new API (mapreduce) or the old API (mapred) based
   * on the configured InputFormat, OutputFormat, Partitioner etc. Also ensures that keys not
   * required by a particular mode are not present. </p>
   *
   * This method should be invoked after completely setting up the configuration. </p>
   *
   * Defaults to using the new API if relevant keys are not present.
   *
   */
  public static void configureMRApiUsage(Configuration conf) {
    String oldMapperClass = "mapred.mapper.class";
    conf.setBooleanIfUnset("mapred.mapper.new-api", conf.get(oldMapperClass) == null);
    try {
      if (conf.getBoolean("mapred.mapper.new-api", false)) {
        String mode = "new map API";
        ensureNotSet(conf, "mapred.input.format.class", mode);
        ensureNotSet(conf, oldMapperClass, mode);
      } else {
        String mode = "map compatability";
        ensureNotSet(conf, MRJobConfig.INPUT_FORMAT_CLASS_ATTR, mode);
        ensureNotSet(conf, MRJobConfig.MAP_CLASS_ATTR, mode);
      }
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
  }

  private static void convertVertexConfToTez(Configuration vertexConf, boolean preferTez) {
    setStageKeysFromBaseConf(vertexConf, vertexConf, "unknown");
    processDirectConversion(vertexConf, preferTez);
    setupMRComponents(vertexConf);
  }

  private static void setupMRComponents(Configuration conf) {
    if (conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS) == null) {
      conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS,
          MRPartitioner.class.getName());
    }

    if (conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS) == null) {
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
    }
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

  private static void processDirectConversion(Configuration conf, boolean preferTez) {
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
        } else if (!preferTez) {
          conf.set(dep.getValue(), mrValue, "TRANSLATED_TO_TEZ_AND_MR_OVERRIDE");
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Config: mr(unset):" + dep.getKey() + ", mr initial value="
              + mrValue
              + ", tez(original):" + dep.getValue() + "=" + tezValue
              + ", tez(final):" + dep.getValue() + "=" + conf.get(dep.getValue()));
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

  private static void ensureNotSet(Configuration conf, String attr, String msg)
      throws IOException {
    if (conf.get(attr) != null) {
      throw new IOException(attr + " is incompatible with " + msg + " mode.");
    }
  }

  private static String getLog4jCmdLineProperties(Configuration conf,
      boolean isMap) {
    Vector<String> logProps = new Vector<String>(4);
    TezUtils.addLog4jSystemProperties(getChildLogLevel(conf, isMap), logProps);
    StringBuilder sb = new StringBuilder();
    for (String str : logProps) {
      sb.append(str).append(" ");
    }
    return sb.toString();
  }

  /**
   * Generate JVM options based on MapReduce AM java options. </p>
   * <p/>
   * This is only meant to be used if frameworks are not setting up their own java options or
   * relying on the defaults specified by Tez, and instead want to use the options which may already
   * have been configured for an MR AppMaster.
   *
   * @param conf Configuration to be used to extract JVM opts specific info
   * @return JAVA_OPTS string to be used in launching the JVM
   */
  public static String getJavaOptsForMRAM(Configuration conf) {
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

  /**
   * Generate JVM options based on MapReduce mapper java options. </p>
   *
   * This is only meant to be used if frameworks are not setting up their own java options,
   * and would like to fallback to using java options which may already be configured for
   * Hadoop MapReduce mappers.
   *
   * Uses mapreduce.admin.map.child.java.opts, mapreduce.map.java.opts and
   * mapreduce.map.log.level from config to generate the opts.
   *
   * @param conf Configuration to be used to extract JVM opts specific info
   * @return JAVA_OPTS string to be used in launching the JVM
   */
  @SuppressWarnings("deprecation")
  public static String getJavaOptsForMRMapper(Configuration conf) {
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
   * Generate JVM options based on MapReduce reducer java options. </p>
   *
   * This is only meant to be used if frameworks are not setting up their own java options,
   * and would like to fallback to using java options which may already be configured for
   * Hadoop MapReduce reducers.
   *
   * Uses mapreduce.admin.reduce.child.java.opts, mapreduce.reduce.java.opts
   * and mapreduce.reduce.log.level from config to generate the opts.
   *
   * @param conf Configuration to be used to extract JVM opts specific info
   * @return JAVA_OPTS string to be used in launching the JVM
   */
  @SuppressWarnings("deprecation")
  public static String getJavaOptsForMRReducer(Configuration conf) {
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
   * Extract the container resource requirements from the provided configuration, which would
   * otherwise have been used when running a Hadoop MapReduce mapper. </p>
   * <p/>
   * This is only meant to be used if frameworks are not setting up their own {@link
   * org.apache.hadoop.yarn.api.records.Resource} and would like to fallback to using resources
   * which may already be configured for Hadoop MapReduce mappers.
   *
   * @param conf Configuration with MR specific settings used to extract
   * information from
   *
   * @return Resource object used to define requirements for containers
   * running Map tasks
   */
  public static Resource getResourceForMRMapper(Configuration conf) {
    return Resource.newInstance(conf.getInt(
        MRJobConfig.MAP_MEMORY_MB, MRJobConfig.DEFAULT_MAP_MEMORY_MB),
        conf.getInt(MRJobConfig.MAP_CPU_VCORES,
            MRJobConfig.DEFAULT_MAP_CPU_VCORES));
  }

  /**
   * Extract the container resource requirements from the provided configuration, which would
   * otherwise have been used when running a Hadoop MapReduce reducer. </p>
   * <p/>
   * This is only meant to be used if frameworks are not setting up their own {@link
   * org.apache.hadoop.yarn.api.records.Resource} and would like to fallback to using resources
   * which may already be configured for Hadoop MapReduce reducers.
   * <p/>
   * Uses mapreduce.reduce.memory.mb and mapreduce.reduce.cpu.vcores from the
   * provided configuration.
   *
   * @param conf Configuration with MR specific settings used to extract
   *             information from
   * @return Resource object used to define requirements for containers
   * running Reduce tasks
   */
  public static Resource getResourceForMRReducer(Configuration conf) {
    return Resource.newInstance(conf.getInt(
            MRJobConfig.REDUCE_MEMORY_MB, MRJobConfig.DEFAULT_REDUCE_MEMORY_MB),
        conf.getInt(MRJobConfig.REDUCE_CPU_VCORES,
            MRJobConfig.DEFAULT_REDUCE_CPU_VCORES));
  }

  /**
   * Setup classpath and other environment variables based on the configured values for MR Mappers
   * or Reducers
   *
   * @param conf        Configuration to retrieve settings from
   * @param environment Environment to update
   * @param isMap       Whether task is a map or reduce task
   */
  public static void updateEnvBasedOnMRTaskEnv(Configuration conf,
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

  /**
   * Setup environment variables based on the configured values for the MR AM
   * @param conf Configuration from which to extract information
   * @param environment Environment map to update
   */
  public static void updateEnvBasedOnMRAMEnv(Configuration conf, Map<String, String> environment) {
    TezYARNUtils.appendToEnvFromInputString(environment, conf.get(MRJobConfig.MR_AM_ADMIN_USER_ENV),
        File.pathSeparator);
    TezYARNUtils.appendToEnvFromInputString(environment, conf.get(MRJobConfig.MR_AM_ENV),
        File.pathSeparator);
  }

}
