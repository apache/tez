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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.ContainerLogAppender;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;

import com.google.common.base.Preconditions;

public class MRHelpers {

  private static final Log LOG = LogFactory.getLog(MRHelpers.class);

  static final String JOB_SPLIT_RESOURCE_NAME = "job.split";
  static final String JOB_SPLIT_METAINFO_RESOURCE_NAME =
      "job.splitmetainfo";

  /**
   * Comparator for org.apache.hadoop.mapreduce.InputSplit
   */
  private static class InputSplitComparator
      implements Comparator<org.apache.hadoop.mapreduce.InputSplit> {
    @Override
    public int compare(org.apache.hadoop.mapreduce.InputSplit o1,
        org.apache.hadoop.mapreduce.InputSplit o2) {
      try {
        long len1 = o1.getLength();
        long len2 = o2.getLength();
        if (len1 < len2) {
          return 1;
        } else if (len1 == len2) {
          return 0;
        } else {
          return -1;
        }
      } catch (IOException ie) {
        throw new RuntimeException("exception in InputSplit compare", ie);
      } catch (InterruptedException ie) {
        throw new RuntimeException("exception in InputSplit compare", ie);
      }
    }
  }

  /**
   * Comparator for org.apache.hadoop.mapred.InputSplit
   */
  private static class OldInputSplitComparator
      implements Comparator<org.apache.hadoop.mapred.InputSplit> {
    @Override
    public int compare(org.apache.hadoop.mapred.InputSplit o1,
        org.apache.hadoop.mapred.InputSplit o2) {
      try {
        long len1 = o1.getLength();
        long len2 = o2.getLength();
        if (len1 < len2) {
          return 1;
        } else if (len1 == len2) {
          return 0;
        } else {
          return -1;
        }
      } catch (IOException ie) {
        throw new RuntimeException("Problem getting input split size", ie);
      }
    }
  }

  /**
   * Generate new-api mapreduce InputFormat splits
   * @param jobContext JobContext required by InputFormat
   * @param inputSplitDir Directory in which to generate splits information
   *
   * @return InputSplitInfo containing the split files' information and the
   * location hints for each split generated to be used to determining parallelism of
   * the map stage.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  private static InputSplitInfo writeNewSplits(JobContext jobContext,
      Path inputSplitDir) throws IOException, InterruptedException,
      ClassNotFoundException {
    Configuration conf = jobContext.getConfiguration();
    InputFormat<?, ?> input =
        ReflectionUtils.newInstance(jobContext.getInputFormatClass(), conf);

    List<org.apache.hadoop.mapreduce.InputSplit> array =
        input.getSplits(jobContext);
    org.apache.hadoop.mapreduce.InputSplit[] splits =
        (org.apache.hadoop.mapreduce.InputSplit[])
        array.toArray(
            new org.apache.hadoop.mapreduce.InputSplit[array.size()]);

    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(splits, new InputSplitComparator());

    JobSplitWriter.createSplitFiles(inputSplitDir, conf,
        inputSplitDir.getFileSystem(conf), splits);

    List<TaskLocationHint> locationHints =
        new ArrayList<TaskLocationHint>(splits.length);
    for (int i = 0; i < splits.length; ++i) {
      locationHints.add(
          new TaskLocationHint(new HashSet<String>(
              Arrays.asList(splits[i].getLocations())), null));
    }

    return new InputSplitInfo(
        JobSubmissionFiles.getJobSplitFile(inputSplitDir),
        JobSubmissionFiles.getJobSplitMetaFile(inputSplitDir),
        splits.length, locationHints);
  }

  /**
   * Generate old-api mapred InputFormat splits
   * @param jobConf JobConf required by InputFormat class
   * @param inputSplitDir Directory in which to generate splits information
   *
   * @return InputSplitInfo containing the split files' information and the
   * number of splits generated to be used to determining parallelism of
   * the map stage.
   *
   * @throws IOException
   */
  private static InputSplitInfo writeOldSplits(JobConf jobConf,
      Path inputSplitDir) throws IOException {
    org.apache.hadoop.mapred.InputSplit[] splits =
        jobConf.getInputFormat().getSplits(jobConf, jobConf.getNumMapTasks());
    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(splits, new OldInputSplitComparator());
    JobSplitWriter.createSplitFiles(inputSplitDir, jobConf,
        inputSplitDir.getFileSystem(jobConf), splits);

    List<TaskLocationHint> locationHints =
        new ArrayList<TaskLocationHint>(splits.length);
    for (int i = 0; i < splits.length; ++i) {
      locationHints.add(
          new TaskLocationHint(new HashSet<String>(
              Arrays.asList(splits[i].getLocations())), null));
    }

    return new InputSplitInfo(
        JobSubmissionFiles.getJobSplitFile(inputSplitDir),
        JobSubmissionFiles.getJobSplitMetaFile(inputSplitDir),
        splits.length, locationHints);
  }

  /**
   * Helper api to generate splits
   * @param conf Configuration with all necessary information set to generate
   * splits. The following are required at a minimum:
   *
   *   - mapred.mapper.new-api: determine whether mapred.InputFormat or
   *     mapreduce.InputFormat is to be used
   *   - mapred.input.format.class or mapreduce.job.inputformat.class:
   *     determines the InputFormat class to be used
   *
   * In addition to this, all the configs needed by the InputFormat class also
   * have to be set. For example, FileInputFormat needs the input directory
   * paths to be set in the config.
   *
   * @param inputSplitsDir Directory in which the splits file and meta info file
   * will be generated. job.split and job.splitmetainfo files in this directory
   * will be overwritten. Should be a fully-qualified path.
   *
   * @return InputSplitInfo containing the split files' information and the
   * number of splits generated to be used to determining parallelism of
   * the map stage.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  public static InputSplitInfo generateInputSplits(Configuration conf,
      Path inputSplitsDir) throws IOException, InterruptedException,
      ClassNotFoundException {
    Job job = Job.getInstance(conf);
    JobConf jobConf = new JobConf(conf);
    if (jobConf.getUseNewMapper()) {
      LOG.info("Generating new input splits"
          + ", splitsDir=" + inputSplitsDir.toString());
      return writeNewSplits(job, inputSplitsDir);
    } else {
      LOG.info("Generating old input splits"
          + ", splitsDir=" + inputSplitsDir.toString());
      return writeOldSplits(jobConf, inputSplitsDir);
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
  private static void addLog4jSystemProperties(String logLevel,
      List<String> vargs) {
    vargs.add("-Dlog4j.configuration=container-log4j.properties");
    vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "="
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    // Setting this to 0 to avoid log size restrictions.
    // Should be enforced by YARN.
    vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_SIZE + "=" + 0);
    vargs.add("-Dhadoop.root.logger=" + logLevel + ",CLA");
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
    Preconditions.checkNotNull(conf, "Configuration must be specified");
    DataOutputBuffer dob = new DataOutputBuffer();
    conf.write(dob);
    return dob.getData();
  }

  @LimitedPrivate("Hive, Pig")
  @Unstable
  public static Configuration createConfFromUserPayload(byte[] bb)
      throws IOException {
    // TODO Avoid copy ?
    Preconditions.checkNotNull(bb, "Bytes must be specified");
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(bb, 0, bb.length);
    Configuration conf = new Configuration(false);
    conf.readFields(dib);
    return conf;
  }

  /**
   * Update provided localResources collection with the required local
   * resources needed by MapReduce tasks with respect to Input splits.
   *
   * @param fs Filesystem instance to access status of splits related files
   * @param inputSplitInfo Information on location of split files
   * @param localResources LocalResources collection to be updated
   * @throws IOException
   */
  public static void updateLocalResourcesForInputSplits(
      FileSystem fs,
      InputSplitInfo inputSplitInfo,
      Map<String, LocalResource> localResources) throws IOException {
    if (localResources.containsKey(JOB_SPLIT_RESOURCE_NAME)) {
      throw new RuntimeException("LocalResources already contains a"
          + " resource named " + JOB_SPLIT_RESOURCE_NAME);
    }
    if (localResources.containsKey(JOB_SPLIT_METAINFO_RESOURCE_NAME)) {
      throw new RuntimeException("LocalResources already contains a"
          + " resource named " + JOB_SPLIT_METAINFO_RESOURCE_NAME);
    }

    FileStatus splitFileStatus =
        fs.getFileStatus(inputSplitInfo.getSplitsFile());
    FileStatus metaInfoFileStatus =
        fs.getFileStatus(inputSplitInfo.getSplitsMetaInfoFile());
    localResources.put(JOB_SPLIT_RESOURCE_NAME,
        LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromPath(inputSplitInfo.getSplitsFile()),
            LocalResourceType.FILE,
            LocalResourceVisibility.APPLICATION,
            splitFileStatus.getLen(), splitFileStatus.getModificationTime()));
    localResources.put(JOB_SPLIT_METAINFO_RESOURCE_NAME,
        LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromPath(
                inputSplitInfo.getSplitsMetaInfoFile()),
            LocalResourceType.FILE,
            LocalResourceVisibility.APPLICATION,
            metaInfoFileStatus.getLen(),
            metaInfoFileStatus.getModificationTime()));
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

    boolean isMiniCluster =
        conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false);
    if (isMiniCluster) {
      Apps.addToEnvironment(environment, Environment.CLASSPATH.name(),
          System.getProperty("java.class.path"));
    }

    // TEZ jars and deps will be localized by the TezClient submission layer
    // Assumption is that MR is also in TEZ dependencies
    Apps.addToEnvironment(environment,
        Environment.CLASSPATH.name(),
        Environment.PWD.$());

    // Add YARN/COMMON/HDFS jars to path
    if (!isMiniCluster) {
      for (String c : conf.getStrings(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
        Apps.addToEnvironment(environment, Environment.CLASSPATH.name(),
            c.trim());
      }
    }

    Apps.addToEnvironment(environment,
        Environment.CLASSPATH.name(),
        Environment.PWD.$() + File.separator + "*");

    // Shell
    environment.put(Environment.SHELL.name(), conf.get(
        MRJobConfig.MAPRED_ADMIN_USER_SHELL, MRJobConfig.DEFAULT_SHELL));

    // Add pwd to LD_LIBRARY_PATH, add this before adding anything else
    Apps.addToEnvironment(environment, Environment.LD_LIBRARY_PATH.name(),
        Environment.PWD.$());

    // Add the env variables passed by the admin
    Apps.setEnvFromInputString(environment, conf.get(
        MRJobConfig.MAPRED_ADMIN_USER_ENV,
        MRJobConfig.DEFAULT_MAPRED_ADMIN_USER_ENV));

    // Add the env variables passed by the user
    String mapredChildEnv = (isMap ?
        conf.get(MRJobConfig.MAP_ENV, "")
        : conf.get(MRJobConfig.REDUCE_ENV, ""));
    Apps.setEnvFromInputString(environment, mapredChildEnv);

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
  public static Configuration getBaseMRConfiguration() {
    return getBaseJobConf(null);
  }

  /**
   * Setup environment for the AM based on MR-based configuration
   * @param conf Configuration from which to extract information
   * @param environment Environment map to update
   */
  public static void updateEnvironmentForMRAM(Configuration conf,
      Map<String, String> environment) {
    MRApps.setEnvFromInputString(environment,
        conf.get(MRJobConfig.MR_AM_ADMIN_USER_ENV));
    MRApps.setEnvFromInputString(environment,
        conf.get(MRJobConfig.MR_AM_ENV));
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
