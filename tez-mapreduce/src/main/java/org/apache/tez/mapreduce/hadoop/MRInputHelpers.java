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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;
import org.apache.hadoop.mapreduce.split.TezGroupedSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos;

@Public
@Unstable
public class MRInputHelpers {

  private static final Logger LOG = LoggerFactory.getLogger(MRInputHelpers.class);
  private static final int SPLIT_SERIALIZED_LENGTH_ESTIMATE = 40;
  static final String JOB_SPLIT_RESOURCE_NAME = "job.split";
  static final String JOB_SPLIT_METAINFO_RESOURCE_NAME = "job.splitmetainfo";

  /**
   * Setup split generation on the client, with splits being distributed via the traditional
   * MapReduce mechanism of distributing splits via the Distributed Cache.
   * <p/>
   * Usage of this technique for handling splits is not advised. Instead, splits should be either
   * generated in the AM, or generated in the client and distributed via the AM. See {@link
   * org.apache.tez.mapreduce.input.MRInput.MRInputConfigBuilder}
   * <p/>
   * Note: Attempting to use this method to add multiple Inputs to a Vertex is not supported.
   *
   * This mechanism of propagating splits may be removed in a subsequent release, and is not recommended.
   *
   * @param conf           configuration to be used by {@link org.apache.tez.mapreduce.input.MRInput}.
   *                       This is expected to be fully configured.
   * @param splitsDir      the path to which splits will be generated.
   * @param useLegacyInput whether to use {@link org.apache.tez.mapreduce.input.MRInputLegacy} or
   *                       {@link org.apache.tez.mapreduce.input.MRInput}
   * @return an instance of {@link org.apache.tez.dag.api.DataSourceDescriptor} which can be added
   * as a data source to a {@link org.apache.tez.dag.api.Vertex}
   */
  @InterfaceStability.Unstable
  public static DataSourceDescriptor configureMRInputWithLegacySplitGeneration(Configuration conf,
                                                                               Path splitsDir,
                                                                               boolean useLegacyInput) {
    InputSplitInfo inputSplitInfo = null;
    try {
      inputSplitInfo = generateInputSplits(conf, splitsDir);

      InputDescriptor inputDescriptor = InputDescriptor.create(useLegacyInput ? MRInputLegacy.class
          .getName() : MRInput.class.getName())
          .setUserPayload(createMRInputPayload(conf, null));
      Map<String, LocalResource> additionalLocalResources = new HashMap<String, LocalResource>();
      updateLocalResourcesForInputSplits(conf, inputSplitInfo,
          additionalLocalResources);
      DataSourceDescriptor dsd =
          DataSourceDescriptor.create(inputDescriptor, null, inputSplitInfo.getNumTasks(),
              inputSplitInfo.getCredentials(),
              VertexLocationHint.create(inputSplitInfo.getTaskLocationHints()),
              additionalLocalResources);
      return dsd;
    } catch (IOException e) {
      throw new TezUncheckedException("Failed to generate InputSplits", e);
    } catch (InterruptedException e) {
      throw new TezUncheckedException("Failed to generate InputSplits", e);
    } catch (ClassNotFoundException e) {
      throw new TezUncheckedException("Failed to generate InputSplits", e);
    }
  }


  /**
   * Parse the payload used by MRInputPayload
   *
   * @param payload the {@link org.apache.tez.dag.api.UserPayload} instance
   * @return an instance of {@link org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto},
   * which provides access to the underlying configuration bytes
   * @throws IOException
   */
  @InterfaceStability.Evolving
  public static MRRuntimeProtos.MRInputUserPayloadProto parseMRInputPayload(UserPayload payload)
      throws IOException {
    return MRRuntimeProtos.MRInputUserPayloadProto.parseFrom(ByteString.copyFrom(payload.getPayload()));
  }

  /**
   * Create an instance of {@link org.apache.hadoop.mapred.InputSplit} from the {@link
   * org.apache.tez.mapreduce.input.MRInput} representation of a split.
   *
   * @param splitProto           The {@link org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto}
   *                             instance representing the split
   * @param serializationFactory the serialization mechanism used to write out the split
   * @return an instance of the split
   * @throws java.io.IOException
   */
  @SuppressWarnings("unchecked")
  @InterfaceStability.Evolving
  public static InputSplit createOldFormatSplitFromUserPayload(
      MRRuntimeProtos.MRSplitProto splitProto, SerializationFactory serializationFactory)
      throws IOException {
    // This may not need to use serialization factory, since OldFormat
    // always uses Writable to write splits.
    Preconditions.checkNotNull(splitProto, "splitProto cannot be null");
    String className = splitProto.getSplitClassName();
    Class<InputSplit> clazz;

    try {
      clazz = (Class<InputSplit>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed to load InputSplit class: [" + className + "]", e);
    }

    Deserializer<InputSplit> deserializer = serializationFactory
        .getDeserializer(clazz);
    deserializer.open(splitProto.getSplitBytes().newInput());
    InputSplit inputSplit = deserializer.deserialize(null);
    deserializer.close();
    return inputSplit;
  }

  /**
   * Create an instance of {@link org.apache.hadoop.mapreduce.InputSplit} from the {@link
   * org.apache.tez.mapreduce.input.MRInput} representation of a split.
   *
   * @param splitProto           The {@link org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto}
   *                             instance representing the split
   * @param serializationFactory the serialization mechanism used to write out the split
   * @return an instance of the split
   * @throws IOException
   */
  @InterfaceStability.Evolving
  @SuppressWarnings("unchecked")
  public static org.apache.hadoop.mapreduce.InputSplit createNewFormatSplitFromUserPayload(
      MRRuntimeProtos.MRSplitProto splitProto, SerializationFactory serializationFactory)
      throws IOException {
    Preconditions.checkNotNull(splitProto, "splitProto must be specified");
    String className = splitProto.getSplitClassName();
    Class<org.apache.hadoop.mapreduce.InputSplit> clazz;

    try {
      clazz = (Class<org.apache.hadoop.mapreduce.InputSplit>) Class
          .forName(className);
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed to load InputSplit class: [" + className + "]", e);
    }

    Deserializer<org.apache.hadoop.mapreduce.InputSplit> deserializer = serializationFactory
        .getDeserializer(clazz);
    deserializer.open(splitProto.getSplitBytes().newInput());
    org.apache.hadoop.mapreduce.InputSplit inputSplit = deserializer
        .deserialize(null);
    deserializer.close();
    return inputSplit;
  }

  @InterfaceStability.Evolving
  public static <T extends org.apache.hadoop.mapreduce.InputSplit> MRRuntimeProtos.MRSplitProto createSplitProto(
      T newSplit, SerializationFactory serializationFactory)
      throws IOException, InterruptedException {
    MRRuntimeProtos.MRSplitProto.Builder builder = MRRuntimeProtos.MRSplitProto
        .newBuilder();

    builder.setSplitClassName(newSplit.getClass().getName());

    @SuppressWarnings("unchecked")
    Serializer<T> serializer = serializationFactory
        .getSerializer((Class<T>) newSplit.getClass());
    ByteString.Output out = ByteString
        .newOutput(SPLIT_SERIALIZED_LENGTH_ESTIMATE);
    serializer.open(out);
    serializer.serialize(newSplit);
    // TODO MR Compat: Check against max block locations per split.
    ByteString splitBs = out.toByteString();
    builder.setSplitBytes(splitBs);

    return builder.build();
  }

  @InterfaceStability.Evolving
  public static MRRuntimeProtos.MRSplitProto createSplitProto(
      org.apache.hadoop.mapred.InputSplit oldSplit) throws IOException {
    MRRuntimeProtos.MRSplitProto.Builder builder = MRRuntimeProtos.MRSplitProto.newBuilder();

    builder.setSplitClassName(oldSplit.getClass().getName());

    ByteString.Output os = ByteString
        .newOutput(SPLIT_SERIALIZED_LENGTH_ESTIMATE);
    oldSplit.write(new DataOutputStream(os));
    ByteString splitBs = os.toByteString();
    builder.setSplitBytes(splitBs);

    return builder.build();
  }

  /**
   * Generates Input splits and stores them in a {@link org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto} instance.
   *
   * Returns an instance of {@link InputSplitInfoMem}
   *
   * With grouping enabled, the eventual configuration used by the tasks, will have
   * the user-specified InputFormat replaced by either {@link org.apache.hadoop.mapred.split.TezGroupedSplitsInputFormat}
   * or {@link org.apache.hadoop.mapreduce.split.TezGroupedSplitsInputFormat}
   *
   * @param conf
   *          an instance of Configuration which is used to determine whether
   *          the mapred of mapreduce API is being used. This Configuration
   *          instance should also contain adequate information to be able to
   *          generate splits - like the InputFormat being used and related
   *          configuration.
   * @param groupSplits whether to group the splits or not
   * @param targetTasks the number of target tasks if grouping is enabled. Specify as 0 otherwise.
   * @return an instance of {@link InputSplitInfoMem} which supports a subset of
   *         the APIs defined on {@link InputSplitInfo}
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @InterfaceStability.Unstable
  public static InputSplitInfoMem generateInputSplitsToMem(Configuration conf, boolean groupSplits,
                                                           int targetTasks)
      throws IOException, ClassNotFoundException, InterruptedException {

    InputSplitInfoMem splitInfoMem = null;
    JobConf jobConf = new JobConf(conf);
    if (jobConf.getUseNewMapper()) {
      LOG.info("Generating mapreduce api input splits");
      Job job = Job.getInstance(conf);
      org.apache.hadoop.mapreduce.InputSplit[] splits =
          generateNewSplits(job, groupSplits, targetTasks);
      splitInfoMem = new InputSplitInfoMem(splits, createTaskLocationHintsFromSplits(splits),
          splits.length, job.getCredentials(), job.getConfiguration());
    } else {
      LOG.info("Generating mapred api input splits");
      org.apache.hadoop.mapred.InputSplit[] splits =
          generateOldSplits(jobConf, groupSplits, targetTasks);
      splitInfoMem = new InputSplitInfoMem(splits, createTaskLocationHintsFromSplits(splits),
          splits.length, jobConf.getCredentials(), jobConf);
    }
    LOG.info("NumSplits: " + splitInfoMem.getNumTasks() + ", SerializedSize: "
        + splitInfoMem.getSplitsProto().getSerializedSize());
    return splitInfoMem;
  }

  private static List<TaskLocationHint> createTaskLocationHintsFromSplits(
      org.apache.hadoop.mapreduce.InputSplit[] newFormatSplits) {
    Iterable<TaskLocationHint> iterable = Iterables
        .transform(Arrays.asList(newFormatSplits),
            new Function<org.apache.hadoop.mapreduce.InputSplit, TaskLocationHint>() {
              @Override

              public TaskLocationHint apply(
                  org.apache.hadoop.mapreduce.InputSplit input) {
                try {
                  if (input instanceof TezGroupedSplit) {
                    String rack =
                        ((org.apache.hadoop.mapreduce.split.TezGroupedSplit) input).getRack();
                    if (rack == null) {
                      if (input.getLocations() != null) {
                        return TaskLocationHint.createTaskLocationHint(
                            new HashSet<String>(Arrays.asList(input.getLocations())), null);
                      } else {
                        return TaskLocationHint.createTaskLocationHint(null, null);
                      }
                    } else {
                      return TaskLocationHint.createTaskLocationHint(null,
                          Collections.singleton(rack));
                    }
                  } else {
                    return TaskLocationHint.createTaskLocationHint(
                        new HashSet<String>(Arrays.asList(input.getLocations())), null);
                  }
                } catch (IOException e) {
                  throw new RuntimeException(e);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            });
    return Lists.newArrayList(iterable);
  }

  private static List<TaskLocationHint> createTaskLocationHintsFromSplits(
      org.apache.hadoop.mapred.InputSplit[] oldFormatSplits) {
    Iterable<TaskLocationHint> iterable = Iterables.transform(Arrays.asList(oldFormatSplits),
        new Function<org.apache.hadoop.mapred.InputSplit, TaskLocationHint>() {
          @Override
          public TaskLocationHint apply(org.apache.hadoop.mapred.InputSplit input) {
            try {
              if (input instanceof org.apache.hadoop.mapred.split.TezGroupedSplit) {
                String rack = ((org.apache.hadoop.mapred.split.TezGroupedSplit) input).getRack();
                if (rack == null) {
                  if (input.getLocations() != null) {
                    return TaskLocationHint.createTaskLocationHint(new HashSet<String>(Arrays.asList(
                        input.getLocations())), null);
                  } else {
                    return TaskLocationHint.createTaskLocationHint(null, null);
                  }
                } else {
                  return TaskLocationHint.createTaskLocationHint(null, Collections.singleton(rack));
                }
              } else {
                return TaskLocationHint.createTaskLocationHint(
                    new HashSet<String>(Arrays.asList(input.getLocations())),
                    null);
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
    return Lists.newArrayList(iterable);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static org.apache.hadoop.mapreduce.InputSplit[] generateNewSplits(
      JobContext jobContext, boolean groupSplits, int numTasks)
      throws ClassNotFoundException, IOException,
      InterruptedException {
    Configuration conf = jobContext.getConfiguration();


    // This is the real input format.
    org.apache.hadoop.mapreduce.InputFormat<?, ?> inputFormat = null;
    try {
      inputFormat = ReflectionUtils.newInstance(jobContext.getInputFormatClass(), conf);
    } catch (ClassNotFoundException e) {
      throw new TezUncheckedException(e);
    }

    org.apache.hadoop.mapreduce.InputFormat<?, ?> finalInputFormat = inputFormat;

    // For grouping, the underlying InputFormatClass class is passed in as a parameter.
    // JobContext has this setup as TezGroupedSplitInputFormat
    if (groupSplits) {
      org.apache.hadoop.mapreduce.split.TezGroupedSplitsInputFormat groupedFormat =
          new org.apache.hadoop.mapreduce.split.TezGroupedSplitsInputFormat();
      groupedFormat.setConf(conf);
      groupedFormat.setInputFormat(inputFormat);
      groupedFormat.setDesiredNumberOfSplits(numTasks);
      finalInputFormat = groupedFormat;
    } else {
      finalInputFormat = inputFormat;
    }

    List<org.apache.hadoop.mapreduce.InputSplit> array = finalInputFormat
        .getSplits(jobContext);
    org.apache.hadoop.mapreduce.InputSplit[] splits = (org.apache.hadoop.mapreduce.InputSplit[]) array
        .toArray(new org.apache.hadoop.mapreduce.InputSplit[array.size()]);

    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(splits, new InputSplitComparator());
    return splits;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static org.apache.hadoop.mapred.InputSplit[] generateOldSplits(
      JobConf jobConf, boolean groupSplits, int numTasks) throws IOException {

    // This is the real InputFormat
    org.apache.hadoop.mapred.InputFormat inputFormat;
    try {
      inputFormat = jobConf.getInputFormat();
    } catch (Exception e) {
      throw new TezUncheckedException(e);
    }

    org.apache.hadoop.mapred.InputFormat finalInputFormat = inputFormat;

    if (groupSplits) {
      org.apache.hadoop.mapred.split.TezGroupedSplitsInputFormat groupedFormat =
          new org.apache.hadoop.mapred.split.TezGroupedSplitsInputFormat();
      groupedFormat.setConf(jobConf);
      groupedFormat.setInputFormat(inputFormat);
      groupedFormat.setDesiredNumberOfSplits(numTasks);
      finalInputFormat = groupedFormat;
    } else {
      finalInputFormat = inputFormat;
    }
    org.apache.hadoop.mapred.InputSplit[] splits = finalInputFormat
        .getSplits(jobConf, jobConf.getNumMapTasks());
    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(splits, new OldInputSplitComparator());
    return splits;
  }

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
  private static InputSplitInfoDisk writeNewSplits(JobContext jobContext,
                                                   Path inputSplitDir) throws IOException, InterruptedException,
      ClassNotFoundException {

    org.apache.hadoop.mapreduce.InputSplit[] splits =
        generateNewSplits(jobContext, false, 0);

    Configuration conf = jobContext.getConfiguration();

    JobSplitWriter.createSplitFiles(inputSplitDir, conf,
        inputSplitDir.getFileSystem(conf), splits);

    List<TaskLocationHint> locationHints =
        new ArrayList<TaskLocationHint>(splits.length);
    for (int i = 0; i < splits.length; ++i) {
      locationHints.add(
          TaskLocationHint.createTaskLocationHint(new HashSet<String>(
              Arrays.asList(splits[i].getLocations())), null)
      );
    }

    return new InputSplitInfoDisk(
        JobSubmissionFiles.getJobSplitFile(inputSplitDir),
        JobSubmissionFiles.getJobSplitMetaFile(inputSplitDir),
        splits.length, locationHints, jobContext.getCredentials());
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
  private static InputSplitInfoDisk writeOldSplits(JobConf jobConf,
                                                   Path inputSplitDir) throws IOException {

    org.apache.hadoop.mapred.InputSplit[] splits =
        generateOldSplits(jobConf, false, 0);

    JobSplitWriter.createSplitFiles(inputSplitDir, jobConf,
        inputSplitDir.getFileSystem(jobConf), splits);

    List<TaskLocationHint> locationHints =
        new ArrayList<TaskLocationHint>(splits.length);
    for (int i = 0; i < splits.length; ++i) {
      locationHints.add(
          TaskLocationHint.createTaskLocationHint(new HashSet<String>(
              Arrays.asList(splits[i].getLocations())), null)
      );
    }

    return new InputSplitInfoDisk(
        JobSubmissionFiles.getJobSplitFile(inputSplitDir),
        JobSubmissionFiles.getJobSplitMetaFile(inputSplitDir),
        splits.length, locationHints, jobConf.getCredentials());
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
  private static InputSplitInfoDisk generateInputSplits(Configuration conf,
                                                        Path inputSplitsDir) throws IOException, InterruptedException,
      ClassNotFoundException {
    Job job = Job.getInstance(conf);
    JobConf jobConf = new JobConf(conf);
    conf.setBoolean(MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS, false);
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

  /**
   * Update provided localResources collection with the required local
   * resources needed by MapReduce tasks with respect to Input splits.
   *
   * @param conf Configuration
   * @param inputSplitInfo Information on location of split files
   * @param localResources LocalResources collection to be updated
   * @throws IOException
   */
  private static void updateLocalResourcesForInputSplits(
      Configuration conf,
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

    FileSystem splitsFS = inputSplitInfo.getSplitsFile().getFileSystem(conf);
    FileStatus splitFileStatus =
        splitsFS.getFileStatus(inputSplitInfo.getSplitsFile());
    FileStatus metaInfoFileStatus =
        splitsFS.getFileStatus(inputSplitInfo.getSplitsMetaInfoFile());

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
   * Called to specify that grouping of input splits be performed by Tez
   * The conf should have the input format class configuration
   * set to the TezGroupedSplitsInputFormat. The real input format class name
   * should be passed as an argument to this method.
   * <p/>
   * With grouping enabled, the eventual configuration used by the tasks, will have
   * the user-specified InputFormat replaced by either {@link org.apache.hadoop.mapred.split.TezGroupedSplitsInputFormat}
   * or {@link org.apache.hadoop.mapreduce.split.TezGroupedSplitsInputFormat}
   */
  @InterfaceAudience.Private
  protected static UserPayload createMRInputPayloadWithGrouping(Configuration conf) throws IOException {
    Preconditions
        .checkArgument(conf != null, "Configuration must be specified");
    return createMRInputPayload(TezUtils.createByteStringFromConf(conf),
        null, true);
  }

  @InterfaceAudience.Private
  protected static UserPayload createMRInputPayload(Configuration conf,
                                               MRRuntimeProtos.MRSplitsProto mrSplitsProto) throws
      IOException {
    Preconditions
        .checkArgument(conf != null, "Configuration must be specified");

    return createMRInputPayload(TezUtils.createByteStringFromConf(conf),
        mrSplitsProto, false);
  }

  private static UserPayload createMRInputPayload(ByteString bytes,
                                             MRRuntimeProtos.MRSplitsProto mrSplitsProto,
                                             boolean isGrouped) throws IOException {
    MRRuntimeProtos.MRInputUserPayloadProto.Builder userPayloadBuilder =
        MRRuntimeProtos.MRInputUserPayloadProto
            .newBuilder();
    userPayloadBuilder.setConfigurationBytes(bytes);
    if (mrSplitsProto != null) {
      userPayloadBuilder.setSplits(mrSplitsProto);
    }
    userPayloadBuilder.setGroupingEnabled(isGrouped);
    return UserPayload.create(userPayloadBuilder.build().toByteString().asReadOnlyByteBuffer());
  }

}
