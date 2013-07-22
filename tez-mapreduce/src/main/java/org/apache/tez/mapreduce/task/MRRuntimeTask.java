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

package org.apache.tez.mapreduce.task;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.engine.api.Input;
import org.apache.tez.engine.api.Master;
import org.apache.tez.engine.api.Output;
import org.apache.tez.engine.api.Processor;
import org.apache.tez.engine.common.security.JobTokenIdentifier;
import org.apache.tez.engine.common.security.TokenCache;
import org.apache.tez.engine.task.RuntimeTask;
import org.apache.tez.mapreduce.hadoop.DeprecatedKeys;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;
import org.apache.tez.mapreduce.processor.MRTask;
import org.apache.tez.mapreduce.task.impl.YarnOutputFiles;

@SuppressWarnings("deprecation")
public class MRRuntimeTask extends RuntimeTask {

  private static final Log LOG = LogFactory.getLog(MRRuntimeTask.class);

  private MRTask mrTask;

  public MRRuntimeTask(TezEngineTaskContext taskContext, Processor processor,
      Input[] inputs, Output[] outputs) {
    super(taskContext, processor, inputs, outputs);
  }

  @Override
  public void initialize(Configuration conf, byte[] userPayload,
      Master master) throws IOException, InterruptedException {

    DeprecatedKeys.init();

    Configuration taskConf = null;
    
    if (userPayload == null) {
      // Fall back to using job.xml
      Configuration mrConf = new Configuration(conf);
      mrConf.addResource(MRJobConfig.JOB_CONF_FILE);
      taskConf = MultiStageMRConfigUtil.getConfForVertex(mrConf,
          taskContext.getVertexName());
    } else {
      taskConf = MRHelpers.createConfFromUserPayload(userPayload);
      copyTezConfigParameters(taskConf, conf);
    }

    // TODO Avoid all this extra config manipulation.
    // FIXME we need I/O/p level configs to be used in init below

    // TODO Post MRR
    // A single file per vertex will likely be a better solution. Does not
    // require translation - client can take care of this. Will work independent
    // of whether the configuration is for intermediate tasks or not. Has the
    // overhead of localizing multiple files per job - i.e. the client would
    // need to write these files to hdfs, add them as local resources per
    // vertex. A solution like this may be more practical once it's possible to
    // submit configuration parameters to the AM and effectively tasks via RPC.

    final JobConf job = new JobConf(taskConf);
    job.set(MRJobConfig.VERTEX_NAME, taskContext.getVertexName());

    MRTask mrTask = (MRTask) getProcessor();
    this.mrTask = mrTask;
    configureMRTask(job, mrTask);

    this.conf = job;
    this.master = master;

    // NOTE: Allow processor to initialize input/output
    processor.initialize(this.conf, this.master);
  }
  
  /*
   * Used when creating a conf from the userPayload. Need to copy all the tez
   * config parameters which are set by YarnTezDagChild
   */
  public static void copyTezConfigParameters(Configuration conf,
      Configuration tezTaskConf) {
    Iterator<Entry<String, String>> iter = tezTaskConf.iterator();
    while (iter.hasNext()) {
      Entry<String, String> entry = iter.next();
      if (conf.get(entry.getKey()) == null) {
        conf.set(entry.getKey(), tezTaskConf.get(entry.getKey()));
      }
    }
  }

  @Override
  public void run() throws IOException, InterruptedException {
    TezTaskUmbilicalProtocol umbilical = (TezTaskUmbilicalProtocol) master;
    try {
      super.run();
    } catch (FSError e) {
      throw e;
    } catch (Exception exception) {
      LOG.warn("Exception running child : "
          + StringUtils.stringifyException(exception));
      try {
        if (mrTask != null) {
          mrTask.taskCleanup(umbilical);
        }
      } catch (Exception e) {
        LOG.info("Exception cleanup up: " + StringUtils.stringifyException(e));
      }
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      exception.printStackTrace(new PrintStream(baos));
      if (taskContext.getTaskAttemptId() != null) {
        umbilical.fatalError(taskContext.getTaskAttemptId(), baos.toString());
      }
    }
  }

  @Override
  public void close() throws IOException, InterruptedException {
    // NOTE: Allow processor to close input/output
    processor.close();
  }

  private static void configureMRTask(JobConf job, MRTask task)
      throws IOException, InterruptedException {

    Credentials credentials = UserGroupInformation.getCurrentUser()
        .getCredentials();
    job.setCredentials(credentials);
    // TODO Can this be avoided all together. Have the MRTezOutputCommitter use
    // the Tez parameter.
    // TODO This could be fetched from the env if YARN is setting it for all
    // Containers.
    // Set it in conf, so as to be able to be used the the OutputCommitter.
    job.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID,
        job.getInt(TezJobConfig.APPLICATION_ATTEMPT_ID, -1));

    job.setClass(MRConfig.TASK_LOCAL_OUTPUT_CLASS, YarnOutputFiles.class,
        MapOutputFile.class); // MR

    Token<JobTokenIdentifier> jobToken = TokenCache.getJobToken(credentials);
    if (jobToken != null) {
      // Will MR ever run without a job token.
      SecretKey sk = JobTokenSecretManager.createSecretKey(jobToken
          .getPassword());
      task.setJobTokenSecret(sk);
    } else {
      LOG.warn("No job token set");
    }

    job.set(MRJobConfig.JOB_LOCAL_DIR, job.get(TezJobConfig.JOB_LOCAL_DIR));
    job.set(MRConfig.LOCAL_DIR, job.get(TezJobConfig.LOCAL_DIRS));
    if (job.get(TezJobConfig.DAG_CREDENTIALS_BINARY) != null) {
      job.set(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY,
          job.get(TezJobConfig.DAG_CREDENTIALS_BINARY));
    }

    // setup the child's attempt directories
    // Do the task-type specific localization
    task.localizeConfiguration(job);

    // Set up the DistributedCache related configs
    setupDistributedCacheConfig(job);

    task.setConf(job);
  }

  /**
   * Set up the DistributedCache related configs to make
   * {@link DistributedCache#getLocalCacheFiles(Configuration)} and
   * {@link DistributedCache#getLocalCacheArchives(Configuration)} working.
   * 
   * @param job
   * @throws IOException
   */
  private static void setupDistributedCacheConfig(final JobConf job)
      throws IOException {

    String localWorkDir = (job.get(TezJobConfig.TASK_LOCAL_RESOURCE_DIR));
    // ^ ^ all symlinks are created in the current work-dir

    // Update the configuration object with localized archives.
    URI[] cacheArchives = DistributedCache.getCacheArchives(job);
    if (cacheArchives != null) {
      List<String> localArchives = new ArrayList<String>();
      for (int i = 0; i < cacheArchives.length; ++i) {
        URI u = cacheArchives[i];
        Path p = new Path(u);
        Path name = new Path((null == u.getFragment()) ? p.getName()
            : u.getFragment());
        String linkName = name.toUri().getPath();
        localArchives.add(new Path(localWorkDir, linkName).toUri().getPath());
      }
      if (!localArchives.isEmpty()) {
        job.set(MRJobConfig.CACHE_LOCALARCHIVES, StringUtils
            .arrayToString(localArchives.toArray(new String[localArchives
                .size()])));
      }
    }

    // Update the configuration object with localized files.
    URI[] cacheFiles = DistributedCache.getCacheFiles(job);
    if (cacheFiles != null) {
      List<String> localFiles = new ArrayList<String>();
      for (int i = 0; i < cacheFiles.length; ++i) {
        URI u = cacheFiles[i];
        Path p = new Path(u);
        Path name = new Path((null == u.getFragment()) ? p.getName()
            : u.getFragment());
        String linkName = name.toUri().getPath();
        localFiles.add(new Path(localWorkDir, linkName).toUri().getPath());
      }
      if (!localFiles.isEmpty()) {
        job.set(MRJobConfig.CACHE_LOCALFILES, StringUtils
            .arrayToString(localFiles.toArray(new String[localFiles.size()])));
      }
    }
  }

}
