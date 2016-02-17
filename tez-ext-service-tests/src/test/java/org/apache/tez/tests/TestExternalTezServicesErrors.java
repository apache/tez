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

package org.apache.tez.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.app.ErrorPluginConfiguration;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.launcher.TezTestServiceContainerLauncherWithErrors;
import org.apache.tez.dag.app.launcher.TezTestServiceNoOpContainerLauncher;
import org.apache.tez.dag.app.rm.TezTestServiceTaskSchedulerService;
import org.apache.tez.dag.app.rm.TezTestServiceTaskSchedulerServiceWithErrors;
import org.apache.tez.dag.app.taskcomm.TezTestServiceTaskCommunicatorImpl;
import org.apache.tez.dag.app.taskcomm.TezTestServiceTaskCommunicatorWithErrors;
import org.apache.tez.examples.JoinValidateConfigured;
import org.apache.tez.serviceplugins.api.ContainerLauncherDescriptor;
import org.apache.tez.serviceplugins.api.ServicePluginErrorDefaults;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
import org.apache.tez.serviceplugins.api.TaskCommunicatorDescriptor;
import org.apache.tez.serviceplugins.api.TaskSchedulerDescriptor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestExternalTezServicesErrors {

  private static final Logger LOG = LoggerFactory.getLogger(TestExternalTezServicesErrors.class);

  private static final String EXT_PUSH_ENTITY_NAME = "ExtServiceTestPush";
  private static final String EXT_THROW_ERROR_ENTITY_NAME = "ExtServiceTestThrowErrors";
  private static final String EXT_REPORT_NON_FATAL_ERROR_ENTITY_NAME = "ExtServiceTestReportNonFatalErrors";
  private static final String EXT_REPORT_FATAL_ERROR_ENTITY_NAME = "ExtServiceTestReportFatalErrors";

  private static final String SUFFIX_LAUNCHER = "ContainerLauncher";
  private static final String SUFFIX_TASKCOMM = "TaskCommunicator";
  private static final String SUFFIX_SCHEDULER = "TaskScheduler";

  private static ExternalTezServiceTestHelper extServiceTestHelper;

  private static ServicePluginsDescriptor servicePluginsDescriptor;

  private static final Path SRC_DATA_DIR = new Path("/tmp/" + TestExternalTezServicesErrors.class.getSimpleName());
  private static final Path HASH_JOIN_EXPECTED_RESULT_PATH = new Path(SRC_DATA_DIR, "expectedOutputPath");
  private static final Path HASH_JOIN_OUTPUT_PATH = new Path(SRC_DATA_DIR, "outPath");

  private static final Vertex.VertexExecutionContext EXECUTION_CONTEXT_EXT_SERVICE_PUSH =
      Vertex.VertexExecutionContext.create(
          EXT_PUSH_ENTITY_NAME, EXT_PUSH_ENTITY_NAME, EXT_PUSH_ENTITY_NAME);
  // Throw error contexts
  private static final Vertex.VertexExecutionContext EXECUTION_CONTEXT_LAUNCHER_THROW =
      Vertex.VertexExecutionContext.create(EXT_PUSH_ENTITY_NAME, EXT_THROW_ERROR_ENTITY_NAME, EXT_PUSH_ENTITY_NAME);
  private static final Vertex.VertexExecutionContext EXECUTION_CONTEXT_TASKCOMM_THROW =
      Vertex.VertexExecutionContext.create(EXT_PUSH_ENTITY_NAME, EXT_PUSH_ENTITY_NAME,
          EXT_THROW_ERROR_ENTITY_NAME);
  private static final Vertex.VertexExecutionContext EXECUTION_CONTEXT_SCHEDULER_THROW =
      Vertex.VertexExecutionContext.create(EXT_THROW_ERROR_ENTITY_NAME, EXT_PUSH_ENTITY_NAME, EXT_PUSH_ENTITY_NAME);

  // Report-non-fatal contexts
  private static final Vertex.VertexExecutionContext EXECUTION_CONTEXT_LAUNCHER_REPORT_NON_FATAL =
      Vertex.VertexExecutionContext.create(EXT_PUSH_ENTITY_NAME, EXT_REPORT_NON_FATAL_ERROR_ENTITY_NAME, EXT_PUSH_ENTITY_NAME);
  private static final Vertex.VertexExecutionContext EXECUTION_CONTEXT_TASKCOMM_REPORT_NON_FATAL =
      Vertex.VertexExecutionContext.create(EXT_PUSH_ENTITY_NAME, EXT_PUSH_ENTITY_NAME,
          EXT_REPORT_NON_FATAL_ERROR_ENTITY_NAME);
  private static final Vertex.VertexExecutionContext EXECUTION_CONTEXT_SCHEDULER_REPORT_NON_FATAL =
      Vertex.VertexExecutionContext.create(EXT_REPORT_NON_FATAL_ERROR_ENTITY_NAME, EXT_PUSH_ENTITY_NAME, EXT_PUSH_ENTITY_NAME);

  // Report fatal contexts
  private static final Vertex.VertexExecutionContext EXECUTION_CONTEXT_LAUNCHER_REPORT_FATAL =
      Vertex.VertexExecutionContext.create(EXT_PUSH_ENTITY_NAME, EXT_REPORT_FATAL_ERROR_ENTITY_NAME, EXT_PUSH_ENTITY_NAME);
  private static final Vertex.VertexExecutionContext EXECUTION_CONTEXT_TASKCOMM_REPORT_FATAL =
      Vertex.VertexExecutionContext.create(EXT_PUSH_ENTITY_NAME, EXT_PUSH_ENTITY_NAME,
          EXT_REPORT_FATAL_ERROR_ENTITY_NAME);
  private static final Vertex.VertexExecutionContext EXECUTION_CONTEXT_SCHEDULER_REPORT_FATAL =
      Vertex.VertexExecutionContext.create(EXT_REPORT_FATAL_ERROR_ENTITY_NAME, EXT_PUSH_ENTITY_NAME, EXT_PUSH_ENTITY_NAME);


  private static final Vertex.VertexExecutionContext EXECUTION_CONTEXT_DEFAULT = EXECUTION_CONTEXT_EXT_SERVICE_PUSH;

  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR + TestExternalTezServicesErrors.class.getName()
      + "-tmpDir";

  @BeforeClass
  public static void setup() throws Exception {

    extServiceTestHelper = new ExternalTezServiceTestHelper(TEST_ROOT_DIR);
    UserPayload userPayload =
        TezUtils.createUserPayloadFromConf(extServiceTestHelper.getConfForJobs());
    UserPayload userPayloadThrowError =
        ErrorPluginConfiguration.toUserPayload(ErrorPluginConfiguration.createThrowErrorConf());

    UserPayload userPayloadReportFatalErrorLauncher = ErrorPluginConfiguration
        .toUserPayload(ErrorPluginConfiguration.createReportFatalErrorConf(SUFFIX_LAUNCHER));
    UserPayload userPayloadReportFatalErrorTaskComm = ErrorPluginConfiguration
        .toUserPayload(ErrorPluginConfiguration.createReportFatalErrorConf(SUFFIX_TASKCOMM));
    UserPayload userPayloadReportFatalErrorScheduler = ErrorPluginConfiguration
        .toUserPayload(ErrorPluginConfiguration.createReportFatalErrorConf(SUFFIX_SCHEDULER));

    UserPayload userPayloadReportNonFatalErrorLauncher = ErrorPluginConfiguration
        .toUserPayload(ErrorPluginConfiguration.createReportNonFatalErrorConf(SUFFIX_LAUNCHER));
    UserPayload userPayloadReportNonFatalErrorTaskComm = ErrorPluginConfiguration
        .toUserPayload(ErrorPluginConfiguration.createReportNonFatalErrorConf(SUFFIX_TASKCOMM));
    UserPayload userPayloadReportNonFatalErrorScheduler = ErrorPluginConfiguration
        .toUserPayload(ErrorPluginConfiguration.createReportNonFatalErrorConf(SUFFIX_SCHEDULER));

    TaskSchedulerDescriptor[] taskSchedulerDescriptors = new TaskSchedulerDescriptor[]{
        TaskSchedulerDescriptor
            .create(EXT_PUSH_ENTITY_NAME, TezTestServiceTaskSchedulerService.class.getName())
            .setUserPayload(userPayload),
        TaskSchedulerDescriptor.create(EXT_THROW_ERROR_ENTITY_NAME,
            TezTestServiceTaskSchedulerServiceWithErrors.class.getName()).setUserPayload(
            userPayloadThrowError),
        TaskSchedulerDescriptor.create(EXT_REPORT_FATAL_ERROR_ENTITY_NAME,
            TezTestServiceTaskSchedulerServiceWithErrors.class.getName()).setUserPayload(
            userPayloadReportFatalErrorScheduler),
        TaskSchedulerDescriptor.create(EXT_REPORT_NON_FATAL_ERROR_ENTITY_NAME,
            TezTestServiceTaskSchedulerServiceWithErrors.class.getName()).setUserPayload(
            userPayloadReportNonFatalErrorScheduler),
    };

    ContainerLauncherDescriptor[] containerLauncherDescriptors = new ContainerLauncherDescriptor[]{
        ContainerLauncherDescriptor
            .create(EXT_PUSH_ENTITY_NAME, TezTestServiceNoOpContainerLauncher.class.getName())
            .setUserPayload(userPayload),
        ContainerLauncherDescriptor.create(EXT_THROW_ERROR_ENTITY_NAME,
            TezTestServiceContainerLauncherWithErrors.class.getName()).setUserPayload(userPayloadThrowError),
        ContainerLauncherDescriptor.create(EXT_REPORT_FATAL_ERROR_ENTITY_NAME,
            TezTestServiceContainerLauncherWithErrors.class.getName()).setUserPayload(userPayloadReportFatalErrorLauncher),
        ContainerLauncherDescriptor.create(EXT_REPORT_NON_FATAL_ERROR_ENTITY_NAME,
            TezTestServiceContainerLauncherWithErrors.class.getName()).setUserPayload(userPayloadReportNonFatalErrorLauncher)
    };

    TaskCommunicatorDescriptor[] taskCommunicatorDescriptors = new TaskCommunicatorDescriptor[]{
        TaskCommunicatorDescriptor
            .create(EXT_PUSH_ENTITY_NAME, TezTestServiceTaskCommunicatorImpl.class.getName())
            .setUserPayload(userPayload),
        TaskCommunicatorDescriptor.create(EXT_THROW_ERROR_ENTITY_NAME,
            TezTestServiceTaskCommunicatorWithErrors.class.getName()).setUserPayload(userPayloadThrowError),
        TaskCommunicatorDescriptor.create(EXT_REPORT_FATAL_ERROR_ENTITY_NAME,
            TezTestServiceTaskCommunicatorWithErrors.class.getName()).setUserPayload(userPayloadReportFatalErrorTaskComm),
        TaskCommunicatorDescriptor.create(EXT_REPORT_NON_FATAL_ERROR_ENTITY_NAME,
            TezTestServiceTaskCommunicatorWithErrors.class.getName()).setUserPayload(userPayloadReportNonFatalErrorTaskComm)
    };

    servicePluginsDescriptor = ServicePluginsDescriptor.create(true, true,
        taskSchedulerDescriptors, containerLauncherDescriptors, taskCommunicatorDescriptors);

    extServiceTestHelper.setupSharedTezClient(servicePluginsDescriptor);

    // Generate the join data set used for each run.
    // Can a timeout be enforced here ?
    Path dataPath1 = new Path(SRC_DATA_DIR, "inPath1");
    Path dataPath2 = new Path(SRC_DATA_DIR, "inPath2");
    extServiceTestHelper
        .setupHashJoinData(SRC_DATA_DIR, dataPath1, dataPath2, HASH_JOIN_EXPECTED_RESULT_PATH, HASH_JOIN_OUTPUT_PATH);

    extServiceTestHelper.shutdownSharedTezClient();
  }

  @AfterClass
  public static void tearDown() throws IOException, TezException {
    extServiceTestHelper.tearDownAll();
  }

  @Test(timeout = 90000)
  public void testContainerLauncherThrowError() throws Exception {
    testFatalError("_testContainerLauncherError_", EXECUTION_CONTEXT_LAUNCHER_THROW,
        SUFFIX_LAUNCHER, Lists.newArrayList("Service Error",
            DAGAppMasterEventType.CONTAINER_LAUNCHER_SERVICE_FATAL_ERROR.name()));
  }

  @Test(timeout = 90000)
  public void testTaskCommunicatorThrowError() throws Exception {
    testFatalError("_testContainerLauncherError_", EXECUTION_CONTEXT_TASKCOMM_THROW,
        SUFFIX_TASKCOMM, Lists.newArrayList("Service Error",
            DAGAppMasterEventType.TASK_COMMUNICATOR_SERVICE_FATAL_ERROR.name()));
  }

  @Test(timeout = 90000)
  public void testTaskSchedulerThrowError() throws Exception {
    testFatalError("_testContainerLauncherError_", EXECUTION_CONTEXT_SCHEDULER_THROW,
        SUFFIX_SCHEDULER, Lists.newArrayList("Service Error",
            DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR.name()));
  }

  @Test (timeout = 150000)
  public void testNonFatalErrors() throws IOException, TezException, InterruptedException {
    String methodName = "testNonFatalErrors";
    TezConfiguration tezClientConf = new TezConfiguration(extServiceTestHelper.getConfForJobs());
    TezClient tezClient = TezClient
        .newBuilder(TestExternalTezServicesErrors.class.getSimpleName() + methodName + "_session",
            tezClientConf)
        .setIsSession(true).setServicePluginDescriptor(servicePluginsDescriptor).build();
    try {
      tezClient.start();
      LOG.info("TezSessionStarted for " + methodName);
      tezClient.waitTillReady();
      LOG.info("TezSession ready for submission for " + methodName);


      runAndVerifyForNonFatalErrors(tezClient, SUFFIX_LAUNCHER, EXECUTION_CONTEXT_LAUNCHER_REPORT_NON_FATAL);
      runAndVerifyForNonFatalErrors(tezClient, SUFFIX_TASKCOMM, EXECUTION_CONTEXT_TASKCOMM_REPORT_NON_FATAL);
      runAndVerifyForNonFatalErrors(tezClient, SUFFIX_SCHEDULER, EXECUTION_CONTEXT_SCHEDULER_REPORT_NON_FATAL);

    } finally {
      tezClient.stop();
    }
  }

  @Test(timeout = 90000)
  public void testContainerLauncherReportFatalError() throws Exception {
    testFatalError("_testContainerLauncherReportFatalError_",
        EXECUTION_CONTEXT_LAUNCHER_REPORT_FATAL, SUFFIX_LAUNCHER, Lists
            .newArrayList(ErrorPluginConfiguration.REPORT_FATAL_ERROR_MESSAGE,
                ServicePluginErrorDefaults.INCONSISTENT_STATE.name()));
  }

  @Test(timeout = 90000)
  public void testTaskCommReportFatalError() throws Exception {
    testFatalError("_testTaskCommReportFatalError_", EXECUTION_CONTEXT_TASKCOMM_REPORT_FATAL,
        SUFFIX_TASKCOMM, Lists.newArrayList(ErrorPluginConfiguration.REPORT_FATAL_ERROR_MESSAGE,
            ServicePluginErrorDefaults.INCONSISTENT_STATE.name()));
  }

  @Test(timeout = 90000)
  public void testTaskSchedulerReportFatalError() throws Exception {
    testFatalError("_testTaskSchedulerReportFatalError_",
        EXECUTION_CONTEXT_SCHEDULER_REPORT_FATAL, SUFFIX_SCHEDULER,
        Lists.newArrayList(ErrorPluginConfiguration.REPORT_FATAL_ERROR_MESSAGE,
            ServicePluginErrorDefaults.INCONSISTENT_STATE.name()));
  }


  private void testFatalError(String methodName,
                              Vertex.VertexExecutionContext lhsExecutionContext,
                              String dagNameSuffix, List<String> expectedDiagMessages) throws
      IOException, TezException, YarnException, InterruptedException {
    TezConfiguration tezClientConf = new TezConfiguration(extServiceTestHelper.getConfForJobs());
    TezClient tezClient = TezClient
        .newBuilder(TestExternalTezServicesErrors.class.getSimpleName() + methodName + "_session",
            tezClientConf)
        .setIsSession(true).setServicePluginDescriptor(servicePluginsDescriptor).build();

    ApplicationId appId= null;
    try {
      tezClient.start();
      LOG.info("TezSessionStarted for " + methodName);
      tezClient.waitTillReady();
      LOG.info("TezSession ready for submission for " + methodName);

      JoinValidateConfigured joinValidate =
          new JoinValidateConfigured(EXECUTION_CONTEXT_DEFAULT, lhsExecutionContext,
              EXECUTION_CONTEXT_EXT_SERVICE_PUSH,
              EXECUTION_CONTEXT_EXT_SERVICE_PUSH, dagNameSuffix);

      DAG dag = joinValidate
          .createDag(new TezConfiguration(extServiceTestHelper.getConfForJobs()),
              HASH_JOIN_EXPECTED_RESULT_PATH,
              HASH_JOIN_OUTPUT_PATH, 3);

      DAGClient dagClient = tezClient.submitDAG(dag);

      DAGStatus dagStatus =
          dagClient.waitForCompletionWithStatusUpdates(Sets.newHashSet(StatusGetOpts.GET_COUNTERS));
      assertEquals(DAGStatus.State.ERROR, dagStatus.getState());
      boolean foundDiag = false;
      for (String diag : dagStatus.getDiagnostics()) {
        foundDiag = checkDiag(diag, expectedDiagMessages);
        if (foundDiag) {
          break;
        }
      }
      appId = tezClient.getAppMasterApplicationId();
      assertTrue(foundDiag);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      tezClient.stop();
    }
    // Verify the state of the application.
    if (appId != null) {
      YarnClient yarnClient = YarnClient.createYarnClient();
      try {
        yarnClient.init(tezClientConf);
        yarnClient.start();

        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (!EnumSet.of(YarnApplicationState.FINISHED, YarnApplicationState.FAILED,
            YarnApplicationState.KILLED).contains(appState)) {
          Thread.sleep(200L);
          appReport = yarnClient.getApplicationReport(appId);
          appState = appReport.getYarnApplicationState();
        }

        // TODO Workaround for YARN-4554. AppReport does not provide diagnostics - need to fetch them from ApplicationAttemptReport
        ApplicationAttemptId appAttemptId = appReport.getCurrentApplicationAttemptId();
        ApplicationAttemptReport appAttemptReport =
            yarnClient.getApplicationAttemptReport(appAttemptId);
        String diag = appAttemptReport.getDiagnostics();
        assertEquals(FinalApplicationStatus.FAILED, appReport.getFinalApplicationStatus());
        assertEquals(YarnApplicationState.FINISHED, appReport.getYarnApplicationState());
        checkDiag(diag, expectedDiagMessages);
      } finally {
        yarnClient.stop();
      }
    }
  }

  private boolean checkDiag(String diag, List<String> expected) {
    boolean found = true;
    for (String exp : expected) {
      if (diag.contains(exp)) {
        found = true;
        continue;
      } else {
        found = false;
        break;
      }
    }
    return found;
  }


  private void runAndVerifyForNonFatalErrors(TezClient tezClient, String componentName,
                                             Vertex.VertexExecutionContext lhsContext) throws
      TezException,
      InterruptedException, IOException {
    LOG.info("Running JoinValidate with componentName reportNonFatalException");
    JoinValidateConfigured joinValidate =
        new JoinValidateConfigured(EXECUTION_CONTEXT_DEFAULT, lhsContext,
            EXECUTION_CONTEXT_EXT_SERVICE_PUSH,
            EXECUTION_CONTEXT_EXT_SERVICE_PUSH, componentName);

    DAG dag = joinValidate
        .createDag(new TezConfiguration(extServiceTestHelper.getConfForJobs()),
            HASH_JOIN_EXPECTED_RESULT_PATH,
            HASH_JOIN_OUTPUT_PATH, 3);

    DAGClient dagClient = tezClient.submitDAG(dag);

    DAGStatus dagStatus =
        dagClient.waitForCompletionWithStatusUpdates(Sets.newHashSet(StatusGetOpts.GET_COUNTERS));
    assertEquals(DAGStatus.State.FAILED, dagStatus.getState());

    boolean foundDiag = false;
    for (String diag : dagStatus.getDiagnostics()) {
      if (diag.contains(ErrorPluginConfiguration.REPORT_NONFATAL_ERROR_MESSAGE) &&
          diag.contains(ServicePluginErrorDefaults.SERVICE_UNAVAILABLE.name())) {
        foundDiag = true;
        break;
      }
    }
    assertTrue(foundDiag);
  }

}
