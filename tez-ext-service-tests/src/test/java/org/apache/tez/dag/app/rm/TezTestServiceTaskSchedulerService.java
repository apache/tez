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

package org.apache.tez.dag.app.rm;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.service.TezTestServiceConfConstants;
import org.apache.tez.serviceplugins.api.TaskScheduler;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TezTestServiceTaskSchedulerService extends TaskScheduler {

  private static final Logger
      LOG = LoggerFactory.getLogger(TezTestServiceTaskSchedulerService.class);

  private final List<String> serviceHosts;
  private final ContainerFactory containerFactory;
  private final Random random = new Random();
  // Currently all services must be running on the same port.
  private final int containerPort;

  private final ConcurrentMap<Object, ContainerId> runningTasks =
      new ConcurrentHashMap<Object, ContainerId>();

  // AppIdIdentifier to avoid conflicts with other containers in the system.

  // Per instance
  private final int memoryPerInstance;
  private final int coresPerInstance;
  private final int executorsPerInstance;

  // Per Executor Thread
  private final Resource resourcePerContainer;


  // Not registering with the RM. Assuming the main TezScheduler will always run (except local mode),
  // and take care of YARN registration.
  public TezTestServiceTaskSchedulerService(TaskSchedulerContext taskSchedulerContext) {
    // Accepting configuration here to allow setting up fields as final
    super(taskSchedulerContext);
    this.serviceHosts = new LinkedList<String>();
    this.containerFactory = new ContainerFactory(taskSchedulerContext.getApplicationAttemptId(),
        taskSchedulerContext.getCustomClusterIdentifier());

    Configuration conf = null;
    try {
      conf = TezUtils.createConfFromUserPayload(taskSchedulerContext.getInitialUserPayload());
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    this.memoryPerInstance = conf
        .getInt(TezTestServiceConfConstants.TEZ_TEST_SERVICE_MEMORY_PER_INSTANCE_MB, -1);
    Preconditions.checkArgument(memoryPerInstance > 0,
        TezTestServiceConfConstants.TEZ_TEST_SERVICE_MEMORY_PER_INSTANCE_MB +
            " must be configured");

    this.executorsPerInstance = conf.getInt(
        TezTestServiceConfConstants.TEZ_TEST_SERVICE_NUM_EXECUTORS_PER_INSTANCE,
        -1);
    Preconditions.checkArgument(executorsPerInstance > 0,
        TezTestServiceConfConstants.TEZ_TEST_SERVICE_NUM_EXECUTORS_PER_INSTANCE +
            " must be configured");

    this.coresPerInstance = conf
        .getInt(TezTestServiceConfConstants.TEZ_TEST_SERVICE_VCPUS_PER_INSTANCE,
            executorsPerInstance);

    this.containerPort = conf.getInt(TezTestServiceConfConstants.TEZ_TEST_SERVICE_RPC_PORT, -1);
    Preconditions.checkArgument(executorsPerInstance > 0,
        TezTestServiceConfConstants.TEZ_TEST_SERVICE_RPC_PORT + " must be configured");

    int memoryPerContainer = (int) (memoryPerInstance / (float) executorsPerInstance);
    int coresPerContainer = (int) (coresPerInstance / (float) executorsPerInstance);
    this.resourcePerContainer = Resource.newInstance(memoryPerContainer, coresPerContainer);

    String[] hosts = conf.getTrimmedStrings(TezTestServiceConfConstants.TEZ_TEST_SERVICE_HOSTS);
    if (hosts == null || hosts.length == 0) {
      hosts = new String[]{"localhost"};
    }
    for (String host : hosts) {
      serviceHosts.add(host);
    }

    LOG.info("Running with configuration: " +
        "memoryPerInstance=" + memoryPerInstance +
        ", vcoresPerInstance=" + coresPerInstance +
        ", executorsPerInstance=" + executorsPerInstance +
        ", resourcePerContainerInferred=" + resourcePerContainer +
        ", hosts=" + serviceHosts.toString());

  }

  @Override
  public Resource getAvailableResources() {
    // TODO This needs information about all running executors, and the amount of memory etc available across the cluster.
    return Resource
        .newInstance(Ints.checkedCast(serviceHosts.size() * memoryPerInstance),
            serviceHosts.size() * coresPerInstance);
  }

  @Override
  public int getClusterNodeCount() {
    return serviceHosts.size();
  }

  @Override
  public void dagComplete() {
  }

  @Override
  public Resource getTotalResources() {
    return Resource
        .newInstance(Ints.checkedCast(serviceHosts.size() * memoryPerInstance),
            serviceHosts.size() * coresPerInstance);
  }

  @Override
  public void blacklistNode(NodeId nodeId) {
    LOG.info("BlacklistNode not supported");
  }

  @Override
  public void unblacklistNode(NodeId nodeId) {
    LOG.info("unBlacklistNode not supported");
  }

  @Override
  public void allocateTask(Object task, Resource capability, String[] hosts, String[] racks,
                           Priority priority, Object containerSignature, Object clientCookie) {
    String host = selectHost(hosts);
    Container container =
        containerFactory.createContainer(resourcePerContainer, priority, host, containerPort);
    runningTasks.put(task, container.getId());
    getContext().taskAllocated(task, clientCookie, container);
  }


  @Override
  public void allocateTask(Object task, Resource capability, ContainerId containerId,
                           Priority priority, Object containerSignature, Object clientCookie) {
    String host = selectHost(null);
    Container container =
        containerFactory.createContainer(resourcePerContainer, priority, host, containerPort);
    runningTasks.put(task, container.getId());
    getContext().taskAllocated(task, clientCookie, container);
  }

  @Override
  public boolean deallocateTask(Object task, boolean taskSucceeded, TaskAttemptEndReason endReason, String diagnostics) {
    ContainerId containerId = runningTasks.remove(task);
    if (containerId == null) {
      LOG.error("Could not determine ContainerId for task: " + task +
          " . Could have hit a race condition. Ignoring." +
          " The query may hang since this \"unknown\" container is now taking up a slot permanently");
      return false;
    }
    getContext().containerBeingReleased(containerId);
    return true;
  }

  @Override
  public Object deallocateContainer(ContainerId containerId) {
    LOG.info("Ignoring deallocateContainer for containerId: " + containerId);
    return null;
  }

  @Override
  public void setShouldUnregister() {

  }

  @Override
  public boolean hasUnregistered() {
    // Nothing to do. No registration involved.
    return true;
  }

  private String selectHost(String[] requestedHosts) {
    String host;
    if (requestedHosts != null && requestedHosts.length > 0) {
      Arrays.sort(requestedHosts);
      host = requestedHosts[0];
      LOG.info("Selected host: " + host + " from requested hosts: " + Arrays.toString(requestedHosts));
    } else {
      host = serviceHosts.get(random.nextInt(serviceHosts.size()));
      LOG.info("Selected random host: " + host + " since the request contained no host information");
    }
    return host;
  }

  static class ContainerFactory {
    AtomicInteger nextId;
    final ApplicationAttemptId customAppAttemptId;

    public ContainerFactory(ApplicationAttemptId appAttemptId, long appIdLong) {
      this.nextId = new AtomicInteger(1);
      ApplicationId appId = ApplicationId
          .newInstance(appIdLong, appAttemptId.getApplicationId().getId());
      this.customAppAttemptId = ApplicationAttemptId
          .newInstance(appId, appAttemptId.getAttemptId());
    }

    @SuppressWarnings("deprecation")
    public Container createContainer(Resource capability, Priority priority, String hostname, int port) {
      ContainerId containerId = ContainerId.newInstance(customAppAttemptId, nextId.getAndIncrement());
      NodeId nodeId = NodeId.newInstance(hostname, port);
      String nodeHttpAddress = "hostname:0";

      Container container = Container.newInstance(containerId,
          nodeId,
          nodeHttpAddress,
          capability,
          priority,
          null);

      return container;
    }
  }
}
