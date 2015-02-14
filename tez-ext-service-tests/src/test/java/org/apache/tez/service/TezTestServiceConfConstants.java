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

package org.apache.tez.service;

public class TezTestServiceConfConstants {

  private static final String TEZ_TEST_SERVICE_PREFIX = "tez.test.service.";

  /** Number of executors per instance - used by the scheduler */
  public static final String TEZ_TEST_SERVICE_NUM_EXECUTORS_PER_INSTANCE = TEZ_TEST_SERVICE_PREFIX + "num.executors.per-instance";

  /** Memory available per instance - used by the scheduler */
  public static final String TEZ_TEST_SERVICE_MEMORY_PER_INSTANCE_MB = TEZ_TEST_SERVICE_PREFIX + "memory.per.instance.mb";

  /** CPUs available per instance - used by the scheduler */
  public static final String TEZ_TEST_SERVICE_VCPUS_PER_INSTANCE = TEZ_TEST_SERVICE_PREFIX + "vcpus.per.instance";


  /** Hosts on which the service is running. Currently assuming a single port for all instances */
  public static final String TEZ_TEST_SERVICE_HOSTS = TEZ_TEST_SERVICE_PREFIX + "hosts";

  /** Port on which the Service(s) listen. Current a single port for all instances */
  public static final String TEZ_TEST_SERVICE_RPC_PORT = TEZ_TEST_SERVICE_PREFIX + "rpc.port";

  /** Number of threads to use in the AM to communicate with the external service */
  public static final String TEZ_TEST_SERVICE_AM_COMMUNICATOR_NUM_THREADS = TEZ_TEST_SERVICE_PREFIX + "communicator.num.threads";
  public static final int TEZ_TEST_SERVICE_AM_COMMUNICATOR_NUM_THREADS_DEFAULT = 2;

}
