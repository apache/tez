/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.client.registry;

import static org.junit.Assert.*;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.registry.zookeeper.ZkConfig;

import org.junit.Test;

public class TestAMRecord {

  @Test
  public void testConstructorWithAllParameters() {
    ApplicationId appId = ApplicationId.newInstance(12345L, 1);
    String hostName = "test-host.example.com";
    String hostIp = "192.168.1.100";
    int port = 8080;
    String externalId = "external-123";
    String computeName = "test-compute";

    AMRecord record = new AMRecord(appId, hostName, hostIp, port, externalId, computeName);

    assertEquals(appId, record.getApplicationId());
    assertEquals(hostName, record.getHostName());
    assertEquals(hostName, record.getHostName());
    assertEquals(hostIp, record.getHostIp());
    assertEquals(port, record.getPort());
    assertEquals(externalId, record.getExternalId());
    assertEquals(computeName, record.getComputeName());
  }

  @Test
  public void testConstructorWithNullExternalIdAndComputeName() {
    ApplicationId appId = ApplicationId.newInstance(12345L, 1);
    String hostName = "test-host.example.com";
    String hostIp = "192.168.1.100";
    int port = 8080;

    AMRecord record = new AMRecord(appId, hostName, hostIp, port, null, null);

    assertEquals("", record.getExternalId());
    assertEquals(ZkConfig.DEFAULT_COMPUTE_GROUP_NAME, record.getComputeName());
  }

  @Test
  public void testCopyConstructor() {
    ApplicationId appId = ApplicationId.newInstance(12345L, 1);
    String hostName = "test-host.example.com";
    String hostIp = "192.168.1.100";
    int port = 8080;
    String externalId = "external-123";
    String computeName = "test-compute";

    AMRecord original = new AMRecord(appId, hostName, hostIp, port, externalId, computeName);
    AMRecord copy = new AMRecord(original);

    assertEquals(original.getApplicationId(), copy.getApplicationId());
    assertEquals(original.getHostName(), copy.getHostName());
    assertEquals(original.getHostIp(), copy.getHostIp());
    assertEquals(original.getPort(), copy.getPort());
    assertEquals(original.getExternalId(), copy.getExternalId());
    assertEquals(original.getComputeName(), copy.getComputeName());
    assertEquals(original, copy);
    assertEquals(original.hashCode(), copy.hashCode());
  }

  @Test
  public void testConstructorFromServiceRecord() {
    ApplicationId appId = ApplicationId.newInstance(12345L, 1);
    String hostName = "test-host.example.com";
    String hostIp = "192.168.1.100";
    int port = 8080;
    String externalId = "external-123";
    String computeName = "test-compute";

    AMRecord original = new AMRecord(appId, hostName, hostIp, port, externalId, computeName);
    ServiceRecord serviceRecord = original.toServiceRecord();
    AMRecord reconstructed = new AMRecord(serviceRecord);

    assertEquals(original.getApplicationId(), reconstructed.getApplicationId());
    assertEquals(original.getHostName(), reconstructed.getHostName());
    assertEquals(original.getHostIp(), reconstructed.getHostIp());
    assertEquals(original.getPort(), reconstructed.getPort());
    assertEquals(original.getExternalId(), reconstructed.getExternalId());
    assertEquals(original.getComputeName(), reconstructed.getComputeName());
    assertEquals(original, reconstructed);
  }

  @Test
  public void testConstructorFromServiceRecordWithNullDefaults() {
    ApplicationId appId = ApplicationId.newInstance(12345L, 1);
    String hostName = "test-host.example.com";
    String hostIp = "192.168.1.100";
    int port = 8080;

    // Create record with null externalId and computeName
    AMRecord original = new AMRecord(appId, hostName, hostIp, port, null, null);

    // Convert to ServiceRecord and back
    ServiceRecord serviceRecord = original.toServiceRecord();
    AMRecord reconstructed = new AMRecord(serviceRecord);

    // Verify defaults are preserved
    assertEquals("", reconstructed.getExternalId());
    assertEquals(ZkConfig.DEFAULT_COMPUTE_GROUP_NAME, reconstructed.getComputeName());
    assertEquals(original, reconstructed);
  }

  @Test
  public void testToServiceRecord() {
    ApplicationId appId = ApplicationId.newInstance(12345L, 1);
    String hostName = "test-host.example.com";
    String hostIp = "192.168.1.100";
    int port = 8080;
    String externalId = "external-123";
    String computeName = "test-compute";

    AMRecord record = new AMRecord(appId, hostName, hostIp, port, externalId, computeName);
    ServiceRecord serviceRecord = record.toServiceRecord();

    assertNotNull(serviceRecord);
    assertEquals(appId.toString(), serviceRecord.get("appId"));
    assertEquals(hostName, serviceRecord.get("hostName"));
    assertEquals(hostIp, serviceRecord.get("hostIp"));
    assertEquals(String.valueOf(port), serviceRecord.get("port"));
    assertEquals(externalId, serviceRecord.get("externalId"));
    assertEquals(computeName, serviceRecord.get("computeName"));
  }

  @Test
  public void testToServiceRecordCaching() {
    ApplicationId appId = ApplicationId.newInstance(12345L, 1);
    String hostName = "test-host.example.com";
    String hostIp = "192.168.1.100";
    int port = 8080;

    AMRecord record = new AMRecord(appId, hostName, hostIp, port, "external-123", "test-compute");
    ServiceRecord serviceRecord1 = record.toServiceRecord();
    ServiceRecord serviceRecord2 = record.toServiceRecord();

    // Should return the same cached instance
    assertSame(serviceRecord1, serviceRecord2);
  }

  @Test
  public void testEquals() {
    ApplicationId appId1 = ApplicationId.newInstance(12345L, 1);
    ApplicationId appId2 = ApplicationId.newInstance(12345L, 1);
    ApplicationId appId3 = ApplicationId.newInstance(12345L, 2);

    AMRecord record1 = new AMRecord(appId1, "host1", "192.168.1.1", 8080, "ext1", "compute1");
    AMRecord record2 = new AMRecord(appId2, "host1", "192.168.1.1", 8080, "ext1", "compute1");
    AMRecord record3 = new AMRecord(appId3, "host1", "192.168.1.1", 8080, "ext1", "compute1");
    AMRecord record4 = new AMRecord(appId1, "host2", "192.168.1.1", 8080, "ext1", "compute1");
    AMRecord record5 = new AMRecord(appId1, "host1", "192.168.1.2", 8080, "ext1", "compute1");
    AMRecord record6 = new AMRecord(appId1, "host1", "192.168.1.1", 8081, "ext1", "compute1");
    AMRecord record7 = new AMRecord(appId1, "host1", "192.168.1.1", 8080, "ext2", "compute1");
    AMRecord record8 = new AMRecord(appId1, "host1", "192.168.1.1", 8080, "ext1", "compute2");

    // Same values should be equal
    assertEquals(record1, record2);
    assertEquals(record2, record1);
    // Different appId
    assertNotEquals(record1, record3);
    // Different hostName
    assertNotEquals(record1, record4);
    // Different hostIp
    assertNotEquals(record1, record5);
    // Different port
    assertNotEquals(record1, record6);
    // Different externalId
    assertNotEquals(record1, record7);
    // Different computeName
    assertNotEquals(record1, record8);
    // Self equality
    assertEquals(record1, record1);
    // Null equality
    assertNotEquals(null, record1);
    // Different type
    assertNotEquals("not an AMRecord", record1);
  }

  @Test
  public void testHashCode() {
    ApplicationId appId1 = ApplicationId.newInstance(12345L, 1);
    ApplicationId appId2 = ApplicationId.newInstance(12345L, 1);

    AMRecord record1 = new AMRecord(appId1, "host1", "192.168.1.1", 8080, "ext1", "compute1");
    AMRecord record2 = new AMRecord(appId2, "host1", "192.168.1.1", 8080, "ext1", "compute1");
    AMRecord record3 = new AMRecord(appId1, "host2", "192.168.1.1", 8080, "ext1", "compute1");

    // Equal objects should have same hashCode
    assertEquals(record1.hashCode(), record2.hashCode());
  }

  @Test
  public void testToString() {
    ApplicationId appId = ApplicationId.newInstance(12345L, 1);
    String hostName = "test-host.example.com";
    String hostIp = "192.168.1.100";
    int port = 8080;
    String externalId = "external-123";
    String computeName = "test-compute";

    AMRecord record = new AMRecord(appId, hostName, hostIp, port, externalId, computeName);
    String str = record.toString();

    assertNotNull(str);
    // Validate actual JSON-like snippets from the string
    assertTrue("Should contain appId=value snippet", str.contains("appId=" + appId.toString()));
    assertTrue("Should contain hostName=value snippet", str.contains("hostName=" + hostName));
    assertTrue("Should contain hostIp=value snippet", str.contains("hostIp=" + hostIp));
    assertTrue("Should contain port=value snippet", str.contains("port=" + port));
    assertTrue("Should contain externalId=value snippet", str.contains("externalId=" + externalId));
    assertTrue("Should contain computeName=value snippet", str.contains("computeName=" + computeName));
  }

  @Test
  public void testRemoveFromCacheByDeserializedRecordAppId() throws Exception {
    ConcurrentHashMap<ApplicationId, AMRecord> amRecordCache = new ConcurrentHashMap<>();

    ApplicationId appId = ApplicationId.newInstance(12345L, 1);
    String hostName = "test-host.example.com";
    String hostIp = "192.168.1.100";
    int port = 8080;
    String externalId = "external-123";
    String computeName = "test-compute";

    AMRecord record = new AMRecord(appId, hostName, hostIp, port, externalId, computeName);
    amRecordCache.put(appId, record);

    assertEquals(1, amRecordCache.size());

    AMRecord deserialized = AMRegistryUtils.jsonStringToRecord(AMRegistryUtils.recordToJsonString(record));
    amRecordCache.remove(deserialized.getApplicationId());

    assertEquals(0, amRecordCache.size());
  }
}
