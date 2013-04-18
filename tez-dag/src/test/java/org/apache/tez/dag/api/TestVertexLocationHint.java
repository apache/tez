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

package org.apache.tez.dag.api;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestVertexLocationHint {

  private DataInput in;
  private DataOutput out;
  private ByteArrayOutputStream bOut;

  @Before
  public void setup() {
    bOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bOut);
  }

  @After
  public void teardown() {
    in = null;
    out = null;
    bOut = null;
  }

  @Test
  public void testNullTaskLocationHintSerDes() throws IOException {
    TaskLocationHint expected = new TaskLocationHint(null, null);
    expected.write(out);
    in = new DataInputStream(new ByteArrayInputStream(bOut.toByteArray()));
    TaskLocationHint actual = new TaskLocationHint();
    actual.readFields(in);
    Assert.assertNull(actual.getDataLocalHosts());
    Assert.assertNull(actual.getRacks());
  }

  @Test
  public void testTaskLocationHintSerDes() throws IOException {
    String[] hosts = { "h1", "h2", "", null };
    String[] racks = { "r1", "r2" };
    TaskLocationHint expected = new TaskLocationHint(hosts, racks);
    expected.write(out);
    in = new DataInputStream(new ByteArrayInputStream(bOut.toByteArray()));
    TaskLocationHint actual = new TaskLocationHint();
    actual.readFields(in);
    Assert.assertNotNull(actual.getDataLocalHosts());
    Assert.assertNotNull(actual.getRacks());
    Assert.assertArrayEquals(hosts, actual.getDataLocalHosts());
    Assert.assertArrayEquals(racks, actual.getRacks());
  }

  @Test
  public void testTaskLocationHintSerDes2() throws IOException {
    String[] hosts = null;
    String[] racks = { "r1", "r2" };
    TaskLocationHint expected = new TaskLocationHint(hosts, racks);
    expected.write(out);
    in = new DataInputStream(new ByteArrayInputStream(bOut.toByteArray()));
    TaskLocationHint actual = new TaskLocationHint();
    actual.readFields(in);
    Assert.assertNull(actual.getDataLocalHosts());
    Assert.assertNotNull(actual.getRacks());
    Assert.assertArrayEquals(racks, actual.getRacks());
  }

  @Test
  public void testEmptyVertexLocationHintSerDes() throws IOException {
    VertexLocationHint expected = new VertexLocationHint(0);
    expected.write(out);
    in = new DataInputStream(new ByteArrayInputStream(bOut.toByteArray()));
    VertexLocationHint actual = new VertexLocationHint();
    actual.readFields(in);
    Assert.assertEquals(0, actual.getNumTasks());
    Assert.assertNotNull(actual.getTaskLocationHints());
    Assert.assertEquals(0, actual.getTaskLocationHints().length);
  }

  @Test
  public void testVertexLocationHintSerDes() throws IOException {
    String[] hosts = { "h1", "h2", "", null };
    String[] racks = { "r1", "r2" };
    VertexLocationHint expected = new VertexLocationHint(4);
    expected.getTaskLocationHints()[0] = new TaskLocationHint(hosts, racks);
    expected.getTaskLocationHints()[1] = null;
    expected.getTaskLocationHints()[2] = new TaskLocationHint(null, racks);
    expected.getTaskLocationHints()[3] = new TaskLocationHint(hosts, null);
    expected.write(out);
    in = new DataInputStream(new ByteArrayInputStream(bOut.toByteArray()));
    VertexLocationHint actual = new VertexLocationHint();
    actual.readFields(in);

    Assert.assertEquals(4, actual.getNumTasks());
    Assert.assertNotNull(actual.getTaskLocationHints());
    Assert.assertEquals(4, actual.getTaskLocationHints().length);

    Assert.assertNotNull(actual.getTaskLocationHints()[0]);
    Assert.assertNull(actual.getTaskLocationHints()[1]);
    Assert.assertNotNull(actual.getTaskLocationHints()[2]);
    Assert.assertNotNull(actual.getTaskLocationHints()[3]);

    Assert.assertArrayEquals(
        expected.getTaskLocationHints()[0].getDataLocalHosts(),
        actual.getTaskLocationHints()[0].getDataLocalHosts());
    Assert.assertArrayEquals(
        expected.getTaskLocationHints()[0].getRacks(),
        actual.getTaskLocationHints()[0].getRacks());
    Assert.assertNull(
        actual.getTaskLocationHints()[2].getDataLocalHosts());
    Assert.assertArrayEquals(
        expected.getTaskLocationHints()[2].getRacks(),
        actual.getTaskLocationHints()[2].getRacks());
    Assert.assertArrayEquals(
        expected.getTaskLocationHints()[3].getDataLocalHosts(),
        actual.getTaskLocationHints()[3].getDataLocalHosts());
    Assert.assertNull(
        actual.getTaskLocationHints()[3].getRacks());
  }

}
