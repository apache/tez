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

public class TestDAGLocationHint {

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
  public void testNullDAGLocationHintSerDes() throws IOException {
    DAGLocationHint expected = new DAGLocationHint();
    expected.write(out);
    in = new DataInputStream(new ByteArrayInputStream(bOut.toByteArray()));
    DAGLocationHint actual = new DAGLocationHint();
    actual.readFields(in);
    Assert.assertNotNull(actual.getVertexLocationHints());
    Assert.assertEquals(0, actual.getVertexLocationHints().size());
  }

  @Test
  public void testDAGLocationHintSerDes() throws IOException {
    String[] hosts = { "h1", "h2", "", null };
    String[] racks = { "r1", "r2" };

    VertexLocationHint vertexLocationHint = new VertexLocationHint(4);
    vertexLocationHint.getTaskLocationHints()[0] =
        new TaskLocationHint(hosts, racks);
    DAGLocationHint expected = new DAGLocationHint();
    expected.getVertexLocationHints().put("v1", null);
    expected.getVertexLocationHints().put("v2", new VertexLocationHint());
    expected.getVertexLocationHints().put("v3", vertexLocationHint);
    expected.write(out);

    in = new DataInputStream(new ByteArrayInputStream(bOut.toByteArray()));
    DAGLocationHint actual = new DAGLocationHint();
    actual.readFields(in);
    Assert.assertNotNull(actual.getVertexLocationHints());
    Assert.assertEquals(3, actual.getVertexLocationHints().size());

    Assert.assertNull(actual.getVertexLocationHint("v1"));
    Assert.assertNotNull(actual.getVertexLocationHint("v2"));
    Assert.assertNotNull(actual.getVertexLocationHint("v3"));

    Assert.assertEquals(0, actual.getVertexLocationHint("v2").getNumTasks());
    Assert.assertEquals(0,
        actual.getVertexLocationHint("v2").getTaskLocationHints().length);

    Assert.assertEquals(4, actual.getVertexLocationHint("v3").getNumTasks());
    Assert.assertEquals(4,
        actual.getVertexLocationHint("v3").getTaskLocationHints().length);
    Assert.assertNotNull(
        actual.getVertexLocationHint("v3").getTaskLocationHints()[0]);
    Assert.assertArrayEquals(racks,
        actual.getVertexLocationHint("v3").getTaskLocationHints()[0].
            getRacks());
    Assert.assertArrayEquals(hosts,
        actual.getVertexLocationHint("v3").getTaskLocationHints()[0].
            getDataLocalHosts());
    Assert.assertNull(
        actual.getVertexLocationHint("v3").getTaskLocationHints()[1]);
    Assert.assertNull(
        actual.getVertexLocationHint("v3").getTaskLocationHints()[2]);
    Assert.assertNull(
        actual.getVertexLocationHint("v3").getTaskLocationHints()[3]);
  }


}

