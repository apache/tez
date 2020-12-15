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

package org.apache.tez.dag.helpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.tez.serviceplugins.api.DagInfo;

import java.util.BitSet;

public class DagInfoImplForTest implements DagInfo {

  private final int index;
  private final String name;

  public DagInfoImplForTest(int index, String name) {
    this.index = index;
    this.name = name;
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Credentials getCredentials() {
    return null;
  }

  @Override
  public int getTotalVertices() {
    return 0;
  }

  @Override
  public BitSet getVertexDescendants(int vertexIndex) {
    return null;
  }

  @Override
  public Configuration getConf() {
    return null;
  }
}
