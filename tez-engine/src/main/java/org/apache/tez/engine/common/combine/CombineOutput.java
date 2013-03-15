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
package org.apache.tez.engine.common.combine;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.api.Master;
import org.apache.tez.api.Output;
import org.apache.tez.engine.common.sort.impl.IFile.Writer;
import org.apache.tez.records.OutputContext;

public class CombineOutput implements Output {

  private final Writer writer;
  
  public CombineOutput(Writer writer) {
    this.writer = writer;
  }

  public void initialize(Configuration conf, Master master) throws IOException,
      InterruptedException {
    // TODO Auto-generated method stub

  }

  public void write(Object key, Object value) throws IOException,
      InterruptedException {
    writer.append(key, value);
  }

  @Override
  public OutputContext getOutputContext() {
    return null;
  }
  
  public void close() throws IOException, InterruptedException {
    writer.close();
  }
}
