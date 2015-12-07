/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.analyzer;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.tez.dag.api.TezException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Simple placeholder for storing CSV results.
 * Contains headers and records in string format.
 */
public class CSVResult implements Result {

  private final String[] headers;
  private final List<String[]> recordsList;
  private String comments;

  public CSVResult(String[] header) {
    this.headers = header;
    recordsList = Lists.newLinkedList();
  }

  public String[] getHeaders() {
    return headers;
  }

  public void addRecord(String[] record) {
    Preconditions.checkArgument(record != null, "Record can't be null");
    Preconditions.checkArgument(record.length == headers.length, "Record length" + record.length +
        " does not match headers length " + headers.length);
    recordsList.add(record);
  }

  public Iterator<String[]> getRecordsIterator() {
    return Iterators.unmodifiableIterator(recordsList.iterator());
  }


  public void setComments(String comments) {
    this.comments = comments;
  }

  @Override public String toJson() throws TezException {
    return "";
  }

  @Override public String getComments() {
    return comments;
  }

  @Override public String toString() {
    return "CSVResult{" +
        "headers=" + Arrays.toString(headers) +
        ", recordsList=" + recordsList +
        '}';
  }

  //For testing
  public void dumpToFile(String fileName) throws IOException {
    OutputStreamWriter writer = new OutputStreamWriter(
        new FileOutputStream(new File(fileName)),
        Charset.forName("UTF-8").newEncoder());
    BufferedWriter bw = new BufferedWriter(writer);
    bw.write(Joiner.on(",").join(headers));
    bw.newLine();
    for (String[] record : recordsList) {

      if (record.length != headers.length) {
        continue; //LOG error msg?
      }

      StringBuilder sb = new StringBuilder();
      for(int i=0;i<record.length;i++) {
        sb.append(!Strings.isNullOrEmpty(record[i]) ? record[i] : " ");
        if (i < record.length - 1) {
          sb.append(",");
        }
      }
      bw.write(sb.toString());
      bw.newLine();
    }
    bw.flush();
    bw.close();
  }
}
