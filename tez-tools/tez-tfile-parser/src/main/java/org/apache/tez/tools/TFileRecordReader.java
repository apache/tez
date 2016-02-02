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

package org.apache.tez.tools;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Simple record reader which reads the TFile and emits it as key, value pair.
 * If value has multiple lines, read one line at a time.
 */
public class TFileRecordReader extends RecordReader<Text, Text> {

  private static final Logger LOG = LoggerFactory.getLogger(TFileRecordReader.class);

  private long start, end;

  @VisibleForTesting
  protected Path splitPath;
  private FSDataInputStream fin;

  @VisibleForTesting
  protected TFile.Reader reader;
  @VisibleForTesting
  protected TFile.Reader.Scanner scanner;

  private Text key = new Text();
  private Text value = new Text();

  private BytesWritable keyBytesWritable = new BytesWritable();

  private BufferedReader currentValueReader;

  @Override public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) split;
    LOG.info("Initializing TFileRecordReader : " + fileSplit.getPath().toString());
    start = fileSplit.getStart();
    end = start + fileSplit.getLength();

    FileSystem fs = fileSplit.getPath().getFileSystem(context.getConfiguration());
    splitPath = fileSplit.getPath();
    fin = fs.open(splitPath);
    reader = new TFile.Reader(fin, fs.getFileStatus(splitPath).getLen(),
        context.getConfiguration());
    scanner = reader.createScannerByByteRange(start, fileSplit.getLength());
  }

  private void populateKV(TFile.Reader.Scanner.Entry entry) throws IOException {
    entry.getKey(keyBytesWritable);
    //splitpath contains the machine name. Create the key as splitPath + realKey
    String keyStr = new StringBuilder()
        .append(splitPath.getName()).append(":")
        .append(new String(keyBytesWritable.getBytes()))
        .toString();

    /**
     * In certain cases, values can be huge (files > 2 GB). Stream is
     * better to handle such scenarios.
     */
    currentValueReader = new BufferedReader(
        new InputStreamReader(entry.getValueStream()));
    key.set(keyStr);
    String line = currentValueReader.readLine();
    value.set((line == null) ? "" : line);
  }

  @Override public boolean nextKeyValue() throws IOException, InterruptedException {
    if (currentValueReader != null) {
      //Still at the old entry reading line by line
      String line = currentValueReader.readLine();
      if (line != null) {
        value.set(line);
        return true;
      } else {
        //Read through all lines in the large value stream. Move to next KV.
        scanner.advance();
      }
    }

    try {
      populateKV(scanner.entry());
      return true;
    } catch(EOFException eofException) {
      key = null;
      value = null;
      return false;
    }
  }

  @Override public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override public float getProgress() throws IOException, InterruptedException {
    return ((fin.getPos() - start) * 1.0f) / ((end - start) * 1.0f);
  }

  @Override public void close() throws IOException {
    IOUtils.closeQuietly(scanner);
    IOUtils.closeQuietly(reader);
    IOUtils.closeQuietly(fin);
  }
}
