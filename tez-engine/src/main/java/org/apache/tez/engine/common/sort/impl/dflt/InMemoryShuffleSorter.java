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

package org.apache.tez.engine.common.sort.impl.dflt;

import java.io.IOException;
import java.io.InputStream;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.tez.common.TezTaskContext;
import org.apache.tez.engine.api.Master;
import org.apache.tez.engine.common.shuffle.impl.ShuffleHeader;
import org.apache.tez.engine.common.shuffle.server.ShuffleHandler;
import org.apache.tez.engine.common.sort.impl.IFile;
import org.apache.tez.engine.records.OutputContext;

public class InMemoryShuffleSorter extends DefaultSorter {

  private static final Log LOG = LogFactory.getLog(InMemoryShuffleSorter.class);
  
  static final int IFILE_EOF_LENGTH = 
      2 * WritableUtils.getVIntSize(IFile.EOF_MARKER);
  static final int IFILE_CHECKSUM_LENGTH = DataChecksum.Type.CRC32.size;
  
  private List<Integer> spillIndices = new ArrayList<Integer>();
  private List<ShuffleHeader> shuffleHeaders = new ArrayList<ShuffleHeader>();

  ShuffleHandler shuffleHandler = new ShuffleHandler(this);
  
  byte[] kvbuffer;
  IntBuffer kvmeta;
  
  public InMemoryShuffleSorter(TezTaskContext task) throws IOException {
    super(task);
  }

  @Override
  public void initialize(Configuration conf, Master master) throws IOException,
      InterruptedException {
    super.initialize(conf, master);
    shuffleHandler.init(conf, runningTaskContext);
  }

  @Override
  protected void spill(int mstart, int mend) 
      throws IOException, InterruptedException {
    // Start the shuffleHandler
    shuffleHandler.start();

    // Don't spill!
    
    // Make a copy
    this.kvbuffer = super.kvbuffer;
    this.kvmeta = super.kvmeta;

    // Just save spill-indices for serving later
    int spindex = mstart;
    for (int i = 0; i < partitions; ++i) {
      spillIndices.add(spindex);
      
      int length = 0;
      while (spindex < mend &&
          kvmeta.get(offsetFor(spindex) + PARTITION) == i) {

        final int kvoff = offsetFor(spindex);
        int keyLen = 
            kvmeta.get(kvoff + VALSTART) - kvmeta.get(kvoff + KEYSTART);
        int valLen = getInMemVBytesLength(kvoff);
        length += 
            (keyLen + WritableUtils.getVIntSize(keyLen)) + 
            (valLen + WritableUtils.getVIntSize(valLen));

        ++spindex;
      }
      length += IFILE_EOF_LENGTH;
      
      shuffleHeaders.add( 
          new ShuffleHeader(
              task.getTaskAttemptId().toString(), 
              length + IFILE_CHECKSUM_LENGTH, length, i)
          );
      LOG.info("shuffleHeader[" + i + "]:" +
      		" rawLen=" + length + " partLen=" + (length + IFILE_CHECKSUM_LENGTH) + 
          " spillIndex=" + spillIndices.get(i));
    }
    
    LOG.info("Saved " + spillIndices.size() + " spill-indices and " + 
        shuffleHeaders.size() + " shuffle headers");
  }

  @Override
  public InputStream getSortedStream(int partition) {
    return new SortBufferInputStream(this, partition);
  }

  @Override
  public void close() throws IOException, InterruptedException{
    // FIXME
    //shuffleHandler.stop();
  }

  @Override
  public ShuffleHeader getShuffleHeader(int reduce) {
    return shuffleHeaders.get(reduce);
  }

  public int getSpillIndex(int partition) {
    return spillIndices.get(partition);
  }

  @Override
  public OutputContext getOutputContext() {
    return new OutputContext(shuffleHandler.getPort());
  }
  
}
