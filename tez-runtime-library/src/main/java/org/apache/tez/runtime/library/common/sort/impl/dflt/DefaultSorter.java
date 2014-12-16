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

package org.apache.tez.runtime.library.common.sort.impl.dflt;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.Progress;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.sort.impl.ExternalSorter;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger.Segment;

import com.google.common.base.Preconditions;

@SuppressWarnings({"unchecked", "rawtypes"})
public class DefaultSorter extends ExternalSorter implements IndexedSortable {
  
  private static final Log LOG = LogFactory.getLog(DefaultSorter.class);

  // TODO NEWTEZ Progress reporting to Tez framework. (making progress vs %complete)
  
  /**
   * The size of each record in the index file for the map-outputs.
   */
  public static final int MAP_OUTPUT_INDEX_RECORD_LENGTH = 24;

  private final static int APPROX_HEADER_LENGTH = 150;

  // k/v accounting
  private final IntBuffer kvmeta; // metadata overlay on backing store
  int kvstart;            // marks origin of spill metadata
  int kvend;              // marks end of spill metadata
  int kvindex;            // marks end of fully serialized records

  int equator;            // marks origin of meta/serialization
  int bufstart;           // marks beginning of spill
  int bufend;             // marks beginning of collectable
  int bufmark;            // marks end of record
  int bufindex;           // marks end of collected
  int bufvoid;            // marks the point where we should stop
                          // reading at the end of the buffer

  private final byte[] kvbuffer;        // main output buffer
  private final byte[] b0 = new byte[0];

  protected static final int VALSTART = 0;         // val offset in acct
  protected static final int KEYSTART = 1;         // key offset in acct
  protected static final int PARTITION = 2;        // partition offset in acct
  protected static final int VALLEN = 3;           // length of value
  protected static final int NMETA = 4;            // num meta ints
  protected static final int METASIZE = NMETA * 4; // size in bytes

  // spill accounting
  final int maxRec;
  final int softLimit;
  boolean spillInProgress;
  int bufferRemaining;
  volatile Throwable sortSpillException = null;

  int numSpills = 0;
  final int minSpillsForCombine;
  final ReentrantLock spillLock = new ReentrantLock();
  final Condition spillDone = spillLock.newCondition();
  final Condition spillReady = spillLock.newCondition();
  final BlockingBuffer bb = new BlockingBuffer();
  volatile boolean spillThreadRunning = false;
  final SpillThread spillThread = new SpillThread();

  final ArrayList<TezSpillRecord> indexCacheList =
    new ArrayList<TezSpillRecord>();
  private final int indexCacheMemoryLimit;
  private int totalIndexCacheMemory;

  private long totalKeys = 0;
  private long sameKey = 0;

  public DefaultSorter(OutputContext outputContext, Configuration conf, int numOutputs,
      long initialMemoryAvailable) throws IOException {
    super(outputContext, conf, numOutputs, initialMemoryAvailable);
    // sanity checks
    final float spillper = this.conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT_DEFAULT);
    final int sortmb = this.availableMemoryMb;
    Preconditions.checkArgument(spillper <= (float) 1.0 && spillper > (float) 0.0,
        TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT
            + " should be greater than 0 and less than or equal to 1");

    indexCacheMemoryLimit = this.conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES,
                                       TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES_DEFAULT);

    // buffers and accounting
    int maxMemUsage = sortmb << 20;
    maxMemUsage -= maxMemUsage % METASIZE;
    kvbuffer = new byte[maxMemUsage];
    bufvoid = kvbuffer.length;
    kvmeta = ByteBuffer.wrap(kvbuffer)
       .order(ByteOrder.nativeOrder())
       .asIntBuffer();
    setEquator(0);
    bufstart = bufend = bufindex = equator;
    kvstart = kvend = kvindex;

    maxRec = kvmeta.capacity() / NMETA;
    softLimit = (int)(kvbuffer.length * spillper);
    bufferRemaining = softLimit;
    if (LOG.isInfoEnabled()) {
      LOG.info(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB + ": " + sortmb);
      LOG.info("soft limit at " + softLimit);
      LOG.info("bufstart = " + bufstart + "; bufvoid = " + bufvoid);
      LOG.info("kvstart = " + kvstart + "; length = " + maxRec);
    }

    // k/v serialization
    valSerializer.open(bb);
    keySerializer.open(bb);

    spillInProgress = false;
    minSpillsForCombine = this.conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINE_MIN_SPILLS, 3);
    spillThread.setDaemon(true);
    spillThread.setName("SpillThread ["
        + TezUtilsInternal.cleanVertexName(outputContext.getDestinationVertexName() + "]"));
    spillLock.lock();
    try {
      spillThread.start();
      while (!spillThreadRunning) {
        spillDone.await();
      }
    } catch (InterruptedException e) {
      throw new IOException("Spill thread failed to initialize", e);
    } finally {
      spillLock.unlock();
    }
    if (sortSpillException != null) {
      throw new IOException("Spill thread failed to initialize",
          sortSpillException);
    }
  }

  @Override
  public void write(Object key, Object value)
      throws IOException {
    collect(
        key, value, partitioner.getPartition(key, value, partitions));
  }

  /**
   * Serialize the key, value to intermediate storage.
   * When this method returns, kvindex must refer to sufficient unused
   * storage to store one METADATA.
   */
  synchronized void collect(Object key, Object value, final int partition
                                   ) throws IOException {

    if (key.getClass() != keyClass) {
      throw new IOException("Type mismatch in key from map: expected "
                            + keyClass.getName() + ", received "
                            + key.getClass().getName());
    }
    if (value.getClass() != valClass) {
      throw new IOException("Type mismatch in value from map: expected "
                            + valClass.getName() + ", received "
                            + value.getClass().getName());
    }
    if (partition < 0 || partition >= partitions) {
      throw new IOException("Illegal partition for " + key + " (" +
          partition + ")" + ", TotalPartitions: " + partitions);
    }
    checkSpillException();
    bufferRemaining -= METASIZE;
    if (bufferRemaining <= 0) {
      // start spill if the thread is not running and the soft limit has been
      // reached
      spillLock.lock();
      try {
        do {
          if (!spillInProgress) {
            final int kvbidx = 4 * kvindex;
            final int kvbend = 4 * kvend;
            // serialized, unspilled bytes always lie between kvindex and
            // bufindex, crossing the equator. Note that any void space
            // created by a reset must be included in "used" bytes
            final int bUsed = distanceTo(kvbidx, bufindex);
            final boolean bufsoftlimit = bUsed >= softLimit;
            if ((kvbend + METASIZE) % kvbuffer.length !=
                equator - (equator % METASIZE)) {
              // spill finished, reclaim space
              resetSpill();
              bufferRemaining = Math.min(
                  distanceTo(bufindex, kvbidx) - 2 * METASIZE,
                  softLimit - bUsed) - METASIZE;
              continue;
            } else if (bufsoftlimit && kvindex != kvend) {
              // spill records, if any collected; check latter, as it may
              // be possible for metadata alignment to hit spill pcnt
              startSpill();
              final int avgRec = (int)
                (mapOutputByteCounter.getValue() /
                mapOutputRecordCounter.getValue());
              // leave at least half the split buffer for serialization data
              // ensure that kvindex >= bufindex
              final int distkvi = distanceTo(bufindex, kvbidx);
              final int newPos = (bufindex +
                Math.max(2 * METASIZE - 1,
                        Math.min(distkvi / 2,
                                 distkvi / (METASIZE + avgRec) * METASIZE)))
                % kvbuffer.length;
              setEquator(newPos);
              bufmark = bufindex = newPos;
              final int serBound = 4 * kvend;
              // bytes remaining before the lock must be held and limits
              // checked is the minimum of three arcs: the metadata space, the
              // serialization space, and the soft limit
              bufferRemaining = Math.min(
                  // metadata max
                  distanceTo(bufend, newPos),
                  Math.min(
                    // serialization max
                    distanceTo(newPos, serBound),
                    // soft limit
                    softLimit)) - 2 * METASIZE;
            }
          }
        } while (false);
      } finally {
        spillLock.unlock();
      }
    }

    try {
      // serialize key bytes into buffer
      int keystart = bufindex;
      keySerializer.serialize(key);
      if (bufindex < keystart) {
        // wrapped the key; must make contiguous
        bb.shiftBufferedKey();
        keystart = 0;
      }
      // serialize value bytes into buffer
      final int valstart = bufindex;
      valSerializer.serialize(value);
      // It's possible for records to have zero length, i.e. the serializer
      // will perform no writes. To ensure that the boundary conditions are
      // checked and that the kvindex invariant is maintained, perform a
      // zero-length write into the buffer. The logic monitoring this could be
      // moved into collect, but this is cleaner and inexpensive. For now, it
      // is acceptable.
      bb.write(b0, 0, 0);

      // the record must be marked after the preceding write, as the metadata
      // for this record are not yet written
      int valend = bb.markRecord();

      mapOutputRecordCounter.increment(1);
      mapOutputByteCounter.increment(
          distanceTo(keystart, valend, bufvoid));

      // write accounting info
      kvmeta.put(kvindex + PARTITION, partition);
      kvmeta.put(kvindex + KEYSTART, keystart);
      kvmeta.put(kvindex + VALSTART, valstart);
      kvmeta.put(kvindex + VALLEN, distanceTo(valstart, valend));
      // advance kvindex
      kvindex = (int)(((long)kvindex - NMETA + kvmeta.capacity()) % kvmeta.capacity());
      totalKeys++;
    } catch (MapBufferTooSmallException e) {
      LOG.info("Record too large for in-memory buffer: " + e.getMessage());
      spillSingleRecord(key, value, partition);
      mapOutputRecordCounter.increment(1);
      return;
    }
  }

  /**
   * Set the point from which meta and serialization data expand. The meta
   * indices are aligned with the buffer, so metadata never spans the ends of
   * the circular buffer.
   */
  private void setEquator(int pos) {
    equator = pos;
    // set index prior to first entry, aligned at meta boundary
    final int aligned = pos - (pos % METASIZE);
    // Cast one of the operands to long to avoid integer overflow
    kvindex = (int) (((long) aligned - METASIZE + kvbuffer.length) % kvbuffer.length) / 4;
    if (LOG.isInfoEnabled()) {
      LOG.info("(EQUATOR) " + pos + " kvi " + kvindex +
          "(" + (kvindex * 4) + ")");
    }
  }

  /**
   * The spill is complete, so set the buffer and meta indices to be equal to
   * the new equator to free space for continuing collection. Note that when
   * kvindex == kvend == kvstart, the buffer is empty.
   */
  private void resetSpill() {
    final int e = equator;
    bufstart = bufend = e;
    final int aligned = e - (e % METASIZE);
    // set start/end to point to first meta record
    // Cast one of the operands to long to avoid integer overflow
    kvstart = kvend = (int) (((long) aligned - METASIZE + kvbuffer.length) % kvbuffer.length) / 4;
    if (LOG.isInfoEnabled()) {
      LOG.info("(RESET) equator " + e + " kv " + kvstart + "(" +
        (kvstart * 4) + ")" + " kvi " + kvindex + "(" + (kvindex * 4) + ")");
    }
  }

  /**
   * Compute the distance in bytes between two indices in the serialization
   * buffer.
   * @see #distanceTo(int,int,int)
   */
  final int distanceTo(final int i, final int j) {
    return distanceTo(i, j, kvbuffer.length);
  }

  /**
   * Compute the distance between two indices in the circular buffer given the
   * max distance.
   */
  int distanceTo(final int i, final int j, final int mod) {
    return i <= j
      ? j - i
      : mod - i + j;
  }

  /**
   * For the given meta position, return the offset into the int-sized
   * kvmeta buffer.
   */
  int offsetFor(int metapos) {
    return (metapos % maxRec) * NMETA;
    
  }

  /**
   * Compare logical range, st i, j MOD offset capacity.
   * Compare by partition, then by key.
   * @see IndexedSortable#compare
   */
  public int compare(final int mi, final int mj) {
    final int kvi = offsetFor(mi);
    final int kvj = offsetFor(mj);
    final int kvip = kvmeta.get(kvi + PARTITION);
    final int kvjp = kvmeta.get(kvj + PARTITION);
    // sort by partition
    if (kvip != kvjp) {
      return kvip - kvjp;
    }
    // sort by key
    int result = comparator.compare(kvbuffer,
        kvmeta.get(kvi + KEYSTART),
        kvmeta.get(kvi + VALSTART) - kvmeta.get(kvi + KEYSTART),
        kvbuffer,
        kvmeta.get(kvj + KEYSTART),
        kvmeta.get(kvj + VALSTART) - kvmeta.get(kvj + KEYSTART));
    if (result == 0) {
      sameKey++;
    }
    return result;
  }

  final byte META_BUFFER_TMP[] = new byte[METASIZE];
  /**
   * Swap metadata for items i,j
   * @see IndexedSortable#swap
   */
  public void swap(final int mi, final int mj) {
    int iOff = (mi % maxRec) * METASIZE;
    int jOff = (mj % maxRec) * METASIZE;
    System.arraycopy(kvbuffer, iOff, META_BUFFER_TMP, 0, METASIZE);
    System.arraycopy(kvbuffer, jOff, kvbuffer, iOff, METASIZE);
    System.arraycopy(META_BUFFER_TMP, 0, kvbuffer, jOff, METASIZE);
  }

  /**
   * Inner class managing the spill of serialized records to disk.
   */
  protected class BlockingBuffer extends DataOutputStream {

    public BlockingBuffer() {
      super(new Buffer());
    }

    /**
     * Mark end of record. Note that this is required if the buffer is to
     * cut the spill in the proper place.
     */
    public int markRecord() {
      bufmark = bufindex;
      return bufindex;
    }

    /**
     * Set position from last mark to end of writable buffer, then rewrite
     * the data between last mark and kvindex.
     * This handles a special case where the key wraps around the buffer.
     * If the key is to be passed to a RawComparator, then it must be
     * contiguous in the buffer. This recopies the data in the buffer back
     * into itself, but starting at the beginning of the buffer. Note that
     * this method should <b>only</b> be called immediately after detecting
     * this condition. To call it at any other time is undefined and would
     * likely result in data loss or corruption.
     * @see #markRecord()
     */
    protected void shiftBufferedKey() throws IOException {
      // spillLock unnecessary; both kvend and kvindex are current
      int headbytelen = bufvoid - bufmark;
      bufvoid = bufmark;
      final int kvbidx = 4 * kvindex;
      final int kvbend = 4 * kvend;
      final int avail =
        Math.min(distanceTo(0, kvbidx), distanceTo(0, kvbend));
      if (bufindex + headbytelen < avail) {
        System.arraycopy(kvbuffer, 0, kvbuffer, headbytelen, bufindex);
        System.arraycopy(kvbuffer, bufvoid, kvbuffer, 0, headbytelen);
        bufindex += headbytelen;
        bufferRemaining -= kvbuffer.length - bufvoid;
      } else {
        byte[] keytmp = new byte[bufindex];
        System.arraycopy(kvbuffer, 0, keytmp, 0, bufindex);
        bufindex = 0;
        out.write(kvbuffer, bufmark, headbytelen);
        out.write(keytmp);
      }
    }
  }

  public class Buffer extends OutputStream {
    private final byte[] scratch = new byte[1];

    @Override
    public void write(int v)
        throws IOException {
      scratch[0] = (byte)v;
      write(scratch, 0, 1);
    }

    /**
     * Attempt to write a sequence of bytes to the collection buffer.
     * This method will block if the spill thread is running and it
     * cannot write.
     * @throws MapBufferTooSmallException if record is too large to
     *    deserialize into the collection buffer.
     */
    @Override
    public void write(byte b[], int off, int len)
        throws IOException {
      // must always verify the invariant that at least METASIZE bytes are
      // available beyond kvindex, even when len == 0
      bufferRemaining -= len;
      if (bufferRemaining <= 0) {
        // writing these bytes could exhaust available buffer space or fill
        // the buffer to soft limit. check if spill or blocking are necessary
        boolean blockwrite = false;
        spillLock.lock();
        try {
          do {
            checkSpillException();

            final int kvbidx = 4 * kvindex;
            final int kvbend = 4 * kvend;
            // ser distance to key index
            final int distkvi = distanceTo(bufindex, kvbidx);
            // ser distance to spill end index
            final int distkve = distanceTo(bufindex, kvbend);

            // if kvindex is closer than kvend, then a spill is neither in
            // progress nor complete and reset since the lock was held. The
            // write should block only if there is insufficient space to
            // complete the current write, write the metadata for this record,
            // and write the metadata for the next record. If kvend is closer,
            // then the write should block if there is too little space for
            // either the metadata or the current write. Note that collect
            // ensures its metadata requirement with a zero-length write
            blockwrite = distkvi <= distkve
              ? distkvi <= len + 2 * METASIZE
              : distkve <= len || distanceTo(bufend, kvbidx) < 2 * METASIZE;

            if (!spillInProgress) {
              if (blockwrite) {
                if ((kvbend + METASIZE) % kvbuffer.length !=
                    equator - (equator % METASIZE)) {
                  // spill finished, reclaim space
                  // need to use meta exclusively; zero-len rec & 100% spill
                  // pcnt would fail
                  resetSpill(); // resetSpill doesn't move bufindex, kvindex
                  bufferRemaining = Math.min(
                      distkvi - 2 * METASIZE,
                      softLimit - distanceTo(kvbidx, bufindex)) - len;
                  continue;
                }
                // we have records we can spill; only spill if blocked
                if (kvindex != kvend) {
                  startSpill();
                  // Blocked on this write, waiting for the spill just
                  // initiated to finish. Instead of repositioning the marker
                  // and copying the partial record, we set the record start
                  // to be the new equator
                  setEquator(bufmark);
                } else {
                  // We have no buffered records, and this record is too large
                  // to write into kvbuffer. We must spill it directly from
                  // collect
                  final int size = distanceTo(bufstart, bufindex) + len;
                  setEquator(0);
                  bufstart = bufend = bufindex = equator;
                  kvstart = kvend = kvindex;
                  bufvoid = kvbuffer.length;
                  throw new MapBufferTooSmallException(size + " bytes");
                }
              }
            }

            if (blockwrite) {
              // wait for spill
              try {
                while (spillInProgress) {
                  spillDone.await();
                }
              } catch (InterruptedException e) {
                  throw new IOException(
                      "Buffer interrupted while waiting for the writer", e);
              }
            }
          } while (blockwrite);
        } finally {
          spillLock.unlock();
        }
      }
      // here, we know that we have sufficient space to write
      if (bufindex + len > bufvoid) {
        final int gaplen = bufvoid - bufindex;
        System.arraycopy(b, off, kvbuffer, bufindex, gaplen);
        len -= gaplen;
        off += gaplen;
        bufindex = 0;
      }
      System.arraycopy(b, off, kvbuffer, bufindex, len);
      bufindex += len;
    }
  }

  @Override
  public void flush() throws IOException {
    LOG.info("Starting flush of map output");
    spillLock.lock();
    try {
      while (spillInProgress) {
        spillDone.await();
      }
      checkSpillException();

      final int kvbend = 4 * kvend;
      if ((kvbend + METASIZE) % kvbuffer.length !=
          equator - (equator % METASIZE)) {
        // spill finished
        resetSpill();
      }
      if (kvindex != kvend) {
        kvend = (kvindex + NMETA) % kvmeta.capacity();
        bufend = bufmark;
        if (LOG.isInfoEnabled()) {
          LOG.info("Sorting & Spilling map output");
          LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark +
                   "; bufvoid = " + bufvoid);
          LOG.info("kvstart = " + kvstart + "(" + (kvstart * 4) +
                   "); kvend = " + kvend + "(" + (kvend * 4) +
                   "); length = " + (distanceTo(kvend, kvstart,
                         kvmeta.capacity()) + 1) + "/" + maxRec);
        }
        sortAndSpill();
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for the writer", e);
    } finally {
      spillLock.unlock();
    }
    assert !spillLock.isHeldByCurrentThread();
    // shut down spill thread and wait for it to exit. Since the preceding
    // ensures that it is finished with its work (and sortAndSpill did not
    // throw), we elect to use an interrupt instead of setting a flag.
    // Spilling simultaneously from this thread while the spill thread
    // finishes its work might be both a useful way to extend this and also
    // sufficient motivation for the latter approach.
    try {
      spillThread.interrupt();
      spillThread.join();
    } catch (InterruptedException e) {
      throw new IOException("Spill failed", e);
    }
    // release sort buffer before the merge
    //FIXME
    //kvbuffer = null;
    mergeParts();
    Path outputPath = mapOutputFile.getOutputFile();
    fileOutputByteCounter.increment(rfs.getFileStatus(outputPath).getLen());
  }

  @Override
  public void close() throws IOException { }

  protected class SpillThread extends Thread {

    @Override
    public void run() {
      spillLock.lock();
      spillThreadRunning = true;
      try {
        while (true) {
          spillDone.signal();
          while (!spillInProgress) {
            spillReady.await();
          }
          try {
            spillLock.unlock();
            sortAndSpill();
          } catch (Throwable t) {
            LOG.warn("Got an exception in sortAndSpill", t);
            sortSpillException = t;
          } finally {
            spillLock.lock();
            if (bufend < bufstart) {
              bufvoid = kvbuffer.length;
            }
            kvstart = kvend;
            bufstart = bufend;
            spillInProgress = false;
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        spillLock.unlock();
        spillThreadRunning = false;
      }
    }
  }

  private void checkSpillException() throws IOException {
    final Throwable lspillException = sortSpillException;
    if (lspillException != null) {
      if (lspillException instanceof Error) {
        final String logMsg = "Task " + outputContext.getUniqueIdentifier()
            + " failed : " + ExceptionUtils.getStackTrace(lspillException);
        outputContext.fatalError(lspillException, logMsg);
      }
      throw new IOException("Spill failed", lspillException);
    }
  }

  private void startSpill() {
    assert !spillInProgress;
    kvend = (kvindex + NMETA) % kvmeta.capacity();
    bufend = bufmark;
    spillInProgress = true;
    if (LOG.isInfoEnabled()) {
      LOG.info("Spilling map output");
      LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark +
               "; bufvoid = " + bufvoid);
      LOG.info("kvstart = " + kvstart + "(" + (kvstart * 4) +
               "); kvend = " + kvend + "(" + (kvend * 4) +
               "); length = " + (distanceTo(kvend, kvstart,
                     kvmeta.capacity()) + 1) + "/" + maxRec);
    }
    spillReady.signal();
  }

  int getMetaStart() {
    return kvend / NMETA;
  }

  int getMetaEnd() {
    return 1 + // kvend is a valid record
        (kvstart >= kvend
        ? kvstart
        : kvmeta.capacity() + kvstart) / NMETA;
  }

  private boolean isRLENeeded() {
    return (sameKey > (0.1 * totalKeys)) || (sameKey < 0);
  }

  protected void sortAndSpill()
      throws IOException, InterruptedException {
    final int mstart = getMetaStart();
    final int mend = getMetaEnd();
    sorter.sort(this, mstart, mend, nullProgressable);
    spill(mstart, mend);
  }

  protected void spill(int mstart, int mend)
      throws IOException, InterruptedException {

    //approximate the length of the output file to be the length of the
    //buffer + header lengths for the partitions
    final long size = (bufend >= bufstart
        ? bufend - bufstart
        : (bufvoid - bufend) + bufstart) +
                partitions * APPROX_HEADER_LENGTH;
    FSDataOutputStream out = null;
    try {
      // create spill file
      final TezSpillRecord spillRec = new TezSpillRecord(partitions);
      final Path filename =
          mapOutputFile.getSpillFileForWrite(numSpills, size);
      out = rfs.create(filename);

      int spindex = mstart;
      final InMemValBytes value = createInMemValBytes();
      boolean rle = isRLENeeded();
      for (int i = 0; i < partitions; ++i) {
        IFile.Writer writer = null;
        try {
          long segmentStart = out.getPos();
          writer = new Writer(conf, out, keyClass, valClass, codec,
                                    spilledRecordsCounter, null, rle);
          if (combiner == null) {
            // spill directly
            DataInputBuffer key = new DataInputBuffer();
            while (spindex < mend &&
                kvmeta.get(offsetFor(spindex) + PARTITION) == i) {
              final int kvoff = offsetFor(spindex);
              int keystart = kvmeta.get(kvoff + KEYSTART);
              int valstart = kvmeta.get(kvoff + VALSTART);
              key.reset(kvbuffer, keystart, valstart - keystart);
              getVBytesForOffset(kvoff, value);
              writer.append(key, value);
              ++spindex;
            }
          } else {
            int spstart = spindex;
            while (spindex < mend &&
                kvmeta.get(offsetFor(spindex)
                          + PARTITION) == i) {
              ++spindex;
            }
            // Note: we would like to avoid the combiner if we've fewer
            // than some threshold of records for a partition
            if (spstart != spindex) {
              TezRawKeyValueIterator kvIter =
                new MRResultIterator(spstart, spindex);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Running combine processor");
              }
              runCombineProcessor(kvIter, writer);
            }
          }

          // close the writer
          writer.close();
          if (numSpills > 0) {
            additionalSpillBytesWritten.increment(writer.getCompressedLength());
            numAdditionalSpills.increment(1);
            // Reset the value will be set during the final merge.
            outputBytesWithOverheadCounter.setValue(0);
          } else {
            // Set this up for the first write only. Subsequent ones will be handled in the final merge.
            outputBytesWithOverheadCounter.increment(writer.getRawLength());
          }
          // record offsets
          final TezIndexRecord rec =
              new TezIndexRecord(
                  segmentStart,
                  writer.getRawLength(),
                  writer.getCompressedLength());
          spillRec.putIndex(rec, i);

          writer = null;
        } finally {
          if (null != writer) writer.close();
        }
      }

      if (totalIndexCacheMemory >= indexCacheMemoryLimit) {
        // create spill index file
        Path indexFilename =
            mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
                * MAP_OUTPUT_INDEX_RECORD_LENGTH);
        spillRec.writeToFile(indexFilename, conf);
      } else {
        indexCacheList.add(spillRec);
        totalIndexCacheMemory +=
          spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
      }
      LOG.info("Finished spill " + numSpills);
      ++numSpills;
    } finally {
      if (out != null) out.close();
    }
  }

  /**
   * Handles the degenerate case where serialization fails to fit in
   * the in-memory buffer, so we must spill the record from collect
   * directly to a spill file. Consider this "losing".
   */
  private void spillSingleRecord(final Object key, final Object value,
                                 int partition) throws IOException {
    long size = kvbuffer.length + partitions * APPROX_HEADER_LENGTH;
    FSDataOutputStream out = null;
    try {
      // create spill file
      final TezSpillRecord spillRec = new TezSpillRecord(partitions);
      final Path filename =
          mapOutputFile.getSpillFileForWrite(numSpills, size);
      out = rfs.create(filename);

      // we don't run the combiner for a single record
      for (int i = 0; i < partitions; ++i) {
        IFile.Writer writer = null;
        try {
          long segmentStart = out.getPos();
          // Create a new codec, don't care!
          writer = new IFile.Writer(conf, out, keyClass, valClass, codec,
                                          spilledRecordsCounter, null);

          if (i == partition) {
            final long recordStart = out.getPos();
            writer.append(key, value);
            // Note that our map byte count will not be accurate with
            // compression
            mapOutputByteCounter.increment(out.getPos() - recordStart);
          }
          writer.close();

          if (numSpills > 0) {
            additionalSpillBytesWritten.increment(writer.getCompressedLength());
            numAdditionalSpills.increment(1);
            outputBytesWithOverheadCounter.setValue(0);
          } else {
            // Set this up for the first write only. Subsequent ones will be handled in the final merge.
            outputBytesWithOverheadCounter.increment(writer.getRawLength());
          }

          // record offsets
          TezIndexRecord rec =
              new TezIndexRecord(
                  segmentStart,
                  writer.getRawLength(),
                  writer.getCompressedLength());
          spillRec.putIndex(rec, i);

          writer = null;
        } catch (IOException e) {
          if (null != writer) writer.close();
          throw e;
        }
      }
      if (totalIndexCacheMemory >= indexCacheMemoryLimit) {
        // create spill index file
        Path indexFilename =
            mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
                * MAP_OUTPUT_INDEX_RECORD_LENGTH);
        spillRec.writeToFile(indexFilename, conf);
      } else {
        indexCacheList.add(spillRec);
        totalIndexCacheMemory +=
          spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
      }
      ++numSpills;
    } finally {
      if (out != null) out.close();
    }
  }

  protected int getInMemVBytesLength(int kvoff) {
    // get the keystart for the next serialized value to be the end
    // of this value. If this is the last value in the buffer, use bufend
    final int vallen = kvmeta.get(kvoff + VALLEN);
    assert vallen >= 0;
    return vallen;
  }

  /**
   * Given an offset, populate vbytes with the associated set of
   * deserialized value bytes. Should only be called during a spill.
   */
  int getVBytesForOffset(int kvoff, InMemValBytes vbytes) {
    int vallen = getInMemVBytesLength(kvoff);
    vbytes.reset(kvbuffer, kvmeta.get(kvoff + VALSTART), vallen);
    return vallen;
  }

  /**
   * Inner class wrapping valuebytes, used for appendRaw.
   */
  static class InMemValBytes extends DataInputBuffer {
    private byte[] buffer;
    private int start;
    private int length;
    private final int bufvoid;

    public InMemValBytes(int bufvoid) {
      this.bufvoid = bufvoid;
    }

    public void reset(byte[] buffer, int start, int length) {
      this.buffer = buffer;
      this.start = start;
      this.length = length;

      if (start + length > bufvoid) {
        this.buffer = new byte[this.length];
        final int taillen = bufvoid - start;
        System.arraycopy(buffer, start, this.buffer, 0, taillen);
        System.arraycopy(buffer, 0, this.buffer, taillen, length-taillen);
        this.start = 0;
      }

      super.reset(this.buffer, this.start, this.length);
    }
  }

  InMemValBytes createInMemValBytes() {
    return new InMemValBytes(bufvoid);
  }

  protected class MRResultIterator implements TezRawKeyValueIterator {
    private final DataInputBuffer keybuf = new DataInputBuffer();
    private final InMemValBytes vbytes = createInMemValBytes();
    private final int end;
    private int current;
    public MRResultIterator(int start, int end) {
      this.end = end;
      current = start - 1;
    }
    public boolean next() throws IOException {
      return ++current < end;
    }
    public DataInputBuffer getKey() throws IOException {
      final int kvoff = offsetFor(current);
      keybuf.reset(kvbuffer, kvmeta.get(kvoff + KEYSTART),
          kvmeta.get(kvoff + VALSTART) - kvmeta.get(kvoff + KEYSTART));
      return keybuf;
    }
    public DataInputBuffer getValue() throws IOException {
      getVBytesForOffset(offsetFor(current), vbytes);
      return vbytes;
    }
    public Progress getProgress() {
      return null;
    }
    public void close() { }
  }

  private void mergeParts() throws IOException {
    // get the approximate size of the final output/index files
    long finalOutFileSize = 0;
    long finalIndexFileSize = 0;
    final Path[] filename = new Path[numSpills];
    final String taskIdentifier = outputContext.getUniqueIdentifier();

    for(int i = 0; i < numSpills; i++) {
      filename[i] = mapOutputFile.getSpillFile(i);
      finalOutFileSize += rfs.getFileStatus(filename[i]).getLen();
    }
    if (numSpills == 1) { //the spill is the final output
      sameVolRename(filename[0],
          mapOutputFile.getOutputFileForWriteInVolume(filename[0]));
      if (indexCacheList.size() == 0) {
        sameVolRename(mapOutputFile.getSpillIndexFile(0),
          mapOutputFile.getOutputIndexFileForWriteInVolume(filename[0]));
      } else {
        indexCacheList.get(0).writeToFile(
          mapOutputFile.getOutputIndexFileForWriteInVolume(filename[0]), conf);
      }
      return;
    }

    // read in paged indices
    for (int i = indexCacheList.size(); i < numSpills; ++i) {
      Path indexFileName = mapOutputFile.getSpillIndexFile(i);
      indexCacheList.add(new TezSpillRecord(indexFileName, conf));
    }

    //make correction in the length to include the sequence file header
    //lengths for each partition
    finalOutFileSize += partitions * APPROX_HEADER_LENGTH;
    finalIndexFileSize = partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH;
    Path finalOutputFile =
        mapOutputFile.getOutputFileForWrite(finalOutFileSize);
    Path finalIndexFile =
        mapOutputFile.getOutputIndexFileForWrite(finalIndexFileSize);

    //The output stream for the final single output file
    FSDataOutputStream finalOut = rfs.create(finalOutputFile, true, 4096);

    if (numSpills == 0) {
      // TODO Change event generation to say there is no data rather than generating a dummy file
      //create dummy files

      TezSpillRecord sr = new TezSpillRecord(partitions);
      try {
        for (int i = 0; i < partitions; i++) {
          long segmentStart = finalOut.getPos();
          Writer writer =
            new Writer(conf, finalOut, keyClass, valClass, codec, null, null);
          writer.close();

          TezIndexRecord rec =
              new TezIndexRecord(
                  segmentStart,
                  writer.getRawLength(),
                  writer.getCompressedLength());
          // Covers the case of multiple spills.
          outputBytesWithOverheadCounter.increment(writer.getRawLength());
          sr.putIndex(rec, i);
        }
        sr.writeToFile(finalIndexFile, conf);
      } finally {
        finalOut.close();
      }
      return;
    }
    else {
      final TezSpillRecord spillRec = new TezSpillRecord(partitions);
      for (int parts = 0; parts < partitions; parts++) {
        //create the segments to be merged
        List<Segment> segmentList =
          new ArrayList<Segment>(numSpills);
        for(int i = 0; i < numSpills; i++) {
          TezIndexRecord indexRecord = indexCacheList.get(i).getIndex(parts);

          Segment s =
            new Segment(conf, rfs, filename[i], indexRecord.getStartOffset(),
                             indexRecord.getPartLength(), codec, ifileReadAhead,
                             ifileReadAheadLength, ifileBufferSize, true);
          segmentList.add(i, s);

          if (LOG.isDebugEnabled()) {
            LOG.debug("TaskIdentifier=" + taskIdentifier + " Partition=" + parts +
                "Spill =" + i + "(" + indexRecord.getStartOffset() + "," +
                indexRecord.getRawLength() + ", " +
                indexRecord.getPartLength() + ")");
          }
        }

        int mergeFactor =
            this.conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR,
                TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR_DEFAULT);
        // sort the segments only if there are intermediate merges
        boolean sortSegments = segmentList.size() > mergeFactor;
        //merge
        TezRawKeyValueIterator kvIter = TezMerger.merge(conf, rfs,
                       keyClass, valClass, codec,
                       segmentList, mergeFactor,
                       new Path(taskIdentifier),
                       (RawComparator)ConfigUtils.getIntermediateOutputKeyComparator(conf),
                       nullProgressable, sortSegments, true,
                       null, spilledRecordsCounter, additionalSpillBytesRead,
                       null); // Not using any Progress in TezMerger. Should just work.

        //write merged output to disk
        long segmentStart = finalOut.getPos();
        Writer writer =
            new Writer(conf, finalOut, keyClass, valClass, codec,
                spilledRecordsCounter, null);
        if (combiner == null || numSpills < minSpillsForCombine) {
          TezMerger.writeFile(kvIter, writer,
              nullProgressable, TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
        } else {
          runCombineProcessor(kvIter, writer);
        }
        writer.close();

        // record offsets
        final TezIndexRecord rec =
            new TezIndexRecord(
                segmentStart,
                writer.getRawLength(),
                writer.getCompressedLength());
        spillRec.putIndex(rec, parts);
      }
      spillRec.writeToFile(finalIndexFile, conf);
      finalOut.close();
      for(int i = 0; i < numSpills; i++) {
        rfs.delete(filename[i],true);
      }
    }
  }
}
