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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.Deflater;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.library.api.IOInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.Progress;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.io.NonSyncDataOutputStream;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.ExternalSorter;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger.DiskSegment;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger.Segment;

import com.google.common.base.Preconditions;

@SuppressWarnings({"unchecked", "rawtypes"})
public final class DefaultSorter extends ExternalSorter implements IndexedSortable {
  
  private static final Logger LOG = LoggerFactory.getLogger(DefaultSorter.class);

  // TODO NEWTEZ Progress reporting to Tez framework. (making progress vs %complete)
  
  /**
   * The size of each record in the index file for the map-outputs.
   */
  public static final int MAP_OUTPUT_INDEX_RECORD_LENGTH = 24;

  private final static int APPROX_HEADER_LENGTH = 150;

  // k/v accounting
  private IntBuffer kvmeta; // metadata overlay on backing store
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

  private byte[] kvbuffer;        // main output buffer
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

  final int minSpillsForCombine;
  final ReentrantLock spillLock = new ReentrantLock();
  final Condition spillDone = spillLock.newCondition();
  final Condition spillReady = spillLock.newCondition();
  final BlockingBuffer bb = new BlockingBuffer();
  volatile boolean spillThreadRunning = false;
  final SpillThread spillThread = new SpillThread();
  private final Deflater deflater;
  private final String auxiliaryService;

  final ArrayList<TezSpillRecord> indexCacheList =
    new ArrayList<TezSpillRecord>();
  private final int indexCacheMemoryLimit;
  private int totalIndexCacheMemory;

  private long totalKeys = 0;
  private long sameKey = 0;

  public static final int MAX_IO_SORT_MB = 1800;


  public DefaultSorter(OutputContext outputContext, Configuration conf, int numOutputs,
      long initialMemoryAvailable) throws IOException {
    super(outputContext, conf, numOutputs, initialMemoryAvailable);
    deflater = TezCommonUtils.newBestCompressionDeflater();
    // sanity checks
    final float spillper = this.conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT_DEFAULT);
    final int sortmb = computeSortBufferSize((int) availableMemoryMb, outputContext.getDestinationVertexName());

    Preconditions.checkArgument(spillper <= (float) 1.0 && spillper > (float) 0.0,
        TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT
            + " should be greater than 0 and less than or equal to 1");

    indexCacheMemoryLimit = this.conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES,
                                       TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES_DEFAULT);

    boolean confPipelinedShuffle = this.conf.getBoolean(TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED, TezRuntimeConfiguration
        .TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED_DEFAULT);

    if (confPipelinedShuffle) {
      LOG.warn(outputContext.getDestinationVertexName() + ": " +
          TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED + " does not work "
          + "with DefaultSorter. It is supported only with PipelinedSorter.");
    }
    auxiliaryService = conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
        TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);

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
      LOG.info(outputContext.getDestinationVertexName() + ": "
          + TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB + "=" + sortmb
          + ", soft limit=" + softLimit
          + ", bufstart=" + bufstart
          + ", bufvoid=" + bufvoid
          + ", kvstart=" + kvstart
          + ", legnth=" + maxRec
          + ", finalMergeEnabled=" + isFinalMergeEnabled());
    }

    // k/v serialization
    valSerializer.open(bb);
    keySerializer.open(bb);

    spillInProgress = false;
    minSpillsForCombine = this.conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINE_MIN_SPILLS, 3);
    spillThread.setDaemon(true);
    spillThread.setName("SpillThread {"
        + TezUtilsInternal.cleanVertexName(outputContext.getDestinationVertexName() + "}"));
    spillLock.lock();
    try {
      spillThread.start();
      while (!spillThreadRunning) {
        spillDone.await();
      }
    } catch (InterruptedException e) {
      //interrupt spill thread
      spillThread.interrupt();
      Thread.currentThread().interrupt();
      throw new IOException("Spill thread failed to initialize", e);
    } finally {
      spillLock.unlock();
    }
    if (sortSpillException != null) {
      throw new IOException("Spill thread failed to initialize",
          sortSpillException);
    }
  }

  @VisibleForTesting
  static int computeSortBufferSize(int availableMemoryMB, String logContext) {

    if (availableMemoryMB <= 0) {
      throw new RuntimeException(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB +
          "=" + availableMemoryMB + ". It should be > 0");
    }

    if (availableMemoryMB > MAX_IO_SORT_MB) {
      LOG.warn(logContext + ": Scaling down " + TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB +
          "=" + availableMemoryMB + " to " + MAX_IO_SORT_MB
          + " (max sort buffer size supported forDefaultSorter)");
    }

    // cap sort buffer to MAX_IO_SORT_MB for DefaultSorter.
    // Not using 2047 to avoid any ArrayIndexOutofBounds in collect() phase.
    return Math.min(MAX_IO_SORT_MB, availableMemoryMB);
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
              /**
               * Reason for capping sort buffer to MAX_IO_SORT_MB
               * E.g
               * kvbuffer.length = 2146435072 (2047 MB)
               * Corner case: bufIndex=2026133899, kvbidx=523629312.
               * distkvi = mod - i + j = 2146435072 - 2026133899 + 523629312 = 643930485
               * newPos = (2026133899 + (max(.., min(643930485/2, 271128624))) (This would
               * overflow)
               */
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
      outputContext.notifyProgress();
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
      LOG.info(outputContext.getDestinationVertexName() + ": Record too large for in-memory buffer: " + e.getMessage());
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
      LOG.info(outputContext.getDestinationVertexName() + ": " + "(EQUATOR) " + pos + " kvi " + kvindex +
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
      LOG.info(outputContext.getDestinationVertexName() + ": " + "(RESET) equator " + e + " kv " + kvstart + "(" +
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
  protected class BlockingBuffer extends NonSyncDataOutputStream {

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
                Thread.currentThread().interrupt();
                  throw new IOInterruptedException(
                      "Buffer interrupted while waiting for the writer", e);
              }
            }
          } while (blockwrite);
        } finally {
          spillLock.unlock();
        }
      }
      // here, we know that we have sufficient space to write
      // int overflow possible with (bufindex + len)
      long futureBufIndex = (long) bufindex + len;
      if (futureBufIndex > bufvoid) {
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

  void interruptSpillThread() throws IOException {
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
      LOG.info(outputContext.getDestinationVertexName() + ": " + "Spill thread interrupted");
      //Reset status
      Thread.currentThread().interrupt();
      throw new IOInterruptedException("Spill failed", e);
    }
  }

  @Override
  public void flush() throws IOException {
    LOG.info(outputContext.getDestinationVertexName() + ": " + "Starting flush of map output");
    outputContext.notifyProgress();
    if (Thread.currentThread().isInterrupted()) {
      /**
       * Possible that the thread got interrupted when flush was happening or when the flush was
       * never invoked. As a part of cleanup activity in TezTaskRunner, it would invoke close()
       * on all I/O. At that time, this is safe to cleanup
       */
      if (cleanup) {
        cleanup();
      }
      try {
        interruptSpillThread();
      } catch(IOException e) {
        //safe to ignore
      }
      return;
    }

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
          LOG.info(
              outputContext.getDestinationVertexName() + ": " + "Sorting & Spilling map output. "
                  + "bufstart = " + bufstart + ", bufend = " + bufmark + ", bufvoid = " + bufvoid
                  + "; " + "kvstart=" + kvstart + "(" + (kvstart * 4) + ")"
                  + ", kvend = " + kvend + "(" + (kvend * 4) + ")"
                  + ", length = " + (distanceTo(kvend, kvstart, kvmeta.capacity()) + 1) + "/" +
                  maxRec);
        }
        long sameKeyCount = 0;
        long totalKeysCount = 0;
        synchronized (this) {
          sameKeyCount = sameKey;
          totalKeysCount = totalKeys;
        }
        outputContext.notifyProgress();
        sortAndSpill(sameKeyCount, totalKeysCount);
      }
    } catch (InterruptedException e) {
      //Reset status
      Thread.currentThread().interrupt();
      interruptSpillThread();
      throw new IOException("Interrupted while waiting for the writer", e);
    } finally {
      spillLock.unlock();
    }

    interruptSpillThread();
    // release sort buffer before the mergecl
    //FIXME
    //kvbuffer = null;

    try {
      mergeParts();
    } catch (InterruptedException e) {
      cleanup();
      Thread.currentThread().interrupt();
    }
    if (isFinalMergeEnabled()) {
      fileOutputByteCounter.increment(rfs.getFileStatus(finalOutputFile).getLen());
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    kvbuffer = null;
    kvmeta = null;
  }

  boolean isClosed() {
    return kvbuffer == null && kvmeta == null;
  }

  protected class SpillThread extends Thread {

    volatile long totalKeysCount;
    volatile long sameKeyCount;

    @Override
    public void run() {
      spillLock.lock();
      try {
        spillThreadRunning = true;
        while (true) {
          spillDone.signal();
          while (!spillInProgress) {
            spillReady.await();
          }
          try {
            spillLock.unlock();
            sortAndSpill(sameKeyCount, totalKeysCount);
          } catch (Throwable t) {
            LOG.warn(outputContext.getDestinationVertexName() + ": " + "Got an exception in sortAndSpill", t);
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
        LOG.info(outputContext.getDestinationVertexName() + ": " + "Spill thread interrupted");
        Thread.currentThread().interrupt();
      } finally {
        spillLock.unlock();
        spillThreadRunning = false;
      }
    }

    public void setTotalKeysProcessed(long sameKeyCount, long totalKeysCount) {
      this.sameKeyCount = sameKeyCount;
      this.totalKeysCount = totalKeysCount;
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
      if (lspillException instanceof InterruptedException) {
        throw new IOInterruptedException("Spill failed", lspillException);
      } else {
        throw new IOException("Spill failed", lspillException);
      }
    }
  }

  private void startSpill() {
    assert !spillInProgress;
    kvend = (kvindex + NMETA) % kvmeta.capacity();
    bufend = bufmark;
    spillInProgress = true;
    if (LOG.isInfoEnabled()) {
      LOG.info(outputContext.getDestinationVertexName() + ": Spilling map output."
          + "bufstart=" + bufstart + ", bufend = " + bufmark + ", bufvoid = " + bufvoid
          +"; kvstart=" + kvstart + "(" + (kvstart * 4) + ")"
          +", kvend = " + kvend + "(" + (kvend * 4) + ")"
          + ", length = " + (distanceTo(kvend, kvstart, kvmeta.capacity()) + 1) + "/" + maxRec);
    }
    spillThread.setTotalKeysProcessed(sameKey, totalKeys);
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

  private boolean isRLENeeded(long sameKeyCount, long totalKeysCount) {
    return (sameKeyCount > (0.1 * totalKeysCount)) || (sameKeyCount < 0);
  }

  protected void sortAndSpill(long sameKeyCount, long totalKeysCount)
      throws IOException, InterruptedException {
    final int mstart = getMetaStart();
    final int mend = getMetaEnd();
    sorter.sort(this, mstart, mend, progressable);
    spill(mstart, mend, sameKeyCount, totalKeysCount);
  }

  private void adjustSpillCounters(long rawLen, long compLength) {
    if (!isFinalMergeEnabled()) {
      outputBytesWithOverheadCounter.increment(rawLen);
    } else {
      if (numSpills > 0) {
        additionalSpillBytesWritten.increment(compLength);
        // Reset the value will be set during the final merge.
        outputBytesWithOverheadCounter.setValue(0);
      } else {
        // Set this up for the first write only. Subsequent ones will be handled in the final merge.
        outputBytesWithOverheadCounter.increment(rawLen);
      }
    }
  }

  protected void spill(int mstart, int mend, long sameKeyCount, long totalKeysCount)
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
      spillFilePaths.put(numSpills, filename);
      out = rfs.create(filename);

      int spindex = mstart;
      final InMemValBytes value = createInMemValBytes();
      boolean rle = isRLENeeded(sameKeyCount, totalKeysCount);
      for (int i = 0; i < partitions; ++i) {
        IFile.Writer writer = null;
        try {
          long segmentStart = out.getPos();
          if (spindex < mend && kvmeta.get(offsetFor(spindex) + PARTITION) == i
              || !sendEmptyPartitionDetails) {
            writer = new Writer(conf, out, keyClass, valClass, codec,
                spilledRecordsCounter, null, rle);
          }
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
                LOG.debug(outputContext.getDestinationVertexName() + ": " + "Running combine processor");
              }
              runCombineProcessor(kvIter, writer);
            }
          }
          long rawLength = 0;
          long partLength = 0;
          // close the writer
          if (writer != null) {
            writer.close();
            rawLength = writer.getRawLength();
            partLength = writer.getCompressedLength();
          }
          adjustSpillCounters(rawLength, partLength);
          // record offsets
          final TezIndexRecord rec =
              new TezIndexRecord(segmentStart, rawLength, partLength);
          spillRec.putIndex(rec, i);
          if (!isFinalMergeEnabled() && reportPartitionStats() && writer != null) {
            partitionStats[i] += partLength;
          }
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
        spillFileIndexPaths.put(numSpills, indexFilename);
        spillRec.writeToFile(indexFilename, conf);
      } else {
        indexCacheList.add(spillRec);
        totalIndexCacheMemory +=
          spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
      }
      LOG.info(outputContext.getDestinationVertexName() + ": " + "Finished spill " + numSpills);
      ++numSpills;
      if (!isFinalMergeEnabled()) {
        numShuffleChunks.setValue(numSpills);
      } else if (numSpills > 1) {
        //Increment only when there was atleast one previous spill
        numAdditionalSpills.increment(1);
      }
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
      spillFilePaths.put(numSpills, filename);
      out = rfs.create(filename);

      // we don't run the combiner for a single record
      for (int i = 0; i < partitions; ++i) {
        IFile.Writer writer = null;
        try {
          long segmentStart = out.getPos();
          // Create a new codec, don't care!
          if (!sendEmptyPartitionDetails || (i == partition)) {
            writer = new Writer(conf, out, keyClass, valClass, codec,
                spilledRecordsCounter, null, false);
          }
          if (i == partition) {
            final long recordStart = out.getPos();
            writer.append(key, value);
            // Note that our map byte count will not be accurate with
            // compression
            mapOutputByteCounter.increment(out.getPos() - recordStart);
          }
          long rawLength =0;
          long partLength =0;
          if (writer != null) {
            writer.close();
            rawLength = writer.getRawLength();
            partLength = writer.getCompressedLength();
          }
          adjustSpillCounters(rawLength, partLength);

          // record offsets
          TezIndexRecord rec = new TezIndexRecord(segmentStart, rawLength, partLength);
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
        spillFileIndexPaths.put(numSpills, indexFilename);
         spillRec.writeToFile(indexFilename, conf);
      } else {
        indexCacheList.add(spillRec);
        totalIndexCacheMemory +=
          spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
      }
      ++numSpills;
      if (!isFinalMergeEnabled()) {
        numShuffleChunks.setValue(numSpills);
      } else if (numSpills > 1) {
        //Increment only when there is atleast one previous spill
        numAdditionalSpills.increment(1);
      }
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

    @Override
    public boolean hasNext() throws IOException {
        return (current + 1) < end;
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

    @Override
    public boolean isSameKey() throws IOException {
      return false;
    }

    public void close() { }
  }

  private void maybeSendEventForSpill(List<Event> events, boolean isLastEvent,
      TezSpillRecord spillRecord, int index, boolean sendEvent) throws IOException {
    if (isFinalMergeEnabled()) {
      return;
    }
    Preconditions.checkArgument(spillRecord != null, "Spill record can not be null");

    String pathComponent = (outputContext.getUniqueIdentifier() + "_" + index);
    ShuffleUtils.generateEventOnSpill(events, isFinalMergeEnabled(), isLastEvent,
        outputContext, index, spillRecord, partitions, sendEmptyPartitionDetails, pathComponent,
        partitionStats, reportDetailedPartitionStats(), auxiliaryService, deflater);

    LOG.info(outputContext.getDestinationVertexName() + ": " +
        "Adding spill event for spill (final update=" + isLastEvent + "), spillId=" + index);

    if (sendEvent) {
      outputContext.sendEvents(events);
    }
  }

  private void maybeAddEventsForSpills() throws IOException {
    if (isFinalMergeEnabled()) {
      return;
    }
    List<Event> events = Lists.newLinkedList();
    for(int i=0; i<numSpills; i++) {

      TezSpillRecord spillRecord = indexCacheList.get(i);
      if (spillRecord == null) {
        //File was already written and location is stored in spillFileIndexPaths
        spillRecord = new TezSpillRecord(spillFileIndexPaths.get(i), conf);
      } else {
        //Double check if this file has to be written
        if (spillFileIndexPaths.get(i) == null) {
          Path indexPath = mapOutputFile.getSpillIndexFileForWrite(i, partitions *
              MAP_OUTPUT_INDEX_RECORD_LENGTH);
          spillRecord.writeToFile(indexPath, conf);
        }
      }

      maybeSendEventForSpill(events, (i == numSpills - 1), spillRecord, i, false);
      fileOutputByteCounter.increment(rfs.getFileStatus(spillFilePaths.get(i)).getLen());
    }
    outputContext.sendEvents(events);
  }

  private void mergeParts() throws IOException, InterruptedException {
    // get the approximate size of the final output/index files
    long finalOutFileSize = 0;
    long finalIndexFileSize = 0;
    final Path[] filename = new Path[numSpills];
    final String taskIdentifier = outputContext.getUniqueIdentifier();

    for(int i = 0; i < numSpills; i++) {
      filename[i] = spillFilePaths.get(i);
      finalOutFileSize += rfs.getFileStatus(filename[i]).getLen();
    }
    if (numSpills == 1) { //the spill is the final output
      TezSpillRecord spillRecord = null;
      if (isFinalMergeEnabled()) {
        finalOutputFile = mapOutputFile.getOutputFileForWriteInVolume(filename[0]);
        finalIndexFile = mapOutputFile.getOutputIndexFileForWriteInVolume(filename[0]);
        sameVolRename(filename[0], finalOutputFile);
        if (indexCacheList.size() == 0) {
          sameVolRename(spillFileIndexPaths.get(0), finalIndexFile);
          spillRecord = new TezSpillRecord(finalIndexFile, conf);
        } else {
          spillRecord = indexCacheList.get(0);
          spillRecord.writeToFile(finalIndexFile, conf);
        }
      } else {
        List<Event> events = Lists.newLinkedList();
        //Since there is only one spill, spill record would be present in cache.
        spillRecord = indexCacheList.get(0);
        Path indexPath = mapOutputFile.getSpillIndexFileForWrite(numSpills-1, partitions *
            MAP_OUTPUT_INDEX_RECORD_LENGTH);
        spillRecord.writeToFile(indexPath, conf);
        maybeSendEventForSpill(events, true, spillRecord, 0, true);
        fileOutputByteCounter.increment(rfs.getFileStatus(spillFilePaths.get(0)).getLen());
        //No need to populate finalIndexFile, finalOutputFile etc when finalMerge is disabled
      }
      if (spillRecord != null && reportPartitionStats()) {
        for(int i=0; i < spillRecord.size(); i++) {
          partitionStats[i] += spillRecord.getIndex(i).getPartLength();
        }
      }
      numShuffleChunks.setValue(numSpills);
      return;
    }

    // read in paged indices
    for (int i = indexCacheList.size(); i < numSpills; ++i) {
      Path indexFileName = spillFileIndexPaths.get(i);
      indexCacheList.add(new TezSpillRecord(indexFileName, conf));
    }

    //Check if it is needed to do final merge. Or else, exit early.
    if (numSpills > 0 && !isFinalMergeEnabled()) {
      maybeAddEventsForSpills();
      //No need to do final merge.
      return;
    }

    //make correction in the length to include the sequence file header
    //lengths for each partition
    finalOutFileSize += partitions * APPROX_HEADER_LENGTH;
    finalIndexFileSize = partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH;

    if (isFinalMergeEnabled()) {
      finalOutputFile = mapOutputFile.getOutputFileForWrite(finalOutFileSize);
      finalIndexFile = mapOutputFile.getOutputIndexFileForWrite(finalIndexFileSize);
    } else if (numSpills == 0) {
      //e.g attempt_1424502260528_0119_1_07_000058_0_10012_0/file.out when final merge is
      // disabled
      finalOutputFile = mapOutputFile.getSpillFileForWrite(numSpills, finalOutFileSize);
      finalIndexFile = mapOutputFile.getSpillIndexFileForWrite(numSpills, finalIndexFileSize);
    }

    //The output stream for the final single output file
    FSDataOutputStream finalOut = rfs.create(finalOutputFile, true, 4096);

    if (numSpills == 0) {
      // TODO Change event generation to say there is no data rather than generating a dummy file
      //create dummy files
      long rawLength = 0;
      long partLength = 0;
      TezSpillRecord sr = new TezSpillRecord(partitions);
      try {
        for (int i = 0; i < partitions; i++) {
          long segmentStart = finalOut.getPos();
          if (!sendEmptyPartitionDetails) {
            Writer writer =
                new Writer(conf, finalOut, keyClass, valClass, codec, null, null);
            writer.close();
            rawLength = writer.getRawLength();
            partLength = writer.getCompressedLength();
          }
          TezIndexRecord rec =
              new TezIndexRecord(segmentStart, rawLength, partLength);
          // Covers the case of multiple spills.
          outputBytesWithOverheadCounter.increment(rawLength);
          sr.putIndex(rec, i);
        }
        sr.writeToFile(finalIndexFile, conf);
      } finally {
        finalOut.close();
      }
      ++numSpills;
      if (!isFinalMergeEnabled()) {
        List<Event> events = Lists.newLinkedList();
        maybeSendEventForSpill(events, true, sr, 0, true);
        fileOutputByteCounter.increment(rfs.getFileStatus(finalOutputFile).getLen());
      }
      numShuffleChunks.setValue(numSpills);
      return;
    }
    else {
      final TezSpillRecord spillRec = new TezSpillRecord(partitions);
      for (int parts = 0; parts < partitions; parts++) {
        boolean shouldWrite = false;
        //create the segments to be merged
        List<Segment> segmentList =
            new ArrayList<Segment>(numSpills);
        for (int i = 0; i < numSpills; i++) {
          outputContext.notifyProgress();
          TezIndexRecord indexRecord = indexCacheList.get(i).getIndex(parts);
          if (indexRecord.hasData() || !sendEmptyPartitionDetails) {
            shouldWrite = true;
            DiskSegment s =
              new DiskSegment(rfs, filename[i], indexRecord.getStartOffset(),
                               indexRecord.getPartLength(), codec, ifileReadAhead,
                               ifileReadAheadLength, ifileBufferSize, true);
            segmentList.add(s);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug(outputContext.getDestinationVertexName() + ": "
                + "TaskIdentifier=" + taskIdentifier + " Partition=" + parts +
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
                       progressable, sortSegments, true,
                       null, spilledRecordsCounter, additionalSpillBytesRead,
                       null); // Not using any Progress in TezMerger. Should just work.

        //write merged output to disk
        long segmentStart = finalOut.getPos();
        long rawLength = 0;
        long partLength = 0;
        if (shouldWrite) {
        Writer writer =
            new Writer(conf, finalOut, keyClass, valClass, codec,
                spilledRecordsCounter, null);
        if (combiner == null || numSpills < minSpillsForCombine) {
          TezMerger.writeFile(kvIter, writer,
              progressable, TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
        } else {
          runCombineProcessor(kvIter, writer);
        }
        writer.close();
        rawLength = writer.getRawLength();
        partLength = writer.getCompressedLength();
      }
      outputBytesWithOverheadCounter.increment(rawLength);
      // record offsets
      final TezIndexRecord rec =
          new TezIndexRecord(segmentStart, rawLength, partLength);
        spillRec.putIndex(rec, parts);
        if (reportPartitionStats()) {
          partitionStats[parts] += partLength;
        }
      }
      numShuffleChunks.setValue(1); //final merge has happened
      spillRec.writeToFile(finalIndexFile, conf);
      finalOut.close();
      for(int i = 0; i < numSpills; i++) {
        rfs.delete(filename[i],true);
      }
    }
  }
}
