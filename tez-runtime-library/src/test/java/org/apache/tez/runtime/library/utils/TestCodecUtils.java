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
package org.apache.tez.runtime.library.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.ZStandardCodec;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.DummyCompressionCodec;
import org.apache.tez.runtime.library.common.sort.impl.IFileInputStream;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class TestCodecUtils {

  @Test
  public void testConcurrentDecompressorCreationWithModifiedBuffersize() throws Exception {
    testConcurrentDecompressorCreationWithModifiedBuffersizeOnCodec(new DefaultCodec());
  }

  private void testConcurrentDecompressorCreationWithModifiedBuffersizeOnCodec(
      CompressionCodec codec) throws InterruptedException, ExecutionException {
    int modifiedBufferSize = 1000;
    int numberOfThreads = 1000;

    ExecutorService service = Executors.newFixedThreadPool(numberOfThreads);

    Configuration conf = new Configuration();
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, true);
    ((Configurable) codec).setConf(conf);

    Future<?>[] futures = new Future[numberOfThreads];
    final CountDownLatch latch = new CountDownLatch(1);

    for (int i = 0; i < numberOfThreads; i++) {
      futures[i] = service.submit(() -> {
        try {
          waitForLatch(latch);

          Decompressor decompressor = CodecUtils.getDecompressor(codec);
          DecompressorStream stream =
              (DecompressorStream) CodecUtils.getDecompressedInputStreamWithBufferSize(codec,
                  mock(IFileInputStream.class), decompressor, modifiedBufferSize);

          Assert.assertEquals("stream buffer size is incorrect", modifiedBufferSize,
              getBufferSize(stream));

          CodecPool.returnDecompressor(decompressor);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
    latch.countDown();

    for (Future<?> f : futures) {
      f.get();
    }
  }

  @Test
  public void testConcurrentCompressorDecompressorCreation() throws Exception {
    testConcurrentCompressorDecompressorCreationOnCodec(new DefaultCodec());
  }

  private void testConcurrentCompressorDecompressorCreationOnCodec(CompressionCodec codec)
      throws IOException, InterruptedException, ExecutionException {
    int modifiedBufferSize = 1000;
    int numberOfThreads = 1000;

    ExecutorService service = Executors.newFixedThreadPool(numberOfThreads);

    Configuration conf = new Configuration();
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, true);
    ((Configurable) codec).setConf(conf);

    Future<?>[] futures = new Future[numberOfThreads];
    final CountDownLatch latch = new CountDownLatch(1);

    for (int i = 0; i < numberOfThreads; i++) {
      // let's "randomly" choose from scenarios and test them concurrently
      // 1. getDecompressedInputStreamWithBufferSize
      if (i % 3 == 0) {
        futures[i] = service.submit(() -> {
          try {
            waitForLatch(latch);

            Decompressor decompressor = CodecUtils.getDecompressor(codec);
            CompressionInputStream stream =
                (CompressionInputStream) CodecUtils.getDecompressedInputStreamWithBufferSize(codec,
                    mock(IFileInputStream.class), decompressor, modifiedBufferSize);

            Assert.assertEquals("stream buffer size is incorrect", modifiedBufferSize,
                getBufferSize(stream));

            CodecPool.returnDecompressor(decompressor);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
        // 2. getCompressor
      } else if (i % 3 == 1) {
        futures[i] = service.submit(() -> {
          try {
            waitForLatch(latch);

            Compressor compressor = CodecUtils.getCompressor(codec);
            CompressionOutputStream stream =
                CodecUtils.createOutputStream(codec, mock(OutputStream.class), compressor);

            Assert.assertEquals("stream buffer size is incorrect",
                CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT, getBufferSize(stream));

            CodecPool.returnCompressor(compressor);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
        // 3. getDecompressor
      } else if (i % 3 == 2) {
        futures[i] = service.submit(() -> {
          try {
            waitForLatch(latch);

            Decompressor decompressor = CodecUtils.getDecompressor(codec);
            CompressionInputStream stream =
                CodecUtils.createInputStream(codec, mock(InputStream.class), decompressor);

            Assert.assertEquals("stream buffer size is incorrect",
                CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT, getBufferSize(stream));

            CodecPool.returnDecompressor(decompressor);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }
    }
    latch.countDown();

    for (Future<?> f : futures) {
      f.get();
    }
  }

  @Test
  public void testDefaultBufferSize() {
    Configuration conf = new Configuration(); // config with no buffersize set

    Assert.assertEquals(CodecUtils.DEFAULT_BUFFER_SIZE,
        CodecUtils.getDefaultBufferSize(conf, new DummyCompressionCodec()));
    Assert.assertEquals(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT,
        CodecUtils.getDefaultBufferSize(conf, new DefaultCodec()));
    Assert.assertEquals(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT,
        CodecUtils.getDefaultBufferSize(conf, new BZip2Codec()));
    Assert.assertEquals(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT,
        CodecUtils.getDefaultBufferSize(conf, new GzipCodec()));
    Assert.assertEquals(CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT,
        CodecUtils.getDefaultBufferSize(conf, new SnappyCodec()));
    Assert.assertEquals(CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_DEFAULT,
        CodecUtils.getDefaultBufferSize(conf, new ZStandardCodec()));
    Assert.assertEquals(CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT,
        CodecUtils.getDefaultBufferSize(conf, new Lz4Codec()));
  }

  private void waitForLatch(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private int getBufferSize(Object stream) {
    try {
      Field field = stream.getClass().getDeclaredField("buffer");
      field.setAccessible(true);
      byte[] buffer = (byte[]) field.get(stream);
      return buffer.length;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
