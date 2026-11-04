/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.runtime.library.utils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.ZStandardCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.DummyCompressionCodec;
import org.apache.tez.runtime.library.common.sort.impl.IFileInputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

          assertEquals(modifiedBufferSize, getBufferSize(stream), "stream buffer size is incorrect");

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

            assertEquals(modifiedBufferSize, getBufferSize(stream), "stream buffer size is incorrect");

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

            assertEquals(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT, getBufferSize(stream),
                "stream buffer size is incorrect");

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

            assertEquals(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT, getBufferSize(stream),
                "stream buffer size is incorrect");

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

    assertEquals(CodecUtils.DEFAULT_BUFFER_SIZE, CodecUtils.getDefaultBufferSize(conf, new DummyCompressionCodec()));
    assertEquals(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT,
        CodecUtils.getDefaultBufferSize(conf, new DefaultCodec()));
    assertEquals(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT,
        CodecUtils.getDefaultBufferSize(conf, new BZip2Codec()));
    assertEquals(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT,
        CodecUtils.getDefaultBufferSize(conf, new GzipCodec()));
    assertEquals(CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT,
        CodecUtils.getDefaultBufferSize(conf, new SnappyCodec()));
    assertEquals(CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_DEFAULT,
        CodecUtils.getDefaultBufferSize(conf, new ZStandardCodec()));
    assertEquals(CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT,
        CodecUtils.getDefaultBufferSize(conf, new Lz4Codec()));
  }

  /**
   * A stream sized to its data writes the bytes of the configured stream, from one byte to several
   * configured blocks of compressible and of incompressible data, and gets a buffer of the
   * compression bound of the data when the data fits one configured block.
   */
  @ParameterizedTest
  @ValueSource(classes = {SnappyCodec.class, Lz4Codec.class})
  public void testOutputStreamSizedToData(Class<? extends CompressionCodec> codecClass) throws IOException {
    Configuration conf = new Configuration(false);
    conf.setInt(CodecUtils.getBufferSizeProperty(codecClass.getName()), 64 * 1024);
    CompressionCodec codec = newBlockCodec(codecClass, conf);
    int bufferSize = CodecUtils.getOutputBufferSize(codec);
    assertEquals(64 * 1024, bufferSize);
    int maxBlockSize = bufferSize - CodecUtils.getBlockCompressionOverhead(codecClass.getName(), bufferSize);
    for (boolean compressible : new boolean[] {true, false}) {
      for (int length : new int[] {1, 1000, maxBlockSize - 1, maxBlockSize, maxBlockSize + 1, 5 * bufferSize}) {
        byte[] data = randomBytes(length, compressible);
        ByteArrayOutputStream configured = new ByteArrayOutputStream();
        ByteArrayOutputStream sized = new ByteArrayOutputStream();
        compress(codec, data, configured, -1, -1);
        int sizedBuffer = compress(codec, data, sized, bufferSize, length);

        String what = "length " + length + ", compressible " + compressible;
        assertEquals(length < maxBlockSize
            ? length + CodecUtils.getBlockCompressionOverhead(codecClass.getName(), length) : bufferSize,
            sizedBuffer, what);
        assertArrayEquals(configured.toByteArray(), sized.toByteArray(), what);
        assertArrayEquals(data, decompress(codec, configured.toByteArray(), length), what);
        assertArrayEquals(data, decompress(codec, sized.toByteArray(), length), what);
      }
    }
  }

  /** A length that underestimates the data costs more blocks, not the data. */
  @ParameterizedTest
  @ValueSource(classes = {SnappyCodec.class, Lz4Codec.class})
  public void testOutputStreamSizedToUnderestimatedLength(Class<? extends CompressionCodec> codecClass)
      throws IOException {
    CompressionCodec codec = newBlockCodec(codecClass, new Configuration(false));
    byte[] data = randomBytes(100_000, false);
    ByteArrayOutputStream sized = new ByteArrayOutputStream();
    compress(codec, data, sized, CodecUtils.getOutputBufferSize(codec), 1000);
    assertArrayEquals(data, decompress(codec, sized.toByteArray(), data.length));
  }

  /** Lz4Codec needs lz4-java, which Hadoop declares provided. */
  private static CompressionCodec newBlockCodec(Class<? extends CompressionCodec> codecClass,
      Configuration conf) {
    if (codecClass == Lz4Codec.class) {
      try {
        Class.forName("net.jpountz.lz4.LZ4Factory");
      } catch (ClassNotFoundException e) {
        assumeTrue(false, "lz4-java is not on the classpath");
      }
    }
    return ReflectionUtils.newInstance(codecClass, conf);
  }

  private static byte[] randomBytes(int length, boolean compressible) {
    byte[] data = new byte[length];
    Random random = new Random(length);
    random.nextBytes(data);
    if (compressible) {
      for (int i = 0; i < length; i++) {
        data[i] &= 0x0f;
      }
    }
    return data;
  }

  /**
   * Writes the data in 64 KiB chunks as IFile.Writer does, through the codec's own stream when
   * bufferSize is -1.
   *
   * @return the length of the stream's buffer
   */
  private static int compress(CompressionCodec codec, byte[] data, OutputStream out, int bufferSize,
      long dataLength) throws IOException {
    Compressor compressor = CodecUtils.getCompressor(codec);
    try {
      CompressionOutputStream stream =
          CodecUtils.createOutputStream(codec, out, compressor, bufferSize, dataLength);
      for (int off = 0; off < data.length; off += 64 * 1024) {
        stream.write(data, off, Math.min(64 * 1024, data.length - off));
      }
      stream.finish();
      return getCompressorStreamBufferSize(stream);
    } finally {
      CodecPool.returnCompressor(compressor);
    }
  }

  private static byte[] decompress(CompressionCodec codec, byte[] compressed, int length)
      throws IOException {
    Decompressor decompressor = CodecUtils.getDecompressor(codec);
    try {
      InputStream in = CodecUtils.createInputStream(codec, new ByteArrayInputStream(compressed),
          decompressor);
      byte[] data = new byte[length];
      IOUtils.readFully(in, data, 0, length);
      assertEquals(-1, in.read());
      return data;
    } finally {
      CodecPool.returnDecompressor(decompressor);
    }
  }

  private static int getCompressorStreamBufferSize(CompressionOutputStream stream) {
    try {
      Field field = CompressorStream.class.getDeclaredField("buffer");
      field.setAccessible(true);
      return ((byte[]) field.get(stream)).length;
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
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
