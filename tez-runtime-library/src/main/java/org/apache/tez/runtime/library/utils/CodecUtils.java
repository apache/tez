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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.IFileInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CodecUtils {

  private static final Logger LOG = LoggerFactory.getLogger(IFile.class);
  private static final int DEFAULT_BUFFER_SIZE = 128 * 1024;

  private CodecUtils() {
  }

  public static CompressionCodec getCodec(Configuration conf) throws IOException {
    if (ConfigUtils.shouldCompressIntermediateOutput(conf)) {
      Class<? extends CompressionCodec> codecClass =
          ConfigUtils.getIntermediateOutputCompressorClass(conf, DefaultCodec.class);
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);

      if (codec != null) {
        Class<? extends Compressor> compressorType = null;
        Throwable cause = null;
        try {
          compressorType = codec.getCompressorType();
        } catch (RuntimeException e) {
          cause = e;
        }
        if (compressorType == null) {
          String errMsg = String.format(
              "Unable to get CompressorType for codec (%s). This is most"
                  + " likely due to missing native libraries for the codec.",
              conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC));
          throw new IOException(errMsg, cause);
        }
      }
      return codec;
    } else {
      return null;
    }
  }

  public static InputStream getDecompressedInputStreamWithBufferSize(CompressionCodec codec,
      IFileInputStream checksumIn, Decompressor decompressor, int compressedLength)
      throws IOException {
    String bufferSizeProp = TezRuntimeUtils.getBufferSizeProperty(codec);
    Configurable configurableCodec = (Configurable) codec;
    int originalSize = configurableCodec.getConf().getInt(bufferSizeProp, DEFAULT_BUFFER_SIZE);

    CompressionInputStream in = null;

    if (bufferSizeProp != null) {
      Configuration conf = configurableCodec.getConf();
      int newBufSize = Math.min(compressedLength, DEFAULT_BUFFER_SIZE);
      LOG.trace("buffer size was set according to min(compressedLength, {}): {}={}",
          DEFAULT_BUFFER_SIZE, bufferSizeProp, newBufSize);

      synchronized (codec) {
        conf.setInt(bufferSizeProp, newBufSize);

        in = codec.createInputStream(checksumIn, decompressor);
        /*
         * We would better reset the original buffer size into the codec. Basically the buffer size
         * is used at 2 places.
         *
         * 1. It can tell the inputstream/outputstream buffersize (which is created by
         * codec.createInputStream/codec.createOutputStream). This is something which might and
         * should be optimized in config, as inputstreams instantiate and use their own buffer and
         * won't reuse buffers from previous streams (TEZ-4135).
         *
         * 2. The same buffersize is used when a codec creates a new Compressor/Decompressor. The
         * fundamental difference is that Compressor/Decompressor instances are expensive and reused
         * by hadoop's CodecPool. Here is a hidden mismatch, which can happen when a codec is
         * created with a small buffersize config. Once it creates a Compressor/Decompressor
         * instance from its config field, the reused Compressor/Decompressor instance will be
         * reused later, even when application handles large amount of data. This way we can end up
         * in large stream buffers + small compressor/decompressor buffers, which can be suboptimal,
         * moreover, it can lead to strange errors, when a compressed output exceeds the size of the
         * buffer (TEZ-4234).
         *
         * An interesting outcome is that - as the codec buffersize config affects both
         * compressor(output) and decompressor(input) paths - an altered codec config can cause the
         * issues above for Compressor instances as well, even when we tried to leverage from
         * smaller buffer size only on decompression paths.
         */
        configurableCodec.getConf().setInt(bufferSizeProp, originalSize);
      }
    } else {
      in = codec.createInputStream(checksumIn, decompressor);
    }

    return in;
  }
}