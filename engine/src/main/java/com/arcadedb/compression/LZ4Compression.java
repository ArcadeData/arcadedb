/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
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
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.compression;

import com.arcadedb.database.Binary;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.util.*;

/**
 * Compression implementation that uses the popular LZ4 algorithm. Two compressors are exposed: the default
 * {@code fast} encoder (block size optimised for throughput) and an on-demand {@code max} (LZ4 HC) encoder
 * that spends more CPU on a deeper match search to produce a smaller output. Both share the same LZ4 byte
 * format and the same decoder, so {@link #decompress(byte[], int)} works regardless of which encoder produced
 * the input.
 */
public class LZ4Compression implements Compression {
  private static final byte[]              EMPTY_BYTES  = new byte[0];
  private static final Binary              EMPTY_BINARY = new Binary(EMPTY_BYTES);
  private final        LZ4Factory          factory;
  private final        LZ4Compressor       compressor;
  private final        LZ4FastDecompressor decompressor;
  // High-compression encoder is built on first use: it allocates ~256KB of internal state and is rarely needed
  // on the read path, so we don't want to pay for it when only the fast encoder is in use.
  private volatile     LZ4Compressor       maxCompressor;

  public LZ4Compression() {
    this.factory = LZ4Factory.fastestInstance();
    this.compressor = factory.fastCompressor();
    this.decompressor = factory.fastDecompressor();
  }

  @Override
  public byte[] compress(final byte[] data) {
    final int maxCompressedLength = compressor.maxCompressedLength(data.length);
    byte[] compressed = new byte[maxCompressedLength];
    final int compressedLength = compressor.compress(data, 0, data.length, compressed, 0, maxCompressedLength);
    if (compressedLength != maxCompressedLength)
      compressed = Arrays.copyOf(compressed, compressedLength);
    return compressed;
  }

  /**
   * Maximum-compression encoder (LZ4 HC). Compresses 8-20x slower than {@link #compress(byte[])} but produces
   * meaningfully smaller output (~10pp on text); decompression is the same speed. Use for write-once /
   * read-many EXTERNAL payloads where bucket file size dominates.
   */
  public byte[] compressMax(final byte[] data) {
    LZ4Compressor c = maxCompressor;
    if (c == null) {
      synchronized (this) {
        c = maxCompressor;
        if (c == null)
          c = maxCompressor = factory.highCompressor();
      }
    }
    final int maxCompressedLength = c.maxCompressedLength(data.length);
    byte[] compressed = new byte[maxCompressedLength];
    final int compressedLength = c.compress(data, 0, data.length, compressed, 0, maxCompressedLength);
    if (compressedLength != maxCompressedLength)
      compressed = Arrays.copyOf(compressed, compressedLength);
    return compressed;
  }

  @Override
  public Binary compress(final Binary data) {
    final int decompressedLength = data.size() - data.position();
    final int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
    final byte[] compressed = new byte[maxCompressedLength];
    final int compressedLength = compressor.compress(data.getContent(), data.position(), data.size(), compressed, 0,
        maxCompressedLength);

    return new Binary(compressed, compressedLength);
  }

  @Override
  public byte[] decompress(final byte[] data, final int decompressedLength) {
    final byte[] decompressed = new byte[decompressedLength];
    decompressor.decompress(data, 0, decompressed, 0, decompressedLength);
    return decompressed;
  }

  @Override
  public Binary decompress(final Binary data, final int decompressedLength) {
    if (decompressedLength == 0)
      return EMPTY_BINARY;

    final int compressedLength = data.size() - data.position();
    if (compressedLength == 0)
      return EMPTY_BINARY;

    final byte[] decompressed = new byte[decompressedLength];
    decompressor.decompress(data.getContent(), data.position(), decompressed, 0, decompressedLength);
    return new Binary(decompressed);
  }
}
