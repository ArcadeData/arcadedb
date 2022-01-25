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

/**
 * Compression implementation that uses the popular LZ4 algorithm.
 */
public class LZ4Compression implements Compression {
  private static final byte[]              EMPTY_BYTES  = new byte[0];
  private static final Binary              EMPTY_BINARY = new Binary(EMPTY_BYTES);
  private final        LZ4Factory          factory;
  private final        LZ4Compressor       compressor;
  private final        LZ4FastDecompressor decompressor;

  public LZ4Compression() {
    this.factory = LZ4Factory.fastestInstance();
    this.compressor = factory.fastCompressor();
    this.decompressor = factory.fastDecompressor();
  }

  @Override
  public Binary compress(final Binary data) {
    final int decompressedLength = data.size() - data.position();
    final int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
    final byte[] compressed = new byte[maxCompressedLength];
    final int compressedLength = compressor
        .compress(data.getContent(), data.position(), data.size(), compressed, 0, maxCompressedLength);

    return new Binary(compressed, compressedLength);
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
