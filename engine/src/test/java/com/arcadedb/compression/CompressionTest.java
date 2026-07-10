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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CompressionTest {
  @Test
  void compression() {
    final Binary buffer = new Binary("This is a test about compression".getBytes());

    final Binary compressed = CompressionFactory.getDefault().compress(buffer);
    final Binary decompressed = CompressionFactory.getDefault().decompress(compressed, buffer.size());

    assertThat(decompressed).isEqualTo(buffer);
  }

  @Test
  void emptyCompression() {
    final Binary buffer = new Binary();

    final Binary compressed = CompressionFactory.getDefault().compress(buffer);
    final Binary decompressed = CompressionFactory.getDefault().decompress(compressed, buffer.size());

    assertThat(decompressed).isEqualTo(buffer);
  }

  @Test
  void compressionWithNonZeroPosition() {
    // Regression test for #4317: compress(Binary) passed data.size() instead of decompressedLength
    // when position > 0, causing silent corruption or ArrayIndexOutOfBoundsException.
    final byte[] prefix = "HEADER".getBytes();
    final byte[] payload = "This is the actual payload to compress".getBytes();
    final Binary buffer = new Binary(prefix.length + payload.length);
    buffer.putByteArray(prefix);
    buffer.putByteArray(payload);
    // Advance position past the prefix so only the payload slice is to be compressed
    buffer.position(prefix.length);

    final int decompressedLength = buffer.size() - buffer.position();
    final Binary compressed = CompressionFactory.getDefault().compress(buffer);
    final Binary decompressed = CompressionFactory.getDefault().decompress(compressed, decompressedLength);

    assertThat(decompressed.getContent()).startsWith(payload);
    assertThat(decompressed.size()).isEqualTo(decompressedLength);
  }

  @Test
  void compressionOfSlicedBinary() {
    // Regression test for #4317: compress(Binary) must account for getContentBeginOffset()
    // when the Binary is a slice (arrayOffset > 0 in the backing ByteBuffer).
    final byte[] prefix = "HEADER".getBytes();
    final byte[] payload = "This is the actual payload to compress".getBytes();
    final Binary buffer = new Binary(prefix.length + payload.length);
    buffer.putByteArray(prefix);
    buffer.putByteArray(payload);
    buffer.flip();
    final Binary slice = buffer.slice(prefix.length);

    final int decompressedLength = slice.size();
    final Binary compressed = CompressionFactory.getDefault().compress(slice);
    final Binary decompressed = CompressionFactory.getDefault().decompress(compressed, decompressedLength);

    assertThat(decompressed.getContent()).startsWith(payload);
    assertThat(decompressed.size()).isEqualTo(decompressedLength);
  }
}
