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
package com.arcadedb.index.columnar;

import com.arcadedb.engine.block.BlockCorruptionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecoverySetFileTest {
  @TempDir
  Path dir;

  @Test
  void roundTrip() throws IOException {
    final Path file = dir.resolve(RecoverySetFile.FILE_NAME);
    // the counts cut the arrays: the slot past the count must not be written
    RecoverySetFile.write(file, new int[] { 3, 9 }, new int[][] { { 1, 5, 7, 99 }, { 2 } }, new int[] { 3, 1 });

    final Map<Integer, int[]> read = RecoverySetFile.read(file);

    assertThat(read).containsOnlyKeys(3, 9);
    assertThat(read.get(3)).containsExactly(1, 5, 7);
    assertThat(read.get(9)).containsExactly(2);
    assertThat(dir.resolve(RecoverySetFile.FILE_NAME + ColumnarStore.TMP_SUFFIX)).doesNotExist();
  }

  @Test
  void anEmptySetRoundTripsAndReplacesTheLastOne() throws IOException {
    final Path file = dir.resolve(RecoverySetFile.FILE_NAME);
    RecoverySetFile.write(file, new int[] { 3 }, new int[][] { { 1 } }, new int[] { 1 });
    RecoverySetFile.write(file, new int[0], new int[0][], new int[0]);

    assertThat(RecoverySetFile.read(file)).isEmpty();
  }

  @Test
  void corruptFileIsBlockCorruption() throws IOException {
    final Path file = dir.resolve(RecoverySetFile.FILE_NAME);
    RecoverySetFile.write(file, new int[] { 3, 9 }, new int[][] { { 1, 5, 7 }, { 2 } }, new int[] { 3, 1 });
    final byte[] valid = Files.readAllBytes(file);

    // a flipped page number: the CRC catches it
    final byte[] flipped = valid.clone();
    flipped[20] ^= 0x10;
    Files.write(file, flipped);
    assertThatThrownBy(() -> RecoverySetFile.read(file)).isInstanceOf(BlockCorruptionException.class).hasMessageContaining("CRC");

    // truncated below the fixed header
    Files.write(file, Arrays.copyOf(valid, 10));
    assertThatThrownBy(() -> RecoverySetFile.read(file)).isInstanceOf(BlockCorruptionException.class);

    // a huge page count with a valid CRC: refused before any allocation sized from it
    final byte[] huge = valid.clone();
    ByteBuffer.wrap(huge).putInt(16, Integer.MAX_VALUE);
    Files.write(file, withCrc(huge));
    assertThatThrownBy(() -> RecoverySetFile.read(file)).isInstanceOf(BlockCorruptionException.class).hasMessageContaining("past its end");

    // a wrong magic with a valid CRC
    final byte[] magic = valid.clone();
    ByteBuffer.wrap(magic).putInt(0, 0x12345678);
    Files.write(file, withCrc(magic));
    assertThatThrownBy(() -> RecoverySetFile.read(file)).isInstanceOf(BlockCorruptionException.class).hasMessageContaining("magic");
  }

  @Test
  void absentFileIsEmpty() throws IOException {
    assertThat(RecoverySetFile.read(dir.resolve(RecoverySetFile.FILE_NAME))).isEmpty();
  }

  private static byte[] withCrc(final byte[] bytes) {
    final CRC32 crc = new CRC32();
    crc.update(bytes, 0, bytes.length - 4);
    ByteBuffer.wrap(bytes).putInt(bytes.length - 4, (int) crc.getValue());
    return bytes;
  }
}
