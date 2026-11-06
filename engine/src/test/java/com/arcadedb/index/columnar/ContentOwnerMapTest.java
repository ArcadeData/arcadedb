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
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContentOwnerMapTest {
  @TempDir
  Path dir;

  private static long[] owners(final ContentOwnerMap map, final int page) {
    final long[] out = new long[map.ownerCountUpperBound(page)];
    final int count = map.owners(page, out);
    final long[] result = Arrays.copyOf(out, count);
    Arrays.sort(result);
    return result;
  }

  @Test
  void ownersOfASharedContentPage() {
    final ContentOwnerMap map = new ContentOwnerMap();
    map.setOwnerPages(10, new int[] { 7 }, 1);
    map.setOwnerPages(20, new int[] { 7, 3 }, 2);

    assertThat(owners(map, 7)).containsExactly(10, 20);
    assertThat(owners(map, 3)).containsExactly(20);
    assertThat(owners(map, 4)).isEmpty();
    assertThat(map.ownerCountUpperBound(4)).isZero();
  }

  @Test
  void setOwnerPagesReplacesOldPages() {
    final ContentOwnerMap map = new ContentOwnerMap();
    map.setOwnerPages(10, new int[] { 7, 8 }, 2);
    map.setOwnerPages(30, new int[] { 8 }, 1);
    map.setOwnerPages(10, new int[] { 9, 9, 99 }, 2); // only the first count entries count, duplicates once

    assertThat(owners(map, 7)).isEmpty();
    assertThat(owners(map, 8)).containsExactly(30);
    assertThat(owners(map, 9)).containsExactly(10);
    assertThat(owners(map, 99)).isEmpty();
    assertThat(map.ownerCountUpperBound(9)).isEqualTo(1);

    map.removeOwner(30);
    map.removeOwner(30);
    map.removeOwner(12345);
    assertThat(owners(map, 8)).isEmpty();
    map.setOwnerPages(10, new int[0], 0);
    assertThat(owners(map, 9)).isEmpty();

    // a removed owner can come back
    map.setOwnerPages(30, new int[] { 7 }, 1);
    assertThat(owners(map, 7)).containsExactly(30);
  }

  @Test
  void middleOfAPageListIsUnlinked() {
    final ContentOwnerMap map = new ContentOwnerMap();
    for (long owner = 1; owner <= 5; owner++)
      map.setOwnerPages(owner, new int[] { 4 }, 1);
    map.removeOwner(3); // middle
    map.removeOwner(5); // head (last added)
    map.removeOwner(1); // tail
    assertThat(owners(map, 4)).containsExactly(2, 4);
    map.setOwnerPages(6, new int[] { 4 }, 1);
    assertThat(owners(map, 4)).containsExactly(2, 4, 6);
  }

  @Test
  void survivesWriteAndRead() throws IOException {
    final ContentOwnerMap map = new ContentOwnerMap();
    map.setOwnerPages(10, new int[] { 7, 8 }, 2);
    map.setOwnerPages(20, new int[] { 7 }, 1);
    map.setOwnerPages(Long.MAX_VALUE / 3, new int[] { Integer.MAX_VALUE }, 1);
    map.setOwnerPages(40, new int[] { 1 }, 1);
    map.removeOwner(40);

    final Path file = dir.resolve("owners.1");
    map.writeTo(file);
    final ContentOwnerMap read = ContentOwnerMap.readFrom(file);
    assertThat(owners(read, 7)).containsExactly(10, 20);
    assertThat(owners(read, 8)).containsExactly(10);
    assertThat(owners(read, Integer.MAX_VALUE)).containsExactly(Long.MAX_VALUE / 3);
    assertThat(owners(read, 1)).isEmpty();

    // setOwnerPages on the read map still replaces what the file held
    read.setOwnerPages(10, new int[] { 9 }, 1);
    assertThat(owners(read, 8)).isEmpty();

    assertThatThrownBy(() -> map.writeTo(file)).isInstanceOf(FileAlreadyExistsException.class);

    final Path empty = dir.resolve("owners.2");
    new ContentOwnerMap().writeTo(empty);
    assertThat(owners(ContentOwnerMap.readFrom(empty), 7)).isEmpty();
  }

  @Test
  void corruptFileIsReported() throws IOException {
    final ContentOwnerMap map = new ContentOwnerMap();
    map.setOwnerPages(10, new int[] { 7, 8 }, 2);
    final Path file = dir.resolve("owners.1");
    map.writeTo(file);
    final long size = Files.size(file);

    for (long offset = 0; offset < size; offset++) {
      final Path copy = dir.resolve("flipped." + offset);
      Files.copy(file, copy);
      try (final FileChannel channel = FileChannel.open(copy, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
        final ByteBuffer one = ByteBuffer.allocate(1);
        channel.read(one, offset);
        one.put(0, (byte) (one.get(0) ^ 0x40)).position(0);
        channel.write(one, offset);
      }
      assertThatThrownBy(() -> ContentOwnerMap.readFrom(copy)).as("byte %d", offset).isInstanceOf(BlockCorruptionException.class);
    }

    final Path truncated = dir.resolve("truncated");
    Files.write(truncated, Arrays.copyOf(Files.readAllBytes(file), (int) size - 3));
    assertThatThrownBy(() -> ContentOwnerMap.readFrom(truncated)).isInstanceOf(BlockCorruptionException.class);
    final Path tiny = dir.resolve("tiny");
    Files.write(tiny, new byte[3]);
    assertThatThrownBy(() -> ContentOwnerMap.readFrom(tiny)).isInstanceOf(BlockCorruptionException.class);
  }

  @Test
  void copyIsIndependent() {
    final ContentOwnerMap map = new ContentOwnerMap();
    map.setOwnerPages(10, new int[] { 7 }, 1);
    map.setOwnerPages(11, new int[] { 7 }, 1);
    map.removeOwner(11);
    final ContentOwnerMap copy = map.copy();
    map.setOwnerPages(10, new int[] { 8 }, 1);
    copy.setOwnerPages(20, new int[] { 7 }, 1);

    assertThat(owners(copy, 7)).containsExactly(10, 20);
    assertThat(owners(copy, 8)).isEmpty();
    assertThat(owners(map, 7)).isEmpty();
    assertThat(owners(map, 8)).containsExactly(10);
  }

  @Test
  void matchesAReferenceModelUnderChurn() {
    final Random random = new Random(42);
    final ContentOwnerMap map = new ContentOwnerMap();
    final int owners = 3000;
    final int[][] model = new int[owners][];
    for (int step = 0; step < 60_000; step++) {
      final int owner = random.nextInt(owners);
      if (random.nextInt(4) == 0) {
        map.removeOwner(owner);
        model[owner] = null;
      } else {
        final int count = 1 + random.nextInt(4);
        final int[] pages = new int[count];
        for (int i = 0; i < count; i++)
          pages[i] = random.nextInt(500);
        map.setOwnerPages(owner, pages, count);
        model[owner] = Arrays.stream(pages).distinct().toArray();
      }
    }
    final ContentOwnerMap copy = map.copy();
    for (int page = 0; page < 500; page++) {
      final int p = page;
      final long[] expected = IntStream.range(0, owners)
          .filter(o -> model[o] != null && Arrays.stream(model[o]).anyMatch(x -> x == p)).asLongStream().toArray();
      assertThat(owners(map, page)).as("page %d", page).containsExactly(expected);
      assertThat(owners(copy, page)).as("copy page %d", page).containsExactly(expected);
      assertThat(map.ownerCountUpperBound(page)).isEqualTo(expected.length);
    }
  }

  @Test
  void scalesWithoutBoxing() {
    final ContentOwnerMap map = new ContentOwnerMap();
    final int owners = 1_000_000;
    final int[] page = new int[1];
    for (int owner = 0; owner < owners; owner++) {
      page[0] = owner / 8;
      map.setOwnerPages(owner, page, 1);
    }
    assertThat(owners(map, 12_345)).containsExactly(98_760, 98_761, 98_762, 98_763, 98_764, 98_765, 98_766, 98_767);
    // structural: the bytes of every array the map holds, never a Runtime heap delta
    assertThat(map.heapBytesForTesting()).isLessThan(64L * owners);
  }
}
