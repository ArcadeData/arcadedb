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

import com.arcadedb.database.RID;
import com.arcadedb.engine.block.BlockCorruptionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GlobalDictionaryTest {
  @TempDir
  Path dir;

  private static List<Object> valuesOf(final GlobalDictionary dictionary, final int[] ordinals) {
    final List<Object> values = new ArrayList<>(ordinals.length);
    for (final int ordinal : ordinals)
      values.add(dictionary.valueOf(ordinal));
    return values;
  }

  @Test
  void ordinalsAreDenseInInsertOrderAndNeverReused() {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    assertThat(dictionary.getOrAdd("b")).isEqualTo(0);
    assertThat(dictionary.getOrAdd("a")).isEqualTo(1);
    assertThat(dictionary.getOrAdd("c")).isEqualTo(2);
    assertThat(dictionary.getOrAdd("a")).isEqualTo(1);
    assertThat(dictionary.size()).isEqualTo(3);
    assertThat(dictionary.valueOf(1)).isEqualTo("a");
    assertThat(dictionary.valueOf(0)).isEqualTo("b");
    assertThat(dictionary.ordinalOf("c")).isEqualTo(2);
  }

  @Test
  void ordinalOfAbsentIsMinusOne() {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    assertThat(dictionary.ordinalOf("x")).isEqualTo(-1);
    dictionary.getOrAdd("a");
    assertThat(dictionary.ordinalOf("x")).isEqualTo(-1);
    assertThat(dictionary.ordinalOf(1L)).isEqualTo(-1);
    assertThatThrownBy(() -> dictionary.valueOf(1)).isInstanceOf(IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> dictionary.valueOf(-1)).isInstanceOf(IndexOutOfBoundsException.class);
  }

  @Test
  void typesDoNotCollide() {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    assertThat(dictionary.getOrAdd("1")).isEqualTo(0);
    assertThat(dictionary.getOrAdd(1L)).isEqualTo(1);
    assertThat(dictionary.getOrAdd(true)).isEqualTo(2);
    assertThat(dictionary.getOrAdd("true")).isEqualTo(3);
    assertThat(dictionary.getOrAdd(1L)).isEqualTo(1);
  }

  @Test
  void mixedValueTypesRoundTrip() throws IOException {
    final Path file = dir.resolve("dict-1.0.dic");
    final Object[] values = { "text", 12L, new BigDecimal("1.5"), 2.5d, true, new RID(3, 7), Double.NaN, "café" };
    final GlobalDictionary dictionary = GlobalDictionary.create(file);
    for (final Object value : values)
      dictionary.getOrAdd(value);
    final long length = dictionary.fsync().bytes();
    dictionary.close();

    final GlobalDictionary reopened = GlobalDictionary.open(file, length, values.length);
    assertThat(reopened.size()).isEqualTo(values.length);
    for (int i = 0; i < values.length; i++) {
      assertThat(reopened.valueOf(i)).isEqualTo(values[i]);
      assertThat(reopened.ordinalOf(values[i])).isEqualTo(i);
    }
    reopened.close();
  }

  @Test
  @Timeout(120)
  void readersSeeAConsistentPrefixWhileWriterAppends() throws Exception {
    final int total = 200_000;
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    final CountDownLatch go = new CountDownLatch(1);
    final CountDownLatch readersRunning = new CountDownLatch(4);
    final AtomicBoolean done = new AtomicBoolean();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final List<Thread> readers = new ArrayList<>();
    for (int r = 0; r < 4; r++) {
      final Random random = new Random(r);
      final Thread reader = new Thread(() -> {
        try {
          go.await();
          boolean first = true;
          while (!done.get()) {
            final int s = dictionary.size();
            if (first) {
              readersRunning.countDown();
              first = false;
            }
            if (s == 0)
              continue;
            assertThat(dictionary.valueOf(s - 1)).isNotNull();
            assertThat(dictionary.ordinalOf(dictionary.valueOf(s - 1))).isEqualTo(s - 1);
            for (int k = 0; k < 16; k++) {
              final int i = random.nextInt(s);
              assertThat(dictionary.ordinalOf(dictionary.valueOf(i))).isEqualTo(i);
            }
          }
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
        }
      });
      reader.start();
      readers.add(reader);
    }
    go.countDown();
    // the writer starts only once every reader is inside its loop, so the appends (and every table resize) race the readers
    readersRunning.await();
    for (int i = 0; i < total; i++)
      dictionary.getOrAdd("value-" + i);
    done.set(true);
    for (final Thread reader : readers)
      reader.join();
    assertThat(failure.get()).isNull();
    assertThat(dictionary.size()).isEqualTo(total);
    for (int i = 0; i < total; i += 997)
      assertThat(dictionary.ordinalOf("value-" + i)).isEqualTo(i);
  }

  @Test
  void crashTailBeyondManifestLengthIsTruncated() throws IOException {
    final Path file = dir.resolve("dict-1.0.dic");
    final GlobalDictionary dictionary = GlobalDictionary.create(file);
    for (int i = 0; i < 10; i++)
      dictionary.getOrAdd("v" + i);
    final long length = dictionary.fsync().bytes();
    for (int i = 10; i < 15; i++)
      dictionary.getOrAdd("v" + i);
    dictionary.close();
    assertThat(Files.size(file)).isGreaterThan(length);
    Files.write(file, new byte[] { 7, 0, 99 }, StandardOpenOption.APPEND);

    final GlobalDictionary reopened = GlobalDictionary.open(file, length, 10);
    assertThat(reopened.size()).isEqualTo(10);
    assertThat(Files.size(file)).isEqualTo(length);
    assertThat(reopened.ordinalOf("v10")).isEqualTo(-1);
    assertThat(reopened.getOrAdd("v10")).isEqualTo(10);
    final long length2 = reopened.fsync().bytes();
    reopened.close();

    final GlobalDictionary again = GlobalDictionary.open(file, length2, 11);
    assertThat(again.valueOf(10)).isEqualTo("v10");
    again.close();
  }

  @Test
  void tornRecordHeaderWithoutPayloadIsTruncated() throws IOException {
    final Path file = dir.resolve("dict-1.0.dic");
    final GlobalDictionary dictionary = GlobalDictionary.create(file);
    dictionary.getOrAdd("a");
    final long length = dictionary.fsync().bytes();
    dictionary.close();
    // the first write of an append (length + type byte) landed, the payload did not
    Files.write(file, new byte[] { 12, DictionaryValueCodec.STRING }, StandardOpenOption.APPEND);

    final GlobalDictionary reopened = GlobalDictionary.open(file, length, 1);
    assertThat(reopened.size()).isEqualTo(1);
    assertThat(Files.size(file)).isEqualTo(length);
    assertThat(reopened.getOrAdd("b")).isEqualTo(1);
    reopened.close();
  }

  @Test
  void damagedValidRegionIsCorruption() throws IOException {
    final Path file = dir.resolve("dict-1.0.dic");
    final GlobalDictionary dictionary = GlobalDictionary.create(file);
    for (int i = 0; i < 5; i++)
      dictionary.getOrAdd("value" + i);
    final long length = dictionary.fsync().bytes();
    dictionary.close();

    // the manifest claims more entries than the valid region holds
    assertThatThrownBy(() -> GlobalDictionary.open(file, length, 6)).isInstanceOf(BlockCorruptionException.class);
    // the manifest claims fewer entries than the valid region holds
    assertThatThrownBy(() -> GlobalDictionary.open(file, length, 4)).isInstanceOf(BlockCorruptionException.class);
    // the file is shorter than the manifest says
    assertThatThrownBy(() -> GlobalDictionary.open(file, length + 1, 5)).isInstanceOf(BlockCorruptionException.class);
    // a valid length that ends in the middle of a record
    assertThatThrownBy(() -> GlobalDictionary.open(file, length - 1, 4)).isInstanceOf(BlockCorruptionException.class);

    final byte[] bytes = Files.readAllBytes(file);
    final byte[] badMagic = bytes.clone();
    badMagic[0] ^= 0x55;
    final Path magicFile = dir.resolve("dict-2.0.dic");
    Files.write(magicFile, badMagic);
    assertThatThrownBy(() -> GlobalDictionary.open(magicFile, length, 5)).isInstanceOf(BlockCorruptionException.class);

    // a record length that runs past the valid region must not size an allocation
    final byte[] hugeLength = bytes.clone();
    hugeLength[8] = (byte) 0xFF;
    hugeLength[9] = (byte) 0xFF;
    hugeLength[10] = (byte) 0xFF;
    hugeLength[11] = 0x0F;
    final Path hugeFile = dir.resolve("dict-3.0.dic");
    Files.write(hugeFile, hugeLength);
    assertThatThrownBy(() -> GlobalDictionary.open(hugeFile, length, 5)).isInstanceOf(BlockCorruptionException.class);
  }

  @Test
  void rankViewOrdersByValue() {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    final int m = dictionary.getOrAdd("m");
    final int a = dictionary.getOrAdd("a");
    final int z = dictionary.getOrAdd("z");
    dictionary.rebuildRankView();
    assertThat(dictionary.rank(a)).isLessThan(dictionary.rank(m));
    assertThat(dictionary.rank(m)).isLessThan(dictionary.rank(z));
    assertThat(dictionary.compare(a, z)).isNegative();
    assertThat(dictionary.compare(z, m)).isPositive();
    assertThat(dictionary.compare(m, m)).isZero();
  }

  @Test
  void rankViewOrdersByClassThenValue() {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    final Object[] insertOrder = { new RID(1, 1), 5L, "b", true, 2.5d, new BigDecimal("0.5"), -3L, "a", false, 1d };
    for (final Object value : insertOrder)
      dictionary.getOrAdd(value);
    dictionary.rebuildRankView();
    final List<Object> sorted = valuesOf(dictionary, dictionary.sortedOrdinals());
    assertThat(sorted).containsExactly("a", "b", -3L, new BigDecimal("0.5"), 1d, 2.5d, 5L, false, true, new RID(1, 1));
  }

  @Test
  void sortedOrdinalsFollowStringCompareTo() {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    final String[] input = { "b", "B", "a", "é" };
    for (final String value : input)
      dictionary.getOrAdd(value);
    dictionary.rebuildRankView();
    final String[] expected = input.clone();
    Arrays.sort(expected, String::compareTo);
    assertThat(expected).containsExactly("B", "a", "b", "é");
    assertThat(valuesOf(dictionary, dictionary.sortedOrdinals())).containsExactly((Object[]) expected);
  }

  @Test
  void newOrdinalsRankBetweenViewNeighbours() {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    final int a = dictionary.getOrAdd("a");
    final int c = dictionary.getOrAdd("c");
    dictionary.rebuildRankView();
    final int b = dictionary.getOrAdd("b");
    assertThat(dictionary.rank(a)).isLessThan(dictionary.rank(b));
    assertThat(dictionary.rank(b)).isLessThan(dictionary.rank(c));
    final int bb = dictionary.getOrAdd("bb");
    assertThat(dictionary.rank(b)).isEqualTo(dictionary.rank(bb));
    assertThat(dictionary.compare(b, bb)).isNegative();
    assertThat(dictionary.compare(bb, b)).isPositive();
    assertThat(dictionary.compare(bb, c)).isNegative();
    // a value below and a value above the whole view
    final int zero = dictionary.getOrAdd("0");
    final int zz = dictionary.getOrAdd("zz");
    assertThat(dictionary.rank(zero)).isLessThan(dictionary.rank(a));
    assertThat(dictionary.rank(zz)).isGreaterThan(dictionary.rank(c));
  }

  @Test
  void rankWithoutAViewComparesByValue() {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    final int b = dictionary.getOrAdd("b");
    final int a = dictionary.getOrAdd("a");
    assertThat(dictionary.sortedOrdinals()).isEmpty();
    assertThat(dictionary.compare(a, b)).isNegative();
    assertThat(dictionary.compare(b, a)).isPositive();
  }

  @Test
  void rankViewRebuiltAfterGrowthCoversEverything() {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    final Random random = new Random(42);
    final List<String> input = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      final String value = Integer.toString(random.nextInt(1_000_000), 36);
      if (dictionary.ordinalOf(value) < 0)
        input.add(value);
      dictionary.getOrAdd(value);
      if (i == 3000)
        dictionary.rebuildRankView();
    }
    // 7000 ordinals newer than the view: their ranks still sort exactly
    final List<Integer> boxed = new ArrayList<>();
    for (int o = 0; o < dictionary.size(); o++)
      boxed.add(o);
    boxed.sort(dictionary::compare);
    final List<String> expected = new ArrayList<>(input);
    expected.sort(String::compareTo);
    final List<String> actual = new ArrayList<>();
    for (final int o : boxed)
      actual.add((String) dictionary.valueOf(o));
    assertThat(actual).isEqualTo(expected);

    dictionary.rebuildRankView();
    assertThat(valuesOf(dictionary, dictionary.sortedOrdinals())).isEqualTo(new ArrayList<Object>(expected));
    for (int i = 1; i < expected.size(); i++)
      assertThat(dictionary.rank(dictionary.ordinalOf(expected.get(i - 1)))).isLessThan(dictionary.rank(dictionary.ordinalOf(expected.get(i))));
  }

  @Test
  void unsupportedValueIsRefused() {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    assertThatThrownBy(() -> dictionary.getOrAdd(Map.of())).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> dictionary.getOrAdd(null)).isInstanceOf(IllegalArgumentException.class);
    assertThat(dictionary.size()).isZero();
  }

  @Test
  void stagedGenerationSurvivesReopen() throws IOException {
    final Path file = dir.resolve("dict-7.1.dic");
    final GlobalDictionary dictionary = GlobalDictionary.create(file);
    for (int i = 0; i < 500; i++)
      dictionary.getOrAdd("staged-" + i);
    final long length = dictionary.fsync().bytes();
    dictionary.close();

    final GlobalDictionary reopened = GlobalDictionary.open(file, length, 500);
    assertThat(reopened.size()).isEqualTo(500);
    for (int i = 0; i < 500; i++)
      assertThat(reopened.valueOf(i)).isEqualTo("staged-" + i);
    reopened.close();
    assertThatThrownBy(() -> GlobalDictionary.create(file)).isInstanceOf(IOException.class);
    // the refused create left the existing generation intact
    final GlobalDictionary intact = GlobalDictionary.open(file, length, 500);
    assertThat(intact.size()).isEqualTo(500);
    intact.close();
  }

  @Test
  void growthAcrossChunksAndTableResizesKeepsEverythingFindable() throws IOException {
    final Path file = dir.resolve("dict-1.0.dic");
    final GlobalDictionary dictionary = GlobalDictionary.create(file);
    final int total = 4096 * 3 + 17;
    for (int i = 0; i < total; i++)
      assertThat(dictionary.getOrAdd((long) i * 31)).isEqualTo(i);
    for (int i = 0; i < total; i++)
      assertThat(dictionary.ordinalOf((long) i * 31)).isEqualTo(i);
    assertThat(dictionary.heapBytes()).isGreaterThan(0);
    final long length = dictionary.fsync().bytes();
    dictionary.close();
    final GlobalDictionary reopened = GlobalDictionary.open(file, length, total);
    assertThat(reopened.size()).isEqualTo(total);
    assertThat(reopened.valueOf(total - 1)).isEqualTo((long) (total - 1) * 31);
    assertThat(reopened.ordinalOf((long) 4096 * 31)).isEqualTo(4096);
    reopened.close();
  }

  @Test
  @Timeout(120)
  void fsyncPairsAlwaysReopenCleanlyWhileTheWriterAppends() throws Exception {
    final Path file = dir.resolve("dict-1.0.dic");
    final GlobalDictionary dictionary = GlobalDictionary.create(file);
    final AtomicBoolean done = new AtomicBoolean();
    final CountDownLatch started = new CountDownLatch(1);
    final Thread writer = new Thread(() -> {
      started.countDown();
      for (int i = 0; i < 15_000; i++)
        dictionary.getOrAdd("value-" + i);
      done.set(true);
    });
    writer.start();
    started.await();
    final List<long[]> pairs = new ArrayList<>();
    while (!done.get()) {
      final GlobalDictionary.DurableState state = dictionary.fsync();
      pairs.add(new long[] { state.bytes(), state.entries() });
    }
    writer.join();
    final GlobalDictionary.DurableState last = dictionary.fsync();
    pairs.add(new long[] { last.bytes(), last.entries() });
    assertThat(pairs.size()).isGreaterThan(1);
    final int step = Math.max(1, pairs.size() / 40);
    for (int p = 0; p < pairs.size(); p += p + step < pairs.size() ? step : pairs.size() - p) {
      final long[] pair = pairs.get(p);
      final GlobalDictionary reopened = GlobalDictionary.openReadOnly(file, pair[0], (int) pair[1]);
      assertThat(reopened.size()).isEqualTo((int) pair[1]);
      reopened.close();
    }
    assertThat(pairs.get(pairs.size() - 1)[1]).isEqualTo(15_000);
    dictionary.close();
  }

  @Test
  void readOnlyOpenNeverTouchesTheFileAndRefusesAppends() throws IOException {
    final Path file = dir.resolve("dict-1.0.dic");
    final GlobalDictionary dictionary = GlobalDictionary.create(file);
    dictionary.getOrAdd("a");
    dictionary.getOrAdd("b");
    final long length = dictionary.fsync().bytes();
    dictionary.getOrAdd("c");
    dictionary.close();
    Files.write(file, new byte[] { 9, 9, 9 }, StandardOpenOption.APPEND);
    final long fileLength = Files.size(file);
    final byte[] before = Files.readAllBytes(file);

    final GlobalDictionary readOnly = GlobalDictionary.openReadOnly(file, length, 2);
    assertThat(readOnly.size()).isEqualTo(2);
    assertThat(readOnly.valueOf(1)).isEqualTo("b");
    assertThat(readOnly.ordinalOf("c")).isEqualTo(-1);
    assertThat(readOnly.getOrAdd("a")).isEqualTo(0);
    assertThatThrownBy(() -> readOnly.getOrAdd("new")).isInstanceOf(IllegalStateException.class);
    assertThat(readOnly.fsync()).isEqualTo(new GlobalDictionary.DurableState(length, 2));
    readOnly.close();
    assertThat(Files.size(file)).isEqualTo(fileLength);
    assertThat(Files.readAllBytes(file)).isEqualTo(before);
  }

  @Test
  void duplicateValueInValidRegionIsCorruption() throws IOException {
    final Path file = dir.resolve("dict-1.0.dic");
    final GlobalDictionary dictionary = GlobalDictionary.create(file);
    dictionary.getOrAdd("a");
    final long oneEntry = dictionary.fsync().bytes();
    dictionary.close();
    final byte[] bytes = Files.readAllBytes(file);
    final byte[] doubled = Arrays.copyOf(bytes, (int) (oneEntry + (oneEntry - 8)));
    System.arraycopy(bytes, 8, doubled, (int) oneEntry, (int) (oneEntry - 8));
    final Path dup = dir.resolve("dict-2.0.dic");
    Files.write(dup, doubled);
    assertThatThrownBy(() -> GlobalDictionary.open(dup, doubled.length, 2)).isInstanceOf(BlockCorruptionException.class);
  }

  @Test
  void recordWithTrailingBytesIsCorruption() throws IOException {
    final Path file = dir.resolve("dict-1.0.dic");
    final GlobalDictionary dictionary = GlobalDictionary.create(file);
    dictionary.getOrAdd(true);
    final long length = dictionary.fsync().bytes();
    dictionary.close();
    // a BOOLEAN payload is one byte; widen the record to two bytes so decode leaves one unread
    final byte[] bytes = Arrays.copyOf(Files.readAllBytes(file), (int) length + 1);
    bytes[8] = 2;
    final Path wide = dir.resolve("dict-2.0.dic");
    Files.write(wide, bytes);
    assertThatThrownBy(() -> GlobalDictionary.open(wide, length + 1, 1)).isInstanceOf(BlockCorruptionException.class);
  }

  @Test
  void heapBytesCountsThePartlyFilledLastChunk() {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    dictionary.getOrAdd(1L);
    // one allocated chunk of 4096 references, though only one slot is used
    assertThat(dictionary.heapBytes()).isGreaterThanOrEqualTo(4096L * 8);
  }

  @Test
  @Timeout(60)
  void appendPartialKillPointLeavesATruncatableTail() throws Exception {
    final Path file = dir.resolve("dict-1.0.dic");
    final GlobalDictionary dictionary = GlobalDictionary.create(file);
    dictionary.getOrAdd("a");
    dictionary.getOrAdd("b");
    dictionary.getOrAdd("c");
    final GlobalDictionary.DurableState durable = dictionary.fsync();

    final KillPoint killPoint = KillPoint.arm("dictionary.appendPartial");
    try {
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          dictionary.getOrAdd("x");
        } catch (final Throwable t) {
          failure.set(t);
        }
      });
      writer.start();
      assertThat(killPoint.awaitReached(10_000)).isTrue();
      // the record header is on disk, its payload is not: varint length 1 and the type byte
      assertThat(Files.size(file)).isEqualTo(durable.bytes() + 2);
      killPoint.releaseAsCrashed();
      writer.join(10_000);
      assertThat(failure.get()).isInstanceOf(ColumnarTestHooks.SimulatedCrash.class);
    } finally {
      killPoint.disarm();
    }
    assertThat(dictionary.size()).isEqualTo(3);
    assertThat(Files.size(file)).isEqualTo(durable.bytes() + 2);
    dictionary.close();

    final GlobalDictionary reopened = GlobalDictionary.open(file, durable.bytes(), durable.entries());
    assertThat(reopened.size()).isEqualTo(3);
    assertThat(Files.size(file)).isEqualTo(durable.bytes());
    assertThat(reopened.ordinalOf("x")).isEqualTo(-1);
    assertThat(reopened.getOrAdd("x")).isEqualTo(3);
    final GlobalDictionary.DurableState after = reopened.fsync();
    reopened.close();
    final GlobalDictionary again = GlobalDictionary.open(file, after.bytes(), after.entries());
    assertThat(again.valueOf(3)).isEqualTo("x");
    again.close();
  }
}
