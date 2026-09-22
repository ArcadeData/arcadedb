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
package performance;

import com.arcadedb.database.Binary;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Random;
import java.util.function.IntSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Benchmark for issue #7840, kept next to {@code Issue7840WideKeyComparisonTest} so the equivalence guarantees and the
 * reason they were worth having live together.
 * <p>
 * The assertion is not a latency bound: it is a RATIO against the byte-at-a-time loop this change replaced, measured
 * in the same run and alternating which side goes first, so both pay the same JIT state, the same heap and the same
 * stalls, and nothing here reads a wall clock as an absolute. It exists to catch a future edit that quietly walks the
 * page one byte at a time again.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class WideKeyComparisonBenchmark {

  private static final int SAMPLES     = 4_096;
  private static final int WARMUP_REPS = 3_000;
  private static final int TIMED_REPS  = 500;
  /** Timed blocks per implementation. Even-numbered pairs run it first, odd-numbered ones run the baseline first. */
  private static final int PAIRS       = 4;

  /**
   * Every timed result lands here. Without an observable consumer the JIT is free to delete a comparison whose answer
   * nothing reads, which would time an empty loop and make the ratios meaningless - and the string comparison has no
   * other consumer in this class at all.
   */
  @SuppressWarnings("unused")
  private static volatile int sink;

  @Test
  void aKeyIsComparedAgainstAPageInBulk() {
    final byte[][] keys = new byte[SAMPLES][];
    final Binary[] pages = new Binary[SAMPLES];
    final String[] left = new String[SAMPLES];
    final String[] right = new String[SAMPLES];
    fillPaths(left, right);
    for (int i = 0; i < SAMPLES; i++) {
      keys[i] = left[i].getBytes(StandardCharsets.UTF_8);
      final Binary page = new Binary();
      page.putBytes(right[i].getBytes(StandardCharsets.UTF_8));
      page.flip();
      pages[i] = page;
    }

    final BinaryComparator comparator = new BinaryComparator();
    for (int i = 0; i < WARMUP_REPS; i++) {
      compareAgainstPages(comparator, keys, pages);
      compareByteByByte(keys, pages);
    }

    // Alternated for the same reason as the string benchmark above
    long bulk = 0;
    long byteAtATime = 0;
    for (int pair = 0; pair < PAIRS; pair++) {
      if (pair % 2 == 0) {
        bulk += time(() -> compareAgainstPages(comparator, keys, pages));
        byteAtATime += time(() -> compareByteByByte(keys, pages));
      } else {
        byteAtATime += time(() -> compareByteByByte(keys, pages));
        bulk += time(() -> compareAgainstPages(comparator, keys, pages));
      }
    }

    System.out.printf(Locale.ROOT, "compareBytes(byte[],Binary)=%.1f ms  byte-at-a-time=%.1f ms  speedup=%.2fx%n", bulk / 1e6,
        byteAtATime / 1e6, (double) byteAtATime / bulk);

    assertThat((double) bulk / byteAtATime).as("the page comparison must read the run in bulk, not one getByte() per byte")
        .isLessThan(0.75);
  }

  private static long time(final IntSupplier body) {
    int consumed = 0;
    final long start = System.nanoTime();
    for (int i = 0; i < TIMED_REPS; i++)
      consumed += body.getAsInt();
    final long elapsed = System.nanoTime() - start;

    // PUBLISHED AFTER THE CLOCK IS READ, SO THE STORE ITSELF IS NOT PART OF WHAT IS TIMED
    sink = consumed;
    return elapsed;
  }

  private static int compareAgainstPages(final BinaryComparator comparator, final byte[][] keys, final Binary[] pages) {
    int total = 0;
    for (int i = 0; i < keys.length; i++) {
      pages[i].position(0);
      total += comparator.compareBytes(keys[i], pages[i]);
    }
    return total;
  }

  /** The 26.9.1 shape of {@code compareBytes(byte[], Binary)}: one bounds-checked read per compared byte. */
  private static int compareByteByByte(final byte[][] keys, final Binary[] pages) {
    int total = 0;
    for (int k = 0; k < keys.length; k++) {
      final byte[] key = keys[k];
      final Binary page = pages[k];
      page.position(0);
      final long storedSize = page.getUnsignedNumber();
      final int minSize = (int) Math.min(key.length, storedSize);
      int result = 0;
      for (int i = 0; i < minSize; ++i) {
        final int b1 = key[i] & 0xFF;
        final int b2 = page.getByte() & 0xFF;
        if (b1 != b2) {
          result = b1 > b2 ? 1 : -1;
          break;
        }
      }
      total += result != 0 ? result : Long.compare(key.length, storedSize);
    }
    return total;
  }

  /** Project-relative paths: a long prefix shared with thousands of siblings and a difference only near the end. */
  private static void fillPaths(final String[] left, final String[] right) {
    final Random random = new Random(42);
    final String[] prefixes = { "src/main/java/com/acme/platform/service/internal/",
        "src/main/java/com/acme/platform/repository/impl/", "packages/frontend/components/dashboard/widgets/" };
    for (int i = 0; i < left.length; i++) {
      final String prefix = prefixes[random.nextInt(prefixes.length)];
      left[i] = prefix + "Handler" + random.nextInt(1000) + "ServiceImplementation.java";
      right[i] = prefix + "Handler" + random.nextInt(1000) + "ServiceImplementation.java";
    }
  }
}
