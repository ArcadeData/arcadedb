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
package com.arcadedb.index.lsm;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Binary;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.BufferBloomFilter;
import com.arcadedb.engine.MurmurHash;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code .bfidx} component on its own, at sizes a real index reaches but a unit-friendly test would not: enough
 * keys per series that the filter spans MANY pages, which is where a key is routed to one block and everything depends
 * on the routing agreeing with itself between the write and the read.
 * <p>
 * The integration test ({@code Issue5517BloomFilterTest}) drives the same code through a compaction, but a 256 KB page
 * holds ~500k bits and therefore the whole filter of any test-sized series - so it never exercises a second block.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMTreeIndexBloomFilterTest extends TestHelper {

  /** Small enough that a few thousand keys need several blocks. */
  private static final int PAGE_SIZE = 4_096;

  /**
   * Stands in for {@code LSMTreeIndexCompacted.seriesFingerprint} of the series' last data page. There is no real
   * series here, so any stable value serves - what matters is that publish and probe agree on it, exactly as the
   * compaction and the lookup do.
   */
  private static final int FINGERPRINT = 0x5117;

  private static long hash(final String key) {
    final byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
    return MurmurHash.hash64(bytes, bytes.length, LSMTreeIndexBloomFilter.HASH_SEED);
  }

  private LSMTreeIndexBloomFilter newFilter(final String name) throws Exception {
    return LSMTreeIndexBloomFilter.createOrLoad((DatabaseInternal) database, name, PAGE_SIZE);
  }

  private static long[] hashes(final int count, final String prefix) {
    final long[] hashes = new long[count];
    for (int i = 0; i < count; i++)
      hashes[i] = hash(prefix + i);
    return hashes;
  }

  /**
   * The guarantee. Every key handed to the filter must probe back positive, whichever of the many blocks it was routed
   * to - a routing that disagreed with itself between add and probe would put the key in one block and look for it in
   * another, and the filter would hide it.
   */
  @Test
  void everyKeyOfEveryBlockProbesBack() throws Exception {
    final int keys = 40_000;
    final LSMTreeIndexBloomFilter filter = newFilter("many-blocks");

    filter.publish(7, 3, FINGERPRINT, hashes(keys, "key-"), keys, 0.01);
    assertThat(filter.getPublishedFilters()).isEqualTo(1);
    assertThat(filter.getTotalPages()).as("%d keys at 1%% must not fit one %d-byte page", keys, PAGE_SIZE)
        .isGreaterThan(3);

    for (int i = 0; i < keys; i++)
      assertThat(filter.mightContain(7, 3, FINGERPRINT, hash("key-" + i))).as("key %d", i).isTrue();
  }

  /**
   * The rate the filter is sized for has to be the rate it delivers once the keys are spread over blocks; a routing
   * that clumped keys into a few blocks would saturate them and quietly answer "maybe" to everything.
   */
  @Test
  void theMeasuredFalsePositiveRateMatchesTheTarget() throws Exception {
    final int keys = 40_000;
    final LSMTreeIndexBloomFilter filter = newFilter("rate");
    filter.publish(11, 5, FINGERPRINT, hashes(keys, "present-"), keys, 0.01);

    int falsePositives = 0;
    final int probed = 100_000;
    for (int i = 0; i < probed; i++)
      if (filter.mightContain(11, 5, FINGERPRINT, hash("absent-" + i)))
        ++falsePositives;

    assertThat((double) falsePositives / probed).as("measured false-positive rate over %d probes", probed)
        .isLessThan(0.02);
  }

  /**
   * The append-only invariant, which is what makes the file crash-safe and replicable: a publish appends its bits and
   * then its directory, so the live directory is always the LAST page. HA ships a compaction as the page range each
   * file grew by, and a directory rewritten in place would simply not travel.
   */
  @Test
  void theDirectoryIsAlwaysTheLastPage() throws Exception {
    final LSMTreeIndexBloomFilter filter = newFilter("append-only");

    filter.publish(4, 2, FINGERPRINT, hashes(5_000, "a-"), 5_000, 0.01);
    final int afterFirst = filter.getTotalPages();
    assertThat(readsAsDirectory(filter, afterFirst - 1)).isTrue();

    filter.publish(40, 2, FINGERPRINT, hashes(5_000, "b-"), 5_000, 0.01);
    assertThat(filter.getTotalPages()).as("a publish only ever appends").isGreaterThan(afterFirst);
    assertThat(readsAsDirectory(filter, filter.getTotalPages() - 1)).isTrue();

    // ... and the pages the first publish wrote are untouched by the second.
    filter.loadDirectory();
    for (int i = 0; i < 5_000; i++)
      assertThat(filter.mightContain(4, 2, FINGERPRINT, hash("a-" + i))).as("key %d of the first series", i).isTrue();
  }

  /**
   * A publish that got its bit pages to disk and died before its directory page leaves the file ending in pages no
   * directory names. The load must walk past them to the previous complete directory rather than read a bit page as
   * one - a random page mistaken for a directory would answer for pages it does not own, which is the one way this
   * file could hide a row.
   */
  @Test
  void aCrashBetweenTheBitsAndTheDirectoryFallsBackToThePreviousOne() throws Exception {
    final LSMTreeIndexBloomFilter filter = newFilter("torn-publish");
    filter.publish(4, 2, FINGERPRINT, hashes(3_000, "a-"), 3_000, 0.01);

    appendPage(filter, 0);          // a plausible bit page
    appendPage(filter, 0x424C4F4D); // and one that even starts with the directory magic

    filter.loadDirectory();

    assertThat(filter.getPublishedFilters()).as("the previous directory must still be the live one").isEqualTo(1);
    for (int i = 0; i < 3_000; i++)
      assertThat(filter.mightContain(4, 2, FINGERPRINT, hash("a-" + i))).as("key %d", i).isTrue();
  }

  private static boolean readsAsDirectory(final LSMTreeIndexBloomFilter filter, final int pageNumber) throws Exception {
    return filter.readDirectoryPage(pageNumber) != null;
  }

  /** Appends a page of {@code fill} ints, standing in for what a crash leaves behind. */
  private void appendPage(final LSMTreeIndexBloomFilter filter, final int fill) throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int pageNumber = filter.getTotalPages();
    final MutablePage page = new MutablePage(new PageId(db, filter.getFileId(), pageNumber), PAGE_SIZE);
    for (int i = 0; i < 128; i++)
      page.writeInt(i * 4, fill == 0 ? 0x5A5A5A5A : fill);

    db.getPageManager().updatePageVersion(page, true);
    filter.updatePageCount(pageNumber + 1);
    db.getPageManager().writePages(List.of(page), false);
  }

  /**
   * The probe used on the lookup path reads bits straight off the page to avoid allocating, and therefore repeats the
   * bit arithmetic of the instance form instead of sharing it. The two MUST answer identically: if they ever drifted,
   * keys written through one and read through the other would go missing.
   */
  @Test
  void theStaticProbeAgreesWithTheInstanceOne() throws Exception {
    final int keys = 20_000;
    final LSMTreeIndexBloomFilter filter = newFilter("static-vs-instance");
    filter.publish(6, 2, FINGERPRINT, hashes(keys, "p-"), keys, 0.01);

    final LSMTreeIndexBloomFilter.Entry entry = filter.readDirectoryPage(filter.getTotalPages() - 1).get(6);
    assertThat(entry).isNotNull();

    int agreed = 0;
    for (int i = 0; i < 40_000; i++) {
      // Half present, half absent, so both answers are exercised.
      final long hash = i < keys ? hash("p-" + i) : hash("q-" + i);
      final int block = blockOfSameWayTheComponentDoes(hash, entry.filterPages());
      final BasePage page = ((DatabaseInternal) database).getPageManager()
          .getImmutablePage(new PageId((DatabaseInternal) database, filter.getFileId(),
              entry.firstFilterPage() + block), PAGE_SIZE, false, false);

      final boolean viaPage = BufferBloomFilter.mightContainHash(page, entry.slotsPerBlock(), entry.probes(), hash);
      final boolean viaInstance = new BufferBloomFilter(new Binary(page.slice()), entry.slotsPerBlock(),
          LSMTreeIndexBloomFilter.HASH_SEED, entry.probes()).mightContainHash(hash);

      assertThat(viaPage).as("hash %d", hash).isEqualTo(viaInstance);
      if (viaPage)
        ++agreed;
    }

    assertThat(agreed).as("the probe must answer TRUE for the keys it holds, not for nothing").isGreaterThan(keys / 2);
  }

  /** Mirrors the component's block routing, so the test addresses the page the component would. */
  private static int blockOfSameWayTheComponentDoes(final long hash, final int filterPages) {
    if (filterPages == 1)
      return 0;
    long mixed = hash;
    mixed ^= mixed >>> 30;
    mixed *= 0xbf58476d1ce4e5b9L;
    mixed ^= mixed >>> 27;
    mixed *= 0x94d049bb133111ebL;
    mixed ^= mixed >>> 31;
    return (int) Long.remainderUnsigned(mixed, filterPages);
  }

  /** A series with no filter must be searched, not skipped. */
  @Test
  void anUnknownSeriesIsAlwaysSearched() throws Exception {
    final LSMTreeIndexBloomFilter filter = newFilter("unknown-series");
    filter.publish(3, 2, FINGERPRINT, hashes(100, "k-"), 100, 0.01);

    assertThat(filter.mightContain(99, 2, FINGERPRINT, hash("k-0"))).as("no filter for series 99").isTrue();
  }

  /**
   * A rolled-back compaction round leaves its series pages unreachable and the NEXT round writes a DIFFERENT series
   * over the same page numbers. A filter still claiming that root page would then filter the new series by the old
   * one's keys - a false negative on everything the new series added. Two defences, both asserted here: the page count
   * recorded with the filter must match the series the reader is looking at, and a rollback must drop the entry.
   */
  @Test
  void aSeriesRewrittenAtTheSameRootPageIsNotFilteredByTheOldOne() throws Exception {
    final LSMTreeIndexBloomFilter filter = newFilter("rollback");
    filter.publish(40, 4, FINGERPRINT, hashes(1_000, "old-"), 1_000, 0.01);

    // Same root page, different series. EITHER half of the identity is enough to disown the entry: the page count,
    // or the fingerprint of the last data page when the count happens to coincide.
    assertThat(filter.mightContain(40, 6, FINGERPRINT, hash("old-1")))
        .as("a series with a different page count must be searched").isTrue();
    assertThat(filter.mightContain(40, 4, FINGERPRINT + 1, hash("old-1")))
        .as("a series whose last data page differs must be searched, even at the same page count").isTrue();

    filter.rollbackFrom(40);
    assertThat(filter.getPublishedFilters()).isZero();
    assertThat(filter.mightContain(40, 4, FINGERPRINT, hash("old-1"))).as("a rolled-back filter must not answer any more").isTrue();
  }

  /** Only the series at or after the rolled-back page go: an earlier round's filters are still valid. */
  @Test
  void aRollbackKeepsTheFiltersOfEarlierRounds() throws Exception {
    final LSMTreeIndexBloomFilter filter = newFilter("partial-rollback");
    filter.publish(5, 2, FINGERPRINT, hashes(500, "first-"), 500, 0.01);
    filter.publish(50, 2, FINGERPRINT, hashes(500, "second-"), 500, 0.01);

    filter.rollbackFrom(50);

    assertThat(filter.getPublishedFilters()).isEqualTo(1);
    for (int i = 0; i < 500; i++)
      assertThat(filter.mightContain(5, 2, FINGERPRINT, hash("first-" + i))).as("key %d of the surviving series", i).isTrue();
  }

  /** What is written must be what is read back: the directory is the only thing a reopened database has. */
  @Test
  void theDirectoryReadsBackWhatWasWritten() throws Exception {
    final int keys = 20_000;
    final LSMTreeIndexBloomFilter filter = newFilter("reload");
    filter.publish(9, 4, FINGERPRINT, hashes(keys, "r-"), keys, 0.01);
    filter.publish(90, 8, FINGERPRINT, hashes(keys, "s-"), keys, 0.01);

    filter.loadDirectory();
    assertThat(filter.getPublishedFilters()).isEqualTo(2);

    for (int i = 0; i < keys; i++) {
      assertThat(filter.mightContain(9, 4, FINGERPRINT, hash("r-" + i))).as("series 9, key %d", i).isTrue();
      assertThat(filter.mightContain(90, 8, FINGERPRINT, hash("s-" + i))).as("series 90, key %d", i).isTrue();
    }
  }

  /** A single-key series is the degenerate case the sizing formulas have to survive. */
  @Test
  void aSeriesOfOneKeyStillWorks() throws Exception {
    final LSMTreeIndexBloomFilter filter = newFilter("tiny");
    filter.publish(2, 1, FINGERPRINT, hashes(1, "only-"), 1, 0.01);

    assertThat(filter.getPublishedFilters()).isEqualTo(1);
    assertThat(filter.mightContain(2, 1, FINGERPRINT, hash("only-0"))).isTrue();

    int falsePositives = 0;
    for (int i = 0; i < 10_000; i++)
      if (filter.mightContain(2, 1, FINGERPRINT, hash("other-" + i)))
        ++falsePositives;
    assertThat(falsePositives).as("a filter holding one key must reject nearly everything").isLessThan(200);
  }
}
