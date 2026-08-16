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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.schema.LocalSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the contract of the per-type tag dictionary that backs the fixed-width tag slot in the
 * mutable row format (issue #5519).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesTagDictionaryTest extends TestHelper {

  private TimeSeriesTagDictionary createDictionary(final String name) throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;
    final TimeSeriesTagDictionary dict = new TimeSeriesTagDictionary(db, name, db.getDatabasePath() + "/" + name);
    ((LocalSchema) db.getSchema()).registerFile(dict);
    dict.initHeaderPage();
    return dict;
  }

  @Test
  void internAssignsDenseIdsFromOne() throws Exception {
    final TimeSeriesTagDictionary dict = createDictionary("tags_dense");

    assertThat(dict.intern("host_a")).isEqualTo(1);
    assertThat(dict.intern("host_b")).isEqualTo(2);
    assertThat(dict.intern("host_c")).isEqualTo(3);

    assertThat(dict.getById(1)).isEqualTo("host_a");
    assertThat(dict.getById(2)).isEqualTo("host_b");
    assertThat(dict.getById(3)).isEqualTo("host_c");
    assertThat(dict.size()).isEqualTo(3);
  }

  @Test
  void internIsIdempotent() throws Exception {
    final TimeSeriesTagDictionary dict = createDictionary("tags_idempotent");

    final int first = dict.intern("eu-west-1");
    assertThat(dict.intern("eu-west-1")).isEqualTo(first);
    assertThat(dict.intern("eu-west-1")).isEqualTo(first);
    assertThat(dict.size()).isEqualTo(1);
  }

  /**
   * The mutable row format used to store a null tag as a zero-length string and hand it back as
   * {@code ""}. Reserving id 0 keeps that round-trip byte-for-byte identical, and costs no entry.
   */
  @Test
  void nullAndEmptyShareTheReservedId() throws Exception {
    final TimeSeriesTagDictionary dict = createDictionary("tags_reserved");

    assertThat(dict.intern(null)).isEqualTo(TimeSeriesTagDictionary.EMPTY_ID);
    assertThat(dict.intern("")).isEqualTo(TimeSeriesTagDictionary.EMPTY_ID);
    assertThat(TimeSeriesTagDictionary.EMPTY_ID).isEqualTo(0);
    assertThat(dict.getById(0)).isEqualTo("");
    // Neither consumed a stored entry
    assertThat(dict.size()).isZero();
  }

  @Test
  void unknownValueResolvesToNoId() throws Exception {
    final TimeSeriesTagDictionary dict = createDictionary("tags_unknown");
    dict.intern("known");

    assertThat(dict.getId("never-seen")).isEqualTo(TimeSeriesTagDictionary.NO_ID);
    assertThat(dict.getId("known")).isEqualTo(1);
  }

  @Test
  void internAllAssignsOneIdPerDistinctValue() throws Exception {
    final TimeSeriesTagDictionary dict = createDictionary("tags_batch");

    dict.internAll(List.of("a", "b", "a", "c", "b", "a"));

    assertThat(dict.size()).isEqualTo(3);
    assertThat(dict.getId("a")).isEqualTo(1);
    assertThat(dict.getId("b")).isEqualTo(2);
    assertThat(dict.getId("c")).isEqualTo(3);
  }

  /**
   * An entry never straddles a page boundary, so the reload walk can read a page's entries from its
   * own counter without a continuation marker.
   */
  @Test
  void entriesSpillOntoFurtherPages() throws Exception {
    final TimeSeriesTagDictionary dict = createDictionary("tags_spill");

    // 250-byte values: at 252 bytes of stored width each, a 64K page holds ~260 of them.
    final String filler = "x".repeat(250);
    final List<String> values = new ArrayList<>();
    for (int i = 0; i < 1000; i++)
      values.add(filler + "_" + i);
    dict.internAll(values);

    assertThat(dict.size()).isEqualTo(1000);
    assertThat(dict.getDataPageCount()).isGreaterThan(1);
    for (int i = 0; i < 1000; i++)
      assertThat(dict.getById(dict.getId(values.get(i)))).isEqualTo(values.get(i));
  }

  @Test
  void reloadRebuildsTheIdenticalMapping() throws Exception {
    final TimeSeriesTagDictionary dict = createDictionary("tags_reload");
    final List<String> values = new ArrayList<>();
    for (int i = 0; i < 500; i++)
      values.add("host_" + i);
    dict.internAll(values);

    final int[] idsBefore = new int[values.size()];
    for (int i = 0; i < values.size(); i++)
      idsBefore[i] = dict.getId(values.get(i));

    // Drop the in-RAM state and rebuild it from the pages alone
    database.transaction(() -> {
      try {
        dict.load();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });

    assertThat(dict.size()).isEqualTo(500);
    for (int i = 0; i < values.size(); i++) {
      assertThat(dict.getId(values.get(i))).isEqualTo(idsBefore[i]);
      assertThat(dict.getById(idsBefore[i])).isEqualTo(values.get(i));
    }
  }

  /**
   * An HA follower receives the dictionary pages through the Raft WAL but never runs the interning
   * that populates the in-RAM mapping, so an id can be readable from a data page while the map has
   * never seen it. Simulated here by a second instance over the same file that was never loaded: the
   * lookup must self-heal rather than hand back {@code null}.
   */
  @Test
  void anIdWrittenByAnotherInstanceIsResolvedByReloading() throws Exception {
    final TimeSeriesTagDictionary writer = createDictionary("tags_follower");
    writer.internAll(List.of("host_a", "host_b", "host_c"));

    final DatabaseInternal db = (DatabaseInternal) database;
    final TimeSeriesTagDictionary follower = new TimeSeriesTagDictionary(db, "tags_follower",
        db.getDatabasePath() + "/tags_follower", writer.getFileId());

    // Nothing loaded yet: the map is empty but the pages are on disk
    assertThat(follower.size()).isZero();

    assertThat(follower.getById(2)).isEqualTo("host_b");
    assertThat(follower.size()).isEqualTo(3);
    assertThat(follower.getById(1)).isEqualTo("host_a");
    assertThat(follower.getById(3)).isEqualTo("host_c");
  }

  /**
   * The follower keeps self-healing across successive waves of interning, not just the first one. A
   * leader interns for as long as it ingests, so a follower that reloaded once and then stopped would
   * read {@code null} for every tag value first seen after that reload.
   */
  @Test
  void aSecondWaveOfIdsIsAlsoResolvedByReloading() throws Exception {
    final TimeSeriesTagDictionary writer = createDictionary("tags_follower_waves");
    writer.internAll(List.of("host_a", "host_b"));

    final DatabaseInternal db = (DatabaseInternal) database;
    final TimeSeriesTagDictionary follower = new TimeSeriesTagDictionary(db, "tags_follower_waves",
        db.getDatabasePath() + "/tags_follower_waves", writer.getFileId());

    // Wave 1: the reload that the single-wave test already covers
    assertThat(follower.getById(2)).isEqualTo("host_b");
    assertThat(follower.size()).isEqualTo(2);

    // Wave 2: more values interned by the writer after the follower has already reloaded once
    writer.internAll(List.of("host_c", "host_d"));

    assertThat(follower.getById(4)).isEqualTo("host_d");
    assertThat(follower.getById(3)).isEqualTo("host_c");
    assertThat(follower.size()).isEqualTo(4);

    // ...and a third wave, to prove the trigger is the growth and not the reload count
    writer.internAll(List.of("host_e"));
    assertThat(follower.getById(5)).isEqualTo("host_e");
  }

  /**
   * #6258, item 3: the self-heal must not be keyed on the PHYSICAL size of the file.
   * <p>
   * This is what made {@link #aSecondWaveOfIdsIsAlsoResolvedByReloading} flake on CI while passing everywhere else,
   * and it was never a timing artefact of the test. A dictionary page is committed into the page cache immediately
   * and written to the file by the asynchronous flush thread whenever it gets round to it. An instance that opens in
   * that window sees a file of zero bytes, so its page counter says the dictionary is empty - and the reload that
   * exists precisely to resolve an id it never interned itself declined to run, handing back {@code null} for a value
   * sitting in the page cache. On a loaded machine the window is wide enough to hit; on an HA follower, where every
   * id arrives from the leader, it is the whole failure mode.
   * <p>
   * Made deterministic by holding the flush thread for the entire fixture, so the file is provably still empty when
   * the second instance opens - which the preconditions below ASSERT rather than assume, because a fixture that
   * quietly stopped producing an unflushed file would go on passing while testing nothing.
   */
  @Test
  void anIdIsResolvedWhileItsPageIsStillOnlyInTheCache() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    final long[] fileSize = new long[1];
    final int[] followerPages = new int[1];
    final String[] resolved = new String[2];
    final int[] followerSize = new int[1];
    final Throwable[] failure = new Throwable[1];

    db.getPageManager().suspendFlushAndExecute(db, () -> {
      try {
        // Created INSIDE the suspension, header page included: everything this dictionary ever wrote is in the page
        // cache and not one byte of it has reached the file.
        final TimeSeriesTagDictionary writer = new TimeSeriesTagDictionary(db, "tags_unflushed",
            db.getDatabasePath() + "/tags_unflushed");
        ((LocalSchema) db.getSchema()).registerFile(writer);
        writer.initHeaderPage();
        writer.internAll(List.of("host_a", "host_b", "host_c"));

        // The follower of the two tests above, opened while the file is still empty - which is what an HA follower
        // does whenever it re-opens between the leader's write and the flush that follows it.
        final TimeSeriesTagDictionary follower = new TimeSeriesTagDictionary(db, "tags_unflushed",
            db.getDatabasePath() + "/tags_unflushed", writer.getFileId());

        fileSize[0] = ((PaginatedComponentFile) db.getFileManager().getFile(writer.getFileId())).getSize();
        followerPages[0] = follower.getTotalPages();
        resolved[0] = follower.getById(2);
        resolved[1] = follower.getById(3);
        followerSize[0] = follower.size();
      } catch (final Throwable e) {
        // suspendFlushAndExecute swallows whatever the callback throws, so carry it out by hand.
        failure[0] = e;
      }
    });

    if (failure[0] != null)
      throw new AssertionError("the fixture failed: " + failure[0], failure[0]);

    assertThat(fileSize[0])
        .as("precondition: the dictionary's pages must still be unwritten, or this test proves nothing").isZero();
    assertThat(followerPages[0])
        .as("precondition: an instance opened on that file must therefore count no pages at all").isZero();

    assertThat(resolved[0]).as("an id whose page is committed but unflushed must still resolve").isEqualTo("host_b");
    assertThat(resolved[1]).isEqualTo("host_c");
    assertThat(followerSize[0]).as("and the reload it triggered must have loaded the whole dictionary").isEqualTo(3);
  }

  /**
   * An id beyond anything ever stored stays unresolvable, and does not send every subsequent lookup
   * back to the pages.
   */
  @Test
  void anIdBeyondTheDictionaryStaysUnresolved() throws Exception {
    final TimeSeriesTagDictionary dict = createDictionary("tags_bogus");
    dict.internAll(List.of("a", "b"));

    assertThat(dict.getById(99)).isNull();
    assertThat(dict.getById(99)).isNull();
    assertThat(dict.size()).isEqualTo(2);
    assertThat(dict.getById(1)).isEqualTo("a");
  }

  @Test
  void valueLongerThanTheRowLimitIsRejected() throws Exception {
    final TimeSeriesTagDictionary dict = createDictionary("tags_toolong");

    assertThatThrownBy(() -> dict.intern("y".repeat(TimeSeriesBucket.MAX_STRING_BYTES + 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exceeds max length");
  }

  @Test
  void exceedingTheConfiguredCapIsRejected() throws Exception {
    final TimeSeriesTagDictionary dict = createDictionary("tags_cap");
    dict.setMaxSize(4);

    dict.internAll(List.of("a", "b", "c", "d"));
    assertThat(dict.size()).isEqualTo(4);

    assertThatThrownBy(() -> dict.intern("e"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("tag dictionary");

    // The rejection left the committed state untouched
    assertThat(dict.size()).isEqualTo(4);
    assertThat(dict.getId("e")).isEqualTo(TimeSeriesTagDictionary.NO_ID);
  }
}
