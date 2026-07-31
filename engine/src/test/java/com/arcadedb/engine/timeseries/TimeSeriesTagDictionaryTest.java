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
