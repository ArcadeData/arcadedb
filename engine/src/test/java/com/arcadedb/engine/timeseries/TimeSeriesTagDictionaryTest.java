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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.WALFile;
import com.arcadedb.engine.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.codec.DictionaryCodec;
import com.arcadedb.engine.codec.GorillaXORCodec;
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore.BlockEntry;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

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
    final TimeSeriesTagDictionary follower = new TimeSeriesTagDictionary(db, "tags_follower", writer.getComponentFile());

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
        writer.getComponentFile());

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
   * The self-heal must not be gated on this instance's page counter (issue #6198). That counter is seeded
   * from the physical file size and afterwards only the schema-registered component gets it bumped at
   * commit, so a second instance built while the writer's committed pages are still in the flush queue
   * reads zero pages for a file that already holds a header and a data page. Gating on it made the
   * lookup answer {@code null} in exactly the state the self-heal exists for - which is why the sibling
   * test above passed alone and failed in a full-suite run, where the flush queue is behind.
   * <p>
   * The flush is suspended here to pin that state down instead of racing the flusher for it.
   */
  @Test
  void anIdIsResolvedWhileTheWriterPagesAreStillInTheFlushQueue() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    final AtomicReference<TimeSeriesTagDictionary> followerRef = new AtomicReference<>();
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    // suspendFlushAndExecute() swallows whatever the callback throws, so nothing is asserted in here:
    // the fixture is built, any failure is carried out, and the assertions run below.
    db.getPageManager().suspendFlushAndExecute(db, () -> {
      try {
        final TimeSeriesTagDictionary writer = createDictionary("tags_unflushed");
        writer.internAll(List.of("host_a", "host_b", "host_c"));
        followerRef.set(new TimeSeriesTagDictionary(db, "tags_unflushed", writer.getComponentFile()));
      } catch (final Throwable t) {
        failure.set(t);
      }
    });

    if (failure.get() != null)
      throw new IllegalStateException("Failed to build the unflushed-writer fixture", failure.get());

    final TimeSeriesTagDictionary follower = followerRef.get();
    assertThat(follower).isNotNull();
    // The fixture: not one page of this file had reached the disk when the follower was built
    assertThat(follower.getTotalPages()).isZero();
    assertThat(follower.size()).isZero();

    assertThat(follower.getById(2)).isEqualTo("host_b");
    assertThat(follower.size()).isEqualTo(3);
    assertThat(follower.getById(1)).isEqualTo("host_a");
    assertThat(follower.getById(3)).isEqualTo("host_c");
    // The reload also brought the page counter in step, so a later append cannot allocate over a page
    // that already holds entries
    assertThat(follower.getTotalPages()).isEqualTo(2);
  }

  /**
   * The highest-stakes reader of the same page counter is {@link TimeSeriesTagDictionary#openOrCreate}:
   * it decides between adopting a file that is already on disk and writing a fresh header over it, and
   * the wrong answer there resets the id space and orphans every id already stored in a data page. This
   * drives that branch in the state the counter gets wrong - a populated file, none of it flushed - and
   * asserts the ids survived and the sequence continues rather than restarting.
   */
  @Test
  void openOrCreateAdoptsAPopulatedFileWhoseComponentIsAbsentFromTheSchema() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final LocalSchema schema = (LocalSchema) db.getSchema();
    final List<ColumnDefinition> columns = List.of(
        new ColumnDefinition("time", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG));

    final AtomicReference<TimeSeriesTagDictionary> adoptedRef = new AtomicReference<>();
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    db.getPageManager().suspendFlushAndExecute(db, () -> {
      try {
        final TimeSeriesTagDictionary writer = createDictionary("Adopted" + TimeSeriesTagDictionary.NAME_SUFFIX);
        writer.internAll(List.of("host_a", "host_b", "host_c"));

        // Drop the component from the schema while leaving its file registered with the file manager:
        // that is the state the adopt branch exists for, and not one page of it has reached the disk.
        schema.removeFile(writer.getFileId());

        adoptedRef.set(TimeSeriesTagDictionary.openOrCreate(db, "Adopted", columns,
            TimeSeriesBucket.VERSION_DICTIONARY_TAGS));
      } catch (final Throwable t) {
        failure.set(t);
      }
    });

    if (failure.get() != null)
      throw new IllegalStateException("Failed to build the unflushed-adopt fixture", failure.get());

    final TimeSeriesTagDictionary adopted = adoptedRef.get();
    assertThat(adopted).isNotNull();

    // Adopted, not re-initialised: every id already assigned still resolves to the value it was given
    assertThat(adopted.size()).isEqualTo(3);
    assertThat(adopted.getById(1)).isEqualTo("host_a");
    assertThat(adopted.getById(2)).isEqualTo("host_b");
    assertThat(adopted.getById(3)).isEqualTo("host_c");
    assertThat(adopted.getId("host_b")).isEqualTo(2);

    // ...and the id space continues where it left off instead of restarting and colliding with them
    assertThat(adopted.intern("host_d")).isEqualTo(4);
    assertThat(adopted.getById(4)).isEqualTo("host_d");
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

  // #10868
  // Issue #10868: one intern on a dictionary whose pages grew from outside the instance - an HA follower receiving them through the
  // Raft WAL, then promoted - disabled its self-heal for good. {@code publish} set {@code entryCount} to the on-disk count while the
  // map held nothing between what it had loaded and the first id it appended, and {@code resolveMiss} refused to reload an id at or
  // below that count, so every id in the gap read back as null; a value only the missed pages held was interned a second time.
  // <p>
  // The writer is the dictionary registered with the schema; the "promoted" instance is a second view over the same file, the
  // stand-in the existing self-heal tests use for a node whose pages grew without its map.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  // #10868
  @Test
  void anInternOnAStaleInstanceKeepsEveryIdResolvableAndInternsNothingTwice() throws Exception {
    final TimeSeriesTagDictionary writer = createDictionary("tags_promoted");
    writer.internAll(values("early", 3));

    final TimeSeriesTagDictionary promoted = new TimeSeriesTagDictionary((DatabaseInternal) database, "tags_promoted",
        writer.getComponentFile());
    assertThat(promoted.getById(3)).isEqualTo("early2");
    assertThat(promoted.size()).isEqualTo(3);

    // pages grow from outside the promoted instance: ids 4..43
    writer.internAll(values("wal", 40));
    assertThat(writer.size()).isEqualTo(43);

    // the promoted node interns a value of its own before reading any of those ids
    final int own = promoted.intern("promoted-wrote-this");
    assertThat(own).as("the next id after everything the file holds").isEqualTo(44);

    // every id the pages hold is resolvable, the ones the intern jumped over included
    for (int id = 1; id <= 43; id++)
      assertThat(promoted.getById(id)).as("id %d", id).isEqualTo(writer.getById(id));
    assertThat(promoted.getById(44)).isEqualTo("promoted-wrote-this");

    // a value the missed pages hold keeps its id: interned again, it is not appended a second time
    final int wal16 = writer.getId("wal16");
    assertThat(promoted.getId("wal16")).isEqualTo(wal16);
    assertThat(promoted.intern("wal16")).isEqualTo(wal16);
    assertThat(promoted.size()).isEqualTo(44);
  }

  // #10868
  @Test
  void aStaleInstanceInterningAValueTheFileAlreadyHoldsGetsItsIdBack() throws Exception {
    final TimeSeriesTagDictionary writer = createDictionary("tags_duplicate");
    writer.internAll(values("a", 2));

    final TimeSeriesTagDictionary promoted = new TimeSeriesTagDictionary((DatabaseInternal) database, "tags_duplicate",
        writer.getComponentFile());
    assertThat(promoted.getById(2)).isEqualTo("a1");

    writer.internAll(values("b", 5));

    // the first thing the stale instance does is intern a batch that mixes values the file holds with a new one
    promoted.internAll(List.of("b3", "fresh", "b0"));
    assertThat(promoted.getId("b3")).isEqualTo(writer.getId("b3"));
    assertThat(promoted.getId("b0")).isEqualTo(writer.getId("b0"));
    assertThat(promoted.getId("fresh")).isEqualTo(8);
    assertThat(promoted.size()).as("one new entry, no duplicate").isEqualTo(8);

    // and the file agrees: a fresh view rebuilt from the pages holds each value once
    final TimeSeriesTagDictionary fresh = new TimeSeriesTagDictionary((DatabaseInternal) database, "tags_duplicate",
        writer.getComponentFile());
    fresh.load();
    assertThat(fresh.size()).isEqualTo(8);
    assertThat(fresh.checkIntegrity()).isEmpty();
  }

  private static List<String> values(final String prefix, final int count) {
    final List<String> values = new ArrayList<>(count);
    for (int i = 0; i < count; i++)
      values.add(prefix + i);
    return values;
  }

  @Nested
  class SharedTagValuesOnLoad {
    // #8793
    // #8793: reopening a sealed store decoded every block's declared tag values into fresh Strings, so the heap held
    // (blocks x distinct values) copies of the same few thousand strings: 188 MB for 1.3M samples in 5,648 blocks, against
    // 2-3 MB for the same samples in 24 blocks. After a reopen all blocks must share ONE instance of each value.
    //
    // @author Luca Garulli (l.garulli@arcadedata.com)

    private static final String BASE_DIR   = "target/databases/Issue8793SharedTagValuesOnLoadTest";
    private static final String STORE_PATH = BASE_DIR + "/store";
    private static final int    BLOCKS     = 300;

    private List<ColumnDefinition> columns;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(BASE_DIR));
      new File(BASE_DIR).mkdirs();
      columns = List.of(
          new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
          new ColumnDefinition("sensor_id", Type.STRING, ColumnDefinition.ColumnRole.TAG),
          new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(BASE_DIR));
    }

    // #8793
    @Test
    void blocksShareTheTagValuesTheyDeclareAfterAReopen() throws Exception {
      final String[] ids = { "entity-0001", "entity-0002", "entity-0003", "entity-0004" };
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, columns)) {
        for (int b = 0; b < BLOCKS; b++) {
          final long[] ts = { b * 10L, b * 10L + 1, b * 10L + 2, b * 10L + 3 };
          final double[] values = { 1.0, 2.0, 3.0, 4.0 };
          // every other block declares the set in another order, so sharing the ARRAY cannot be what makes this pass
          final String[] declared = b % 2 == 0 ? ids.clone() : new String[] { ids[3], ids[2], ids[1], ids[0] };
          store.appendBlock(4, ts[0], ts[3],
              new byte[][] { DeltaOfDeltaCodec.encode(ts), DictionaryCodec.encode(ids), GorillaXORCodec.encode(values) },
              new double[] { Double.NaN, Double.NaN, 1.0 }, new double[] { Double.NaN, Double.NaN, 4.0 },
              new double[] { Double.NaN, Double.NaN, 10.0 }, new long[] { 0, 0, 4 }, new String[][] { null, declared, null });
        }
      }

      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, columns)) {
        final List<BlockEntry> blocks = store.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks();
        assertThat(blocks).hasSize(BLOCKS);

        final Set<String> instances = Collections.newSetFromMap(new IdentityHashMap<>());
        for (final BlockEntry block : blocks) {
          assertThat(block.tagDistinctValues[1]).hasSize(4);
          for (final String v : block.tagDistinctValues[1])
            instances.add(v);
        }
        assertThat(instances).as("distinct String instances held for 4 distinct values across " + BLOCKS + " blocks").hasSize(4);
        assertThat(store.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(BLOCKS * 4);
      }
    }
  }

  // #8574
  // Issue #8574: a newest-first scan of the mutable bucket for one tag read and tag-checked every row of every page
  // that did not hold the tag, because the only thing that stopped the walk was a cut-off built from matching rows.
  // A shard holding no row of the tag, or a series that stopped reporting long ago, meant reading the whole bucket.
  // Each data page now carries a summary of the dictionary ids it holds, trusted only for the page version it was built
  // from, and {@code appendBatch} hands each shard a contiguous run of the batch instead of striping it row by row
  // (which put every row of a series on one shard for a time-major load). {@code COMPACT TIMESERIES TYPE} seals the
  // tail on demand.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final int  HOSTS   = 4;
  private static final long BASE_TS = 1_700_000_000_000L;
  private static final long STEP_MS = 1_000L;

  private TimeSeriesEngine create(final int shards) {
    database.command("sql",
        "CREATE TIMESERIES TYPE Point TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS " + shards);
    return ((LocalTimeSeriesType) database.getSchema().getType("Point")).getEngine();
  }

  /**
   * Time-major, as a line-protocol load arrives: every host at t, then every host at t + 1.
   */
  private static void appendTimeMajor(final TimeSeriesEngine engine, final int fromTick, final int toTick) throws IOException {
    final int rows = (toTick - fromTick) * HOSTS;
    final long[] timestamps = new long[rows];
    final Object[] hosts = new Object[rows];
    final Object[] values = new Object[rows];
    int i = 0;
    for (int t = fromTick; t < toTick; t++)
      for (int h = 0; h < HOSTS; h++) {
        timestamps[i] = BASE_TS + (long) t * STEP_MS;
        hosts[i] = "host_" + h;
        values[i] = (double) (t * HOSTS + h);
        i++;
      }
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
  }

  private static void appendOne(final TimeSeriesEngine engine, final String host, final int tick) throws IOException {
    engine.appendBatch(new long[] { BASE_TS + (long) tick * STEP_MS }, new Object[][] { { host }, { -1.0 * tick } });
  }

  private static List<List<Object>> deep(final List<Object[]> rows) {
    final List<List<Object>> out = new ArrayList<>(rows.size());
    for (final Object[] row : rows)
      out.add(Arrays.asList(row));
    return out;
  }

  /**
   * Reference: every row, filtered and sorted here, so it does not go through the tag filter under test.
   */
  private static List<Object[]> expected(final TimeSeriesEngine engine, final String host, final boolean descending,
      final int limit) throws IOException {
    final List<Object[]> all = new ArrayList<>();
    for (final Object[] row : engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null))
      if (host.equals(row[1]))
        all.add(row);
    all.sort((a, b) -> descending ? Long.compare((long) b[0], (long) a[0]) : Long.compare((long) a[0], (long) b[0]));
    return limit > 0 && all.size() > limit ? new ArrayList<>(all.subList(0, limit)) : all;
  }

  private int dataPages(final TimeSeriesEngine engine, final int shard) throws IOException {
    return engine.getShard(shard).getMutableBucket().getDataPageCount();
  }

  // #8574
  /**
   * A series that stopped reporting: its only rows are on the first page, and the newest-first walk used to read
   * every row of every newer page before reaching them.
   */
  @Test
  void pagesWithoutTheTagAreSkippedOnTheirSummary() throws Exception {
    final TimeSeriesEngine engine = create(1);
    appendOne(engine, "stale", 0);
    appendOne(engine, "stale", 1);
    appendTimeMajor(engine, 2, 20_000);

    final int pages = dataPages(engine, 0);
    assertThat(pages).isGreaterThan(10);

    final AggregationMetrics metrics = new AggregationMetrics();
    final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "stale"), 1,
        metrics);

    assertThat(deep(newest)).isEqualTo(deep(expected(engine, "stale", true, 1)));
    assertThat((long) newest.getFirst()[0]).isEqualTo(BASE_TS + STEP_MS);
    // Only the first page holds the tag: every other one is dropped on its summary.
    assertThat(metrics.getScannedPages()).isEqualTo(1);
    assertThat(metrics.getScannedPages() + metrics.getSkippedPages()).isEqualTo(pages);

    // A tag that is nowhere at all reads no page.
    final AggregationMetrics none = new AggregationMetrics();
    appendOne(engine, "elsewhere", 0);
    assertThat(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "missing"), 1, none)).isEmpty();
    assertThat(none.getScannedPages()).isZero();
  }

  // #8574
  @Test
  void ascendingAndUnlimitedScansAgreeWithAFullScan() throws Exception {
    final TimeSeriesEngine engine = create(2);
    appendTimeMajor(engine, 0, 5_000);
    appendOne(engine, "rare", 5_000);
    appendTimeMajor(engine, 5_001, 10_000);
    appendOne(engine, "rare", 10_000);

    for (final String host : new String[] { "rare", "host_0", "host_3" }) {
      for (final int limit : new int[] { 1, 2, 5, 0 }) {
        assertThat(deep(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, host), limit, null)))
            .as("descending %s limit %d", host, limit).isEqualTo(deep(expected(engine, host, true, limit)));
        assertThat(deep(engine.queryAscending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, host), limit, null)))
            .as("ascending %s limit %d", host, limit).isEqualTo(deep(expected(engine, host, false, limit)));
      }
      assertThat(deep(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, host))))
          .as("unlimited %s", host).isEqualTo(deep(expected(engine, host, false, 0)));
    }

    // An IN-style condition: the page is kept when it holds any of the candidates.
    final TagFilter either = TagFilter.in(0, Set.<Object>of("rare", "missing"));
    assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, either)).hasSize(2);
  }

  // #8574
  /**
   * The summary describes one image of the page. A row appended to a page already summarised, and a page cleared by
   * compaction and then reused, both change the page version and must not be answered from the old summary.
   */
  @Test
  void aSummaryIsNeverTrustedForAnotherImageOfThePage() throws Exception {
    final TimeSeriesEngine engine = create(1);
    appendTimeMajor(engine, 0, 100);

    // Summarise every page, the last one included, with no row of "late" anywhere.
    assertThat(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_1"), 0, null)).hasSize(100);
    assertThat(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "late"), 1, null)).isEmpty();

    // Into the same page the summary was built from.
    appendOne(engine, "late", 50);
    assertThat(deep(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "late"), 1, null)))
        .isEqualTo(deep(expected(engine, "late", true, 1)))
        .hasSize(1);

    // Clear the pages, then write them again from page 1: same page numbers, different rows.
    engine.compactAll();
    assertThat(engine.getShard(0).getMutableBucket().getSampleCount()).isZero();
    appendOne(engine, "reused", 200);
    appendOne(engine, "late", 201);
    assertThat(deep(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "reused"), 1, null)))
        .isEqualTo(deep(expected(engine, "reused", true, 1)))
        .hasSize(1);
    assertThat(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "late"), 0, null)).hasSize(2);
  }

  // #8574
  /**
   * A time-major batch used to be striped one row at a time, so with a host count that is a multiple of the shard
   * count every row of a host landed on one shard. Each shard now receives a contiguous run: a time slice of every
   * host, in the order the caller wrote it.
   */
  @Test
  void aTimeMajorBatchGivesEveryShardEverySeries() throws Exception {
    final int shards = 4;
    final TimeSeriesEngine engine = create(shards);
    appendTimeMajor(engine, 0, 1_000);

    long total = 0;
    for (int s = 0; s < shards; s++) {
      final TimeSeriesShard shard = engine.getShard(s);
      total += shard.getMutableBucket().getSampleCount();
      for (int h = 0; h < HOSTS; h++)
        assertThat(shard.scanRangeDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_" + h), 1, null))
            .as("shard %d host_%d", s, h).hasSize(1);
      // A contiguous run of a time-ordered batch stays time-ordered.
      final List<Object[]> rows = shard.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      for (int i = 1; i < rows.size(); i++)
        assertThat((long) rows.get(i)[0]).isGreaterThanOrEqualTo((long) rows.get(i - 1)[0]);
    }
    assertThat(total).isEqualTo(1_000L * HOSTS);

    // The newest reading of a host is answered from one page: every other shard is pruned by the running bound.
    final AggregationMetrics metrics = new AggregationMetrics();
    final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_2"), 1,
        metrics);
    assertThat(deep(newest)).isEqualTo(deep(expected(engine, "host_2", true, 1)));
    assertThat(metrics.getMaterializedRows()).isLessThanOrEqualTo(shards);
  }

  // #8574
  /**
   * Batches smaller than the shard count must still rotate across every shard: advancing the routing counter by the
   * shard count instead of by the runs handed out sent every two-row batch to the same two shards.
   */
  @Test
  void smallBatchesRotateAcrossEveryShard() throws Exception {
    final int shards = 4;
    final TimeSeriesEngine engine = create(shards);
    for (int b = 0; b < 8; b++)
      engine.appendBatch(new long[] { BASE_TS + b * 2L, BASE_TS + b * 2L + 1 }, new Object[][] { { "h", "h" }, { 1.0, 2.0 } });

    for (int s = 0; s < shards; s++)
      assertThat(engine.getShard(s).getMutableBucket().getSampleCount()).as("shard %d", s).isEqualTo(4L);
  }

  // #8574
  /**
   * A page rewritten by replay at its own version drops its cached summary, so the next scan rebuilds it.
   */
  @Test
  void invalidatingASummaryKeepsTheAnswerExact() throws Exception {
    final TimeSeriesEngine engine = create(1);
    appendTimeMajor(engine, 0, 3_000);
    final TimeSeriesBucket bucket = engine.getShard(0).getMutableBucket();
    final List<Object[]> before = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_1"), 3, null);

    for (int p = 0; p <= bucket.getDataPageCount() + 5; p++)
      bucket.invalidatePageTagSummary(p);

    assertThat(deep(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_1"), 3, null)))
        .isEqualTo(deep(before))
        .isEqualTo(deep(expected(engine, "host_1", true, 3)));
  }

  // #8574
  /**
   * Drives the real replay path: {@code TransactionManager.applyChanges} re-applies a data page at the version it
   * already has, as the torn-write repair does, with different tag ids in it. The page's summary was cached for that
   * version beforehand, and must not be reused to skip the page afterwards.
   */
  @Test
  void anEqualVersionReplayOfAPageInvalidatesItsSummary() throws Exception {
    final TimeSeriesEngine engine = create(1);
    // Put "replayed" into the dictionary, then seal it away so no mutable page holds it
    appendOne(engine, "replayed", 0);
    engine.compactAll();
    appendTimeMajor(engine, 1, 50);

    final TimeSeriesBucket bucket = engine.getShard(0).getMutableBucket();
    assertThat(bucket.getDataPageCount()).isEqualTo(1);
    // Caches page 1's summary, which holds no "replayed"
    assertThat(bucket.scanRangeDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "replayed"), 1, null)).isEmpty();

    // Page 1 at its current version, with the first row's host id replaced by the id of "replayed"
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageId pageId = new PageId(db, bucket.getFileId(), 1);
    final ImmutablePage page = db.getPageManager().getImmutablePage(pageId, bucket.getPageSize(), false, true);
    final byte[] content = new byte[page.getContentSize()];
    page.readByteArray(0, content);
    final int replayedId = bucket.getTagDictionary().getId("replayed");
    // Data page content: sample count (2) + min ts (8) + max ts (8), then rows of [ts(8) | host id(4) | value(8)]
    final int hostIdOffset = 18 + 8;
    content[hostIdOffset] = (byte) (replayedId >>> 24);
    content[hostIdOffset + 1] = (byte) (replayedId >>> 16);
    content[hostIdOffset + 2] = (byte) (replayedId >>> 8);
    content[hostIdOffset + 3] = (byte) replayedId;

    final WALFile.WALPage walPage = new WALFile.WALPage();
    walPage.fileId = bucket.getFileId();
    walPage.pageNumber = 1;
    walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
    walPage.changesTo = BasePage.PAGE_HEADER_SIZE + content.length - 1;
    walPage.currentContent = new Binary(content);
    walPage.currentPageVersion = (int) page.getVersion();
    walPage.currentPageSize = page.getContentSize() + BasePage.PAGE_HEADER_SIZE;
    final WALFile.WALTransaction tx = new WALFile.WALTransaction();
    tx.txId = -1;
    tx.pages = new WALFile.WALPage[] { walPage };
    db.getTransactionManager().applyChanges(tx, new HashMap<>(), false);

    assertThat(db.getPageManager().getImmutablePage(pageId, bucket.getPageSize(), false, true).getVersion())
        .isEqualTo(page.getVersion());
    assertThat(bucket.scanRangeDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "replayed"), 1, null))
        .as("the page now holds the tag: its pre-replay summary must not be reused").hasSize(1);
  }

  // #8574
  @Test
  void compactTimeSeriesTypeSealsTheTail() throws Exception {
    final TimeSeriesEngine engine = create(2);
    appendTimeMajor(engine, 0, 2_000);

    try (final ResultSet rs = database.command("sql", "COMPACT TIMESERIES TYPE Point")) {
      final Result r = rs.next();
      assertThat(r.<String>getProperty("operation")).isEqualTo("compact timeseries type");
      assertThat(r.<String>getProperty("typeName")).isEqualTo("Point");
      assertThat(r.<Long>getProperty("mutableSamplesBefore")).isEqualTo(2_000L * HOSTS);
      assertThat(r.<Long>getProperty("mutableSamples")).isZero();
    }
    for (int s = 0; s < 2; s++) {
      assertThat(engine.getShard(s).getMutableBucket().getSampleCount()).isZero();
      assertThat(engine.getShard(s).getSealedStore().getBlockCount()).isPositive();
    }

    try (final ResultSet rs = database.query("sql", "SELECT ts, value FROM Point WHERE host = 'host_3' ORDER BY ts DESC LIMIT 1")) {
      assertThat(rs.next().<Double>getProperty("value")).isEqualTo((double) (1_999 * HOSTS + 3));
    }
  }

  // #8574
  @Test
  void compactTimeSeriesTypeRejectsAnotherKindOfType() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    assertThatThrownBy(() -> database.command("sql", "COMPACT TIMESERIES TYPE Doc"))
        .hasMessageContaining("is not a TimeSeries type");
  }

  // #10874
  // Issue #10874: a BOOLEAN tag a sample left unset answered {@code null} in the mutable pages and {@code false} once sealed,
  // because {@link ColumnDefinition#boxString} parsed the empty text the sealed layer stores for "no value" as a boolean. The
  // sealed aggregation push-down, which tested the raw dictionary text, then disagreed with the row scan, which tested the
  // decoded value, about {@code WHERE flag = false}; and a rewrite of the block (a DELETE of an unrelated row) stored the
  // text {@code "false"} for the unset tag and changed the answer again.
  // <p>
  // Also issue #10878: which columns carry block statistics is ONE definition, {@link TimeSeriesCodec#isNumeric()}, which
  // every block writer and reader reaches through {@link ColumnDefinition#carriesBlockStatistics()}.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  // #10874
  @Test
  void anUnsetBooleanTagAnswersTheSameInEveryLayerAndEveryPathAgrees() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE M TIMESTAMP ts TAGS (flag BOOLEAN) FIELDS (v DOUBLE) SHARDS 1");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO M SET ts = 1000, flag = true, v = 1");
      database.command("sql", "INSERT INTO M SET ts = 2000, flag = false, v = 2");
      database.command("sql", "INSERT INTO M SET ts = 3000, v = 3");
    });

    assertTheAnswersOfThreeRows("mutable");

    engine().compactAll();
    assertThat(engine().getShard(0).getSealedStore().getBlockCount()).as("the samples are sealed").isPositive();
    assertTheAnswersOfThreeRows("sealed");

    // A rewrite of the block that holds the unset tag, triggered by a row neither query below ever matched
    assertThat(engine().deleteSamples(1000, 1000, null)).isEqualTo(1L);
    assertThat(flagOf(3000)).as("rebuilt: the unset tag").isNull();
    assertThat(timestamps("SELECT ts FROM M WHERE flag IS NULL")).as("rebuilt: IS NULL").containsExactly(3000L);
    assertThat(timestamps("SELECT ts FROM M WHERE flag = false")).as("rebuilt: the scan").containsExactly(2000L);
    assertThat(single("SELECT count(*) AS c FROM M WHERE flag = false", "c")).as("rebuilt: the push-down count").isEqualTo(1L);
    assertThat(single("SELECT sum(v) AS s FROM M WHERE flag = false", "s")).as("rebuilt: the push-down sum").isEqualTo(2.0);
  }

  // #10874
  /**
   * A block whose rows hold more than one value of the tag goes through the per-row test of the aggregation push-down; this one
   * holds a set value, an unset one and the other set value in every block, so both of its paths (ungrouped, grouped) read rows.
   */
  @Test
  void theAggregationPushDownTestsTheValueTheScanReads() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE N TIMESTAMP ts TAGS (flag BOOLEAN, host STRING) FIELDS (v DOUBLE) SHARDS 1");
    database.transaction(() -> {
      for (int i = 0; i < 30; i++) {
        final long ts = 1000L * (i + 1);
        if (i % 3 == 2)
          database.command("sql", "INSERT INTO N SET ts = ?, host = ?, v = ?", ts, "h" + (i % 2), (double) i);
        else
          database.command("sql", "INSERT INTO N SET ts = ?, flag = ?, host = ?, v = ?", ts, i % 3 == 0, "h" + (i % 2), (double) i);
      }
    });
    ((LocalTimeSeriesType) database.getSchema().getType("N")).getEngine().compactAll();

    final List<Long> falseRows = timestamps("SELECT ts FROM N WHERE flag = false");
    assertThat(falseRows).as("one in three rows set the tag false").hasSize(10);
    assertThat(single("SELECT count(*) AS c FROM N WHERE flag = false", "c")).as("ungrouped").isEqualTo(10L);

    long grouped = 0;
    try (final ResultSet rs = database.query("sql", "SELECT host, count(*) AS c FROM N WHERE flag = false GROUP BY host")) {
      while (rs.hasNext())
        grouped += ((Number) rs.next().getProperty("c")).longValue();
    }
    assertThat(grouped).as("grouped").isEqualTo(10L);
    assertThat(timestamps("SELECT ts FROM N WHERE flag IS NULL")).hasSize(10);
  }

  // #10874
  /**
   * A tag declared with a numeric codec is not a dictionary: the per-row test of the push-down decodes it with its own codec, and a
   * sample that left it unset is {@code null} there as in the scan (review of PR #10902).
   */
  @Test
  void aTagStoredWithANumericCodecIsFilteredByItsOwnCodec() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE B TIMESTAMP ts TAGS (flag BOOLEAN CODEC SIMPLE8B, host STRING) FIELDS (v DOUBLE) SHARDS 1");
    database.transaction(() -> {
      for (int i = 0; i < 30; i++) {
        final long ts = 1000L * (i + 1);
        if (i % 3 == 2)
          database.command("sql", "INSERT INTO B SET ts = ?, host = ?, v = ?", ts, "h" + (i % 2), (double) i);
        else
          database.command("sql", "INSERT INTO B SET ts = ?, flag = ?, host = ?, v = ?", ts, i % 3 == 0, "h" + (i % 2), (double) i);
      }
    });
    ((LocalTimeSeriesType) database.getSchema().getType("B")).getEngine().compactAll();

    assertThat(timestamps("SELECT ts FROM B WHERE flag = false")).hasSize(10);
    assertThat(single("SELECT count(*) AS c FROM B WHERE flag = false", "c")).as("ungrouped").isEqualTo(10L);
    assertThat(single("SELECT count(*) AS c FROM B WHERE flag = true", "c")).isEqualTo(10L);
    long grouped = 0;
    try (final ResultSet rs = database.query("sql", "SELECT host, count(*) AS c FROM B WHERE flag = true GROUP BY host")) {
      while (rs.hasNext())
        grouped += ((Number) rs.next().getProperty("c")).longValue();
    }
    assertThat(grouped).as("grouped").isEqualTo(10L);
    assertThat(timestamps("SELECT ts FROM B WHERE flag IS NULL")).hasSize(10);
  }

  // #10874
  /** The empty text is "no value" for every type but STRING, BOOLEAN included. */
  @Test
  void theEmptyTextReadsBackAsNoValueForEveryNonStringType() {
    for (final Type type : new Type[] { Type.BOOLEAN, Type.INTEGER, Type.LONG, Type.DOUBLE, Type.DATETIME })
      assertThat(new ColumnDefinition("c", type, ColumnDefinition.ColumnRole.TAG).boxString("")).as("%s", type).isNull();
    assertThat(new ColumnDefinition("c", Type.STRING, ColumnDefinition.ColumnRole.TAG).boxString("")).isEqualTo("");
    assertThat(new ColumnDefinition("c", Type.BOOLEAN, ColumnDefinition.ColumnRole.TAG).boxString("false")).isEqualTo(Boolean.FALSE);
    assertThat(new ColumnDefinition("c", Type.BOOLEAN, ColumnDefinition.ColumnRole.TAG).boxString("true")).isEqualTo(Boolean.TRUE);
  }

  // #10874
  /** Issue #10878: the block statistics, the push-down and the codec agree on which columns are numeric, for every codec. */
  @Test
  void blockStatisticsAreKeyedOffTheOneNumericCodecDefinition() {
    for (final TimeSeriesCodec codec : TimeSeriesCodec.values()) {
      final ColumnDefinition column = new ColumnDefinition("c", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD, codec);
      assertThat(column.carriesBlockStatistics()).as("%s", codec).isEqualTo(codec.isNumeric());
      assertThat(column.isNumericallyAggregatable()).as("%s", codec).isEqualTo(codec.isNumeric());
    }
    assertThat(TimeSeriesCodec.GORILLA_XOR.isNumeric()).isTrue();
    assertThat(TimeSeriesCodec.SIMPLE8B.isNumeric()).isTrue();
    assertThat(TimeSeriesCodec.DICTIONARY.isNumeric()).isFalse();
    assertThat(TimeSeriesCodec.DELTA_OF_DELTA.isNumeric()).isFalse();
  }

  private void assertTheAnswersOfThreeRows(final String layer) {
    assertThat(flagOf(3000)).as("%s: the unset tag", layer).isNull();
    assertThat(flagOf(2000)).as("%s: the tag set false", layer).isEqualTo(Boolean.FALSE);
    assertThat(timestamps("SELECT ts FROM M WHERE flag IS NULL")).as("%s: IS NULL", layer).containsExactly(3000L);
    assertThat(timestamps("SELECT ts FROM M WHERE flag = false")).as("%s: the scan", layer).containsExactly(2000L);
    assertThat(single("SELECT count(*) AS c FROM M WHERE flag = false", "c")).as("%s: the push-down count", layer).isEqualTo(1L);
    assertThat(single("SELECT sum(v) AS s FROM M WHERE flag = false", "s")).as("%s: the push-down sum", layer).isEqualTo(2.0);
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType("M")).getEngine();
  }

  private Object flagOf(final long ts) {
    try (final ResultSet rs = database.query("sql", "SELECT flag FROM M WHERE ts = ?", ts)) {
      assertThat(rs.hasNext()).as("the row at %s", ts).isTrue();
      return rs.next().getProperty("flag");
    }
  }

  private List<Long> timestamps(final String query) {
    final List<Long> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Object ts = rs.next().getProperty("ts");
        result.add(switch (ts) {
          case Date date -> date.getTime();
          case LocalDateTime local -> local.toInstant(ZoneOffset.UTC).toEpochMilli();
          default -> ((Number) ts).longValue();
        });
      }
    }
    result.sort(null);
    return result;
  }

  private Object single(final String query, final String property) {
    try (final ResultSet rs = database.query("sql", query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      final Result row = rs.next();
      final Object value = row.getProperty(property);
      return value instanceof Number n && !(value instanceof Double) ? (Object) n.longValue() : value;
    }
  }
}
