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
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5519: {@code TimeSeriesBucket.calculateRowSize()} reserved {@code 2 + MAX_STRING_BYTES} for
 * every STRING column, so the TSBS cpu-only schema - ten tags and three fields - paid a 2612-byte
 * stride to store ~110 bytes of payload. That fits 25 rows in a 64 KB page, a 4% fill, and since
 * {@code MutablePage.MAX_MODIFIED_RANGES} is 8, the scattered writes collapsed to the page hull and
 * the WAL shipped whole pages: 131 MB written to store 5.5 MB.
 * <p>
 * TAG STRING columns are now dictionary-encoded into a 4-byte id, which is what the sealed layer
 * already did for the same columns. The tests below pin the new stride and, more importantly, that
 * nothing observable changed: the same values come back, through the same readers, before and after
 * compaction and across a reopen.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5519TagStrideTest extends TestHelper {

  /**
   * The schema {@code cpu_influx.lp} actually ships: ten tags, plus fields.
   */
  private static final String[] TSBS_TAGS = { "hostname", "region", "datacenter", "rack", "os", "arch", "team", "service",
      "service_version", "service_environment" };

  private TimeSeriesEngine createTsbsType(final String typeName, final int shards) {
    final StringBuilder tags = new StringBuilder();
    for (final String tag : TSBS_TAGS) {
      if (!tags.isEmpty())
        tags.append(", ");
      tags.append(tag).append(" STRING");
    }
    database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts TAGS (" + tags
        + ") FIELDS (usage_user DOUBLE, usage_system DOUBLE, usage_idle DOUBLE) SHARDS " + shards);
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  /**
   * Appends {@code rows} samples whose tag values cycle over {@code hosts} distinct hostnames.
   */
  private void appendRows(final TimeSeriesEngine engine, final int rows, final int hosts) throws IOException {
    final long[] timestamps = new long[rows];
    final Object[][] columns = new Object[TSBS_TAGS.length + 3][rows];

    for (int i = 0; i < rows; i++) {
      timestamps[i] = 1_700_000_000_000L + i * 1_000L;
      columns[0][i] = "host_" + (i % hosts);
      columns[1][i] = "eu-west-1";
      columns[2][i] = "dc_" + (i % 4);
      columns[3][i] = "rack_" + (i % 8);
      columns[4][i] = "Ubuntu16";
      columns[5][i] = "x86";
      columns[6][i] = "SF";
      columns[7][i] = "7";
      columns[8][i] = "1";
      columns[9][i] = "production";
      columns[10][i] = (double) i;
      columns[11][i] = i * 2.0;
      columns[12][i] = i * 3.0;
    }
    engine.appendBatch(timestamps, columns);
  }

  /**
   * The defect, stated as a number. Before: {@code 8 + 10 * 258 + 3 * 8 = 2612} bytes and 25 rows per
   * 64 KB page. After: {@code 8 + 10 * 4 + 3 * 8 = 72}.
   */
  @Test
  void tenTagStrideCollapsesToFourBytesPerTag() {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 1);
    final TimeSeriesBucket bucket = engine.getShard(0).getMutableBucket();

    assertThat(bucket.getRowSize()).isEqualTo(8 + TSBS_TAGS.length * 4 + 3 * 8);
    assertThat(bucket.getRowSize()).isEqualTo(72);

    // 25 rows per page was the reported symptom; the page now carries an order of magnitude more.
    assertThat(bucket.getMaxSamplesPerPage()).isGreaterThan(900);
  }

  /**
   * The shape {@code cpu_influx.lp} actually ships - ten tags and ten fields, not the ten-and-three
   * the issue's own table used. Reported on #5519 by @tae898, whose measurement put it at a 2,668-byte
   * stride and 24 rows per page, the worst of the three arms they ran.
   */
  @Test
  void theShippingTsbsShapeOfTenTagsAndTenFields() {
    final StringBuilder tags = new StringBuilder();
    for (final String tag : TSBS_TAGS) {
      if (!tags.isEmpty())
        tags.append(", ");
      tags.append(tag).append(" STRING");
    }
    final StringBuilder fields = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      if (!fields.isEmpty())
        fields.append(", ");
      fields.append("f").append(i).append(" DOUBLE");
    }
    database.command("sql",
        "CREATE TIMESERIES TYPE CpuFull TIMESTAMP ts TAGS (" + tags + ") FIELDS (" + fields + ") SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("CpuFull")).getEngine();
    final TimeSeriesBucket bucket = engine.getShard(0).getMutableBucket();

    // Was 8 + 10 * 258 + 10 * 8 = 2668 bytes and 24 rows per 64KB page
    assertThat(bucket.getRowSize()).isEqualTo(8 + 10 * 4 + 10 * 8);
    assertThat(bucket.getRowSize()).isEqualTo(128);
    assertThat(bucket.getMaxSamplesPerPage()).isEqualTo(511);
  }

  @Test
  void tagValuesRoundTripThroughTheDictionary() throws Exception {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 1);
    appendRows(engine, 2_000, 7);

    final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(2_000);

    rows.sort((a, b) -> Long.compare((long) a[0], (long) b[0]));
    for (int i = 0; i < 2_000; i++) {
      final Object[] row = rows.get(i);
      assertThat(row[0]).isEqualTo(1_700_000_000_000L + i * 1_000L);
      assertThat(row[1]).isEqualTo("host_" + (i % 7));
      assertThat(row[2]).isEqualTo("eu-west-1");
      assertThat(row[3]).isEqualTo("dc_" + (i % 4));
      assertThat(row[4]).isEqualTo("rack_" + (i % 8));
      assertThat(row[10]).isEqualTo("production");
      assertThat(row[11]).isEqualTo((double) i);
      assertThat(row[13]).isEqualTo(i * 3.0);
    }
  }

  /**
   * Id 0 is reserved so a null tag still reads back as {@code ""}, which is what the inline encoding
   * did with its zero-length payload.
   */
  @Test
  void nullTagStillReadsBackAsEmptyString() throws Exception {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 1);

    final Object[][] columns = new Object[TSBS_TAGS.length + 3][1];
    for (int c = 0; c < TSBS_TAGS.length; c++)
      columns[c][0] = null;
    columns[10][0] = 1.0;
    columns[11][0] = 2.0;
    columns[12][0] = 3.0;
    engine.appendBatch(new long[] { 1_700_000_000_000L }, columns);

    final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(1);
    for (int c = 1; c <= TSBS_TAGS.length; c++)
      assertThat(rows.getFirst()[c]).isEqualTo("");
  }

  @Test
  void tagFilterMatchesThroughTheDictionary() throws Exception {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 1);
    appendRows(engine, 1_000, 5);

    final List<Object[]> matching = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_3"));
    assertThat(matching).hasSize(200);
    for (final Object[] row : matching)
      assertThat(row[1]).isEqualTo("host_3");

    // Newest-first path evaluates the filter straight off the page, now as an int compare.
    final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null,
        TagFilter.eq(0, "host_3"), 3, null);
    assertThat(newest).hasSize(3);
    for (final Object[] row : newest)
      assertThat(row[1]).isEqualTo("host_3");
    assertThat((long) newest.getFirst()[0]).isGreaterThan((long) newest.get(1)[0]);
  }

  /**
   * A filter value the dictionary has never seen cannot appear in any row, so the condition is
   * unsatisfiable and the scan short-circuits without decoding a page.
   */
  @Test
  void filterOnANeverStoredTagValueMatchesNothing() throws Exception {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 1);
    appendRows(engine, 500, 3);

    assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_does_not_exist"))).isEmpty();
    assertThat(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_does_not_exist"), 10,
        null)).isEmpty();
  }

  /**
   * A STRING FIELD is where high-cardinality text belongs, so it is not interned: it keeps the inline
   * encoding and the wide reservation that goes with it.
   */
  @Test
  void stringFieldsAreNotDictionaryEncoded() {
    database.command("sql",
        "CREATE TIMESERIES TYPE Logs TIMESTAMP ts TAGS (host STRING) FIELDS (message STRING, level INTEGER) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Logs")).getEngine();

    // 8 (ts) + 4 (dictionary-encoded host) + 258 (inline message) + 4 (level)
    assertThat(engine.getShard(0).getMutableBucket().getRowSize())
        .isEqualTo(8 + 4 + (2 + TimeSeriesBucket.MAX_STRING_BYTES) + 4);
  }

  @Test
  void stringFieldValuesStillRoundTrip() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Logs TIMESTAMP ts TAGS (host STRING) FIELDS (message STRING, level INTEGER) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Logs")).getEngine();

    engine.appendBatch(new long[] { 1_000L, 2_000L },
        new Object[][] { { "web01", "web02" }, { "started", "stopped" }, { 1, 2 } });

    final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    rows.sort((a, b) -> Long.compare((long) a[0], (long) b[0]));
    assertThat(rows.get(0)[1]).isEqualTo("web01");
    assertThat(rows.get(0)[2]).isEqualTo("started");
    assertThat(rows.get(1)[1]).isEqualTo("web02");
    assertThat(rows.get(1)[2]).isEqualTo("stopped");
  }

  /**
   * The dictionary is per type: every shard resolves a value to the same id, and one file backs them
   * all.
   */
  @Test
  void allShardsShareOneDictionary() throws Exception {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 4);
    appendRows(engine, 2_000, 11);

    final TimeSeriesTagDictionary dictionary = engine.getTagDictionary();
    assertThat(dictionary).isNotNull();
    for (int i = 0; i < 4; i++)
      assertThat(engine.getShard(i).getMutableBucket().getTagDictionary()).isSameAs(dictionary);

    // 11 hostnames + 1 region + 4 datacenters + 8 racks + 6 single-valued tags
    assertThat(dictionary.size()).isEqualTo(11 + 1 + 4 + 8 + 6);

    final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_9"));
    assertThat(rows).hasSize(2_000 / 11 + (2_000 % 11 > 9 ? 1 : 0));
  }

  @Test
  void tagValuesSurviveCompaction() throws Exception {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 1);
    appendRows(engine, 3_000, 6);

    engine.getShard(0).compact();
    assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isPositive();

    final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_4"));
    assertThat(rows).hasSize(500);
    for (final Object[] row : rows) {
      assertThat(row[1]).isEqualTo("host_4");
      assertThat(row[10]).isEqualTo("production");
    }
  }

  @Test
  void tagValuesSurviveAReopen() throws Exception {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 2);
    appendRows(engine, 1_500, 9);

    reopenDatabase();

    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("Cpu");
    final TimeSeriesEngine reopened = type.getEngine();
    assertThat(reopened.getTagDictionary()).isNotNull();
    assertThat(reopened.getTagDictionary().size()).isEqualTo(9 + 1 + 4 + 8 + 6);

    final List<Object[]> rows = reopened.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(1_500);

    rows.sort((a, b) -> Long.compare((long) a[0], (long) b[0]));
    for (int i = 0; i < 1_500; i++) {
      assertThat(rows.get(i)[1]).isEqualTo("host_" + (i % 9));
      assertThat(rows.get(i)[4]).isEqualTo("rack_" + (i % 8));
    }

    // And an append after the reopen keeps assigning ids from where the reload left off
    appendRows(reopened, 100, 20);
    assertThat(reopened.getTagDictionary().size()).isEqualTo(20 + 1 + 4 + 8 + 6);
    assertThat(reopened.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_15"))).isNotEmpty();
  }

  /**
   * The dictionary file is now discovered on open like any other component, so dropping the type has
   * to take it with it. A leftover would be re-adopted by a type recreated under the same name and
   * hand back tag values that belong to the dropped one.
   */
  @Test
  void droppingTheTypeRemovesTheDictionaryFile() throws Exception {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 1);
    appendRows(engine, 200, 5);
    assertThat(listTagDictionaryFiles()).hasSize(1);

    database.command("sql", "DROP TYPE Cpu");
    assertThat(listTagDictionaryFiles()).isEmpty();

    // Recreating under the same name starts from an empty id space, and survives a reopen
    final TimeSeriesEngine recreated = createTsbsType("Cpu", 1);
    assertThat(recreated.getTagDictionary().size()).isZero();
    appendRows(recreated, 100, 3);

    reopenDatabase();
    final TimeSeriesEngine reopened = ((LocalTimeSeriesType) database.getSchema().getType("Cpu")).getEngine();
    assertThat(reopened.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(100);
    assertThat(reopened.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_2"))).isNotEmpty();
  }

  private List<String> listTagDictionaryFiles() {
    final List<String> found = new ArrayList<>();
    final java.io.File[] files = new java.io.File(database.getDatabasePath()).listFiles();
    if (files != null)
      for (final java.io.File file : files)
        if (file.getName().endsWith("." + TimeSeriesTagDictionary.DICT_EXT))
          found.add(file.getName());
    return found;
  }

  /**
   * Backward compatibility: a bucket built without a dictionary is a format-version-0 bucket. It
   * keeps the inline tag encoding, the wide stride that comes with it, and reads back the same
   * values, which is what lets a database written before this change open unchanged.
   */
  @Test
  void aBucketWithoutADictionaryKeepsTheInlineFormat() throws Exception {
    final List<ColumnDefinition> columns = new ArrayList<>();
    columns.add(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP));
    columns.add(new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG));
    columns.add(new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    final DatabaseInternal db = (DatabaseInternal) database;
    database.begin();
    final TimeSeriesBucket bucket =
        new TimeSeriesBucket(db, "legacy_inline", db.getDatabasePath() + "/legacy_inline", columns);
    ((LocalSchema) db.getSchema()).registerFile(bucket);
    bucket.initHeaderPage();

    assertThat(bucket.getTagDictionary()).isNull();
    assertThat(bucket.getRowSize()).isEqualTo(8 + (2 + TimeSeriesBucket.MAX_STRING_BYTES) + 8);

    bucket.appendSamples(new long[] { 1_000L, 2_000L }, new Object[] { "web01", null },
        new Object[] { 1.5, 2.5 });
    database.commit();

    database.begin();
    final List<Object[]> rows = bucket.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0)[1]).isEqualTo("web01");
    assertThat(rows.get(0)[2]).isEqualTo(1.5);
    // A null tag reads back as "" in both formats
    assertThat(rows.get(1)[1]).isEqualTo("");
    database.commit();
  }

  /**
   * The compatibility claim end to end, through a real open rather than through a null dictionary: a
   * database whose stored schema says mutable format 0 keeps the inline layout when this build opens
   * it, and never adopts the dictionary. Simulated by relabelling the type on disk before the pages
   * hold anything, since a v1 type whose rows already carry 4-byte ids could not honestly be called v0.
   */
  @Test
  void aTypeStoredAsVersion0OpensWithTheInlineFormat() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Legacy TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

    final String databasePath = database.getDatabasePath();
    database.close();

    // Rewrite the stored schema as a pre-#5519 one: mutable format 0, and no dictionary component
    final File schemaFile = new File(databasePath + "/schema.json");
    final JSONObject schema = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
    final JSONObject legacy = schema.getJSONObject("types").getJSONObject("Legacy");
    assertThat(legacy.getInt("mutableFormatVersion", -1)).isEqualTo(TimeSeriesBucket.CURRENT_VERSION);
    legacy.put("mutableFormatVersion", 0);
    Files.writeString(schemaFile.toPath(), schema.toString(), StandardCharsets.UTF_8);

    final File[] dictFiles = new File(databasePath).listFiles((dir, fileName) -> fileName.startsWith("Legacy_tags."));
    if (dictFiles != null)
      for (final File dictFile : dictFiles)
        assertThat(dictFile.delete()).isTrue();

    database = factory.open();

    final LocalTimeSeriesType reopened = (LocalTimeSeriesType) database.getSchema().getType("Legacy");
    final TimeSeriesEngine engine = reopened.getEngine();

    // No dictionary, and the row is back to the reserved inline slot
    assertThat(engine.getTagDictionary()).isNull();
    assertThat(engine.getShard(0).getMutableBucket().getRowSize())
        .isEqualTo(8 + (2 + TimeSeriesBucket.MAX_STRING_BYTES) + 8);

    // ...and it still reads and writes tags correctly through the normal path
    database.begin();
    engine.appendSamples(new long[] { 1_000L, 2_000L }, new Object[] { "web01", "web02" }, new Object[] { 1.5, 2.5 });
    database.commit();

    database.begin();
    final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0)[1]).isEqualTo("web01");
    assertThat(rows.get(1)[1]).isEqualTo("web02");
    assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "web02"))).hasSize(1);
    database.commit();

    // Writing it back out must not relabel it as current, or the next open would misread these rows
    assertThat(reopened.toJSON().getInt("mutableFormatVersion", -1)).isZero();
  }
}
