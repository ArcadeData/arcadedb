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
import com.arcadedb.engine.timeseries.codec.Simple8bCodec;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9721: a LONG/INTEGER (and SHORT, BYTE, BOOLEAN, DATETIME) TimeSeries field written without a value read back as 0, so
 * {@code min}/{@code avg}/{@code count} counted a missing measurement as a measured 0. Such a column has no value of its own to
 * spend on absence, so the mutable row now carries a null bitmap and a sealed SIMPLE8B block a null bitmap of its own: a sample
 * without a value reads back as NULL and every aggregate skips it, exactly as on a document twin.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9721TimeSeriesIntegralNullTest extends TestHelper {
  private static final long T0 = 1_767_225_600_000L;

  @Test
  void theReportedScenario() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (bg LONG, f LONG)");
    database.command("sql", "INSERT INTO T SET ts = 1767225600000, host = 'a', bg = null, f = 0");
    database.command("sql", "INSERT INTO T SET ts = 1767225601000, host = 'a', bg = 5, f = 1");
    for (int pass = 0; pass < 2; pass++) {
      assertThat(column("SELECT bg FROM T ORDER BY ts", "bg")).containsExactly(null, 5L);
      assertThat(column("SELECT f FROM T ORDER BY ts", "f")).containsExactly(0L, 1L);
      assertThat(row("SELECT min(bg) AS mn, max(bg) AS mx, count(bg) AS c, avg(bg) AS a, sum(bg) AS s, count(*) AS n FROM T")).isEqualTo(
          "[a=5.0(Double), c=1(Long), mn=5(Long), mx=5(Long), n=2(Long), s=5(Long)]");
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
  }

  @Test
  void everyNullableTypeAnswersLikeADocumentTwin() {
    for (final int shards : new int[] { 1, 3 }) {
      database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (l LONG, i INTEGER, s SHORT, b BYTE, "
          + "o BOOLEAN, d DOUBLE) SHARDS " + shards);
      database.command("sql", "CREATE DOCUMENT TYPE D");
      database.command("sql", "CREATE PROPERTY D.l LONG");
      database.command("sql", "CREATE PROPERTY D.i INTEGER");
      database.command("sql", "CREATE PROPERTY D.s SHORT");
      database.command("sql", "CREATE PROPERTY D.b BYTE");
      database.command("sql", "CREATE PROPERTY D.o BOOLEAN");
      database.command("sql", "CREATE PROPERTY D.d DOUBLE");
      load(0, 90);

      final String[] queries = {
          "SELECT min(l) AS a, max(l) AS b, count(l) AS c, avg(l) AS d, sum(l) AS e FROM X",
          "SELECT min(i) AS a, max(i) AS b, count(i) AS c, avg(i) AS d, sum(i) AS e FROM X",
          "SELECT min(s) AS a, max(s) AS b, count(s) AS c, sum(s) AS e FROM X",
          "SELECT min(b) AS a, max(b) AS b, count(b) AS c FROM X",
          "SELECT count(o) AS c FROM X",
          "SELECT min(l) AS a, max(i) AS b, count(l) AS c, count(*) AS n FROM X WHERE host = 'h1'",
          "SELECT min(l) AS a, avg(i) AS b, count(d) AS c FROM X WHERE l >= 20" };
      for (int pass = 0; pass < 3; pass++) {
        for (final String sql : queries)
          assertThat(row(sql.replace("FROM X", "FROM T"))).as(sql + " pass " + pass).isEqualTo(row(sql.replace("FROM X", "FROM D")));
        assertThat(rows("SELECT l, i, s, b, o FROM T ORDER BY ts")).isEqualTo(rows("SELECT l, i, s, b, o FROM D ORDER BY ts"));
        if (pass == 0)
          database.command("sql", "COMPACT TIMESERIES TYPE T"); // sealed blocks
        else if (pass == 1)
          load(90, 120); // sealed blocks plus a mutable tail
      }
      database.command("sql", "DROP TYPE T");
      database.command("sql", "DROP TYPE D");
    }
  }

  @Test
  void groupedAndBucketedAggregatesSkipTheMissingSamples() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (l LONG, i INTEGER, s SHORT, b BYTE, "
        + "o BOOLEAN, d DOUBLE) SHARDS 2");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    load(0, 90);
    final String grouped = "SELECT host, min(l) AS a, max(i) AS b, count(l) AS c, avg(l) AS d, sum(i) AS e FROM X GROUP BY host ORDER BY host";
    final String bucketed = "SELECT ts.timeBucket('20s', ts) AS t, min(l) AS a, max(l) AS b, avg(i) AS d FROM X GROUP BY t ORDER BY t";
    for (int pass = 0; pass < 2; pass++) {
      assertThat(rows(grouped.replace("FROM X", "FROM T"))).isEqualTo(rows(grouped.replace("FROM X", "FROM D")));
      assertThat(rows(bucketed.replace("FROM X", "FROM T"))).as(bucketed).isEqualTo(rows(bucketed.replace("FROM X", "FROM D")));
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
  }

  @Test
  void aBatchColumnLeftUntouchedIsAMissingSampleOnBothLayers() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (l LONG, i INTEGER, d DOUBLE)");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("T")).getEngine();
    final TimeSeriesBatch batch = engine.newBatch(4);
    for (int r = 0; r < 4; r++) {
      final int row = batch.addRow(T0 + r * 1_000L);
      batch.setString(row, 0, "h");
      if (r % 2 == 0)
        batch.setLong(row, 1, 10L + r);
      batch.setValue(row, 2, r == 1 ? null : r);
      batch.setDouble(row, 3, r);
    }
    database.transaction(() -> {
      try {
        engine.appendBatch(batch);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
    for (int pass = 0; pass < 2; pass++) {
      assertThat(column("SELECT l FROM T ORDER BY ts", "l")).containsExactly(10L, null, 12L, null);
      assertThat(column("SELECT i FROM T ORDER BY ts", "i")).containsExactly(0, null, 2, 3);
      assertThat(row("SELECT min(l) AS a, count(l) AS c, min(i) AS b FROM T")).isEqualTo("[a=10(Long), b=0(Integer), c=2(Long)]");
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }

    // A refilled batch starts every row over: a value set on the previous fill does not leak into an untouched column
    batch.clear();
    final int row = batch.addRow(T0 + 10_000L);
    batch.setString(row, 0, "h");
    assertThat(batch.isNull(row, 1)).isTrue();
    assertThat(batch.isNull(row, 2)).isTrue();
  }

  @Test
  void theEngineApiAndTheIntegrityCheckSeeTheMissingSamples() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (l LONG)");
    database.command("sql", "INSERT INTO T SET ts = ?, host = 'a', l = null", T0);
    database.command("sql", "INSERT INTO T SET ts = ?, host = 'a', l = 7", T0 + 1_000L);
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("T")).getEngine();
    for (int pass = 0; pass < 2; pass++) {
      final List<Object> values = new ArrayList<>();
      final Iterator<Object[]> it = engine.iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      while (it.hasNext())
        values.add(it.next()[2]);
      assertThat(values).containsExactly(null, 7L);

      final MultiColumnAggregationResult result = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
          List.of(new MultiColumnAggregationRequest(2, AggregationType.MIN, "mn"), MultiColumnAggregationRequest.count("c")), 0, null);
      final long bucket = result.getBucketTimestamps().getFirst();
      assertThat(result.getValue(bucket, 0)).isEqualTo(7.0);
      assertThat(result.getCount(bucket, 0)).isEqualTo(1L);
      assertThat(result.getValue(bucket, 1)).isEqualTo(2.0);
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
    // the DEEP check decodes every block and reconciles it with the exact statistics its header declares
    try (final ResultSet check = database.command("sql", "CHECK DATABASE DEEP")) {
      final Result result = check.next();
      assertThat(result.<Collection<?>>getProperty("corruptedTimeSeries")).isEmpty();
      assertThat(result.<Collection<?>>getProperty("warnings")).isEmpty();
    }
  }

  @Test
  void theMissingSamplesSurviveEveryRouteToASealedBlock() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (l LONG, i INTEGER, s SHORT, b BYTE, "
        + "o BOOLEAN, d DOUBLE) SHARDS 1");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("T")).getEngine();
    // several small compactions, each sealing a block with missing samples, then a merge of those blocks into one, then a
    // downsampling pass: every route that rewrites a sealed block has to carry the null bitmap through
    for (int round = 0; round < 6; round++) {
      load(round * 15, round * 15 + 15);
      engine.compactAll();
    }
    assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isGreaterThan(1);
    engine.mergeSmallBlocks();
    assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(1);

    final String[] queries = { "SELECT min(l) AS a, max(l) AS b, count(l) AS c, avg(l) AS d, sum(l) AS e FROM X",
        "SELECT min(i) AS a, count(i) AS c, avg(i) AS d FROM X", "SELECT count(s) AS c, count(b) AS c2, count(o) AS c3 FROM X",
        "SELECT host, min(l) AS a, count(l) AS c FROM X GROUP BY host ORDER BY host" };
    for (final String sql : queries)
      assertThat(rows(sql.replace("FROM X", "FROM T"))).as(sql).isEqualTo(rows(sql.replace("FROM X", "FROM D")));
    assertThat(rows("SELECT l, i, s, b, o FROM T ORDER BY ts")).isEqualTo(rows("SELECT l, i, s, b, o FROM D ORDER BY ts"));
    try (final ResultSet check = database.command("sql", "CHECK DATABASE DEEP")) {
      assertThat(check.next().<Collection<?>>getProperty("corruptedTimeSeries")).isEmpty();
    }
  }

  @Test
  void aNullBitmapSurvivesTheSimple8bRoundTrip() throws Exception {
    final long[] values = new long[200];
    final long[] nulls = new long[Simple8bCodec.bitmapWords(values.length)];
    for (int i = 0; i < values.length; i++) {
      if (i % 7 == 3)
        nulls[i >>> 6] |= 1L << i;
      else
        values[i] = i % 5 == 0 ? Long.MAX_VALUE - i : i;
    }
    final byte[] encoded = Simple8bCodec.encode(values, nulls);
    assertThat(Simple8bCodec.decode(encoded)).containsExactly(values);
    assertThat(Simple8bCodec.nulls(encoded)).containsExactly(nulls);
    // without an absent sample the layout is the one every older block has
    assertThat(Simple8bCodec.encode(values, new long[nulls.length])).containsExactly(Simple8bCodec.encode(values));
    assertThat(Simple8bCodec.nulls(Simple8bCodec.encode(values))).isNull();
  }

  @Test
  void theNullBitmapRoundTripsAtItsWordBoundaries() throws Exception {
    for (final int count : new int[] { 1, 63, 64, 65, 128, 129 })
      for (final boolean wide : new boolean[] { false, true }) {
        final long[] values = new long[count];
        final long[] nulls = new long[Simple8bCodec.bitmapWords(count)];
        for (int i = 0; i < count; i++)
          if (i == count - 1 || i % 3 == 0)
            nulls[i >>> 6] |= 1L << i; // the last sample is absent, so the bit at the boundary is the one tested
          else
            values[i] = wide ? Long.MIN_VALUE + i : i; // a wide value forces the raw layout under the bitmap
        final byte[] encoded = Simple8bCodec.encode(values, nulls);
        assertThat(Simple8bCodec.decode(encoded)).as(count + (wide ? " wide" : "")).containsExactly(values);
        assertThat(Simple8bCodec.nulls(encoded)).as(count + (wide ? " wide" : "")).containsExactly(nulls);
      }
  }

  @Test
  void bitsPastTheLastSampleAreNotEncoded() throws Exception {
    final long[] values = { 1, 2, 3 };
    final byte[] tidy = Simple8bCodec.encode(values, new long[] { 0b010L });
    assertThat(Simple8bCodec.encode(values, new long[] { 0b010L | (1L << 40) })).containsExactly(tidy);
    assertThat(Simple8bCodec.nulls(tidy)).containsExactly(0b010L);
    // only a bit past the last sample: no sample is absent, so the block is the one without a bitmap
    final byte[] none = Simple8bCodec.encode(values, new long[] { 1L << 40 });
    assertThat(none).containsExactly(Simple8bCodec.encode(values));
    assertThat(Simple8bCodec.nulls(none)).isNull();
  }

  @Test
  void aCorruptNullableHeaderIsRefusedBeforeAnythingIsAllocated() {
    // the nullable marker followed by a count whose bitmap the buffer cannot hold: refused, not a 256 MB array or a wrapped size
    for (final int count : new int[] { Integer.MAX_VALUE, Integer.MAX_VALUE - 10, 1_000_000 }) {
      final byte[] corrupt = ByteBuffer.allocate(16).putInt(Integer.MIN_VALUE).putInt(count).array();
      assertThatThrownBy(() -> Simple8bCodec.nulls(corrupt)).as("count " + count).isInstanceOf(IOException.class);
      assertThatThrownBy(() -> Simple8bCodec.decode(corrupt)).as("count " + count).isInstanceOf(IOException.class);
    }
    assertThat(Simple8bCodec.bitmapWords(Integer.MAX_VALUE)).isEqualTo(33_554_432);
  }

  @Test
  void aTypeCreatedBeforeTheNullBitmapKeepsItsRowLayout() throws Exception {
    // Version 1 rows have no bitmap, so their stride - and the rows already on their pages - must stay what they were
    final List<ColumnDefinition> columns = List.of(new ColumnDefinition("ts", Type.DATETIME, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("l", Type.LONG, ColumnDefinition.ColumnRole.FIELD));
    final TimeSeriesEngine v1 = new TimeSeriesEngine((DatabaseInternal) database, "LegacyRows", columns, 1, 0,
        TimeSeriesBucket.VERSION_DICTIONARY_TAGS);
    final TimeSeriesEngine v2 = new TimeSeriesEngine((DatabaseInternal) database, "BitmapRows", columns, 1, 0,
        TimeSeriesBucket.VERSION_NULL_BITMAP);
    try {
      assertThat(v1.getShard(0).getMutableBucket().getRowSize()).isEqualTo(8 + 4 + 8);
      assertThat(v2.getShard(0).getMutableBucket().getRowSize()).isEqualTo(8 + 1 + 4 + 8);
      database.transaction(() -> {
        try {
          v1.appendSamples(new long[] { T0 }, new Object[] { "a" }, new Object[] { null });
          v2.appendSamples(new long[] { T0 }, new Object[] { "a" }, new Object[] { null });
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
      assertThat(v1.iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, null, null).next()[2]).isEqualTo(0L);
      assertThat(v2.iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, null, null).next()[2]).isNull();
    } finally {
      v1.close();
      v2.close();
    }
  }

  private void load(final int from, final int to) {
    database.transaction(() -> {
      for (int i = from; i < to; i++)
        for (final String type : new String[] { "T", "D" })
          database.command("sql", "INSERT INTO " + type + " SET ts = ?, host = ?, l = ?, i = ?, s = ?, b = ?, o = ?, d = ?", T0 + i * 1_000L,
              "h" + (i % 3), i % 4 == 0 ? null : (long) i * 3, i % 5 == 1 ? null : i - 40, i % 6 == 2 ? null : (short) (i * 2),
              i % 3 == 0 ? null : (byte) (i - 60), i % 7 == 0 ? null : i % 2 == 0, i % 8 == 0 ? null : (double) i);
    });
  }

  /** The only row of an ungrouped answer, each value spelled with its Java class. */
  private String row(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final String row = describe(rs.next());
      assertThat(rs.hasNext()).isFalse();
      return row;
    }
  }

  private List<String> rows(final String sql) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        out.add(describe(rs.next()));
    }
    return out;
  }

  private List<Object> column(final String sql, final String name) {
    final List<Object> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        out.add(rs.next().getProperty(name));
    }
    return out;
  }

  private static String describe(final Result r) {
    final List<String> out = new ArrayList<>();
    for (final String name : r.getPropertyNames().stream().sorted().toList()) {
      final Object value = r.getProperty(name);
      out.add(name + "=" + value + (value == null ? "" : "(" + value.getClass().getSimpleName() + ")"));
    }
    return out.toString();
  }
}
