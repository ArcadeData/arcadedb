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
import com.arcadedb.engine.timeseries.LineProtocolParser.Sample;
import com.arcadedb.engine.timeseries.TimeSeriesGateway.WriteReport;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Column constraints, property DDL, supertype rules and DML limits of time series types.
 */
class TimeSeriesColumnConstraintsTest extends TestHelper {
  // Issue #10238: {@code ALTER PROPERTY} accepted MANDATORY, NOTNULL, MIN, MAX and REGEXP on a column of a TIMESERIES type, and no
  // write path ever enforced them: SQL hands the row to the engine without the document validation, and the line protocol, gRPC,
  // Prometheus and the JSONL import never build a document. Every surface now holds the value the column stores to the constraints
  // of its column ({@link TimeSeriesColumnConstraints}); DEFAULT and HIDDEN, which no path of such a type honours, are refused; a
  // constraint is refused over stored samples that already violate it, as over records.
  // #10238
  @Test
  void theReportedScenarioIsRefusedAndNothingIsStored() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE M TIMESTAMP ts FIELDS (i INTEGER, s STRING) SHARDS 1");
    database.command("sql", "ALTER PROPERTY M.i MAX 10");
    database.command("sql", "ALTER PROPERTY M.s REGEXP '[a-z]+'");
    database.command("sql", "ALTER PROPERTY M.i MANDATORY true");

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO M SET ts = 2, i = 50, s = 'x'"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("'M.i'").hasMessageContaining("MAX 10");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO M SET ts = 3, i = '5', s = '1'"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("'M.s'").hasMessageContaining("REGEXP '[a-z]+'");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO M SET ts = 4, s = 'a'"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("'M.i'").hasMessageContaining("MANDATORY");
    assertThat(rows("M")).as("a refused statement stores no sample").isEmpty();

    database.command("sql", "INSERT INTO M SET ts = 5, i = 10, s = 'ok'");
    assertThat(rows("M")).hasSize(1);
  }

  // #10238
  @Test
  void theLineProtocolAndGrpcGatewayRefusesTheSameSamplesAndKeepsTheOthers() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE G TIMESTAMP ts TAGS (host STRING) FIELDS (i INTEGER, s STRING) SHARDS 1");
    database.command("sql", "ALTER PROPERTY G.i MIN 0");
    database.command("sql", "ALTER PROPERTY G.i MAX 10");
    database.command("sql", "ALTER PROPERTY G.s REGEXP '[a-z]+'");
    database.command("sql", "ALTER PROPERTY G.host NOTNULL true");

    final List<Sample> samples = new ArrayList<>();
    samples.add(new Sample("G", Map.of("host", "a"), Map.of("i", 5L, "s", "ok"), 1));
    samples.add(new Sample("G", Map.of("host", "a"), Map.of("i", 50L, "s", "ok"), 2)); // over MAX
    samples.add(new Sample("G", Map.of("host", "a"), Map.of("i", -1L, "s", "ok"), 3)); // under MIN
    samples.add(new Sample("G", Map.of("host", "a"), Map.of("i", 5L, "s", "NO"), 4)); // REGEXP
    samples.add(new Sample("G", Map.of(), Map.of("i", 5L, "s", "ok"), 5)); // no host
    samples.add(new Sample("G", Map.of("host", "b"), Map.of("i", 10L, "s", "z"), 6));

    final WriteReport report = TimeSeriesGateway.write((DatabaseInternal) database, samples);
    assertThat(report.written()).isEqualTo(2);
    assertThat(report.dropped()).isEqualTo(4);
    assertThat(report.unreadableValues()).anyMatch(r -> r.startsWith("G.i (field): ") && r.contains("MAX 10"));
    assertThat(report.unreadableValues()).anyMatch(r -> r.startsWith("G.i (field): ") && r.contains("MIN 0"));
    assertThat(report.unreadableValues()).anyMatch(r -> r.startsWith("G.s (field): ") && r.contains("REGEXP"));
    assertThat(report.unreadableValues()).anyMatch(r -> r.startsWith("G.host (tag): ") && r.contains("NOTNULL"));
    // the reason goes back to the client and to the log: it never repeats the value the client sent
    assertThat(report.unreadableValues()).noneMatch(r -> r.contains("50") || r.contains("'NO'"));

    assertThat(rows("G")).extracting(row -> ((Number) row[0]).longValue()).containsExactlyInAnyOrder(1L, 6L);
  }

  // #10238
  /**
   * The Prometheus remote write converts the labels of a series once and broadcasts them (shared tags): a label a constraint
   * refuses refuses the whole series, and a sample value refuses that sample alone.
   */
  @Test
  void aSharedTagThatViolatesAConstraintRefusesTheWholeBatch() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE P TIMESTAMP ts TAGS (job STRING) FIELDS (value DOUBLE) SHARDS 1");
    database.command("sql", "ALTER PROPERTY P.job REGEXP 'api|web'");
    database.command("sql", "ALTER PROPERTY P.value MAX 100");
    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("P");

    final Set<String> unreadable = new LinkedHashSet<>();
    final TimeSeriesStorableRows refused = TimeSeriesStorableRows.convert(database, "P", type, new long[] { 1, 2 }, 2, true,
        (row, valueColumn, col) -> col.getRole() == ColumnDefinition.ColumnRole.TAG ? "db" : 1.0d, unreadable);
    assertThat(refused.sharedValueRefused()).isTrue();
    assertThat(refused.size()).isZero();
    assertThat(unreadable).singleElement().asString().startsWith("P.job (tag): ").contains("REGEXP");

    unreadable.clear();
    final TimeSeriesStorableRows kept = TimeSeriesStorableRows.convert(database, "P", type, new long[] { 1, 2, 3 }, 3, true,
        (row, valueColumn, col) -> col.getRole() == ColumnDefinition.ColumnRole.TAG ? "api" : (row == 1 ? 1000.0d : 1.0d),
        unreadable);
    assertThat(kept.sharedValueRefused()).isFalse();
    assertThat(kept.size()).isEqualTo(2);
    assertThat(kept.refused()).isEqualTo(1);
    assertThat(kept.timestamps()).containsExactly(1L, 3L);
    assertThat(unreadable).singleElement().asString().startsWith("P.value (field): ").contains("MAX 100");
  }

  // #10238
  @Test
  void aStringIsBoundedByItsLengthAndANumberByItsValue() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE L TIMESTAMP ts FIELDS (s STRING, d DOUBLE, f FLOAT, l LONG) SHARDS 1");
    database.command("sql", "ALTER PROPERTY L.s MIN 2");
    database.command("sql", "ALTER PROPERTY L.s MAX 4");
    database.command("sql", "ALTER PROPERTY L.d MIN -1.5");
    database.command("sql", "ALTER PROPERTY L.f MAX 0.5");
    database.command("sql", "ALTER PROPERTY L.l MAX 9007199254740993");

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO L SET ts = 1, s = 'a'")).isInstanceOf(ValidationException.class)
        .hasMessageContaining("fewer characters than the MIN 2");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO L SET ts = 1, s = 'abcde'")).isInstanceOf(ValidationException.class)
        .hasMessageContaining("more characters than the MAX 4");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO L SET ts = 1, s = 'abc', d = -1.6")).isInstanceOf(ValidationException.class)
        .hasMessageContaining("less than the MIN -1.5");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO L SET ts = 1, s = 'abc', f = 0.6")).isInstanceOf(ValidationException.class)
        .hasMessageContaining("greater than the MAX 0.5");
    // compared exactly as a long, not through a double, where 2^53 + 2 and 2^53 + 1 are the same number
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO L SET ts = 1, s = 'abc', l = 9007199254740994"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("greater than the MAX 9007199254740993");

    database.command("sql", "INSERT INTO L SET ts = 2, s = 'abcd', d = -1.5, f = 0.5, l = 9007199254740993");
    // a numeric column with no value is not bounded, only a MANDATORY/NOTNULL one requires one; a STRING column stores the empty
    // text for no value, and its MIN judges that text (issue #10336)
    database.command("sql", "INSERT INTO L SET ts = 3, s = 'ab'");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO L SET ts = 4")).isInstanceOf(ValidationException.class)
        .hasMessageContaining("empty text").hasMessageContaining("fewer characters than the MIN 2");
    assertThat(rows("L")).hasSize(2);
  }

  // #10238
  /**
   * A temporal bound is read as SQL reads a literal and compared to the stored count in the column's own unit, so a nanosecond
   * column is bounded to the nanosecond; and a gateway count (in the column's unit) is held to the same bound.
   */
  @Test
  void aTemporalColumnIsBoundedInItsOwnPrecision() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE N TIMESTAMP ts FIELDS (dn DATETIME_NANOS, d DATE) SHARDS 1");
    database.command("sql", "ALTER PROPERTY N.dn MAX '2023-11-14 22:13:20'");
    database.command("sql", "ALTER PROPERTY N.d MIN '2020-01-01'");

    // 1700000000000 ms is 2023-11-14 22:13:20 UTC: one nanosecond past it is over the bound
    final long boundNanos = 1_700_000_000_000L * 1_000_000L;
    final Set<String> unreadable = new LinkedHashSet<>();
    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("N");
    final TimeSeriesStorableRows rows = TimeSeriesStorableRows.convert(database, "N", type, new long[] { 1, 2 }, 2, false,
        (row, valueColumn, col) -> col.getName().equals("dn") ? (row == 0 ? boundNanos : boundNanos + 1) : null, unreadable);
    assertThat(rows.size()).isEqualTo(1);
    assertThat(rows.timestamps()).containsExactly(1L);
    assertThat(unreadable).singleElement().asString().startsWith("N.dn (field): ").contains("after the MAX");

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO N SET ts = 1, d = '2019-12-31'"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("before the MIN '2020-01-01'");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO N SET ts = 1, dn = '2023-11-14 22:13:21'"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("after the MAX");
    database.command("sql", "INSERT INTO N SET ts = 1, dn = '2023-11-14 22:13:20', d = '2020-01-01'");
    assertThat(rows("N")).hasSize(1);
  }

  // #10238
  /**
   * A REGEXP is matched against the text SQL returns for the value: a number as its declared type writes it, a temporal as the
   * date it stands for, never as the count the column stores.
   */
  @Test
  void aPatternIsMatchedAgainstTheTextSqlReturns() {
    database.command("sql", "CREATE TIMESERIES TYPE R TIMESTAMP ts TAGS (code INTEGER) FIELDS (d DATE) SHARDS 1");
    database.command("sql", "ALTER PROPERTY R.code REGEXP '[0-9]{3}'");
    database.command("sql", "ALTER PROPERTY R.d REGEXP '2023-.*'");

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO R SET ts = 1, code = 1234")).isInstanceOf(ValidationException.class)
        .hasMessageContaining("'R.code'");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO R SET ts = 1, code = 123, d = '2024-01-01'"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("'R.d'");
    database.command("sql", "INSERT INTO R SET ts = 1, code = 123, d = '2023-05-01'");
    assertThat(database.query("sql", "SELECT count(*) AS c FROM R").next().<Long>getProperty("c")).isEqualTo(1L);
  }

  // #10238
  /**
   * MANDATORY and NOTNULL both require a value: a sample cannot carry a column set to null, the storage keeps no difference between
   * a null and an absent value, so a null accepted by MANDATORY would store the very row MANDATORY refuses.
   */
  @Test
  void mandatoryAndNotNullBothRequireAValue() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Q TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    database.command("sql", "ALTER PROPERTY Q.host MANDATORY true");
    database.command("sql", "ALTER PROPERTY Q.v NOTNULL true");

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO Q SET ts = 1, v = 1.0")).isInstanceOf(ValidationException.class)
        .hasMessageContaining("MANDATORY");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO Q SET ts = 1, host = null, v = 1.0"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("MANDATORY");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO Q SET ts = 1, host = 'a', v = null"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("NOTNULL");

    final Map<String, Object> noValue = new HashMap<>();
    final WriteReport report = TimeSeriesGateway.write((DatabaseInternal) database,
        new ArrayList<>(List.of(new Sample("Q", Map.of("host", "a"), noValue, 2), new Sample("Q", Map.of("host", "a"), Map.of("v", 2.0d), 3),
            // NaN is how a DOUBLE column stores no value (TimeSeriesNaN), so it is no value for NOTNULL either
            new Sample("Q", Map.of("host", "a"), Map.of("v", Double.NaN), 4))));
    assertThat(report.written()).isEqualTo(1);
    assertThat(report.dropped()).isEqualTo(2);
    assertThat(report.unreadableValues()).singleElement().asString().startsWith("Q.v (field): ").contains("NOTNULL");
    assertThat(rows("Q")).hasSize(1);
  }

  // #10238
  /**
   * NaN is no value in a FLOAT/DOUBLE column, so a column that does not require one takes it whatever its other constraints: a
   * sample with no value for a field, which the storage keeps as NaN, must not be refused by a pattern its text "NaN" cannot match.
   */
  @Test
  void aNaNIsNoValueForAColumnThatDoesNotRequireOne() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE V TIMESTAMP ts FIELDS (d DOUBLE, f FLOAT) SHARDS 1");
    database.command("sql", "ALTER PROPERTY V.d REGEXP '[0-9.]+'");
    database.command("sql", "ALTER PROPERTY V.d MIN 0");
    database.command("sql", "ALTER PROPERTY V.f REGEXP '[0-9.]+'");

    final WriteReport report = TimeSeriesGateway.write((DatabaseInternal) database,
        new ArrayList<>(List.of(new Sample("V", Map.of(), Map.of("d", Double.NaN, "f", Float.NaN), 1),
            new Sample("V", Map.of(), Map.of("d", 1.5d, "f", 2.5f), 2))));
    assertThat(report.unreadableValues()).isEmpty();
    assertThat(report.written()).isEqualTo(2);

    // a stored sample whose field is NaN does not stop a pattern from being declared either
    database.command("sql", "ALTER PROPERTY V.f REGEXP null");
    database.command("sql", "ALTER PROPERTY V.f REGEXP '[0-9.]+'");
    assertThat(rows("V")).hasSize(2);
  }

  // #10238
  @Test
  void theTimestampColumnIsBoundedToo() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts FIELDS (v DOUBLE) SHARDS 1");
    database.command("sql", "ALTER PROPERTY T.ts MIN 1000");

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO T SET ts = 999, v = 1.0")).isInstanceOf(ValidationException.class)
        .hasMessageContaining("'T.ts'").hasMessageContaining("MIN 1000");
    final WriteReport report = TimeSeriesGateway.write((DatabaseInternal) database,
        new ArrayList<>(List.of(new Sample("T", Map.of(), Map.of("v", 1.0d), 999), new Sample("T", Map.of(), Map.of("v", 1.0d), 1000))));
    assertThat(report.written()).isEqualTo(1);
    assertThat(report.unreadableValues()).singleElement().asString().startsWith("T.ts (timestamp): ").contains("MIN 1000");
    assertThat(rows("T")).extracting(row -> ((Number) row[0]).longValue()).containsExactly(1000L);
  }

  // #10238
  /** As over records (#8943, #9112): a constraint is refused over stored samples that already violate it. */
  @Test
  void aConstraintIsRefusedOverSamplesThatAlreadyViolateIt() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE E TIMESTAMP ts TAGS (host STRING) FIELDS (i INTEGER, s STRING, v DOUBLE) SHARDS 2");
    database.command("sql", "INSERT INTO E SET ts = 1, host = 'a', i = 5, s = 'ok', v = 1.0");
    database.command("sql", "INSERT INTO E SET ts = 2, host = 'a', i = 50");

    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY E.i MAX 10")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("Cannot set MAX 10").hasMessageContaining("timestamp 2");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY E.s REGEXP '[0-9]+'")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("timestamp 1");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY E.v MANDATORY true")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("Cannot set MANDATORY").hasMessageContaining("timestamp 2");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY E.v NOTNULL true")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("Cannot set NOTNULL");
    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("E");
    assertThat(type.getProperty("i").getMax()).isNull();
    assertThat(type.getProperty("v").isMandatory()).isFalse();

    // a STRING column stores no value as the empty text, which a stored sample cannot tell from an empty text written on purpose:
    // the samples cannot refuse it, the write path holds every NEW sample to it
    database.command("sql", "ALTER PROPERTY E.s MANDATORY true");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO E SET ts = 3, host = 'a', i = 1"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("'E.s'");

    // constraints every stored sample satisfies are accepted
    database.command("sql", "ALTER PROPERTY E.i MAX 50");
    database.command("sql", "ALTER PROPERTY E.host MANDATORY true");
    assertThat(type.getProperty("i").getMax()).isEqualTo("50");
    assertThat(type.getProperty("host").isMandatory()).isTrue();
  }

  // #10238
  /**
   * The engine hands the check of stored samples the same representation the write path stores, a temporal one as its count in the
   * column's unit, so a date bound is checked against stored samples as against a new one.
   */
  @Test
  void aTemporalBoundIsCheckedAgainstStoredSamplesInTheirOwnUnit() {
    database.command("sql", "CREATE TIMESERIES TYPE W TIMESTAMP ts TAGS (day DATE) FIELDS (at DATETIME_MICROS, n LONG) SHARDS 1");
    database.command("sql", "INSERT INTO W SET ts = 1, day = '2023-11-14', at = '2023-11-14 22:13:20', n = 1");
    database.command("sql", "INSERT INTO W SET ts = 2, day = '2023-11-15', at = '2023-11-14 22:13:21', n = 2");

    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY W.at MAX '2023-11-14 22:13:20'"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("timestamp 2").hasMessageContaining("after the MAX");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY W.day REGEXP '2023-11-14'"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("timestamp 2");
    database.command("sql", "ALTER PROPERTY W.at MAX '2023-11-14 22:13:21'");
    database.command("sql", "ALTER PROPERTY W.day MIN '2023-11-14'");
    database.command("sql", "ALTER PROPERTY W.day REGEXP '2023-11-1[45]'");
    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("W");
    assertThat(type.getProperty("day").getRegexp()).isEqualTo("2023-11-1[45]");
  }

  // #10238
  /**
   * The check of stored samples finds a column by its place in the engine row, the timestamp first and the other columns in
   * declaration order, wherever the TIMESTAMP clause was declared.
   */
  @Test
  void theStoredSampleCheckFindsTheColumnWhereverTheTimestampIsDeclared() {
    database.command("sql", "CREATE TIMESERIES TYPE O TAGS (host STRING, port INTEGER) TIMESTAMP ts FIELDS (v DOUBLE) SHARDS 1");
    database.command("sql", "INSERT INTO O SET ts = 1, host = 'a', port = 80, v = 1.0");
    database.command("sql", "INSERT INTO O SET ts = 2, host = 'b', port = 8080, v = 2.0");

    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY O.port MAX 100")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("timestamp 2");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY O.v MAX 1.5")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("timestamp 2");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY O.host REGEXP 'a'")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("timestamp 2");
    database.command("sql", "ALTER PROPERTY O.port MAX 8080");
    database.command("sql", "ALTER PROPERTY O.v MAX 2.0");
    database.command("sql", "ALTER PROPERTY O.host REGEXP '[ab]'");
  }

  // #10238
  /**
   * A multi-row INSERT refused at one row keeps the rows appended before it: each row is appended in its own shard transaction, so an
   * INSERT into a TIMESERIES type is not atomic (issue #7410), and a constraint is one more way for a later row to be refused. The
   * rows before it stay, the row and the rows after it are not written.
   */
  @Test
  void aMultiRowInsertRefusedAtOneRowKeepsTheRowsBeforeIt() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE A TIMESTAMP ts FIELDS (i INTEGER) SHARDS 1");
    database.command("sql", "ALTER PROPERTY A.i MAX 10");

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO A CONTENT [{\"ts\":1,\"i\":1},{\"ts\":2,\"i\":2},{\"ts\":3,\"i\":50},{\"ts\":4,\"i\":4}]"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("MAX 10");
    assertThat(rows("A")).extracting(row -> ((Number) row[0]).longValue()).doesNotContain(3L, 4L).containsAnyOf(1L, 2L);
  }

  // #10238
  @Test
  void aLongValueIsTruncatedInTheRefusal() {
    database.command("sql", "CREATE TIMESERIES TYPE K TIMESTAMP ts FIELDS (s STRING) SHARDS 1");
    database.command("sql", "ALTER PROPERTY K.s MAX 10");
    final String longText = "x".repeat(200);
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO K SET ts = 1, s = ?", longText)).isInstanceOf(ValidationException.class)
        .hasMessageContaining("x".repeat(100) + "...").satisfies(e -> assertThat(e.getMessage()).doesNotContain(longText));
  }

  // #10238
  /** DEFAULT and HIDDEN are honoured by no path of a TIMESERIES type, so they are refused rather than stored for nothing. */
  @Test
  void anAttributeNoPathHonoursIsRefused() {
    database.command("sql", "CREATE TIMESERIES TYPE D TIMESTAMP ts FIELDS (i INTEGER, s STRING) SHARDS 1");

    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY D.i DEFAULT 42")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("DEFAULT").hasMessageContaining("TIMESERIES");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY D.s HIDDEN true")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("HIDDEN").hasMessageContaining("TIMESERIES");
    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("D");
    assertThat(type.getProperty("i").getDefaultValue()).isNull();
    assertThat(type.getProperty("s").isHidden()).isFalse();

    // taking them back, and READONLY (a sample is immutable), are accepted
    database.command("sql", "ALTER PROPERTY D.i DEFAULT null");
    database.command("sql", "ALTER PROPERTY D.s HIDDEN false");
    database.command("sql", "ALTER PROPERTY D.s READONLY true");
    assertThat(type.getProperty("s").isReadonly()).isTrue();
  }

  // #10238
  @Test
  void aTypeWithNoConstraintCostsNothing() {
    database.command("sql", "CREATE TIMESERIES TYPE Z TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("Z");
    assertThat(TimeSeriesColumnConstraints.of(database, type)).isNull();

    database.command("sql", "ALTER PROPERTY Z.v MAX 10");
    assertThat(TimeSeriesColumnConstraints.of(database, type)).isNotNull();
    database.command("sql", "ALTER PROPERTY Z.v MAX null");
    assertThat(TimeSeriesColumnConstraints.of(database, type)).isNull();
  }

  private List<Object[]> rows(final String typeName) throws Exception {
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine().query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
  }

  private static final String TYPE = "Reading";

  private void createReadingType() {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (sensor STRING) FIELDS (value DOUBLE)");
  }

  // Issue #7567: a TIMESERIES type's columns live in {@code LocalTimeSeriesType.tsColumns}, filled once by
  // {@code CREATE TIMESERIES TYPE}, and the write path reads a document under those names and no others. A property
  // added afterwards with {@code CREATE PROPERTY} landed in the type's ordinary property map instead: it showed up in
  // the schema listing, and every write silently dropped whatever arrived under its name. {@code ALTER PROPERTY ...
  // CUSTOM role = "FIELD"} was the second half of the same misunderstanding - a column's role is fixed at creation and
  // is never read back out of property metadata.
  // <p>
  // Both are now refused at DDL time, which is the closest point to the mistake. The mirror case is refused with them:
  // dropping a declared column's property would leave the engine storing a column the type no longer declares.
  // #7567
  /**
   * The shape the reporter expected to work: everything declared inline is stored and read back. This is the control
   * for every rejection below - the rejections must not be the reason the values survive.
   */
  @Test
  void declaredColumnsAreStoredAndReadBack() {
    createReadingType();

    database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE + " SET ts = 1000, sensor = 'a', value = 7.5"));

    final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE);
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<String>getProperty("sensor")).isEqualTo("a");
    assertThat(((Number) row.getProperty("value")).doubleValue()).isEqualTo(7.5);
  }

  // #7567
  /**
   * Entry point 1: SQL {@code CREATE PROPERTY} on a TIMESERIES type.
   */
  @Test
  void sqlCreatePropertyOnTimeSeriesTypeIsRefused() {
    createReadingType();

    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY " + TYPE + ".humidity DOUBLE"))
        .hasMessageContaining("humidity")
        .hasMessageContaining("TIMESERIES");

    assertThat(database.getSchema().getType(TYPE).existsProperty("humidity")).isFalse();
  }

  // #7567
  /**
   * Entry point 2: the {@code IF NOT EXISTS} form. A name that is NOT a declared column still has to be refused - it
   * reaches the same funnel - while a name that IS one keeps the documented no-op behaviour of issue #7143.
   */
  @Test
  void sqlCreatePropertyIfNotExistsOnTimeSeriesTypeIsRefused() {
    createReadingType();

    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY " + TYPE + ".humidity IF NOT EXISTS DOUBLE"))
        .hasMessageContaining("humidity")
        .hasMessageContaining("TIMESERIES");

    assertThat(database.getSchema().getType(TYPE).existsProperty("humidity")).isFalse();

    final ResultSet existing = database.command("sql", "CREATE PROPERTY " + TYPE + ".value IF NOT EXISTS DOUBLE");
    assertThat(existing.next().<Boolean>getProperty("created")).isFalse();
  }

  // #7567
  /**
   * Entry point 3: the schema API. Every {@code createProperty}/{@code getOrCreateProperty} overload delegates to
   * {@code createProperty(String, Type, String)}, so both spellings below exercise the one funnel that the SQL
   * statement above also reaches.
   */
  @Test
  void schemaApiCreatePropertyOnTimeSeriesTypeIsRefused() {
    createReadingType();
    final DocumentType type = database.getSchema().getType(TYPE);

    assertThatThrownBy(() -> type.createProperty("humidity", Type.DOUBLE))
        .hasMessageContaining("humidity")
        .hasMessageContaining("TIMESERIES");

    assertThatThrownBy(() -> type.getOrCreateProperty("humidity", Type.DOUBLE))
        .hasMessageContaining("humidity")
        .hasMessageContaining("TIMESERIES");

    assertThat(type.existsProperty("humidity")).isFalse();
  }

  // #7567
  /**
   * Entry point 4: {@code ALTER PROPERTY ... CUSTOM role}, the second route in the issue. Any other CUSTOM key stays
   * free-form - the refusal is about the one key that pretends to reconfigure the column.
   */
  @Test
  void sqlAlterPropertyCustomRoleOnTimeSeriesTypeIsRefused() {
    createReadingType();

    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY " + TYPE + ".value CUSTOM role = 'TAG'"))
        .hasMessageContaining("role")
        .hasMessageContaining("TIMESERIES");

    assertThat(database.getSchema().getType(TYPE).getProperty("value").getCustomValue("role")).isNull();

    assertThatCode(() -> database.command("sql", "ALTER PROPERTY " + TYPE + ".value CUSTOM unit = 'celsius'"))
        .doesNotThrowAnyException();
    assertThat(database.getSchema().getType(TYPE).getProperty("value").getCustomValue("unit")).isEqualTo("celsius");
  }

  // #7567
  /**
   * Entry point 5: the same key through the schema API, and spelled in any case - SQL lower-cases nothing on the way
   * in, so {@code CUSTOM ROLE} reaches the setter verbatim.
   */
  @Test
  void schemaApiSetCustomRoleOnTimeSeriesTypeIsRefused() {
    createReadingType();
    final Property value = database.getSchema().getType(TYPE).getProperty("value");

    assertThatThrownBy(() -> value.setCustomValue("role", "TAG"))
        .hasMessageContaining("TIMESERIES")
        .hasMessageContaining("FIELD");

    assertThatThrownBy(() -> value.setCustomValue("ROLE", "TAG"))
        .hasMessageContaining("TIMESERIES");

    assertThat(value.getCustomValue("role")).isNull();
  }

  // #7567
  /**
   * Removing the key is not making the claim. A database written before this fix can carry a CUSTOM {@code role} it
   * can no longer set, so the refusal must not also be what stops it being cleaned up: {@code = null} is the
   * documented removal form and stays allowed.
   */
  @Test
  void removingAStaleCustomRoleIsAllowed() throws Exception {
    createReadingType();
    final String databasePath = database.getDatabasePath();
    database.close();

    final File schemaFile = new File(databasePath, "schema.json");
    final JSONObject schema = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
    final JSONObject value = schema.getJSONObject("types").getJSONObject(TYPE).getJSONObject("properties")
        .getJSONObject("value");
    value.put("custom", new JSONObject().put("role", "TAG"));
    Files.writeString(schemaFile.toPath(), schema.toString(), StandardCharsets.UTF_8);

    database = factory.open();
    assertThat(database.getSchema().getType(TYPE).getProperty("value").getCustomValue("role")).isEqualTo("TAG");

    database.command("sql", "ALTER PROPERTY " + TYPE + ".value CUSTOM role = null");
    assertThat(database.getSchema().getType(TYPE).getProperty("value").getCustomValue("role")).isNull();
  }

  // #7567
  /**
   * Entry point 6: the mirror case. Dropping a declared column's property would leave the engine storing a column the
   * type had stopped declaring - the same divergence seen from the other side.
   */
  @Test
  void sqlDropPropertyOfDeclaredColumnIsRefused() {
    createReadingType();

    for (final String column : new String[] { "ts", "sensor", "value" })
      assertThatThrownBy(() -> database.command("sql", "DROP PROPERTY " + TYPE + "." + column))
          .as("dropping the declared column '%s' must be refused", column)
          .hasMessageContaining(column)
          .hasMessageContaining("TIMESERIES");

    assertThat(database.getSchema().getType(TYPE).getPropertyNames()).contains("ts", "sensor", "value");
  }

  // #7567
  /**
   * Back-compat: a database created BEFORE this fix can already carry a stray property, because nothing refused it.
   * Refusing it at schema load would make that database unopenable, so the load path warns and carries on - and
   * {@code DROP PROPERTY} on the stray name stays allowed, which is how the database is cleaned up.
   * <p>
   * The stray property is injected straight into {@code schema.json} because there is, deliberately, no longer an
   * API that can produce one.
   */
  @Test
  void preFixStrayPropertyStillOpensAndCanBeDropped() throws Exception {
    createReadingType();
    final String databasePath = database.getDatabasePath();
    database.close();

    final File schemaFile = new File(databasePath, "schema.json");
    final JSONObject schema = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
    final JSONObject properties = schema.getJSONObject("types").getJSONObject(TYPE).getJSONObject("properties");
    properties.put("humidity", new JSONObject().put("type", "DOUBLE"));
    Files.writeString(schemaFile.toPath(), schema.toString(), StandardCharsets.UTF_8);

    database = factory.open();

    final DocumentType reopened = database.getSchema().getType(TYPE);
    assertThat(reopened).isInstanceOf(LocalTimeSeriesType.class);
    assertThat(reopened.existsProperty("humidity")).as("the stray property must survive the open").isTrue();
    assertThat(((LocalTimeSeriesType) reopened).isDeclaredColumn("humidity")).isFalse();

    database.command("sql", "DROP PROPERTY " + TYPE + ".humidity");
    assertThat(database.getSchema().getType(TYPE).existsProperty("humidity")).isFalse();

    // And the type still works after the cleanup.
    database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE + " SET ts = 2000, sensor = 'b', value = 1.25"));
    final List<Result> rows = database.query("sql", "SELECT FROM " + TYPE).stream().toList();
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.getFirst().getProperty("value")).doubleValue()).isEqualTo(1.25);
  }

  // #7567
  /**
   * A plain DOCUMENT type is untouched: the refusals are scoped to TIMESERIES types and nothing else.
   */
  @Test
  void documentTypePropertyDDLIsUnaffected() {
    database.command("sql", "CREATE DOCUMENT TYPE Note");
    database.command("sql", "CREATE PROPERTY Note.body STRING");
    database.command("sql", "ALTER PROPERTY Note.body CUSTOM role = 'whatever'");

    assertThat(database.getSchema().getType("Note").getProperty("body").getCustomValue("role")).isEqualTo("whatever");

    database.command("sql", "DROP PROPERTY Note.body");
    assertThat(database.getSchema().getType("Note").existsProperty("body")).isFalse();
  }

  private void createTimeSeriesType() {
    database.command("sql", "CREATE TIMESERIES TYPE R TIMESTAMP ts TAGS (s STRING) FIELDS (v DOUBLE)");
  }

  // Regression test for issue #7581: {@code ALTER TYPE <timeseries> SUPERTYPE ...} let a polymorphic property reach
  // a TIMESERIES type through inheritance rather than {@code CREATE PROPERTY}, which is the same silent-drop shape
  // issue #7567 refused for a property declared directly on the type. {@code LocalTimeSeriesType.tsColumns} is
  // filled once by {@code TimeSeriesTypeBuilder.create()} and has no other caller, so a super type linked afterwards
  // cannot extend it: the time-series write path ({@code SaveElementStep#saveToTimeSeries}) reads the document under
  // the declared column names only and silently drops a value written under an inherited property name.
  // <p>
  // Refused from both ends: a TIMESERIES type may not gain a super type, and may not become anyone else's super type.
  // #7581
  @Test
  void timeSeriesTypeCannotReceiveASuperType() {
    createTimeSeriesType();
    database.command("sql", "CREATE DOCUMENT TYPE Base");
    database.command("sql", "CREATE PROPERTY Base.humidity DOUBLE");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE R SUPERTYPE +Base"))
        .hasMessageContaining("TIMESERIES");

    assertThat(database.getSchema().getType("R").getSuperTypes()).isEmpty();
  }

  // #7581
  @Test
  void timeSeriesTypeCannotBecomeASuperType() {
    createTimeSeriesType();
    database.command("sql", "CREATE DOCUMENT TYPE Sub");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Sub SUPERTYPE +R"))
        .hasMessageContaining("TIMESERIES");

    assertThat(database.getSchema().getType("Sub").getSuperTypes()).isEmpty();
  }

  // #7581
  @Test
  void javaApiAlsoRefusesBothDirections() {
    createTimeSeriesType();
    final DocumentType r = database.getSchema().getType("R");
    final DocumentType base = database.getSchema().buildDocumentType().withName("Base2").create();

    assertThatThrownBy(() -> r.addSuperType(base)).hasMessageContaining("TIMESERIES");
    assertThatThrownBy(() -> base.addSuperType(r)).hasMessageContaining("TIMESERIES");
  }

  // #7581
  /**
   * Control: two ordinary document types must still be free to form a hierarchy - the refusal above must not be
   * an accidental blanket refusal of SUPERTYPE.
   */
  @Test
  void twoOrdinaryTypesCanStillFormAHierarchy() {
    database.command("sql", "CREATE DOCUMENT TYPE Animal");
    database.command("sql", "CREATE DOCUMENT TYPE Dog");
    database.command("sql", "ALTER TYPE Dog SUPERTYPE +Animal");

    assertThat(database.getSchema().getType("Dog").getSuperTypes())
        .extracting(DocumentType::getName)
        .containsExactly("Animal");
  }

  /**
   * Issue #10336: {@code DELETE FROM} a TIMESERIES type reported its matching samples as deleted and kept them, {@code UPDATE}
   * reported {@code count: 0} and {@code TRUNCATE TYPE} reported success, so the advice of the {@code ALTER PROPERTY} refusal ("delete
   * the non-conforming samples first") could not be followed. A sample is immutable: UPDATE is refused, naming what does work, and
   * since #10342 DELETE (by time range and tags) and TRUNCATE TYPE do remove samples. And a STRING column stores a sample with no value for it as the empty text, so a MIN or REGEXP accepted that sample
   * at write time and refused it once stored: the write path now judges the text the column will hold, as {@code ALTER} does, and
   * the outcome no longer depends on the order of the two.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TimeSeriesDmlAndAbsentString {
    private static final String SAMPLES = "[{\"t\":\"h\",\"s\":\"ab\",\"i\":1}, {\"t\":\"h\",\"s\":\"cd\",\"i\":2}]";

    @BeforeEach
    void seed() {
      database.command("sql", "CREATE TIMESERIES TYPE D1 TIMESTAMP ts TAGS (t STRING) FIELDS (s STRING, i INTEGER)");
      database.command("sql", "INSERT INTO D1 SET ts = 1000, t = 'h', s = 'ab', i = 1");
      database.command("sql", "INSERT INTO D1 SET ts = 2000, t = 'h', s = 'cd', i = 2");
    }

    private String rows(final String type) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT t, s, i FROM " + type + " ORDER BY ts")) {
        while (rs.hasNext())
          rows.add(rs.next().toJSON().toString());
      }
      return rows.toString();
    }

    // #10336
    /**
     * Since #10342 a DELETE by time range and tags removes samples; what it cannot name exactly (a predicate on a FIELD column) is still
     * refused, and keeps every sample, as the DELETE that used to report them deleted did not.
     */
    @Test
    void deleteWithAPredicateItCannotNameExactlyIsRefusedAndKeepsTheSamples() {
      assertThatThrownBy(() -> database.command("sql", "DELETE FROM D1 WHERE i = 1").close())
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("TIMESERIES type 'D1'").hasMessageContaining("DELETE");
      assertThatThrownBy(() -> database.command("sql", "DELETE FROM D1 WHERE ts = 1000 OR t = 'h'").close())
          .isInstanceOf(CommandExecutionException.class);
      assertThat(rows("D1")).isEqualTo(SAMPLES);
    }

    // #10336
    @Test
    void deleteByTimeRemovesTheSampleAndReportsIt() {
      try (final ResultSet rs = database.command("sql", "DELETE FROM D1 WHERE ts = 1000")) {
        assertThat(rs.next().<Number>getProperty("count").longValue()).isEqualTo(1L);
      }
      assertThat(rows("D1")).isEqualTo("[{\"t\":\"h\",\"s\":\"cd\",\"i\":2}]");
    }

    // #10336
    @Test
    void updateIsRefusedAndKeepsTheSamples() {
      assertThatThrownBy(() -> database.command("sql", "UPDATE D1 SET i = 6 WHERE ts = 2000").close())
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("TIMESERIES type 'D1'").hasMessageContaining("UPDATE");
      assertThatThrownBy(() -> database.command("sql", "UPDATE D1 SET i = 6 UPSERT WHERE ts = 3000").close())
          .isInstanceOf(CommandExecutionException.class);
      assertThat(rows("D1")).isEqualTo(SAMPLES);
    }

    // #10336
    /** Since #10342 TRUNCATE TYPE removes the samples (it used to report success and keep them). */
    @Test
    void truncateTypeRemovesTheSamples() {
      try (final ResultSet rs = database.command("sql", "TRUNCATE TYPE D1")) {
        assertThat(rs.next().<Number>getProperty("count").longValue()).isEqualTo(2L);
      }
      assertThat(rows("D1")).isEqualTo("[]");
    }

    // #10336
    @Test
    void deleteFromASubqueryOfSamplesDoesNotCountWhatItCannotDelete() {
      // THE ROWS OF A TIMESERIES SCAN ARE NOT RECORDS: NOTHING TO DELETE, SO NOTHING MAY BE REPORTED DELETED
      assertThatThrownBy(() -> database.command("sql", "DELETE FROM (SELECT FROM D1)").close())
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("not a record");
      assertThat(rows("D1")).isEqualTo(SAMPLES);
    }

    // #10336
    @Test
    void deleteFromASubqueryOfProjectionsOfRecordsIsRefusedToo() {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET name = 'a'"));
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "DELETE FROM (SELECT name FROM Doc)").close()))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("not a record");
      try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM Doc")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
      }
      // THE RECORDS THEMSELVES ARE DELETED AS BEFORE
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", "DELETE FROM (SELECT FROM Doc)")) {
          assertThat(rs.next().<Number>getProperty("count").longValue()).isEqualTo(1L);
        }
      });
    }

    // #10336
    @Test
    void updateOverASubqueryOfRowsThatAreNotRecordsIsRefused() {
      // THE ROWS OF A TIMESERIES SCAN: NOTHING TO WRITE TO, WHICH USED TO BE REPORTED AS count 0
      assertThatThrownBy(() -> database.command("sql", "UPDATE (SELECT FROM D1) SET i = 6").close())
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("not a record");
      assertThat(rows("D1")).isEqualTo(SAMPLES);

      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET name = 'a'"));
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE (SELECT name FROM Doc) SET x = 1").close()))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("not a record");
      // A PROJECTION THAT CARRIES THE @rid OF A RECORD WRITES TO THAT RECORD, AS BEFORE
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", "UPDATE (SELECT @rid FROM Doc) SET x = 1")) {
          assertThat(rs.next().<Number>getProperty("count").longValue()).isEqualTo(1L);
        }
      });
      try (final ResultSet rs = database.query("sql", "SELECT x FROM Doc")) {
        assertThat(rs.next().<Number>getProperty("x").intValue()).isEqualTo(1);
      }
    }

    // #10336
    @Test
    void deleteOverASubqueryOfRidsDeletesTheRecords() {
      // THE SHAPE UPDATE ALREADY HONOURED: THE @rid OF THE ROW NAMES THE RECORD. DELETE USED TO COUNT IT AND DELETE NOTHING
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Doc SET name = 'a'");
        database.command("sql", "INSERT INTO Doc SET name = 'b'");
      });
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", "DELETE FROM (SELECT @rid FROM Doc WHERE name = 'a')")) {
          assertThat(rs.next().<Number>getProperty("count").longValue()).isEqualTo(1L);
        }
      });
      try (final ResultSet rs = database.query("sql", "SELECT name FROM Doc")) {
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("b");
        assertThat(rs.hasNext()).isFalse();
      }
    }

    // #10336
    @Test
    void theAlterRefusalNamesARemedyThatWorks() {
      database.command("sql", "INSERT INTO D1 SET ts = 3000, t = 'h', s = 'x', i = 3");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY D1.s MIN 2").close())
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("timestamp 3000")
          .hasMessageContaining("Delete the non-conforming samples first").hasMessageContaining("DELETE FROM D1 WHERE ts = 3000");

      // THE REMEDY THE MESSAGE NAMES (issue #10342): THE SAMPLE IS DELETED BY ITS TIME, AND THE CONSTRAINT CAN BE DECLARED
      database.command("sql", "DELETE FROM D1 WHERE ts = 3000").close();
      database.command("sql", "ALTER PROPERTY D1.s MIN 2").close();
      assertThat(rows("D1")).isEqualTo(SAMPLES);
    }

    // #10336
    /** The other way out, which needs no delete: a new type that declares the constraint, and the conforming samples copied into it. */
    @Test
    void theConformingSamplesCanBeCopiedIntoANewType() {
      database.command("sql", "INSERT INTO D1 SET ts = 3000, t = 'h', s = 'x', i = 3");
      database.command("sql", "CREATE TIMESERIES TYPE D2 TIMESTAMP ts TAGS (t STRING) FIELDS (s STRING, i INTEGER)");
      database.command("sql", "ALTER PROPERTY D2.s MIN 2");
      database.command("sql", "INSERT INTO D2 FROM SELECT ts, t, s, i FROM D1 WHERE s.length() >= 2");
      assertThat(rows("D2")).isEqualTo(SAMPLES);
    }

    // #10336
    @Test
    void anAbsentStringIsJudgedAsTheEmptyTextWhicheverComesFirst() {
      // THE REPORTED ORDER: THE SAMPLE FIRST, THEN THE CONSTRAINT
      database.command("sql", "CREATE TIMESERIES TYPE W1 TIMESTAMP ts TAGS (t STRING) FIELDS (s STRING, i INTEGER)");
      database.command("sql", "INSERT INTO W1 SET ts = 1000, t = 'h', i = 1");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY W1.s MIN 2").close())
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("empty text");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY W1.s REGEXP '[a-z]+'").close())
          .isInstanceOf(CommandExecutionException.class);

      // THE OTHER ORDER: THE CONSTRAINT FIRST, THEN THE SAME SAMPLE, NOW REFUSED THE SAME WAY
      database.command("sql", "CREATE TIMESERIES TYPE A1 TIMESTAMP ts TAGS (t STRING) FIELDS (s STRING, i INTEGER)");
      database.command("sql", "ALTER PROPERTY A1.s MIN 2");
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO A1 SET ts = 1000, t = 'h', i = 1").close())
          .isInstanceOf(ValidationException.class).hasMessageContaining("empty text").hasMessageContaining("MIN 2");
      database.command("sql", "ALTER PROPERTY A1.s MIN null");
      database.command("sql", "ALTER PROPERTY A1.s REGEXP '[a-z]+'");
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO A1 SET ts = 1000, t = 'h', i = 1").close())
          .isInstanceOf(ValidationException.class).hasMessageContaining("REGEXP");
      assertThat(rows("A1")).isEqualTo("[]");

      // A VALUE THAT SATISFIES THEM IS STILL TAKEN, AND A CONSTRAINT THE EMPTY TEXT SATISFIES TAKES AN ABSENT VALUE
      database.command("sql", "INSERT INTO A1 SET ts = 1000, t = 'h', s = 'ab', i = 1");
      database.command("sql", "ALTER PROPERTY A1.s REGEXP '[a-z]*'");
      database.command("sql", "ALTER PROPERTY A1.s MAX 5");
      database.command("sql", "INSERT INTO A1 SET ts = 2000, t = 'h', i = 2");
      assertThat(rows("A1")).isEqualTo("[{\"t\":\"h\",\"s\":\"ab\",\"i\":1}, {\"t\":\"h\",\"s\":\"\",\"i\":2}]");
    }

    // #10336
    @Test
    void anAbsentTagIsJudgedAsTheEmptyTextToo() {
      database.command("sql", "CREATE TIMESERIES TYPE T1 TIMESTAMP ts TAGS (t STRING) FIELDS (i INTEGER)");
      database.command("sql", "ALTER PROPERTY T1.t MIN 1");
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO T1 SET ts = 1000, i = 1").close())
          .isInstanceOf(ValidationException.class);
      database.command("sql", "INSERT INTO T1 SET ts = 1000, t = 'h', i = 1");
      assertThat(rows("T1")).isEqualTo("[{\"t\":\"h\",\"s\":null,\"i\":1}]");
    }

    // #10336
    @Test
    void anAbsentNumberIsStillNoValue() {
      // A NUMERIC COLUMN KEEPS ITS NULL BIT: NO BOUND APPLIES TO NO VALUE, ON EITHER SIDE
      database.command("sql", "CREATE TIMESERIES TYPE N1 TIMESTAMP ts FIELDS (s STRING, i INTEGER)");
      database.command("sql", "INSERT INTO N1 SET ts = 1000, s = 'ab'");
      database.command("sql", "ALTER PROPERTY N1.i MIN 5");
      database.command("sql", "INSERT INTO N1 SET ts = 2000, s = 'cd'");
      try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM N1")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(2L);
      }
    }
  }
}
