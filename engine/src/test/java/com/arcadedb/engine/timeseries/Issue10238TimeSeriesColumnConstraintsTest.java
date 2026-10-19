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
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10238: {@code ALTER PROPERTY} accepted MANDATORY, NOTNULL, MIN, MAX and REGEXP on a column of a TIMESERIES type, and no
 * write path ever enforced them: SQL hands the row to the engine without the document validation, and the line protocol, gRPC,
 * Prometheus and the JSONL import never build a document. Every surface now holds the value the column stores to the constraints
 * of its column ({@link TimeSeriesColumnConstraints}); DEFAULT and HIDDEN, which no path of such a type honours, are refused; a
 * constraint is refused over stored samples that already violate it, as over records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10238TimeSeriesColumnConstraintsTest extends TestHelper {

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

  @Test
  void aLongValueIsTruncatedInTheRefusal() {
    database.command("sql", "CREATE TIMESERIES TYPE K TIMESTAMP ts FIELDS (s STRING) SHARDS 1");
    database.command("sql", "ALTER PROPERTY K.s MAX 10");
    final String longText = "x".repeat(200);
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO K SET ts = 1, s = ?", longText)).isInstanceOf(ValidationException.class)
        .hasMessageContaining("x".repeat(100) + "...").satisfies(e -> assertThat(e.getMessage()).doesNotContain(longText));
  }

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
}
