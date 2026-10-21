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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.function.DistinctNumericKey;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10526: openCypher {@code DISTINCT}, {@code GROUP BY} and {@code UNION} keyed a temporal value by its Java class, so a stored
 * DATETIME ({@code LocalDateTime}) and {@code datetime()} ({@code ZonedDateTime}) that {@code =} reports equal landed in two groups,
 * and so did a {@code Set} and the list of the same elements. {@code DistinctNumericKey.canonicalize()} had no arm for either.
 * <p>
 * The rule, stated by #10380 for the other kinds: a value that {@code =} reports equal is one value to {@code DISTINCT}, {@code GROUP BY}
 * and {@code UNION}. Two zoned datetimes at one instant in different zones, and a {@code localdatetime()} against a {@code datetime()},
 * are different values to {@code =} and stay different groups.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherDistinctTemporalKeyIssue10526Test extends TestHelper {
  private static final String DATETIME = "datetime('2026-01-02T03:04:05Z')";

  private void storedDatetime() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE PROPERTY A.t DATETIME");
    database.transaction(() -> database.command("sql", "INSERT INTO A SET t = '2026-01-02 03:04:05'"));
  }

  // ---------------------------------------------------------------------------------------------------- the repro of the issue

  @Test
  void aStoredDatetimeAndDatetimeAreOneValueToEveryDuplicateElimination() {
    storedDatetime();

    assertThat(scalar("MATCH (a:A) RETURN a.t = " + DATETIME + " AS v")).isEqualTo(true);
    assertThat(scalar("MATCH (a:A) UNWIND [a.t, " + DATETIME + "] AS x RETURN count(DISTINCT x) AS v")).isEqualTo(1L);
    assertThat(scalar("MATCH (a:A) UNWIND [a.t, " + DATETIME + "] AS x WITH x AS g, count(*) AS c RETURN count(*) AS v")).isEqualTo(1L);
    assertThat(rows("MATCH (a:A) RETURN a.t AS t UNION RETURN " + DATETIME + " AS t")).hasSize(1);
    assertThat(rows("MATCH (a:A) UNWIND [a.t, " + DATETIME + "] AS x RETURN DISTINCT x")).hasSize(1);
    assertThat(scalar("MATCH (a:A) UNWIND [a.t, " + DATETIME + "] AS t RETURN size(collect(DISTINCT t)) AS v")).isEqualTo(1L);
  }

  @Test
  void aDifferentInstantIsAnotherGroup() {
    storedDatetime();

    assertThat(scalar("MATCH (a:A) UNWIND [a.t, datetime('2026-01-02T03:04:06Z')] AS x RETURN count(DISTINCT x) AS v")).isEqualTo(2L);
  }

  @Test
  void aSetAndTheListOfItsElementsAreOneValue() {
    // an ordered set: Set.of() iterates in an order that changes from one JVM run to the next, and = reads a Set as a sequence
    assertThat(scalar("RETURN $a = $b AS v", Map.of("a", new int[] { 1, 2 }, "b", new LinkedHashSet<>(List.of(1, 2))))).isEqualTo(true);
    assertThat(scalar("UNWIND [$a, $b] AS x RETURN count(DISTINCT x) AS v", Map.of("a", new int[] { 1, 2 }, "b", new LinkedHashSet<>(List.of(1, 2)))))
        .isEqualTo(1L);
    assertThat(scalar("UNWIND [$a, $b] AS x RETURN count(DISTINCT x) AS v", Map.of("a", List.of(1L, 2L), "b", new LinkedHashSet<>(List.of(1.0, 2.0)))))
        .isEqualTo(1L);
  }

  @Test
  void twoSetsOfTheSameElementsInAnotherOrderAreTreatedAsEqualsTreatsThem() {
    // a Set has no order, but = reads it as a sequence, so the order it iterates in is part of the value: whichever way = answers, the
    // duplicate elimination has to answer the same, or two sets that = calls equal would be two groups (or the reverse)
    final Map<String, Object> parameters = Map.of("a", new LinkedHashSet<>(List.of(1, 2, 3)), "b", new LinkedHashSet<>(List.of(3, 2, 1)));
    final boolean equal = Boolean.TRUE.equals(scalar("RETURN $a = $b AS v", parameters));

    // = compares element by element in iteration order, so [1, 2, 3] and [3, 2, 1] differ; pinned, so a change of that rule shows here
    assertThat(equal).isFalse();
    assertThat(scalar("UNWIND [$a, $b] AS x RETURN count(DISTINCT x) AS v", parameters)).isEqualTo(equal ? 1L : 2L);
    assertThat(scalar("UNWIND [$a, $b] AS x WITH x AS g, count(*) AS c RETURN count(*) AS v", parameters)).isEqualTo(equal ? 1L : 2L);

    // the same set, iterated in the same order, is one value
    final Map<String, Object> same = Map.of("a", new LinkedHashSet<>(List.of(1, 2, 3)), "b", new LinkedHashSet<>(List.of(1, 2, 3)));
    assertThat(scalar("RETURN $a = $b AS v", same)).isEqualTo(true);
    assertThat(scalar("UNWIND [$a, $b] AS x RETURN count(DISTINCT x) AS v", same)).isEqualTo(1L);
  }

  // ------------------------------------------------------------------------------------- every representation of one instant

  /** One instant, as every temporal class an openCypher query can meet. */
  private static List<Object> oneInstant() {
    final LocalDateTime local = LocalDateTime.of(2026, 1, 2, 3, 4, 5);
    final Instant instant = local.toInstant(ZoneOffset.UTC);
    final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.setTimeInMillis(instant.toEpochMilli());
    return List.of(local, new Date(instant.toEpochMilli()), instant, calendar, ZonedDateTime.of(local, ZoneOffset.UTC),
        OffsetDateTime.of(local, ZoneOffset.UTC));
  }

  @Test
  void everyRepresentationOfOneInstantInUtcIsOneValueWhereverTheEqualityIs() {
    final List<Object> values = oneInstant();
    for (final Object a : values)
      for (final Object b : values) {
        final String what = a.getClass().getSimpleName() + " vs " + b.getClass().getSimpleName();
        assertThat(scalar("RETURN $a = $b AS v", Map.of("a", a, "b", b))).as("= " + what).isEqualTo(true);
        assertThat(scalar("UNWIND [$a, $b] AS x RETURN count(DISTINCT x) AS v", Map.of("a", a, "b", b))).as("DISTINCT " + what).isEqualTo(1L);
        assertThat(scalar("UNWIND [$a, $b] AS x WITH x AS g, count(*) AS c RETURN count(*) AS v", Map.of("a", a, "b", b))).as("GROUP BY " + what)
            .isEqualTo(1L);
        assertThat(DistinctNumericKey.canonicalize(a)).as("key " + what).isEqualTo(DistinctNumericKey.canonicalize(b));
      }
  }

  @Test
  void datetimesAtOneInstantInDifferentZonesAreDifferentValuesAndStayDifferentGroups() {
    final LocalDateTime local = LocalDateTime.of(2026, 1, 2, 3, 4, 5);
    final ZonedDateTime utc = ZonedDateTime.of(local, ZoneOffset.UTC);
    final ZonedDateTime paris = utc.withZoneSameInstant(ZoneId.of("Europe/Paris"));

    assertThat(scalar("RETURN $a = $b AS v", Map.of("a", utc, "b", paris))).isEqualTo(false);
    assertThat(scalar("UNWIND [$a, $b] AS x RETURN count(DISTINCT x) AS v", Map.of("a", utc, "b", paris))).isEqualTo(2L);
    assertThat(DistinctNumericKey.canonicalize(utc)).isNotEqualTo(DistinctNumericKey.canonicalize(paris));
  }

  @Test
  void aLocalDatetimeBuiltByTheQueryIsNotADatetime() {
    assertThat(scalar("RETURN localdatetime('2026-01-02T03:04:05') = " + DATETIME + " AS v")).isEqualTo(false);
    assertThat(scalar("UNWIND [localdatetime('2026-01-02T03:04:05'), " + DATETIME + "] AS x RETURN count(DISTINCT x) AS v")).isEqualTo(2L);
    assertThat(scalar("UNWIND [localdatetime('2026-01-02T03:04:05'), localdatetime('2026-01-02T03:04:05')] AS x RETURN count(DISTINCT x) AS v"))
        .isEqualTo(1L);
  }

  @Test
  void theOtherTemporalClassesAreOneValueWithTheirCypherTwin() {
    assertThat(scalar("UNWIND [$a, date('2026-01-02')] AS x RETURN count(DISTINCT x) AS v", Map.of("a", LocalDate.of(2026, 1, 2)))).isEqualTo(1L);
    assertThat(scalar("UNWIND [$a, localtime('03:04:05')] AS x RETURN count(DISTINCT x) AS v", Map.of("a", LocalTime.of(3, 4, 5)))).isEqualTo(1L);
    assertThat(scalar("UNWIND [$a, time('03:04:05Z')] AS x RETURN count(DISTINCT x) AS v", Map.of("a", OffsetTime.of(3, 4, 5, 0, ZoneOffset.UTC))))
        .isEqualTo(1L);
    assertThat(scalar("UNWIND [$a, duration('PT24H')] AS x RETURN count(DISTINCT x) AS v", Map.of("a", Duration.ofHours(24)))).isEqualTo(1L);
    assertThat(scalar("UNWIND [$a, duration('P1M')] AS x RETURN count(DISTINCT x) AS v", Map.of("a", Period.ofMonths(1)))).isEqualTo(1L);
    // components, as = reads them: a day is not 24 hours
    assertThat(scalar("UNWIND [duration('P1D'), duration('PT24H')] AS x RETURN count(DISTINCT x) AS v")).isEqualTo(2L);
  }

  // ------------------------------------------------------------------------------------------------ the sweep the report asks for

  /**
   * One value per {@link Type}, and one alternative Java representation of it where there is one, which is what a query can be handed:
   * {@code canonicalize()} and {@code =} are two lists of value kinds kept by hand, and this is what makes the next drift between them a
   * failing build. The kinds left out hold records or documents, which a parameter cannot carry; they key by their RID.
   */
  private static Map<Type, List<Object>> samplesByType() {
    final LocalDateTime local = LocalDateTime.of(2026, 1, 2, 3, 4, 5);
    final Map<Type, List<Object>> samples = new LinkedHashMap<>();
    samples.put(Type.BOOLEAN, List.of(true, false));
    samples.put(Type.BYTE, List.of((byte) 1));
    samples.put(Type.SHORT, List.of((short) 1));
    samples.put(Type.INTEGER, List.of(1, 2));
    samples.put(Type.LONG, List.of(1L));
    samples.put(Type.FLOAT, List.of(1.0f, 0.1f));
    samples.put(Type.DOUBLE, List.of(1.0d, 0.1d));
    samples.put(Type.DECIMAL, List.of(BigDecimal.ONE, new BigDecimal("0.1")));
    samples.put(Type.STRING, List.of("1", "a"));
    samples.put(Type.BINARY, List.of(new byte[] { 1, 2 }, new byte[] { 1, 2 }, new byte[] { 1, 3 }));
    samples.put(Type.LIST, List.of(List.of(1L, 2L), new LinkedList<>(List.of(1L, 2L)), List.of(1.0d, 2.0d)));
    samples.put(Type.MAP, List.of(Map.of("a", 1L), Map.of("a", 1.0d)));
    samples.put(Type.DATE, List.of(LocalDate.of(2026, 1, 2)));
    samples.put(Type.DATETIME, List.of(local, new Date(local.toInstant(ZoneOffset.UTC).toEpochMilli()), local.toInstant(ZoneOffset.UTC)));
    samples.put(Type.DATETIME_SECOND, List.of(local.plusSeconds(1)));
    samples.put(Type.DATETIME_MICROS, List.of(local.withNano(123_000)));
    samples.put(Type.DATETIME_NANOS, List.of(local.withNano(123_456_789)));
    samples.put(Type.ZONED_DATETIME, List.of(ZonedDateTime.of(local, ZoneOffset.UTC), ZonedDateTime.of(local, ZoneId.of("Europe/Paris")),
        OffsetDateTime.of(local, ZoneOffset.UTC)));
    samples.put(Type.LOCAL_TIME, List.of(LocalTime.of(3, 4, 5)));
    samples.put(Type.OFFSET_TIME, List.of(OffsetTime.of(3, 4, 5, 0, ZoneOffset.UTC)));
    samples.put(Type.DURATION, List.of(Duration.ofHours(24), Period.ofDays(1)));
    samples.put(Type.ARRAY_OF_SHORTS, List.of(new short[] { 1, 2 }));
    samples.put(Type.ARRAY_OF_INTEGERS, List.of(new int[] { 1, 2 }, new LinkedHashSet<>(List.of(1, 2))));
    samples.put(Type.ARRAY_OF_LONGS, List.of(new long[] { 1, 2 }));
    samples.put(Type.ARRAY_OF_FLOATS, List.of(new float[] { 1, 2 }));
    samples.put(Type.ARRAY_OF_DOUBLES, List.of(new double[] { 1, 2 }));
    return samples;
  }

  @Test
  void everyKindOfValueHasASampleExceptTheRecords() {
    // a Type added later fails here until it is given a sample (or named as a record kind), so the sweep below covers it
    final Set<Type> records = Set.of(Type.LINK, Type.EMBEDDED);
    for (final Type type : Type.values())
      if (!records.contains(type))
        assertThat(samplesByType()).as("a sample of %s", type).containsKey(type);
  }

  @Test
  void equalsAndDistinctAgreeOnEveryPairOfValues() {
    final List<Object> values = new ArrayList<>();
    for (final List<Object> sample : samplesByType().values())
      values.addAll(sample);

    final List<String> disagreements = new ArrayList<>();
    for (int i = 0; i < values.size(); i++)
      for (int j = i; j < values.size(); j++) {
        final Map<String, Object> parameters = Map.of("a", values.get(i), "b", values.get(j));
        final boolean equal = Boolean.TRUE.equals(scalar("RETURN $a = $b AS v", parameters));
        final boolean oneValue = Long.valueOf(1L).equals(scalar("UNWIND [$a, $b] AS x RETURN count(DISTINCT x) AS v", parameters));
        final boolean oneGroup = Long.valueOf(1L).equals(scalar("UNWIND [$a, $b] AS x WITH x AS g, count(*) AS c RETURN count(*) AS v", parameters));
        if (equal != oneValue || equal != oneGroup)
          disagreements.add(describe(values.get(i)) + " vs " + describe(values.get(j)) + ": = " + equal + ", DISTINCT " + oneValue + ", GROUP BY " + oneGroup);
      }

    assertThat(disagreements).as("pairs on which = and the duplicate elimination disagree").isEmpty();
  }

  private static String describe(final Object value) {
    return value.getClass().getSimpleName() + (value.getClass().isArray() ? "" : " " + value);
  }

  // ---------------------------------------------------------------------------------------------------- the key is the same one

  @Test
  void theKeyOfAValueNeedingNoKeyIsTheValueItself() {
    assertThat(DistinctNumericKey.canonicalize("text")).isEqualTo("text");
    assertThat(DistinctNumericKey.canonicalize(true)).isEqualTo(true);
    assertThat(DistinctNumericKey.canonicalize(null)).isNull();
  }

  @Test
  void needsKeyAnswersForEveryKindCanonicalizeChanges() {
    final Map<String, Object> samples = new LinkedHashMap<>();
    samples.put("LocalDateTime", LocalDateTime.of(2026, 1, 2, 3, 4, 5));
    samples.put("ZonedDateTime", ZonedDateTime.of(LocalDateTime.of(2026, 1, 2, 3, 4, 5), ZoneOffset.UTC));
    samples.put("Date", new Date(0));
    samples.put("Instant", Instant.EPOCH);
    samples.put("LocalDate", LocalDate.EPOCH);
    samples.put("Duration", Duration.ofHours(1));
    samples.put("Set", Set.of(1, 2));
    for (final Map.Entry<String, Object> sample : samples.entrySet())
      assertThat(DistinctNumericKey.needsKey(sample.getValue())).as(sample.getKey()).isTrue();
    assertThat(DistinctNumericKey.needsKey("text")).isFalse();
  }

  // ---------------------------------------------------------------------------------------------------------------------- helpers

  private Object scalar(final String query) {
    return scalar(query, Map.of());
  }

  private Object scalar(final String query, final Map<String, Object> parameters) {
    try (final ResultSet resultSet = database.query("opencypher", query, parameters)) {
      final Result row = resultSet.next();
      return row.getProperty("v");
    }
  }

  private List<Result> rows(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      return resultSet.stream().toList();
    }
  }
}
