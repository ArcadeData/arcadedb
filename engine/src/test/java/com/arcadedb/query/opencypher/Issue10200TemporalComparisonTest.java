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
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10200: Cypher comparison and ordering of temporal values differed from Neo4j. Every expected value below was
 * checked against Neo4j 2026.09 (the text is ArcadeDB's, which drops zero seconds).
 * <ul>
 *   <li>two durations compared with {@code <}/{@code >}: they are not comparable, only equal or not;</li>
 *   <li>{@code ORDER BY}, {@code min()} and {@code max()} ordered durations component by component instead of by length;</li>
 *   <li>a {@code localdatetime()} equalled the {@code datetime()} at the same wall clock time;</li>
 *   <li>{@code ORDER BY} over two temporal types threw instead of ordering by type.</li>
 * </ul>
 * A DATETIME read from storage, which ArcadeDB keeps as an instant and reads back as a local datetime, still compares with
 * a zoned datetime by instant, so filters on stored DATETIME properties keep working.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10200TemporalComparisonTest extends TestHelper {

  @Test
  void durationsAreEqualOrNotButNotComparable() {
    assertThat(value("duration('P1D') < duration('P2D')")).isNull();
    assertThat(value("duration('P1D') > duration('PT24H')")).isNull();
    assertThat(value("duration('P1D') <= duration('P2D')")).isNull();
    assertThat(value("duration('P1D') < duration('P1D')")).isNull();
    assertThat(value("duration('P1D') <= duration('PT24H')")).isNull();
    assertThat(value("duration('PT1H') < duration('PT2H')")).isNull();
    assertThat(value("[duration('P1D')] < [duration('P2D')]")).isNull();
    // Equality settles what it can
    assertThat(value("duration('P1D') <= duration('P1D')")).isEqualTo(true);
    assertThat(value("duration('P1D') >= duration('P1D')")).isEqualTo(true);
    assertThat(value("duration('P1D') = duration('P1D')")).isEqualTo(true);
    assertThat(value("duration('P1D') = duration('PT24H')")).isEqualTo(false);
    assertThat(value("duration('P1D') <> duration('PT24H')")).isEqualTo(true);
    assertThat(value("duration('P1D') IN [duration('PT24H')]")).isEqualTo(false);
    assertThat(value("duration('P1D') IN [duration('PT24H'), duration('P1D')]")).isEqualTo(true);
    assertThat(value("[duration('P1D'), 1] = [duration('P1D'), 1]")).isEqualTo(true);
    assertThat(value("duration('P1D') = date('2024-01-01')")).isEqualTo(false);
    assertThat(value("duration('P1D') < date('2024-01-01')")).isNull();
  }

  @Test
  void durationsOrderByAverageLengthThenMonthsThenDays() {
    assertThat(sorted("duration('P1D'), duration('PT25H')")).containsExactly("P1D", "PT25H");
    assertThat(sorted("duration('P1M'), duration('P31D')")).containsExactly("P1M", "P31D");
    assertThat(sorted("duration('P1D'), duration('PT23H')")).containsExactly("PT23H", "P1D");
    // The same average length: fewer months first, then fewer days
    assertThat(sorted("duration('P1D'), duration('PT24H')")).containsExactly("PT24H", "P1D");
    assertThat(sorted("duration('P30D'), duration('P1M'), duration('PT720H')")).containsExactly("PT720H", "P30D", "P1M");
    assertThat(sorted("duration('P1M'), duration('P30DT10H29M6S'), duration('PT730H29M6S')"))
        .containsExactly("PT730H29M6S", "P30DT10H29M6S", "P1M");
    assertThat(sorted("duration('P1DT-1S'), duration('PT86399S')")).containsExactly("PT23H59M59S", "P1DT-1S");
    assertThat(sorted("duration('PT0.5S'), duration('PT0S'), duration('-PT1S')")).containsExactly("PT-1S", "PT0S", "PT0.5S");

    final List<String> descending = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "UNWIND [duration('P1D'), duration('PT24H')] AS x RETURN toString(x) AS v ORDER BY x DESC")) {
      while (rs.hasNext())
        descending.add(rs.next().getProperty("v"));
    }
    assertThat(descending).containsExactly("P1D", "PT24H");
  }

  @Test
  void minAndMaxFollowTheSameOrder() {
    assertThat(text("max(x)", "duration('P1D'), duration('PT25H')")).isEqualTo("PT25H");
    assertThat(text("min(x)", "duration('P1M'), duration('P1D')")).isEqualTo("P1D");
    assertThat(text("max(x)", "duration('P1D'), duration('PT24H')")).isEqualTo("P1D");
    assertThat(text("min(x)", "duration('PT24H'), duration('P1D')")).isEqualTo("PT24H");
    try (final ResultSet rs = database.query("opencypher",
        "RETURN toString(coll.max([duration('P1M'), duration('P31D')])) AS hi, toString(coll.min([duration('P1M'), duration('P31D')])) AS lo")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("hi")).isEqualTo("P31D");
      assertThat(row.<String>getProperty("lo")).isEqualTo("P1M");
    }
  }

  @Test
  void aLocalAndAZonedDatetimeAreTwoTypes() {
    assertThat(value("localdatetime('2024-01-01T12:00') = datetime('2024-01-01T12:00Z')")).isEqualTo(false);
    assertThat(value("localdatetime('2024-01-01T12:00') <> datetime('2024-01-01T12:00Z')")).isEqualTo(true);
    assertThat(value("localdatetime('2024-01-01T12:00') < datetime('2024-01-01T13:00Z')")).isNull();
    assertThat(value("localdatetime('2024-01-01T12:00') <= datetime('2024-01-01T12:00Z')")).isNull();
    assertThat(value("datetime('2024-01-01T13:00Z') > localdatetime('2024-01-01T12:00')")).isNull();
    assertThat(value("localdatetime('2024-01-01T12:00') IN [datetime('2024-01-01T12:00Z')]")).isEqualTo(false);
    // The controls that already agreed
    assertThat(value("date('2024-01-01') < datetime('2024-01-01T13:00Z')")).isNull();
    assertThat(value("date('2024-01-01') = datetime('2024-01-01T00:00Z')")).isEqualTo(false);
    assertThat(value("localtime('12:00') < time('13:00Z')")).isNull();
    assertThat(value("time('12:00Z') = localtime('12:00')")).isEqualTo(false);
    assertThat(value("localdatetime('2024-01-01T12:00') >= localdatetime('2024-01-01T12:00')")).isEqualTo(true);
    assertThat(value("time('12:00+01:00') = time('11:00Z')")).isEqualTo(false);
    assertThat(value("time('12:00+01:00') < time('11:30Z')")).isEqualTo(true);
  }

  /** The design point of the fix: a DATETIME column is an instant, so it still meets a zoned datetime as one. */
  @Test
  void aStoredDatetimeStillComparesByInstant() {
    database.getSchema().createVertexType("Event").createProperty("at", Type.DATETIME);
    database.transaction(() -> {
      database.newVertex("Event").set("name", "a", "at", LocalDateTime.of(2024, 1, 1, 12, 0)).save();
      database.newVertex("Event").set("name", "b", "at", LocalDateTime.of(2024, 1, 1, 14, 0)).save();
    });

    assertThat(names("MATCH (e:Event) WHERE e.at > datetime('2024-01-01T13:00Z') RETURN e.name AS name")).containsExactly("b");
    assertThat(names("MATCH (e:Event) WHERE e.at = datetime('2024-01-01T13:00+01:00') RETURN e.name AS name")).containsExactly("a");
    assertThat(names("MATCH (e:Event) WITH e, e.at AS at WHERE at < datetime('2024-01-01T13:00Z') RETURN e.name AS name"))
        .containsExactly("a");
    try (final ResultSet rs = database.query("opencypher", "MATCH (e:Event) WHERE e.at >= $from RETURN e.name AS name",
        Map.of("from", ZonedDateTime.of(2024, 1, 1, 13, 0, 0, 0, ZoneOffset.UTC)))) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("b");
      assertThat(rs.hasNext()).isFalse();
    }
    // A java.time.LocalDateTime parameter is the same raw value a DATETIME read gives: it compares with a zoned datetime by
    // instant too (a deliberate deviation from Neo4j, which answers null), where a localdatetime() of the query does not
    try (final ResultSet rs = database.query("opencypher", "RETURN $p = datetime('2024-01-01T12:00Z') AS v, $p < datetime('2024-01-01T13:00Z') AS w",
        Map.of("p", LocalDateTime.of(2024, 1, 1, 12, 0)))) {
      final Result row = rs.next();
      assertThat(row.<Boolean>getProperty("v")).isTrue();
      assertThat(row.<Boolean>getProperty("w")).isTrue();
    }
    // Against a local datetime of the query, a stored DATETIME is a local datetime like any other
    assertThat(names("MATCH (e:Event) WHERE e.at = localdatetime('2024-01-01T14:00') RETURN e.name AS name")).containsExactly("b");
    // A value derived from it is an ordinary local datetime
    assertThat(names("MATCH (e:Event) WHERE e.at + duration('PT1H') = datetime('2024-01-01T13:00Z') RETURN e.name AS name")).isEmpty();
  }

  @Test
  void valuesOfDifferentTemporalTypesOrderByType() {
    assertThat(sorted("date('2024-01-01'), datetime('2024-01-01T00:00Z')")).containsExactly("2024-01-01T00:00Z", "2024-01-01");
    assertThat(sorted("time('12:00Z'), localtime('12:00')")).containsExactly("12:00Z", "12:00");
    assertThat(sorted("localdatetime('2024-01-01T12:00'), datetime('2024-01-01T12:00Z')"))
        .containsExactly("2024-01-01T12:00Z", "2024-01-01T12:00");
    assertThat(sorted("duration('P1M'), localtime('12:00'), time('12:00Z'), date('2024-01-01'), localdatetime('2024-01-01T00:00'), "
        + "datetime('2024-01-01T00:00Z')"))
        .containsExactly("2024-01-01T00:00Z", "2024-01-01T00:00", "2024-01-01", "12:00Z", "12:00", "P1M");
    // Between PATH and STRING, as Neo4j orders them
    final List<String> mixed = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "UNWIND [duration('P1M'), 'a', 1, true, date('2024-01-01'), [1]] AS x RETURN x AS v ORDER BY x")) {
      while (rs.hasNext())
        mixed.add(String.valueOf(rs.next().<Object>getProperty("v")));
    }
    assertThat(mixed).containsExactly("[1]", "2024-01-01", "P1M", "a", "true", "1");
    // One temporal type still orders by value
    assertThat(sorted("datetime('2024-01-01T12:00+01:00'), datetime('2024-01-01T11:00Z')"))
        .containsExactly("2024-01-01T11:00Z", "2024-01-01T12:00+01:00");
  }

  /** A native java.time / java.util.Date parameter orders by value among Cypher temporals of its type. */
  @Test
  void aNativeTemporalParameterOrdersByValue() {
    assertThat(sortedNames(Map.of("p", Duration.ofHours(25)), "[$p, 'param'], [duration('P1D'), 'literal']")).containsExactly("literal", "param");
    assertThat(sortedNames(Map.of("p", Duration.ofHours(23)), "[$p, 'param'], [duration('P1D'), 'literal']")).containsExactly("param", "literal");
    assertThat(sortedNames(Map.of("p", Period.ofMonths(1)), "[$p, 'param'], [duration('P31D'), 'literal']")).containsExactly("param", "literal");
    final Date noon = Date.from(ZonedDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC).toInstant());
    assertThat(sortedNames(Map.of("p", noon), "[$p, 'param'], [datetime('2024-01-01T11:00Z'), 'literal']")).containsExactly("literal", "param");
    assertThat(sortedNames(Map.of("p", noon), "[$p, 'param'], [datetime('2024-01-01T13:00Z'), 'literal']")).containsExactly("param", "literal");
  }

  private List<String> sortedNames(final Map<String, Object> parameters, final String pairs) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "UNWIND [" + pairs + "] AS pair WITH pair[0] AS x, pair[1] AS name RETURN name ORDER BY x", parameters)) {
      while (rs.hasNext())
        out.add(rs.next().getProperty("name"));
    }
    return out;
  }

  @Test
  void minAndMaxOverMixedTemporalTypesDoNotThrow() {
    assertThat(text("max(x)", "date('2024-01-01'), datetime('2024-01-01T00:00Z'), duration('P1D')")).isEqualTo("P1D");
    assertThat(text("min(x)", "date('2024-01-01'), datetime('2024-01-01T00:00Z'), duration('P1D')")).isEqualTo("2024-01-01T00:00Z");
  }

  /** The whole Cypher order of types, so a type added to the rank table cannot silently reorder the others. */
  @Test
  void theOrderOfTypesIsPinned() {
    final Object[] ascending = { Map.of("a", 1), List.of(1), ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
        LocalDateTime.of(2024, 1, 1, 0, 0), LocalDate.of(2024, 1, 1), OffsetTime.of(12, 0, 0, 0, ZoneOffset.UTC),
        LocalTime.of(12, 0), CypherDuration.parse("P1D"), "a", true, 1, Double.NaN, null };
    for (int i = 1; i < ascending.length; i++)
      assertThat(CypherFunctionHelper.cypherTypeRank(ascending[i - 1])).as("%s before %s", ascending[i - 1], ascending[i])
          .isLessThan(CypherFunctionHelper.cypherTypeRank(ascending[i]));
  }

  private Object value(final String expression) {
    try (final ResultSet rs = database.query("opencypher", "RETURN " + expression + " AS v")) {
      return rs.next().getProperty("v");
    }
  }

  private String text(final String aggregate, final String values) {
    try (final ResultSet rs = database.query("opencypher", "UNWIND [" + values + "] AS x RETURN toString(" + aggregate + ") AS v")) {
      return rs.next().getProperty("v");
    }
  }

  private List<String> sorted(final String values) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "UNWIND [" + values + "] AS x RETURN toString(x) AS v ORDER BY x")) {
      while (rs.hasNext())
        out.add(rs.next().getProperty("v"));
    }
    return out;
  }

  private List<String> names(final String query) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        out.add(rs.next().getProperty("name"));
    }
    return out;
  }
}
