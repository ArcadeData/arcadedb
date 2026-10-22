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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10595: SQL {@code =} on a list or a map compared a temporal element with Java {@code equals()}, so {@code [<Date>]} was not
 * equal to the stored {@code [<LocalDateTime>]} of the same instant, where the scalar {@code =}, {@code CONTAINS},
 * {@code CONTAINSALL} and openCypher all say it is - and {@code <>} answered the same row, asserting the two differ. The numeric
 * half (#10448) routed the list and map arms into the comparator that knows numbers; the scalar rule (a {@code Calendar} is its
 * instant, a value is converted to the class of the other side) was never applied to an element.
 * <p>
 * The rule the engine states is: two values that {@code =} reports equal are equal as elements of a list or a map too. Both halves
 * are pinned: the statements of the report, and the invariant over a pool of values of every temporal representation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10595ListMapTemporalEqualityTest extends TestHelper {

  private static final LocalDateTime STORED  = LocalDateTime.of(2020, 1, 2, 3, 4, 5);
  private static final Instant       INSTANT = STORED.toInstant(ZoneOffset.UTC);

  /** One instant, as every representation a client can bind it in. */
  private static Map<String, Object> representations() {
    final Map<String, Object> reps = new LinkedHashMap<>();
    reps.put("LocalDateTime", STORED);
    reps.put("java.util.Date", Date.from(INSTANT));
    reps.put("Instant", INSTANT);
    final Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("Asia/Tokyo"));
    calendar.setTimeInMillis(INSTANT.toEpochMilli());
    reps.put("Calendar", calendar);
    reps.put("java.sql.Timestamp", Timestamp.from(INSTANT));
    reps.put("epoch Long", INSTANT.toEpochMilli());
    reps.put("ISO String", "2020-01-02T03:04:05");
    return reps;
  }

  private void createStoredValues() {
    database.command("sql", "CREATE VERTEX TYPE L").close();
    database.transaction(() -> {
      database.newVertex("L").set("k", "l").set("v", new ArrayList<>(List.of(STORED))).save();
      database.newVertex("L").set("k", "s").set("v", STORED).save();
      database.newVertex("L").set("k", "m").set("v", new LinkedHashMap<>(Map.of("x", STORED))).save();
      database.newVertex("L").set("k", "n").set("v", new ArrayList<>(List.of(new ArrayList<>(List.of(STORED))))).save();
    });
  }

  private long count(final String sql, final Object... params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      return rs.stream().count();
    }
  }

  @Test
  void aListEqualsTheStoredListForEveryRepresentationTheScalarEqualityAccepts() {
    createStoredValues();
    for (final Map.Entry<String, Object> rep : representations().entrySet()) {
      final long scalar = count("SELECT FROM L WHERE k = 's' AND v = ?", rep.getValue());
      assertThat(scalar).as("the scalar = of %s is the oracle", rep.getKey()).isEqualTo(1L);
      assertThat(count("SELECT FROM L WHERE k = 's' AND ? = v", rep.getValue())).as("the scalar = of %s, reversed, is the oracle too",
          rep.getKey()).isEqualTo(1L);

      assertThat(count("SELECT FROM L WHERE k = 'l' AND v = ?", List.of(rep.getValue()))).as("[%s] =", rep.getKey()).isEqualTo(scalar);
      // the same value in the other implementation of List: no conversion step is skipped for a pair of one class
      assertThat(count("SELECT FROM L WHERE k = 'l' AND v = ?", new ArrayList<>(List.of(rep.getValue())))).as("ArrayList [%s] =",
          rep.getKey()).isEqualTo(scalar);
      assertThat(count("SELECT FROM L WHERE k = 'l' AND ? = v", List.of(rep.getValue()))).as("[%s] = (reversed)", rep.getKey())
          .isEqualTo(scalar);
    }
  }

  @Test
  void aMapEqualsTheStoredMapForEveryRepresentation() {
    createStoredValues();
    for (final Map.Entry<String, Object> rep : representations().entrySet()) {
      assertThat(count("SELECT FROM L WHERE k = 'm' AND v = ?", Map.of("x", rep.getValue()))).as("{x: %s} =", rep.getKey()).isEqualTo(1L);
      assertThat(count("SELECT FROM L WHERE k = 'm' AND ? = v", Map.of("x", rep.getValue()))).as("{x: %s} = (reversed)", rep.getKey())
          .isEqualTo(1L);
    }
  }

  @Test
  void nestedCollectionsFollowTheSameRule() {
    createStoredValues();
    for (final Map.Entry<String, Object> rep : representations().entrySet())
      assertThat(count("SELECT FROM L WHERE k = 'n' AND v = ?", List.of(List.of(rep.getValue())))).as("[[%s]] =", rep.getKey())
          .isEqualTo(1L);
  }

  @Test
  void inContainsAndNotEqualsAgreeWithEquals() {
    createStoredValues();
    for (final Map.Entry<String, Object> rep : representations().entrySet()) {
      final String name = rep.getKey();
      final List<Object> asList = List.of(rep.getValue());
      assertThat(count("SELECT FROM L WHERE k = 'l' AND v IN ?", List.of(asList))).as("IN [[%s]]", name).isEqualTo(1L);
      assertThat(count("SELECT FROM L WHERE k = 'l' AND ? CONTAINS v", List.of(asList))).as("[[%s]] CONTAINS v", name).isEqualTo(1L);
      assertThat(count("SELECT FROM L WHERE k = 'l' AND v CONTAINSALL ?", asList)).as("CONTAINSALL [%s]", name).isEqualTo(1L);
      assertThat(count("SELECT FROM L WHERE k = 'l' AND v <> ?", asList)).as("<> [%s]", name).isZero();
      assertThat(count("SELECT FROM L WHERE k = 'l' AND NOT (v = ?)", asList)).as("NOT (= [%s])", name).isZero();
      assertThat(count("SELECT FROM L WHERE k = 'm' AND v <> ?", Map.of("x", rep.getValue()))).as("<> {x: %s}", name).isZero();
    }
  }

  @Test
  void anotherInstantStaysDifferent() {
    createStoredValues();
    final Date other = Date.from(INSTANT.plusSeconds(1));
    assertThat(count("SELECT FROM L WHERE k = 'l' AND v = ?", List.of(other))).isZero();
    assertThat(count("SELECT FROM L WHERE k = 'l' AND v <> ?", List.of(other))).isEqualTo(1L);
    assertThat(count("SELECT FROM L WHERE k = 'l' AND v = ?", List.of(Date.from(INSTANT), other))).as("size").isZero();
    assertThat(count("SELECT FROM L WHERE k = 'm' AND v = ?", Map.of("x", other))).isZero();
    assertThat(count("SELECT FROM L WHERE k = 'n' AND v = ?", List.of(List.of(other)))).isZero();
  }

  @Test
  void aListWithAnElementOfAnotherKindStaysDifferent() {
    createStoredValues();
    assertThat(count("SELECT FROM L WHERE k = 'l' AND v = ?", List.of("not a date"))).isZero();
    assertThat(count("SELECT FROM L WHERE k = 'l' AND v = ?", List.of(true))).isZero();
    assertThat(count("SELECT FROM L WHERE k = 'l' AND v = ?", List.of(List.of(INSTANT)))).as("a list is not its element").isZero();
  }

  /**
   * The invariant, over a pool of values rather than the cases of the report: whatever the scalar {@code =} says of a pair, the
   * same pair as the elements of two lists, of two maps, or nested says too - in both directions and in both implementations
   * of the collection.
   */
  @Test
  void theScalarRuleHoldsForEveryPairOfAPoolAsAnElement() {
    final List<Object> pool = new ArrayList<>(representations().values());
    pool.addAll(List.of(Date.from(INSTANT.plusSeconds(1)), STORED.plusSeconds(1), INSTANT.plusMillis(1), LocalDate.of(2020, 1, 2),
        Date.from(LocalDate.of(2020, 1, 2).atStartOfDay().toInstant(ZoneOffset.UTC)), ZonedDateTime.of(STORED, ZoneId.of("UTC")),
        OffsetDateTime.of(STORED, ZoneOffset.UTC), 1, 1L, (short) 1, 1.0f, 1.0d, new BigDecimal("1.00"), "1", "a", true, false,
        new byte[] { 1 }, new byte[] { 1 }, new RID(1, 2), new RID(1, 3), List.of(1), Map.of("a", 1)));

    int agreed = 0;
    for (final Object a : pool)
      for (final Object b : pool) {
        final boolean scalar = QueryOperatorEquals.equals(a, b);
        final String pair = a.getClass().getSimpleName() + " " + a + " vs " + b.getClass().getSimpleName() + " " + b;

        assertThat(QueryOperatorEquals.equals(List.of(a), List.of(b))).as("[a] = [b] for " + pair).isEqualTo(scalar);
        assertThat(QueryOperatorEquals.equals(new ArrayList<>(List.of(a)), new ArrayList<>(List.of(b)))).as("ArrayLists for " + pair)
            .isEqualTo(scalar);
        assertThat(QueryOperatorEquals.equals(Map.of("x", a), Map.of("x", b))).as("{x: a} = {x: b} for " + pair).isEqualTo(scalar);
        assertThat(QueryOperatorEquals.equals(List.of(List.of(a)), List.of(List.of(b)))).as("[[a]] = [[b]] for " + pair)
            .isEqualTo(scalar);
        assertThat(QueryOperatorEquals.equals(List.of(a, a), List.of(b, b))).as("[a, a] = [b, b] for " + pair).isEqualTo(scalar);
        if (scalar)
          agreed++;
      }
    // not vacuous: every value is equal to itself and the representations of the instant are equal to each other
    assertThat(agreed).isGreaterThan(pool.size() + 20);
  }

  /**
   * {@code Timestamp.equals(Date)} is false for a {@code Date} that is not a {@code Timestamp}, where {@code Date.equals(Timestamp)} is
   * true: a Timestamp on the left of {@code =} was never equal to the same instant converted to a plain Date, so the answer depended
   * on the side the parameter stood on.
   */
  @Test
  void aTimestampAndADateOfTheSameInstantAreEqualWhicheverSideTheyStandOn() {
    final Timestamp timestamp = Timestamp.from(INSTANT);
    final Date date = Date.from(INSTANT);
    assertThat(BinaryComparator.equals(timestamp, date)).isTrue();
    assertThat(BinaryComparator.equals(date, timestamp)).isTrue();
    assertThat(BinaryComparator.equals(timestamp, Date.from(INSTANT.plusMillis(1)))).isFalse();
    // two Timestamps keep their own comparison, nanoseconds included
    assertThat(BinaryComparator.equals(Timestamp.from(INSTANT.plusNanos(1_000)), Timestamp.from(INSTANT))).isFalse();
    assertThat(BinaryComparator.equals(Timestamp.from(INSTANT), Timestamp.from(INSTANT))).isTrue();

    assertThat(QueryOperatorEquals.equals(timestamp, STORED)).isTrue();
    assertThat(QueryOperatorEquals.equals(STORED, timestamp)).isTrue();
  }

  @Test
  void nullElementsKeepTheirMeaning() {
    final List<Object> withNull = new ArrayList<>();
    withNull.add(null);
    final List<Object> otherNull = new ArrayList<>();
    otherNull.add(null);
    assertThat(QueryOperatorEquals.equals(withNull, otherNull)).as("[null] = [null], as a list is").isTrue();
    assertThat(QueryOperatorEquals.equals(withNull, List.of(1))).isFalse();
    assertThat(QueryOperatorEquals.equals(List.of(1), withNull)).isFalse();
  }
}
