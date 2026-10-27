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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10736: the element rule #10595 gave SQL {@code =} (two values {@code =} reports equal are equal as elements of a list or a map
 * too) was guarded on {@code java.util.List}, so a {@code Set} fell past it to the conversion fallback, became an {@code ArrayList}
 * and was compared with the comparator's own element rule: a {@code LinkedHashSet} holding a {@code Date} was not equal to the
 * stored {@code [LocalDateTime]} of the same instant, and {@code <>} answered the row.
 * <p>
 * The rule is now reached however the collection arrives: every {@code Collection} and every array but a {@code byte[]} (BINARY) on
 * either side, and the collection a conversion produces. The census of #10595 is widened by the dimension it missed: the container.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10736CollectionElementEqualityTest extends TestHelper {

  private static final LocalDateTime STORED  = LocalDateTime.of(2020, 1, 2, 3, 4, 5);
  private static final Instant       INSTANT = STORED.toInstant(ZoneOffset.UTC);

  /** Every container the engine can be handed on either side of {@code =}, as a function of its one element. */
  private static Map<String, Function<Object, Object>> containers() {
    final Map<String, Function<Object, Object>> containers = new LinkedHashMap<>();
    containers.put("List.of", List::of);
    containers.put("ArrayList", e -> new ArrayList<>(List.of(e)));
    containers.put("LinkedList", e -> new LinkedList<>(List.of(e)));
    containers.put("LinkedHashSet", e -> new LinkedHashSet<>(List.of(e)));
    containers.put("HashSet", e -> new HashSet<>(List.of(e)));
    containers.put("Set.of", Set::of);
    containers.put("TreeSet", e -> e instanceof Comparable<?> ? new TreeSet<>(List.of(e)) : null);
    containers.put("Object[]", e -> new Object[] { e });
    return containers;
  }

  private void createStoredValues() {
    database.command("sql", "CREATE VERTEX TYPE L").close();
    database.transaction(() -> {
      database.newVertex("L").set("k", "l").set("v", new ArrayList<>(List.of(STORED))).save();
      database.newVertex("L").set("k", "m").set("v", new LinkedHashMap<>(Map.of("x", new ArrayList<>(List.of(STORED))))).save();
      database.newVertex("L").set("k", "n").set("v", new ArrayList<>(List.of(new ArrayList<>(List.of(STORED))))).save();
    });
  }

  private long count(final String sql, final Object... params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      return rs.stream().count();
    }
  }

  @Test
  void aSetOfTheSameInstantEqualsTheStoredList() {
    createStoredValues();
    final Date date = Date.from(INSTANT);
    assertThat(count("SELECT FROM L WHERE k = 'l' AND v = ?", date)).as("the scalar oracle").isEqualTo(1L);
    assertThat(count("SELECT FROM L WHERE k = 'l' AND v = ?", List.of(date))).isEqualTo(1L);
    assertThat(count("SELECT FROM L WHERE k = 'l' AND v = ?", new LinkedHashSet<>(List.of(date)))).isEqualTo(1L);
    assertThat(count("SELECT FROM L WHERE k = 'l' AND v <> ?", new LinkedHashSet<>(List.of(date)))).isZero();
    assertThat(count("SELECT FROM L WHERE k = 'l' AND v = ?", (Object) new Date[] { date })).as("Date[]").isEqualTo(1L);
    assertThat(count("SELECT FROM L WHERE k = 'l' AND v = ?", (Object) new Object[] { date })).as("Object[]").isEqualTo(1L);
    assertThat(count("SELECT FROM L WHERE k = 'n' AND v = ?", List.of(new LinkedHashSet<>(List.of(date))))).as("nested").isEqualTo(1L);
    assertThat(count("SELECT FROM L WHERE k = 'm' AND v = ?", Map.of("x", new LinkedHashSet<>(List.of(date))))).as("map").isEqualTo(1L);
    assertThat(count("SELECT FROM L WHERE k = 'l' AND v IN ?", List.of(new LinkedHashSet<>(List.of(date))))).as("IN").isEqualTo(1L);

    // another instant stays different, in a Set too
    assertThat(count("SELECT FROM L WHERE k = 'l' AND v = ?", new LinkedHashSet<>(List.of(Date.from(INSTANT.plusSeconds(1)))))).isZero();
    assertThat(count("SELECT FROM L WHERE k = 'l' AND v <> ?", new LinkedHashSet<>(List.of(Date.from(INSTANT.plusSeconds(1))))))
        .isEqualTo(1L);
  }

  @Test
  void theDirectCallsOfTheReport() {
    final Date date = Date.from(INSTANT);
    assertThat(QueryOperatorEquals.equals(new ArrayList<>(List.of(STORED)), new LinkedHashSet<>(List.of(date)))).isTrue();
    assertThat(QueryOperatorEquals.equals(new LinkedHashSet<>(List.of(date)), new ArrayList<>(List.of(STORED)))).isTrue();
    assertThat(QueryOperatorEquals.equals(new ArrayList<>(List.of(STORED)), List.of(date))).isTrue();
    // a list against the scalar it holds converts the scalar into a list: that list is compared under the same rule
    assertThat(QueryOperatorEquals.equals(new ArrayList<>(List.of(STORED)), date)).isTrue();
  }

  @Test
  void twoSetsAreEqualWhateverTheirOrderAndUnderTheElementRule() {
    assertThat(QueryOperatorEquals.equals(new LinkedHashSet<>(List.of(1, 2)), new LinkedHashSet<>(List.of(2L, 1L)))).isTrue();
    assertThat(QueryOperatorEquals.equals(new HashSet<>(List.of(STORED, "a")), new LinkedHashSet<>(List.of("a", Date.from(INSTANT)))))
        .isTrue();
    assertThat(QueryOperatorEquals.equals(new HashSet<>(List.of(1, 2)), new HashSet<>(List.of(1, 3)))).isFalse();
    assertThat(QueryOperatorEquals.equals(new HashSet<>(List.of(1, 2)), new HashSet<>(List.of(1)))).isFalse();
    // a list keeps its order against a list, and against a set it is compared in the set's iteration order, as before
    assertThat(QueryOperatorEquals.equals(List.of(1, 2), List.of(2, 1))).isFalse();
    assertThat(QueryOperatorEquals.equals(List.of(1, 2), new LinkedHashSet<>(List.of(1L, 2L)))).isTrue();
    assertThat(QueryOperatorEquals.equals(List.of(1, 2), new LinkedHashSet<>(List.of(2L, 1L)))).as("the set's order").isFalse();
    assertThat(QueryOperatorEquals.equals(new LinkedHashSet<>(List.of(2L, 1L)), List.of(1, 2))).as("either side").isFalse();
    // the comparator holds one rule for both kinds of collection too: a Long and an Integer are one value
    assertThat(BinaryComparator.equals(new HashSet<>(List.of(1L, 2L)), new LinkedHashSet<>(List.of(2, 1)))).isTrue();
  }

  /**
   * The census of #10595 ({@code theScalarRuleHoldsForEveryPairOfAPoolAsAnElement}) widened by the container: whatever the scalar
   * {@code =} says of a pair, the same pair held in any two containers, nested in a list, or as the value of a map, says too.
   */
  @Test
  void theScalarRuleHoldsForEveryPairOfAPoolInEveryPairOfContainers() {
    final List<Object> pool = new ArrayList<>();
    pool.add(STORED);
    pool.add(Date.from(INSTANT));
    pool.add(INSTANT);
    final Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("Asia/Tokyo"));
    calendar.setTimeInMillis(INSTANT.toEpochMilli());
    pool.add(calendar);
    pool.add(Timestamp.from(INSTANT));
    pool.add(INSTANT.toEpochMilli());
    pool.add("2020-01-02T03:04:05");
    pool.addAll(List.of(Date.from(INSTANT.plusSeconds(1)), STORED.plusSeconds(1), LocalDate.of(2020, 1, 2),
        ZonedDateTime.of(STORED, ZoneId.of("UTC")), OffsetDateTime.of(STORED, ZoneOffset.UTC), 1, 1L, (short) 1, 1.0f, 1.0d,
        new BigDecimal("1.00"), "1", "a", true, false, new byte[] { 1 }, new RID(1, 2), new RID(1, 3)));

    final Map<String, Function<Object, Object>> containers = containers();
    int agreed = 0;
    for (final Object a : pool)
      for (final Object b : pool) {
        final boolean scalar = QueryOperatorEquals.equals(a, b);
        final String pair = a.getClass().getSimpleName() + " " + a + " vs " + b.getClass().getSimpleName() + " " + b;
        for (final Map.Entry<String, Function<Object, Object>> left : containers.entrySet())
          for (final Map.Entry<String, Function<Object, Object>> right : containers.entrySet()) {
            final Object ca = left.getValue().apply(a);
            final Object cb = right.getValue().apply(b);
            if (ca == null || cb == null)
              continue;
            final String as = left.getKey() + " = " + right.getKey() + " for " + pair;
            assertThat(QueryOperatorEquals.equals(ca, cb)).as(as).isEqualTo(scalar);
            assertThat(QueryOperatorEquals.equals(List.of(ca), List.of(cb))).as("nested " + as).isEqualTo(scalar);
            assertThat(QueryOperatorEquals.equals(Map.of("x", ca), Map.of("x", cb))).as("map of " + as).isEqualTo(scalar);
          }
        if (scalar)
          agreed++;
      }
    // not vacuous: every value is equal to itself and the representations of the instant are equal to each other
    assertThat(agreed).isGreaterThan(pool.size() + 20);
  }
}
