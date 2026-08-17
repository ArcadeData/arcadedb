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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6188: {@code WHERE @rid > :p} bound to a RID, or to its string spelling, answered zero rows while the
 * literal form {@code WHERE @rid > #x:y} was correct.
 * <p>
 * Root cause: {@link com.arcadedb.query.sql.parser.GtOperator} alone among the range operators refused any
 * comparison between an {@code Identifiable} left operand and a non-{@code Identifiable} right operand, even
 * though {@link RID#compareTo(Object)} already knows how to compare a RID against its string spelling. A record's
 * {@code @rid} resolves to a {@code DatabaseRID}, a subclass of {@code RID} that {@link com.arcadedb.schema.Type#convert}
 * does not recognize (it only special-cases an exact {@code RID.class} target), so the attempted string-to-RID
 * coercion silently failed and left a bare {@code String} on the right - which the now-removed guard then rejected
 * outright. {@code <}, {@code >=} and {@code <=} never had that guard and were unaffected.
 */
class Issue6188RidRangeBoundParameterTest extends TestHelper {

  private RID lowRid;
  private List<RID> allRidsAscending;
  private int expectedGreaterThanLow;
  private int expectedGreaterOrEqualLow;
  private int expectedLessThanLow;
  private int expectedLessOrEqualLow;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc", 4);

    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        database.newDocument("Doc").set("i", i).save();
    });

    // Pick a RID roughly in the middle of the RID space so every range below is meaningfully selective.
    final ResultSet all = database.query("sql", "SELECT @rid AS r FROM Doc ORDER BY @rid");
    allRidsAscending = new ArrayList<>();
    while (all.hasNext())
      allRidsAscending.add(all.next().getProperty("r"));
    all.close();

    lowRid = allRidsAscending.get(allRidsAscending.size() / 2);

    for (final RID r : allRidsAscending) {
      final int cmp = r.compareTo(lowRid);
      if (cmp > 0)
        expectedGreaterThanLow++;
      if (cmp >= 0)
        expectedGreaterOrEqualLow++;
      if (cmp < 0)
        expectedLessThanLow++;
      if (cmp <= 0)
        expectedLessOrEqualLow++;
    }

    assertThat(expectedGreaterThanLow).as("fixture must be selective for the test to mean anything").isGreaterThan(0);
    assertThat(expectedLessThanLow).as("fixture must be selective for the test to mean anything").isGreaterThan(0);
  }

  private long count(final String sql, final Object paramValue) {
    final ResultSet rs = paramValue == null ? database.query("sql", sql) : database.query("sql", sql, Map.of("p", paramValue));
    assertThat(rs.hasNext()).isTrue();
    final long count = rs.next().<Number>getProperty("c").longValue();
    rs.close();
    return count;
  }

  @Test
  void literalRidRangeReturnsCorrectCounts() {
    assertThat(count("SELECT count() as c FROM Doc WHERE @rid > " + lowRid, null)).isEqualTo(expectedGreaterThanLow);
    assertThat(count("SELECT count() as c FROM Doc WHERE @rid >= " + lowRid, null)).isEqualTo(expectedGreaterOrEqualLow);
    assertThat(count("SELECT count() as c FROM Doc WHERE @rid < " + lowRid, null)).isEqualTo(expectedLessThanLow);
    assertThat(count("SELECT count() as c FROM Doc WHERE @rid <= " + lowRid, null)).isEqualTo(expectedLessOrEqualLow);
  }

  @Test
  void boundRidObjectParameterMatchesLiteralOnEveryRangeOperator() {
    assertThat(count("SELECT count() as c FROM Doc WHERE @rid > :p", lowRid)).as(">").isEqualTo(expectedGreaterThanLow);
    assertThat(count("SELECT count() as c FROM Doc WHERE @rid >= :p", lowRid)).as(">=").isEqualTo(expectedGreaterOrEqualLow);
    assertThat(count("SELECT count() as c FROM Doc WHERE @rid < :p", lowRid)).as("<").isEqualTo(expectedLessThanLow);
    assertThat(count("SELECT count() as c FROM Doc WHERE @rid <= :p", lowRid)).as("<=").isEqualTo(expectedLessOrEqualLow);
  }

  @Test
  void boundRidStringParameterMatchesLiteralOnEveryRangeOperator() {
    final String p = lowRid.toString();
    assertThat(count("SELECT count() as c FROM Doc WHERE @rid > :p", p)).as(">").isEqualTo(expectedGreaterThanLow);
    assertThat(count("SELECT count() as c FROM Doc WHERE @rid >= :p", p)).as(">=").isEqualTo(expectedGreaterOrEqualLow);
    assertThat(count("SELECT count() as c FROM Doc WHERE @rid < :p", p)).as("<").isEqualTo(expectedLessThanLow);
    assertThat(count("SELECT count() as c FROM Doc WHERE @rid <= :p", p)).as("<=").isEqualTo(expectedLessOrEqualLow);
  }

  @Test
  void boundParameterCursorPagingReturnsEveryRowExactlyOnce() {
    // The shape called out in the issue: paging through a type with WHERE @rid > :last ORDER BY @rid LIMIT n.
    // Every row must be visited exactly once and the walk must terminate.
    final List<RID> visited = new ArrayList<>();
    Object cursor = null;
    for (int guard = 0; guard < allRidsAscending.size() + 2; guard++) {
      final String sql = cursor == null
          ? "SELECT @rid AS r FROM Doc ORDER BY @rid LIMIT 7"
          : "SELECT @rid AS r FROM Doc WHERE @rid > :p ORDER BY @rid LIMIT 7";
      final ResultSet rs = cursor == null ? database.query("sql", sql) : database.query("sql", sql, Map.of("p", cursor.toString()));
      int fetched = 0;
      RID last = null;
      while (rs.hasNext()) {
        last = rs.next().getProperty("r");
        visited.add(last);
        fetched++;
      }
      rs.close();
      if (fetched == 0)
        break;
      cursor = last;
    }

    assertThat(visited).as("cursor paging with a bound, string-spelled RID must not stop after the first page")
        .containsExactlyElementsOf(allRidsAscending);
  }

  @Test
  void malformedRidStringParameterIsSafelyIncomparableRatherThanThrowing() {
    // Review follow-up on #6188: a right-hand String that isn't RID-shaped reaches RID#compareTo(Object), which
    // parses it as "#bucket:position" and throws IllegalArgumentException/NumberFormatException for anything
    // that doesn't fit. All four range operators must report "not comparable" (false) rather than let that parse
    // failure escape mid-scan - the same "no defined ordering" contract this file already applies to a numeric
    // conversion failure (#5900).
    for (final String malformed : List.of("not-a-rid", "1:2", "#1", "#abc:def")) {
      assertThat(count("SELECT count() as c FROM Doc WHERE @rid > :p", malformed)).as("> %s", malformed).isEqualTo(0);
      assertThat(count("SELECT count() as c FROM Doc WHERE @rid >= :p", malformed)).as(">= %s", malformed).isEqualTo(0);
      assertThat(count("SELECT count() as c FROM Doc WHERE @rid < :p", malformed)).as("< %s", malformed).isEqualTo(0);
      assertThat(count("SELECT count() as c FROM Doc WHERE @rid <= :p", malformed)).as("<= %s", malformed).isEqualTo(0);
    }
  }
}
