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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for <a href="https://github.com/ArcadeData/arcadedb/issues/6926">#6926</a>.
 * <p>
 * {@code SelectExecutionPlanner.fullySorted()} collected the ORDER BY items with {@code OrderByItem.getAlias()},
 * which is null for a record attribute (such as {@code @rid}) and for a complex expression (such as
 * {@code CASE WHEN}). With a composite index the size guard on the index property list did not stop the null from
 * reaching the {@code equals()} comparison, so the query blew up with an NPE at plan time instead of returning its
 * rows.
 * <p>
 * The same method also accepted an item carrying a modifier ({@code ORDER BY s.right(1)}) as if the index order on
 * {@code s} already satisfied it, which silently returned the rows in the wrong order.
 */
class Issue6926OrderByRecordAttributeWithIndexTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE I");
      database.command("sql", "CREATE PROPERTY I.p INTEGER");
      database.command("sql", "CREATE PROPERTY I.q INTEGER");
      database.command("sql", "CREATE INDEX ON I (p, q) NOTUNIQUE");

      // insert the p=1 rows with descending q, so the index order (p, q ASC) is the reverse of the RID order
      for (int q = 5; q >= 1; q--)
        database.command("sql", "INSERT INTO I SET p = 1, q = " + q);

      database.command("sql", "INSERT INTO I SET p = 2, q = 9");

      database.command("sql", "CREATE DOCUMENT TYPE J");
      database.command("sql", "CREATE PROPERTY J.p INTEGER");
      database.command("sql", "CREATE PROPERTY J.s STRING");
      database.command("sql", "CREATE INDEX ON J (p, s) NOTUNIQUE");

      // the index order on s (a5, b4, c3, d2, e1) is the reverse of the order on the last character
      database.command("sql", "INSERT INTO J SET p = 1, s = 'a5'");
      database.command("sql", "INSERT INTO J SET p = 1, s = 'b4'");
      database.command("sql", "INSERT INTO J SET p = 1, s = 'c3'");
      database.command("sql", "INSERT INTO J SET p = 1, s = 'd2'");
      database.command("sql", "INSERT INTO J SET p = 1, s = 'e1'");
    });
  }

  @Test
  void orderByRidWithIndexedEqualityOnCompositeIndex() {
    final List<RID> rids = ridsOf("SELECT FROM I WHERE p = 1 ORDER BY @rid");

    assertThat(rids).hasSize(5);
    for (int i = 0; i < rids.size() - 1; i++)
      assertThat(rids.get(i).getPosition()).isLessThan(rids.get(i + 1).getPosition());
  }

  @Test
  void orderByRidDescWithIndexedEqualityOnCompositeIndex() {
    final List<RID> rids = ridsOf("SELECT FROM I WHERE p = 1 ORDER BY @rid DESC");

    assertThat(rids).hasSize(5);
    for (int i = 0; i < rids.size() - 1; i++)
      assertThat(rids.get(i).getPosition()).isGreaterThan(rids.get(i + 1).getPosition());
  }

  @Test
  void orderByIndexedPropertyThenRidWithIndexedEqualityOnCompositeIndex() {
    final List<RID> rids = ridsOf("SELECT FROM I WHERE p = 1 ORDER BY p, @rid");

    assertThat(rids).hasSize(5);
    for (int i = 0; i < rids.size() - 1; i++)
      assertThat(rids.get(i).getPosition()).isLessThan(rids.get(i + 1).getPosition());
  }

  @Test
  void orderByCaseExpressionWithIndexedEqualityOnCompositeIndex() {
    final List<Integer> qs = intsOf("q", "SELECT FROM I WHERE p = 1 ORDER BY CASE WHEN q > 3 THEN 0 ELSE 1 END");

    assertThat(qs).hasSize(5);
    assertThat(qs.subList(0, 2)).containsExactlyInAnyOrder(4, 5);
    assertThat(qs.subList(2, 5)).containsExactlyInAnyOrder(1, 2, 3);
  }

  @Test
  void orderByIndexedPropertyWithModifierIsNotSatisfiedByTheIndexOrder() {
    final List<String> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM J WHERE p = 1 ORDER BY s.right(1)")) {
      while (rs.hasNext())
        values.add(rs.next().getProperty("s"));
    }

    assertThat(values).containsExactly("e1", "d2", "c3", "b4", "a5");
  }

  @Test
  void orderByIndexedPropertiesIsStillSatisfiedByTheIndexOrder() {
    assertThat(intsOf("q", "SELECT FROM I WHERE p = 1 ORDER BY p, q")).containsExactly(1, 2, 3, 4, 5);
    assertThat(intsOf("q", "SELECT FROM I WHERE p = 1 ORDER BY q")).containsExactly(1, 2, 3, 4, 5);

    // the rows would come out in this order anyway if the ORDER BY step were kept, so assert on the plan too:
    // without it, this test would still pass if fullySorted() had simply been made to always answer false
    assertThat(explain("SELECT FROM I WHERE p = 1 ORDER BY p, q")).doesNotContain("ORDER BY");
    assertThat(explain("SELECT FROM I WHERE p = 1 ORDER BY q")).doesNotContain("ORDER BY");
  }

  @Test
  void indexedEqualityStillUsesTheIndexWhileTheOrderByStepIsKept() {
    final String plan = explain("SELECT FROM I WHERE p = 1 ORDER BY @rid");
    assertThat(plan).contains("FETCH FROM INDEX");
    assertThat(plan).contains("ORDER BY");
  }

  private String explain(final String query) {
    final StringBuilder plan = new StringBuilder();
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      while (rs.hasNext())
        plan.append(rs.next().toJSON());
    }
    return plan.toString();
  }

  private List<Integer> intsOf(final String property, final String query) {
    final List<Integer> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty(property));
    }
    return values;
  }

  private List<RID> ridsOf(final String query) {
    final List<RID> rids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        rs.next().getIdentity().ifPresent(rids::add);
    }
    return rids;
  }
}
