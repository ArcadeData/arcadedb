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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.parser.LocalResultSet;
import com.arcadedb.query.sql.parser.MatchStatement;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Generated statements answered twice, once with the key indexes of the type and once by a scan
 * ({@link SelectExecutionPlanner#withoutKeyIndexes()}, {@link MatchExecutionPlanner#withoutKeyIndexes()}): the scan filtered by the
 * condition is the reference, and an index plan must return the same rows, in the same order when the statement orders them
 * totally. The conditions mix equalities, ranges, BETWEEN, IN, IS NULL, negations, AND, OR, nulls, absent properties and values of
 * another type than the property's, over single-property, composite and unique indexes; some statements add ORDER BY, LIMIT and
 * SKIP, or count the rows instead.
 */
class IndexVersusScanDifferentialTest extends TestHelper {
  private static final int    RECORDS    = 1_500;
  private static final int    STATEMENTS = 1_500;
  private static final long[] SEEDS      = { 1, 2, 3 };

  private static final String[] PROPERTIES = { "a", "b", "c", "d", "e" };

  @Test
  void selectStatementsAnswerTheSameRowsWithAndWithoutTheirIndexes() {
    createData();
    final List<String> differences = new ArrayList<>();
    int indexed = 0;
    for (final long seed : SEEDS) {
      final Random random = new Random(seed);
      for (int i = 0; i < STATEMENTS; i++) {
        final String sql = randomSelect(random);
        if (compareSelect(sql, differences))
          ++indexed;
      }
    }
    assertThat(indexed).as("statements an index plan answered").isGreaterThan(STATEMENTS / 3);
    assertThat(differences).as("statements whose index plan and scan disagree").isEmpty();
  }

  @Test
  void matchStatementsAnswerTheSameRowsWithAndWithoutTheirIndexes() {
    createData();
    final List<String> differences = new ArrayList<>();
    int indexed = 0;
    for (final long seed : SEEDS) {
      final Random random = new Random(100 + seed);
      for (int i = 0; i < STATEMENTS / 3; i++) {
        final String match = "MATCH {type: T, as: t, where: (" + condition(random, 0) + ")} RETURN t.id AS id";
        if (compareMatch(match, differences))
          ++indexed;
      }
    }
    assertThat(indexed).as("statements an index plan answered").isGreaterThan(STATEMENTS / 15);
    assertThat(differences).as("statements whose index plan and scan disagree").isEmpty();
  }

  private void createData() {
    database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS 4");
    database.command("sql", "CREATE PROPERTY T.id INTEGER");
    database.command("sql", "CREATE PROPERTY T.a INTEGER");
    database.command("sql", "CREATE PROPERTY T.b STRING");
    database.command("sql", "CREATE PROPERTY T.c LONG");
    database.command("sql", "CREATE PROPERTY T.d DOUBLE");
    database.command("sql", "CREATE INDEX ON T (id) UNIQUE");
    database.command("sql", "CREATE INDEX ON T (a) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON T (b) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON T (c) UNIQUE");
    database.command("sql", "CREATE INDEX ON T (a, b) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON T (d) NOTUNIQUE");

    final Random random = new Random(7);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++) {
        final MutableDocument document = database.newDocument("T").set("id", i);
        // Every value set has a gap of nulls and of absent properties; c is unique when present
        if (random.nextInt(10) > 0)
          document.set("a", random.nextInt(20) - 5);
        else if (random.nextBoolean())
          document.set("a", null);
        if (random.nextInt(10) > 0)
          document.set("b", String.valueOf((char) ('a' + random.nextInt(8))) + random.nextInt(4));
        if (random.nextInt(8) > 0)
          document.set("c", (long) i * 3 - 1_000);
        if (random.nextInt(6) > 0)
          document.set("d", random.nextInt(40) / 4.0 - 2);
        // an unindexed property, to mix scanned and indexed conditions
        document.set("e", random.nextInt(5));
        document.save();
      }
    });
  }

  private static String randomSelect(final Random random) {
    final String where = condition(random, 0);
    if (random.nextInt(6) == 0)
      return "SELECT count(*) AS n FROM T WHERE " + where;
    final StringBuilder sql = new StringBuilder("SELECT id FROM T WHERE ").append(where);
    if (random.nextInt(3) == 0) {
      // id last makes the order total, so LIMIT and SKIP select the same rows on both plans
      final String first = PROPERTIES[random.nextInt(4)];
      sql.append(" ORDER BY ").append(first).append(random.nextBoolean() ? " ASC" : " DESC").append(", id")
          .append(random.nextBoolean() ? " ASC" : " DESC");
      if (random.nextBoolean())
        sql.append(" SKIP ").append(random.nextInt(20));
      if (random.nextBoolean())
        sql.append(" LIMIT ").append(1 + random.nextInt(40));
    }
    return sql.toString();
  }

  private static String condition(final Random random, final int depth) {
    final int kind = random.nextInt(depth >= 2 ? 6 : 9);
    if (kind == 6)
      return "(" + condition(random, depth + 1) + " AND " + condition(random, depth + 1) + ")";
    if (kind == 7)
      return "(" + condition(random, depth + 1) + " OR " + condition(random, depth + 1) + ")";
    if (kind == 8)
      return "NOT (" + condition(random, depth + 1) + ")";

    final String property = PROPERTIES[random.nextInt(PROPERTIES.length)];
    return switch (random.nextInt(9)) {
      case 0 -> property + " = " + literal(random, property);
      case 1 -> property + " <> " + literal(random, property);
      case 2 -> property + " < " + literal(random, property);
      case 3 -> property + " >= " + literal(random, property);
      case 4 -> property + " > " + literal(random, property) + " AND " + property + " <= " + literal(random, property);
      case 5 -> property + " BETWEEN " + literal(random, property) + " AND " + literal(random, property);
      case 6 -> property + " IN [" + literal(random, property) + ", " + literal(random, property) + ", " + literal(random, property) + "]";
      case 7 -> property + (random.nextBoolean() ? " IS NULL" : " IS NOT NULL");
      default -> property + " <= " + literal(random, property);
    };
  }

  /** A value of the property's type, now and then one of another type or null, which the index must answer as the scan does. */
  private static String literal(final Random random, final String property) {
    final int odd = random.nextInt(20);
    if (odd == 0)
      return "null";
    if (odd == 1)
      return "'" + random.nextInt(10) + "'";
    if (odd == 2)
      return String.valueOf(random.nextInt(20) - 5 + 0.5);
    return switch (property) {
      case "a", "e" -> String.valueOf(random.nextInt(24) - 7);
      case "b" -> "'" + (char) ('a' + random.nextInt(9)) + (random.nextBoolean() ? String.valueOf(random.nextInt(5)) : "") + "'";
      case "c" -> String.valueOf((long) random.nextInt(RECORDS * 3) - 1_000);
      default -> String.valueOf(random.nextInt(48) / 4.0 - 3);
    };
  }

  /** Runs {@code sql} with both plans; returns whether the index plan used an index. */
  private boolean compareSelect(final String sql, final List<String> differences) {
    final Statement parsed = ((DatabaseInternal) database).getStatementCache().get(sql);
    final SelectStatement statement = (SelectStatement) parsed;

    final InternalExecutionPlan indexed = statement.createExecutionPlanNoCache(context());
    final InternalExecutionPlan scan = new SelectExecutionPlanner(statement).withoutKeyIndexes().createExecutionPlan(context(), false);
    return compare(sql, indexed, scan, sql.contains("ORDER BY"), differences);
  }

  private boolean compareMatch(final String match, final List<String> differences) {
    final MatchStatement statement = (MatchStatement) ((DatabaseInternal) database).getStatementCache().get(match);
    final InternalExecutionPlan indexed = new MatchExecutionPlanner(statement).createExecutionPlan(context());
    final InternalExecutionPlan scan = new MatchExecutionPlanner(statement).withoutKeyIndexes().createExecutionPlan(context());
    return compare(match, indexed, scan, false, differences);
  }

  private boolean compare(final String statement, final InternalExecutionPlan indexed, final InternalExecutionPlan scan,
      final boolean ordered, final List<String> differences) {
    final String plan = indexed.prettyPrint(0, 2);
    final List<String> expected;
    final List<String> actual;
    try {
      expected = rows(new LocalResultSet(scan));
    } catch (final RuntimeException e) {
      // A statement the scan cannot answer either is not a difference
      return false;
    }
    try {
      actual = rows(new LocalResultSet(indexed));
    } catch (final RuntimeException e) {
      differences.add(statement + "\n  index plan failed: " + e + "\n  " + plan);
      return plan.contains("FETCH FROM INDEX");
    }
    if (!ordered) {
      expected.sort(null);
      actual.sort(null);
    }
    if (!expected.equals(actual))
      differences.add(statement + "\n  scan:  " + abbreviate(expected) + "\n  index: " + abbreviate(actual) + "\n  " + plan);
    return plan.contains("FETCH FROM INDEX");
  }

  private static List<String> rows(final LocalResultSet resultSet) {
    final List<String> rows = new ArrayList<>();
    try (resultSet) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        rows.add(String.valueOf(row.getPropertyNames().contains("n") ? row.getProperty("n") : row.getProperty("id")));
      }
    }
    return rows;
  }

  private BasicCommandContext context() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  private static String abbreviate(final List<String> rows) {
    return rows.size() <= 12 ? rows.toString() : rows.subList(0, 12) + "... (" + rows.size() + " rows)";
  }
}
