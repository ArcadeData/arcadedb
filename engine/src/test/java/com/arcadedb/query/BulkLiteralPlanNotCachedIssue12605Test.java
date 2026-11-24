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
package com.arcadedb.query;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.opencypher.query.CypherPlanCache;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.ExecutionPlanCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.StringJoiner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12605: a text with more literals than can be extracted into parameters is kept out of the statement cache (#12350), since
 * only an exact resend could share its entry. Its plan still reached the execution plan cache, keyed by a text that holds every
 * literal: the read side of an UPDATE or DELETE under the rendered {@code SELECT FROM <target> WHERE <where>}, a SELECT under its
 * own text, the statements of a script and the openCypher plan under the query. Each distinct bulk list left an entry nothing else
 * could hit, holding a plan that references the whole WHERE tree, up to the size of the cache. Such a statement is now planned for
 * its execution only, on every one of those paths; a text with fewer literals is still cached and shared.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class BulkLiteralPlanNotCachedIssue12605Test extends TestHelper {
  /** More literals than {@code LiteralParameterizer.MAX_LITERALS} (4096). */
  private static final int BULK    = 5000;
  private static final int RECORDS = 10;

  @BeforeEach
  void createTypes() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE VERTEX TYPE V");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++) {
        database.newDocument("T").set("k", i).save();
        database.newVertex("V").set("k", i).save();
      }
    });
    plans().invalidate();
    cypherPlans().invalidate();
  }

  @Test
  void aBulkDeleteLeavesNoPlanInTheCache() {
    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE k IN " + list(BULK, 0)).close());

    // before the count, whose own plan is cached
    assertThat(plans().size()).as("the read side of a bulk DELETE").isZero();
    assertThat(countT()).as("every record the list names is deleted").isZero();
  }

  @Test
  void aBulkUpdateLeavesNoPlanInTheCache() {
    database.transaction(() -> database.command("sql", "UPDATE T SET z = 1 WHERE k IN " + list(BULK, 0)).close());

    assertThat(count("SELECT count(@rid) AS n FROM T WHERE z = 1")).as("every record the list names is updated").isEqualTo(RECORDS);
    plans().invalidate();

    database.transaction(() -> database.command("sql", "UPDATE T SET z = 2 WHERE k IN " + list(BULK, 0)).close());
    assertThat(plans().size()).as("the read side of a bulk UPDATE").isZero();
  }

  @Test
  void aBulkSelectLeavesNoPlanInTheCache() {
    final String bulk = "SELECT FROM T WHERE k IN " + list(BULK, 0);
    try (final ResultSet rs = database.query("sql", bulk)) {
      assertThat(rs.stream().count()).isEqualTo(RECORDS);
    }
    assertThat(plans().contains(bulk)).as("a bulk SELECT, under its own text").isFalse();
    assertThat(plans().size()).as("a bulk SELECT").isZero();
  }

  @Test
  void aBulkLetSubqueryLeavesNoPlanInTheCache() {
    final String bulk = "SELECT $ids.size() AS n FROM T LET $ids = (SELECT FROM T WHERE k IN " + list(BULK, 0) + ") LIMIT 1";
    try (final ResultSet rs = database.query("sql", bulk)) {
      assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(RECORDS);
    }
    assertThat(plans().size()).as("the global LET of a bulk SELECT").isZero();
  }

  @Test
  void theStatementsOfABulkScriptLeaveNoPlanInTheCache() {
    // a script is parsed on every execution, never through the statement cache, and its statements plan like any other
    try (final ResultSet rs = database.command("sqlscript", "SELECT FROM T WHERE k IN " + list(BULK, 0) + ";")) {
      assertThat(rs.stream().count()).isEqualTo(RECORDS);
    }
    assertThat(plans().size()).as("a bulk SELECT of a script").isZero();

    database.transaction(() -> database.command("sqlscript", "DELETE FROM T WHERE k IN " + list(BULK, 0) + ";").close());
    assertThat(plans().size()).as("the read side of a bulk DELETE of a script").isZero();
    assertThat(countT()).isZero();
  }

  @Test
  void aBulkCypherQueryLeavesNoPlanInTheCache() {
    final String bulk = "MATCH (n:V) WHERE n.k IN " + list(BULK, 0) + " RETURN count(n) AS c";
    try (final ResultSet rs = database.query("opencypher", bulk)) {
      assertThat(rs.next().<Number>getProperty("c").intValue()).isEqualTo(RECORDS);
    }
    assertThat(cypherPlans().contains(bulk)).as("a bulk openCypher query").isFalse();
    assertThat(cypherPlans().size()).as("a bulk openCypher query").isZero();
  }

  @Test
  void aTextUnderTheLimitStillSharesOnePlan() {
    // the first text of a shape runs as written, the second builds the shared statement: from then on every text of the shape
    // shares one plan, so a third text adds nothing. Under the limit with the literal of the SET too
    final int atTheLimit = 4000;
    for (int i = 0; i < 2; i++) {
      final int offset = i;
      database.transaction(() -> database.command("sql", "UPDATE T SET z = 1 WHERE k IN " + list(atTheLimit, offset)).close());
      try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE k IN " + list(atTheLimit, offset))) {
        rs.stream().count();
      }
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:V) WHERE n.k IN " + list(atTheLimit, offset) + " RETURN n")) {
        rs.stream().count();
      }
    }
    final int sqlPlans = plans().size();
    final int cypherCached = cypherPlans().size();
    assertThat(sqlPlans).as("a text at the limit is cached").isPositive();
    assertThat(cypherCached).as("a Cypher text at the limit is cached").isPositive();

    database.transaction(() -> database.command("sql", "UPDATE T SET z = 1 WHERE k IN " + list(atTheLimit, 2)).close());
    try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE k IN " + list(atTheLimit, 2))) {
      rs.stream().count();
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:V) WHERE n.k IN " + list(atTheLimit, 2) + " RETURN n")) {
      rs.stream().count();
    }
    assertThat(plans().size()).as("a third text of the shape shares the plan").isEqualTo(sqlPlans);
    assertThat(cypherPlans().size()).as("a third Cypher text of the shape shares the plan").isEqualTo(cypherCached);
  }

  /** {@code count} distinct integers as a list; the offset keeps two lists of one size apart, all of them naming 0 to 9. */
  private static String list(final int count, final int offset) {
    final StringJoiner values = new StringJoiner(", ", "[", "]");
    for (int i = 0; i < RECORDS; i++)
      values.add(String.valueOf(i));
    for (int i = RECORDS; i < count; i++)
      values.add(String.valueOf(offset * count + i + RECORDS));
    return values.toString();
  }

  private long countT() {
    return count("SELECT count(@rid) AS n FROM T");
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }

  private ExecutionPlanCache plans() {
    return ((DatabaseInternal) database).getExecutionPlanCache();
  }

  private CypherPlanCache cypherPlans() {
    return ((DatabaseInternal) database).getCypherPlanCache();
  }
}
