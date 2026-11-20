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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11891: answering a one-hop pattern off the edge list must ask the read permissions the general path asks, no
 * fewer and no more, whichever end of the pattern the row binds. A pattern predicate and a pattern comprehension load the
 * relationship records they walk, so a user who cannot read the relationship type is refused as before; the count
 * push-down of {@code COUNT { }} and an {@code EXISTS { }} body never load them, so the answer stays the one they gave. None
 * of them loads the far end (its label is read off the type of its bucket), so a far-end type the user cannot read
 * refuses nothing, except that the count push-down is refused a labelled type (so is the fast path, up front). A
 * predicate or a comprehension that binds only the last node scans the first node's types on the general path, which asks
 * for other permissions than a walk off the bound vertex, so they stay on it. The oracle is the same expression on the
 * general path: with a relationship variable, or for a pattern predicate, which cannot bind one, with an inline
 * {@code WHERE} that accepts every relationship. Every case passes on the code before the change too.
 * <p>
 * Not a {@code TestHelper}: the database needs a security manager, which only a factory configured before the database is
 * created installs.
 */
class OneHopPatternSecurityIssue11891Test {
  private static final String   PATH          = "target/databases/OneHopPatternSecurityIssue11891Test";
  // The relationship, as a template of its variable and its inline WHERE
  private static final String[] RELATIONSHIPS = { "-[%s:K%s]->", "<-[%s:L%s]-", "-[%s:K|L%s]-", "-[%s%s]->", "<-[%s:K%s]-" };
  private static final String[] FAR_ENDS      = { "()", "(:P)", "(:Q)", "(:P|Q)" };

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void createGraph() {
    // Without a security manager the engine asks no file permission at all, whatever user is bound
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE VERTEX TYPE Q");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE EDGE TYPE L");
    database.transaction(() -> database.command("opencypher",
        "CREATE (p1:P {i: 1}), (p2:P {i: 2}), (:P {i: 3}), (p5:P {i: 5}), (q1:Q {i: 4}) "
            + "CREATE (p1)-[:K]->(q1), (p1)-[:K]->(p2), (p2)-[:L]->(p1), (q1)-[:K]->(q1), (p5)-[:K]->(p5), (p5)-[:L]->(q1)"));
    // The records just written sit in the cache, which a lookup answers without the bucket read that asks the permission
    database.close();
    database = factory.open();
  }

  @AfterEach
  void dropDatabase() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void aUserWhoCannotReadTheFarEndTypeGetsTheAnswerTheGeneralPathGives() {
    assertSameOutcomes(Set.of("Q"));
  }

  @Test
  void aUserWhoCannotReadTheRelationshipTypeGetsTheAnswerTheGeneralPathGives() {
    assertSameOutcomes(Set.of("L"));
  }

  /** A bound user who may read everything still has the checks asked, and the answers they give are the counts. */
  @Test
  void aUserWhoCanReadEverythingGetsTheAnswerTheGeneralPathGives() {
    assertSameOutcomes(Set.of());
  }

  @Test
  void aUserWhoCannotReadTheEdgeTypeNorTheFarEndTypeGetsTheAnswerTheGeneralPathGives() {
    assertSameOutcomes(Set.of("K", "Q"));
  }

  /**
   * The count push-down is refused a labelled type whatever the graph holds, so the refusal is not an oracle for whether a
   * neighbour of the type exists: {@code p3} has no relationship at all, and no neighbour of the denied type either.
   */
  @Test
  void aDeniedLabelRefusesACountEvenForAVertexWithNoNeighbour() {
    bindUserDenying(Set.of("Q"));
    for (final String pattern : List.of("(p)-[:K]->(:Q)", "(:Q)-[:K]->(p)", "(p)<-[:L]-(:Q)", "(:Q)<-[:L]-(p)"))
      assertThat(outcome("MATCH (p:P {i: 3}) RETURN COUNT { " + pattern + " } AS v")).as(pattern).isEqualTo("refused");
  }

  private void assertSameOutcomes(final Set<String> deniedTypes) {
    final List<String[]> pairs = new ArrayList<>();
    // Either end may be the bound one, and the far end carries no label, one of the types, or a disjunction of them
    for (final String farEnd : FAR_ENDS)
      for (final String relationship : RELATIONSHIPS)
        for (final boolean boundFirst : new boolean[] { true, false }) {
          final String fast = pattern(relationship, farEnd, boundFirst, "", "");
          addExpressions(pairs, fast, pattern(relationship, farEnd, boundFirst, "r", ""));
          // A pattern predicate cannot bind a variable: an inline WHERE that accepts every relationship keeps it general
          final String filtered = pattern(relationship, farEnd, boundFirst, "", " WHERE true");
          pairs.add(new String[] { "WITH p WHERE " + fast + " RETURN 1 AS v", "WITH p WHERE " + filtered + " RETURN 1 AS v" });
          pairs.add(new String[] { "WITH p WHERE NOT " + fast + " RETURN 1 AS v", "WITH p WHERE NOT " + filtered + " RETURN 1 AS v" });
        }

    bindUserDenying(deniedTypes);
    final List<String> mismatches = new ArrayList<>();
    int refused = 0;
    for (final String[] pair : pairs) {
      final String actual = outcome("MATCH (p:P) " + pair[0]);
      final String expected = outcome("MATCH (p:P) " + pair[1]);
      if (!actual.equals(expected))
        mismatches.add(pair[0] + " -> " + actual + ", general " + expected);
      if (expected.equals("refused"))
        ++refused;
    }
    assertThat(mismatches).isEmpty();
    // The oracle has to be refused somewhere, or a user bound to nothing but grants would pass for the wrong reason
    if (!deniedTypes.isEmpty())
      assertThat(refused).as("the general path refuses a user denied " + deniedTypes).isPositive();
  }

  private static String pattern(final String relationship, final String farEnd, final boolean boundFirst, final String variable,
      final String where) {
    final String hop = relationship.formatted(variable, where);
    return boundFirst ? "(p)" + hop + farEnd : farEnd + hop + "(p)";
  }

  private static void addExpressions(final List<String[]> pairs, final String fast, final String general) {
    pairs.add(new String[] { "RETURN COUNT { " + fast + " } AS v", "RETURN COUNT { " + general + " } AS v" });
    pairs.add(new String[] { "RETURN EXISTS { " + fast + " } AS v", "RETURN EXISTS { " + general + " } AS v" });
    pairs.add(new String[] { "RETURN size([" + fast + " | 1]) AS v", "RETURN size([" + general + " | 1]) AS v" });
  }

  /** The values the query answers in vertex order, or the refusal it fails with. */
  private String outcome(final String query) {
    try (final ResultSet rs = database.query("opencypher", query + ", p.i AS i ORDER BY i")) {
      final List<String> rows = new ArrayList<>();
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.getProperty("i") + "=" + row.getProperty("v"));
      }
      return rows.toString();
    } catch (final RuntimeException e) {
      for (Throwable cause = e; cause != null; cause = cause.getCause())
        if (cause instanceof SecurityException)
          return "refused";
      throw e;
    }
  }

  private void bindUserDenying(final Set<String> deniedTypes) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    for (final String typeName : deniedTypes)
      deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "scoped";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return !deniedBucketIds.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return !deniedTypes.contains(typeName);
      }

      @Override
      public long getResultSetLimit() {
        return -1;
      }

      @Override
      public long getReadTimeout() {
        return -1;
      }
    });
  }
}
