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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6400: the native Cypher engine plans a MATCH clause with two independent implementations - the
 * cost-based physical plan built by {@code CypherOptimizer} and the traditional plan built by
 * {@code CypherExecutionPlan.buildMatchStep} - and which one runs a given clause is decided by syntax that has
 * nothing to do with the clause's meaning. Wrapping a query in {@code CALL { ... }}, writing {@code OPTIONAL}
 * in front of it, or binding a named path on one of its parts all route it to the traditional plan; the same
 * clause on its own goes to the cost-based one.
 * <p>
 * That arrangement is only safe while the two agree, and nothing checked that they did. Issue #6310 was exactly
 * this: relationship uniqueness across a comma was implemented in one planner and not the other, so the same
 * clause answered 0 rows as written and 4 rows inside a {@code CALL} subquery. The 3897-scenario openCypher TCK
 * does not catch this class of defect either - each scenario exercises whichever planner its own spelling
 * happens to route to, so a divergence only shows up if some scenario is written in the shape that lands on the
 * broken side.
 * <p>
 * This harness closes that gap generically. It generates a corpus of MATCH clauses combinatorially - the #6310
 * defect needed three properties to line up at once (two parts, a shared variable, and one part unable to
 * collide with itself), which is the kind of conjunction a hand-written list misses - runs each clause down both
 * planners against one fixture, and asserts the two answers are equal as multisets. Ordinary
 * variable-length MATCH patterns are included now that the physical planner can represent them.
 * <h2>How the routing is forced, and why it is verified rather than assumed</h2>
 * The routing is forcible from the query text, so no engine hook is needed: the clause as written goes to the
 * cost-based planner and the same clause wrapped in {@code CALL { ... }} goes to the traditional one. But the
 * cost-based planner still declines unsupported syntax, so a corpus entry cannot be assumed to have exercised
 * both. {@code EXPLAIN} reports which planner ran, and every pair is classified by it before being compared:
 * a pair that lands on the traditional plan twice is counted as non-differential and
 * {@link #theCorpusActuallyExercisesBothPlanners()} holds the differential count to a floor, so the harness
 * cannot quietly become an expensive way of comparing one planner against itself.
 * <p>
 * On disagreement the cost-based answer is not automatically the authority. For #6310 it happened to be right,
 * but nothing guarantees that direction, so a failure here is a question to answer rather than a side to
 * believe: {@link CypherMatchClauseUniquenessIssue6310Test} is where the expected values for the shapes settled
 * so far are pinned.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherPlannerDifferentialIssue6400Test {
  /** What {@code EXPLAIN} prints when the cost-based optimizer declined the statement. */
  private static final String TRADITIONAL_MARKER = "Traditional Execution (Non-Optimized)";

  private Database database;

  /** Which planner {@code EXPLAIN} says ran a statement. */
  private enum Planner {
    COST_BASED, TRADITIONAL
  }

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-planner-differential-6400");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      // Sub-typing on both sides, so a pattern written against a super-type also selects the sub-type's records
      // and the two planners have to agree about that too.
      database.command("sql", "CREATE VERTEX TYPE A");
      database.command("sql", "CREATE VERTEX TYPE B");
      database.command("sql", "CREATE VERTEX TYPE C");
      database.command("sql", "CREATE VERTEX TYPE E");
      database.command("sql", "CREATE VERTEX TYPE SUBA EXTENDS A");
      database.command("sql", "CREATE EDGE TYPE R");
      database.command("sql", "CREATE EDGE TYPE S");
      database.command("sql", "CREATE EDGE TYPE SUBR EXTENDS R");

      // Three C -> E -> A chains, an extra C feeding the first E (so a clause that ignores relationship
      // uniqueness has a strictly larger answer than one that honours it), a multi-labelled node, a sub-typed
      // node and a sub-typed edge, and one undirected-only pair reachable solely by walking an edge backwards.
      database.command("opencypher", "CREATE (a1:A {n:'a1', v:1})<-[:R]-(e1:E {n:'e1', v:2})<-[:R]-(c1:C {n:'c1', v:3})");
      database.command("opencypher", "CREATE (a2:A {n:'a2', v:1})<-[:R]-(e2:E {n:'e2', v:2})<-[:R]-(c2:C {n:'c2', v:4})");
      database.command("opencypher", "CREATE (a3:A {n:'a3', v:5})<-[:R]-(e3:E {n:'e3', v:2})<-[:R]-(c3:C {n:'c3', v:3})");
      database.command("opencypher", "MATCH (e:E {n:'e1'}), (c:C {n:'c2'}) CREATE (c)-[:R]->(e)");
      database.command("opencypher", "CREATE (:A:B {n:'ab1', v:1})<-[:S]-(:E {n:'e4', v:2})");
      database.command("opencypher", "CREATE (:SUBA {n:'sa1', v:1})<-[:SUBR]-(:C {n:'c4', v:3})");
      database.command("opencypher", "MATCH (a:A {n:'a1'}), (c:C {n:'c3'}) CREATE (a)-[:S]->(c)");

      // Then one hop of every (start label x relationship type x far label) combination the corpus writes, so a
      // comparison of two empty result sets is the exception rather than the rule. Two empty answers are equal,
      // and a sparse fixture would therefore pass every comparison in the harness without comparing anything -
      // which is what the row-count floor in theTwoPlannersAnswerEveryCorpusClauseIdentically refuses.
      int id = 0;
      for (final String startLabel : new String[] { "A", "C", "E", "SUBA", "A:B" })
        for (final String relType : new String[] { "R", "S", "SUBR" })
          for (final String farLabel : new String[] { "E", "A" }) {
            final String tag = "g" + (id++);
            database.command("opencypher", "CREATE (:" + startLabel + " {n:'" + tag + "s', v:1})-[:" + relType
                + "]->(:" + farLabel + " {n:'" + tag + "t', v:2})");
          }
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  // ---------------------------------------------------------------------------------------------------------
  // The corpus
  // ---------------------------------------------------------------------------------------------------------

  /**
   * Label expressions written on an endpoint. Every endpoint carries one: the cost-based planner declines a node
   * pattern with no label at all, so a corpus that left endpoints bare would route entirely to the traditional
   * plan and compare it against itself - which is the vacuity {@link #theCorpusActuallyExercisesBothPlanners()}
   * exists to refuse.
   * <p>
   * The conjunction and disjunction forms are declined by the cost-based planner today and are kept in the
   * corpus deliberately: they cost one {@code EXPLAIN} each and start being compared for free on the day the
   * optimizer learns them, which is exactly when a divergence would otherwise go unnoticed.
   */
  private static final String[] LABELS = { ":A", ":C", ":E", ":SUBA", ":A:B", ":A|C" };

  /** Labels for the far endpoint of a hop, kept short because the near endpoint already varies over all six. */
  private static final String[] FAR_LABELS = { ":E", ":A" };

  /** Relationship type expressions: untyped, typed, disjunction, sub-type, and an unrelated type. */
  private static final String[] REL_TYPES = { "", ":R", ":S", ":R|S", ":SUBR" };

  /** The three hop directions. Uniqueness proofs read the ends of a hop, and an undirected hop has none. */
  private static final String[] DIRECTIONS = { "->", "<-", "--" };

  /** Inline property filters, which the cost-based planner may push into the scan and the other may not. */
  private static final String[] INLINE_FILTERS = { "", " {v: 1}" };

  /** Bounded ranges including the identity path; unbounded routing is covered by the focused #5358 test. */
  private static final String[] VLP_RANGES = { "*0..0", "*1..1", "*1..2" };

  /**
   * One corpus entry: the MATCH clause and the projection that reads its bindings back.
   *
   * @param match      the whole {@code MATCH ...} clause
   * @param projection the {@code RETURN} item list, written so it is valid both on its own and inside a
   *                   {@code CALL} subquery
   */
  private record Query(String match, String projection) {
    String costBased() {
      return match + " RETURN " + projection;
    }

    String traditional() {
      // A CALL subquery routes the clause inside it to the traditional plan whatever the clause is, and the
      // outer statement only forwards the columns, so the two spellings ask exactly the same question.
      return "CALL { " + match + " RETURN " + projection + " } RETURN " + outerProjection();
    }

    private String outerProjection() {
      final StringBuilder outer = new StringBuilder();
      for (final String item : projection.split(",")) {
        if (!outer.isEmpty())
          outer.append(", ");
        outer.append(item.substring(item.lastIndexOf(" AS ") + 4).trim());
      }
      return outer.toString();
    }
  }

  /**
   * Every MATCH clause the harness compares, built as the cross product of the dimensions above rather than
   * written out: the defect this exists for needed three properties to hold at once, and picking the triples by
   * hand is picking the ones somebody already thought of.
   */
  private static List<Query> corpus() {
    final List<Query> queries = new ArrayList<>();

    // Single-part patterns: one hop, every direction x relationship type x endpoint labels x inline filter.
    for (final String direction : DIRECTIONS)
      for (final String relType : REL_TYPES)
        for (final String label : LABELS)
          for (final String farLabel : FAR_LABELS)
            for (final String filter : INLINE_FILTERS)
              queries.add(new Query("MATCH " + hop("x" + label + filter, "r1" + relType, "y" + farLabel, direction),
                  "x.n AS xn, y.n AS yn"));

    // Variable-length counterparts: all directions, identity/single/multi-hop ranges, and both endpoint labels.
    for (final String direction : DIRECTIONS)
      for (final String range : VLP_RANGES)
        for (final String label : new String[] { ":C", ":E", ":SUBA" })
          for (final String farLabel : FAR_LABELS)
            queries.add(new Query("MATCH " + hop("x" + label, "r1:R" + range, "y" + farLabel, direction),
                "x.n AS xn, y.n AS yn"));

    // Two-part patterns sharing a variable, which is where relationship uniqueness across the comma bites
    // (issue #6310) and also the shape the cost-based planner is willing to join.
    for (final String direction : DIRECTIONS)
      for (final String relType : REL_TYPES)
        for (final String label : LABELS)
          queries.add(new Query("MATCH " + hop("x" + label, "r1" + relType, "y:E", direction) + ", "
              + hop("y:E", "r2", "z:A", "->"), "x.n AS xn, y.n AS yn, z.n AS zn"));

    // Two-part patterns sharing no variable: a Cartesian product, where uniqueness across the comma still
    // applies and the row count is the product of the two parts less the pairings that are the same edge.
    for (final String relType : REL_TYPES)
      for (final String label : LABELS)
        queries.add(new Query("MATCH " + hop("x" + label, "r1" + relType, "y:E", "->") + ", "
            + hop("p:E", "r2", "q:A", "->"), "x.n AS xn, p.n AS pn"));

    // A variable-length component beside a disconnected fixed hop exercises the post-product
    // MATCH-clause relationship-uniqueness filter introduced with VarLengthExpand.
    for (final String range : new String[] { "*0..0", "*1..2" })
      queries.add(new Query("MATCH " + hop("x:C", "r1:R" + range, "y:A", "->") + ", "
          + hop("p:E", "r2:R", "q:A", "->"), "x.n AS xn, p.n AS pn"));

    // Anonymous hops, which the plan has to bind itself to be able to compare them at all.
    for (final String direction : DIRECTIONS)
      for (final String label : LABELS)
        queries.add(new Query("MATCH " + hop("x" + label, "", "y:E", direction) + ", " + hop("y:E", "", "z:A", "->"),
            "x.n AS xn, z.n AS zn"));

    // Anonymous variable-length relationships still need internal edge tracking when another hop can collide.
    queries.add(new Query("MATCH " + hop("x:C", ":R*1..2", "y:A", "->") + ", "
        + hop("p:E", "", "q:A", "->"), "x.n AS xn, p.n AS pn"));

    // A WHERE predicate beside the pattern: what the cost-based planner may push into the scan.
    for (final String label : LABELS)
      queries.add(new Query("MATCH " + hop("x" + label, "r1:R", "y:E", "->") + " WHERE y.v = 2 AND x.n <> 'c2'",
          "x.n AS xn, y.n AS yn"));

    return queries;
  }

  /** Renders one hop with the given endpoints, relationship spec and direction. */
  private static String hop(final String start, final String rel, final String end, final String direction) {
    final String relPart = rel.isEmpty() ? "[]" : "[" + rel + "]";
    return switch (direction) {
      case "->" -> "(" + start + ")-" + relPart + "->(" + end + ")";
      case "<-" -> "(" + start + ")<-" + relPart + "-(" + end + ")";
      default -> "(" + start + ")-" + relPart + "-(" + end + ")";
    };
  }

  // ---------------------------------------------------------------------------------------------------------
  // The assertions
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void theTwoPlannersAnswerEveryCorpusClauseIdentically() {
    final List<String> divergent = new ArrayList<>();
    int comparedWithRows = 0;

    for (final Query query : corpus()) {
      if (planner(query.costBased()) != Planner.COST_BASED)
        // The cost-based planner declined this clause, so both spellings would run the traditional plan and
        // there is nothing to compare. Counted, and held to a floor, by theCorpusActuallyExercisesBothPlanners.
        continue;

      final List<String> optimized = rows(query.costBased());
      final List<String> traditional = rows(query.traditional());
      if (!optimized.equals(traditional))
        divergent.add(query.match() + "\n    cost-based : " + optimized + "\n    traditional: " + traditional);
      else if (!optimized.isEmpty())
        comparedWithRows++;
    }

    assertThat(divergent)
        .as("the same MATCH clause must answer the same rows whichever planner ran it - see issue #6400")
        .isEmpty();

    // Two empty result sets are equal, so a fixture that matched nothing would pass every comparison above
    // while comparing nothing at all. This is the second vacuity guard, on the data rather than the routing.
    assertThat(comparedWithRows)
        .as("the fixture must make most corpus clauses actually match something")
        .isGreaterThan(100);
  }

  @Test
  void theCorpusActuallyExercisesBothPlanners() {
    int differential = 0;
    for (final Query query : corpus())
      if (planner(query.costBased()) == Planner.COST_BASED)
        differential++;

    // The failure mode that would make the whole harness vacuous is every corpus entry routing to the
    // traditional plan twice over, which comparing them would then "pass" without having compared anything.
    // The floor is well under the count observed so the assertion tracks the harness, not the optimizer's
    // current appetite; if the optimizer ever declines this much of the corpus, that is itself the finding.
    assertThat(differential)
        .as("the corpus must contain clauses the cost-based planner actually accepts")
        .isGreaterThan(100);
  }

  @Test
  void theComparisonItselfDiscriminates() {
    // The third vacuity guard, on the comparison rather than on the routing or the data: a rows() that flattened
    // everything to the same string would make the harness pass whatever the two planners answered.
    assertThat(rows("MATCH (x:C)-[:R]->(y:E) RETURN x.n AS xn, y.n AS yn"))
        .isNotEqualTo(rows("MATCH (x:C)-[:R]->(y:E) RETURN y.n AS xn, x.n AS yn"));
  }

  @Test
  void everyCallWrappedSpellingReallyRunsTheTraditionalPlan() {
    // The other half of the routing claim: the CALL wrapper is what forces the traditional plan, and if that
    // ever stopped being true the comparison above would be running the same planner twice.
    for (final Query query : corpus())
      assertThat(planner(query.traditional()))
          .as("CALL { ... } must route its body to the traditional plan: %s", query.traditional())
          .isEqualTo(Planner.TRADITIONAL);
  }

  // ---------------------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------------------

  private Planner planner(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", "EXPLAIN " + query)) {
      final String plan = resultSet.next().getProperty("executionPlanAsString");
      return plan.contains(TRADITIONAL_MARKER) ? Planner.TRADITIONAL : Planner.COST_BASED;
    }
  }

  /** The result rows as a sorted list of flattened strings, so the comparison is a multiset comparison. */
  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final StringBuilder flattened = new StringBuilder();
        for (final String property : row.getPropertyNames())
          flattened.append(property).append('=').append(String.valueOf((Object) row.getProperty(property))).append('|');
        rows.add(flattened.toString());
      }
    }
    Collections.sort(rows);
    return rows;
  }
}
