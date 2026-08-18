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
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6323 item 3: {@code EXPLAIN} used to describe only the count push-down and the
 * cost-based physical plan, so a query the optimizer does not claim - which is where the queries a user is most
 * likely to be investigating land - got a reason and nothing else, while {@code PROFILE} of the same query printed
 * the whole step chain. {@code EXPLAIN} is the command whose entire purpose is inspecting a plan WITHOUT running
 * it, so being less informative than the one that does run the query left no way to inspect a slow query, and no
 * way at all to inspect a writing one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherExplainTraditionalPlanTest {
  private Database database;
  private String   aliceRid;
  private String   bobRid;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-explain-traditional-plan").create();
    database.getSchema().createVertexType("Person");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Alice'})");
      database.command("opencypher", "CREATE (n:Person {name: 'Bob'})");
    });
    aliceRid = ridOf("Alice");
    bobRid = ridOf("Bob");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /** The reported case: the plan the traditional path would run is now described instead of withheld. */
  @Test
  void explainDescribesTheStepChainOfAQueryTheOptimizerDoesNotClaim() {
    final Map<String, Object> parameters = new HashMap<>();
    parameters.put("sourceId", aliceRid);
    parameters.put("targetId", bobRid);

    final String plan = explain("MATCH (a),(b) WHERE ID(a) = $sourceId AND ID(b) = $targetId RETURN a, b", parameters);

    assertThat(plan)
        .contains("Using Traditional Execution (Non-Optimized)")
        .contains("MATCH NODE (a)")
        .contains("MATCH NODE (b)")
        .contains("FILTER")
        .contains("PROJECT RETURN a, b");
  }

  /**
   * The {@code [id: ...]} marker added by #6307 exists so a user can confirm the RID push-down fired, and the
   * queries that push a RID down are exactly the ones that land on the traditional path. Before this fix the
   * marker was reachable only through PROFILE, which executes.
   */
  @Test
  void explainShowsTheRidPushDownMarker() {
    final Map<String, Object> parameters = new HashMap<>();
    parameters.put("sourceId", aliceRid);

    assertThat(explain("MATCH (a) WHERE ID(a) = $sourceId RETURN a", parameters)).contains("[id: " + aliceRid + "]");
  }

  /**
   * EXPLAIN and PROFILE must describe the same chain: the difference between them is execution, not detail.
   * The step lines are compared with the per-step timings PROFILE appends stripped off, since those are what
   * running the query buys and are the only thing EXPLAIN cannot know.
   */
  @Test
  void explainAndProfileDescribeTheSameChain() {
    final Map<String, Object> parameters = new HashMap<>();
    parameters.put("sourceId", aliceRid);
    parameters.put("targetId", bobRid);
    final String query = "MATCH (a),(b) WHERE ID(a) = $sourceId AND ID(b) = $targetId RETURN a, b";

    final List<String> explained = stepLines(explain("EXPLAIN " + query, parameters));
    final List<String> profiled = stepLines(explain("PROFILE " + query, parameters));

    assertThat(explained).isNotEmpty().isEqualTo(profiled);
  }

  /**
   * Describing a write must stay a description. The steps are built and never pulled, which is what makes this
   * safe - and is why the workaround of running PROFILE (which does write) was never one.
   */
  @Test
  void explainingAWriteDescribesItWithoutPerformingIt() {
    final String plan = explain("CREATE (n:Person {name: 'Carol'}) RETURN n", Map.of());

    assertThat(plan).contains("CREATE");
    assertThat(count("MATCH (n:Person) RETURN count(n) AS c")).isEqualTo(2L);
    assertThat(count("MATCH (n:Person {name: 'Carol'}) RETURN count(n) AS c")).isZero();
  }

  /**
   * The same for every other statement that writes. Building a chain is now the answer to EXPLAIN for the whole
   * executor package, not just for {@code CreateStep}, so the property being relied on - a step does its work in
   * {@code syncPull} and nothing in its constructor - is asserted against each kind of step that could break it.
   */
  @Test
  void explainingAnyWriteDescribesItWithoutPerformingIt() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:Person {name: 'Dave', age: 30})"));

    final Map<String, String> writesAndTheirStep = Map.of(
        "MATCH (n:Person {name: 'Dave'}) SET n.age = 99 RETURN n", "SET",
        "MATCH (n:Person {name: 'Dave'}) REMOVE n.age RETURN n", "REMOVE",
        "MATCH (n:Person {name: 'Dave'}) DELETE n", "DELETE",
        "MERGE (n:Person {name: 'Erin'}) RETURN n", "MERGE",
        "FOREACH (i IN range(1, 3) | CREATE (:Person {name: 'foreach'}))", "FOREACH",
        "MATCH (a:Person {name: 'Dave'}), (b:Person {name: 'Alice'}) CREATE (a)-[:KNOWS]->(b)", "CREATE",
        // A CALL is the one step whose body is a plan of its own, so it is the one that could do eager work -
        // opening a cursor, taking a lock - while being built rather than while being pulled.
        "MATCH (n:Person {name: 'Dave'}) CALL { WITH n SET n.age = 77 RETURN n AS m } RETURN m", "CALL",
        "CALL db.labels() YIELD label RETURN label", "CALL");

    for (final Map.Entry<String, String> write : writesAndTheirStep.entrySet()) {
      assertThat(explain(write.getKey(), Map.of()))
          .as("EXPLAIN describes '%s'", write.getKey())
          .containsIgnoringCase(write.getValue());

      assertThat(count("MATCH (n:Person) RETURN count(n) AS c")).as("after EXPLAIN of '%s'", write.getKey()).isEqualTo(3L);
      assertThat(count("MATCH (n:Person {name: 'Dave'}) RETURN n.age AS c")).as("after EXPLAIN of '%s'", write.getKey())
          .isEqualTo(30L);
      assertThat(count("MATCH ()-[r]->() RETURN count(r) AS c")).as("after EXPLAIN of '%s'", write.getKey()).isZero();
    }
  }

  /** A UNION is answered branch by branch, so describing it means describing every branch. */
  @Test
  void explainDescribesEveryUnionBranch() {
    database.getSchema().createVertexType("Company");

    final String plan = explain("""
        MATCH (n:Person) RETURN n.name AS name \
        UNION \
        MATCH (n:Company) RETURN n.name AS name""", Map.of());

    assertThat(plan)
        .contains("UNION")
        .contains("Branch 1:")
        .contains("Branch 2:")
        .contains("n:Person")
        .contains("n:Company");

    // A UNION has no plan of its own to be optimized or not - each branch is planned separately, and here both
    // branches are the optimizer's. Claiming the optimizer was not used contradicted the branches underneath.
    assertThat(plan)
        .contains("Using Per-Branch Planning (UNION)")
        .doesNotContain("Query pattern not yet supported by optimizer");
    assertThat(plan.split("OPTIMIZED MATCH", -1)).as("both branches report the optimizer they actually use").hasSize(3);
  }

  /**
   * The structured step list backs the text, so a client reading the plan as data sees the same chain - including
   * on the optimized path, where the steps are built from the very {@code PhysicalPlan} instance the text is
   * printed from, and so cannot describe a second, separately optimized plan.
   */
  @Test
  void explainPublishesTheStepsAsStructuredData() {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN MATCH (n:Person) RETURN n.name AS name")) {
      final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();
      final List<ExecutionStep> steps = plan.getSteps();
      assertThat(steps).isNotEmpty();

      // The operator the printed physical plan names is the one the structured steps describe.
      assertThat(plan.prettyPrint(0, 2)).contains("NodeByLabelScan(n:Person)");
      assertThat(steps.stream().map(ExecutionStep::getDescription).reduce("", String::concat))
          .contains("NodeByLabelScan(n:Person)")
          .contains("PROJECT RETURN name");
    }
  }

  /**
   * The per-step timings PROFILE appends closed their parenthesis before the row count was added to the same
   * group, so a step that returned rows printed {@code (511us), 1 rows)} - one bracket short at the front and one
   * too many at the end. Found comparing the EXPLAIN and PROFILE chains above, which is where the two texts became
   * comparable at all.
   */
  @Test
  void profiledStepsAreBalanced() {
    final Map<String, Object> parameters = new HashMap<>();
    parameters.put("sourceId", aliceRid);
    parameters.put("targetId", bobRid);

    for (final String line : stepLinesRaw(
        explain("PROFILE MATCH (a),(b) WHERE ID(a) = $sourceId AND ID(b) = $targetId RETURN a, b", parameters))) {
      int open = 0;
      for (int i = 0; i < line.length(); i++) {
        if (line.charAt(i) == '(')
          open++;
        else if (line.charAt(i) == ')')
          open--;
        assertThat(open).as("unbalanced parenthesis in '%s'", line).isNotNegative();
      }
      assertThat(open).as("unbalanced parenthesis in '%s'", line).isZero();
    }
  }

  /**
   * EXPLAIN answers a question about a query, so whatever a query that parses does to the planner, it answers with
   * a plan text: a statement that cannot be planned has its failure named inside the description rather than
   * raised in place of it, or dropped into a log line nobody has enabled. A query that does not parse is a
   * different answer and keeps raising, since there is no plan to describe and the parse error says why.
   */
  @Test
  void explainAnswersWithAPlanWhateverTheQueryAsksFor() {
    for (final String query : new String[] {
        "MATCH (n:NoSuchType) RETURN n",
        "MATCH (n:Person)-[:NO_SUCH_EDGE*2..3]->(m) RETURN m",
        "MATCH (n:Person) WHERE n.name = $neverBound RETURN n",
        "MATCH (n:Person) WITH n ORDER BY n.nothing SKIP 1 LIMIT 0 RETURN count(n) AS c" }) {
      final String plan = explain(query, Map.of());
      assertThat(plan).as("%s", query).contains("OpenCypher Native Execution Plan");
      assertThat(plan).as("%s", query).doesNotContain("Execution plan not available: null");
    }
  }

  /** A query the optimizer does claim keeps describing the physical plan it will run. */
  @Test
  void explainStillDescribesTheOptimizedPlanWhenThereIsOne() {
    final String plan = explain("MATCH (n:Person) RETURN count(n) AS c", Map.of());
    assertThat(plan).containsAnyOf("Using Count Push-Down", "Using Cost-Based Query Optimizer");
  }

  /**
   * The step lines of a plan, with the timings PROFILE appends to each of them removed. The pattern matches the
   * shape {@code AbstractExecutionStep} actually appends - a duration in μs, optionally followed by a row count -
   * rather than any parenthesis that starts with a digit, so a step whose own text ends in one keeps taking part
   * in the comparison.
   */
  private static List<String> stepLines(final String plan) {
    final List<String> lines = new ArrayList<>();
    for (final String line : stepLinesRaw(plan))
      lines.add(line.replaceAll("\\s*\\([\\d,.]+\\s*[μµ]s(, [\\d,.]+ rows)?\\)$", ""));
    return lines;
  }

  /** The step lines of a plan, verbatim. */
  private static List<String> stepLinesRaw(final String plan) {
    final List<String> lines = new ArrayList<>();
    for (final String line : plan.split("\n")) {
      final String trimmed = line.trim();
      if (trimmed.startsWith("+"))
        lines.add(trimmed);
    }
    return lines;
  }

  private String explain(final String query, final Map<String, Object> parameters) {
    final String prefixed = query.startsWith("EXPLAIN ") || query.startsWith("PROFILE ") ? query : "EXPLAIN " + query;
    try (final ResultSet rs = database.query("opencypher", prefixed, parameters)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private String ridOf(final String name) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Person {name: $name}) RETURN elementId(n) AS id",
        Map.of("name", name))) {
      return rs.next().getProperty("id");
    }
  }
}
