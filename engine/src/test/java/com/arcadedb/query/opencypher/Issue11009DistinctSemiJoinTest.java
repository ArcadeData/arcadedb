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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.opencypher.executor.DistinctSemiJoin;
import com.arcadedb.query.opencypher.parser.FunctionValidator;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11009: a read-only {@code RETURN DISTINCT} whose items read only variables bound early enumerated every
 * combination the later clauses produce, only for DISTINCT to keep one row of each. Those later clauses decide whether
 * a row survives and nothing else, so they now run as a semi-join: once per input row, stopping at their first row,
 * not at all for an input row whose returned values already qualified, and once per value of what they read when they
 * read none of the returned variables.
 * <p>
 * Every test compares against the full expansion, which is the same query without DISTINCT, deduplicated here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11009DistinctSemiJoinTest {
  private static final String DB_PATH = "./target/databases/issue-11009";

  // The shape of the report: everything after the rule reads the rule and what it reaches, never the host
  private static final String RULE_QUERY = """
      MATCH (h:Host)
      OPTIONAL MATCH (h)-[:IN_RULE]-(r:Rule)
      WITH h, r WHERE r.direction = $direction
      OPTIONAL MATCH (r)-[:FROM]-(s:Source)
      WITH h, r, s
      OPTIONAL MATCH (r)-[:USES]-(svc:Service)
      WITH svc, h, r, s
      OPTIONAL MATCH (s)-[:MEMBER]-(m:Host)
      WITH svc, h, r, s, m WHERE $port IN svc.ports OR $port IN r.ports
      MATCH (m)-[:HAS]-(p:Port)
      WITH h, r, s, m, p WHERE p.public = $public
      """;

  private static final int HOSTS = 40;
  private static final int RULES = 8;

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> {
      for (final String type : List.of("Host", "Rule", "Source", "Service", "Port"))
        database.getSchema().createVertexType(type);
      for (final String type : List.of("IN_RULE", "FROM", "USES", "MEMBER", "HAS"))
        database.getSchema().createEdgeType(type);

      final List<MutableVertex> hosts = new ArrayList<>();
      for (int i = 0; i < HOSTS; i++) {
        final MutableVertex host = database.newVertex("Host").set("name", String.format("h%02d", i)).save();
        // Two ports on some hosts, so a member reaches the same answer more than once
        if (i % 3 != 0)
          host.newEdge("HAS", database.newVertex("Port").set("public", i % 2 == 0).save());
        if (i % 5 == 0)
          host.newEdge("HAS", database.newVertex("Port").set("public", true).save());
        hosts.add(host);
      }

      // Each rule applies to many hosts, and a host to several rules
      for (int i = 0; i < RULES; i++) {
        final MutableVertex rule = database.newVertex("Rule").set("direction", i % 4 == 3 ? "OUT" : "IN")
            .set("ports", i % 2 == 0 ? List.of(161) : List.of()).save();
        final MutableVertex source = database.newVertex("Source").save();
        rule.newEdge("FROM", source);
        if (i % 3 == 2)
          rule.newEdge("USES", database.newVertex("Service").set("ports", List.of(161, 162)).save());
        for (int h = i; h < HOSTS; h += 2)
          hosts.get(h).newEdge("IN_RULE", rule);
        for (int m = 0; m < HOSTS; m += i + 3)
          source.newEdge("MEMBER", hosts.get(m));
      }
      // Two hosts with no rule at all: their OPTIONAL MATCH answers a null rule
      for (int i = 0; i < 2; i++)
        database.newVertex("Host").set("name", "lonely" + i).save();
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void reportedShapeAnswersTheFullExpansionAndRunsTheChainOncePerRule() {
    final Map<String, Object> params = Map.of("direction", "IN", "port", 161, "public", true);
    final List<String> expected = sortedDistinct(column(RULE_QUERY + "RETURN h.name AS name", params, "name"));
    assertThat(expected).isNotEmpty().hasSizeLessThan(HOSTS);

    final String query = RULE_QUERY + "RETURN DISTINCT h AS host ORDER BY host.name SKIP 0 LIMIT 50";
    assertThat(explain(query, params)).contains("SEMI JOIN").contains("remembered per r");
    final List<String> hosts = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      rs.forEachRemaining(r -> hosts.add(r.<Vertex>getProperty("host").getString("name")));
    }
    assertThat(hosts).isEqualTo(expected);

    // The chain reads the rule and never the host: it runs at most once per rule (plus once for the null rule), not
    // once per (host, rule) pair
    final String profile = profile(query, params);
    final long pairs = column(RULE_QUERY.substring(0, RULE_QUERY.indexOf("OPTIONAL MATCH (r)")) + "RETURN h.name AS name",
        params, "name").size();
    assertThat(pairs).isGreaterThan(RULES + 1);
    assertThat(chainRuns(profile)).isPositive().isLessThanOrEqualTo(RULES + 1);

    // A page of the result
    final String paged = RULE_QUERY + "RETURN DISTINCT h.name AS name ORDER BY name SKIP 1 LIMIT 3";
    assertThat(column(paged, params, "name")).isEqualTo(expected.subList(1, Math.min(4, expected.size())));

    // Other parameters, including one no row passes
    for (final Map<String, Object> other : List.of(Map.<String, Object>of("direction", "IN", "port", 162, "public", true),
        Map.<String, Object>of("direction", "IN", "port", 161, "public", false),
        Map.<String, Object>of("direction", "OUT", "port", 161, "public", true),
        Map.<String, Object>of("direction", "IN", "port", 7, "public", true)))
      assertThat(column(RULE_QUERY + "RETURN DISTINCT h.name AS name ORDER BY name", other, "name")).as(other.toString())
          .isEqualTo(sortedDistinct(column(RULE_QUERY + "RETURN h.name AS name", other, "name")));
  }

  @Test
  void keepsTheOrderOfFirstAppearanceWithoutOrderBy() {
    final Map<String, Object> params = Map.of("direction", "IN", "port", 161, "public", true);
    final String query = RULE_QUERY + "RETURN DISTINCT h.name AS name";
    assertThat(explain(query, params)).contains("SEMI JOIN");
    assertThat(column(query, params, "name")).isEqualTo(
        new ArrayList<>(new LinkedHashSet<>(column(RULE_QUERY + "RETURN h.name AS name", params, "name"))));
  }

  @Test
  void anInputRowWhoseReturnedValuesQualifiedIsNotTestedAgain() {
    // The chain reads the host, so nothing is remembered; a host that qualified through one rule skips the chain for
    // every other rule it has
    final String query = """
        MATCH (h:Host)-[:IN_RULE]-(r:Rule) WITH h, r
        MATCH (h)-[:HAS]-(p:Port) WHERE r.direction = 'IN'
        RETURN DISTINCT h.name AS name ORDER BY name""";
    assertThat(explain(query, Map.of())).contains("SEMI JOIN").doesNotContain("remembered per");
    assertThat(column(query, Map.of(), "name")).isEqualTo(sortedDistinct(column(query.replace("DISTINCT ", ""), Map.of(), "name")));

    final String profile = profile(query, Map.of());
    final long pairs = column("MATCH (h:Host)-[:IN_RULE]-(r:Rule) RETURN h.name AS name", Map.of(), "name").size();
    assertThat(qualifiedSkips(profile)).isPositive();
    assertThat(chainRuns(profile) + qualifiedSkips(profile)).isEqualTo(pairs);
  }

  @Test
  void optionalMatchAloneKeepsEveryRow() {
    final String query = "MATCH (h:Host) WITH h OPTIONAL MATCH (h)-[:HAS]-(p:Port) RETURN DISTINCT h.name AS name ORDER BY name";
    assertThat(explain(query, Map.of())).contains("SEMI JOIN");
    assertThat(column(query, Map.of(), "name")).hasSize(HOSTS + 2);
  }

  @Test
  void unwindInTheChainFiltersPerRow() {
    final String query = """
        MATCH (h:Host) WITH h
        MATCH (h)-[:IN_RULE]-(r:Rule) UNWIND r.ports AS port WITH h, port WHERE port = 161
        RETURN DISTINCT h.name AS name ORDER BY name""";
    assertThat(explain(query, Map.of())).contains("SEMI JOIN");
    assertThat(column(query, Map.of(), "name")).isEqualTo(sortedDistinct(column(query.replace("DISTINCT ", ""), Map.of(), "name")));
  }

  @Test
  void keepsMatchingRowsAfterManyThatDoNotMatch() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Bulk");
      final MutableVertex rule = database.newVertex("Rule").set("direction", "BULK").save();
      final MutableVertex other = database.newVertex("Rule").set("direction", "BULK").save();
      for (int i = 0; i < 1_000; i++) {
        final MutableVertex bulk = database.newVertex("Bulk").set("i", i).save();
        // Only the last 50 reach the rule with a port: the first 950 fail, and many batches go by first
        bulk.newEdge("IN_RULE", i >= 950 ? rule : other);
        if (i >= 990)
          bulk.newEdge("HAS", database.newVertex("Port").set("public", true).save());
      }
    });

    final String perRow = "MATCH (b:Bulk) WITH b MATCH (b)-[:HAS]-(p:Port) RETURN DISTINCT b.i AS i";
    assertThat(explain(perRow, Map.of())).contains("SEMI JOIN");
    assertThat(column(perRow, Map.of(), "i")).hasSize(10);

    // Remembered per rule: the 950 rows of the rule without a port fail from memory, the 50 of the other pass
    final String remembered = """
        MATCH (b:Bulk) OPTIONAL MATCH (b)-[:IN_RULE]-(r:Rule) WITH b, r
        MATCH (r)-[:IN_RULE]-(o:Bulk)-[:HAS]-(p:Port) RETURN DISTINCT b.i AS i ORDER BY i""";
    assertThat(explain(remembered, Map.of())).contains("remembered per r");
    final List<String> expected = new ArrayList<>();
    for (int i = 950; i < 1_000; i++)
      expected.add(String.valueOf(i));
    assertThat(column(remembered, Map.of(), "i")).isEqualTo(expected);
    assertThat(chainRuns(profile(remembered, Map.of()))).isEqualTo(2);
  }

  @Test
  void notRememberedWhenTheChainCanAnswerDifferentlyForTheSameValues() {
    // rand() can answer differently each time, and a pattern predicate is not modelled as a function of the names
    for (final String filter : List.of("WHERE rand() < 2", "WHERE (s)<-[:FROM]-(:Rule)")) {
      final String query = "MATCH (h:Host) OPTIONAL MATCH (h)-[:IN_RULE]-(r:Rule) WITH h, r MATCH (r)-[:FROM]-(s:Source) "
          + filter + " RETURN DISTINCT h.name AS name ORDER BY name";
      assertThat(explain(query, Map.of())).as(filter).contains("SEMI JOIN").doesNotContain("remembered per");
      assertThat(column(query, Map.of(), "name")).as(filter).hasSize(HOSTS);
    }
  }

  /**
   * The no-argument temporal functions answer from one clock per statement, so they are functions of nothing in a
   * RETURN item and keep the semi-join: still one row per host.
   */
  @Test
  void aStatementClockInTheReturnKeepsOneRowPerHost() {
    final String query = BASE.replace("AS v", "AS v, datetime() AS t, date() AS d");
    assertThat(explain(query, Map.of())).contains("SEMI JOIN");
    assertThat(column(query, Map.of(), "v")).isEqualTo(
        new ArrayList<>(new LinkedHashSet<>(column(BASE.replace("DISTINCT ", ""), Map.of(), "v"))));
  }

  /**
   * A RETURN item calling a function that answers differently on every call keeps a row per combination, whichever
   * engine the function comes from: a Cypher built-in, an APOC-style registered function, or a bare SQL fallback.
   */
  @Test
  void aVaryingFunctionInTheReturnKeepsEveryRow() {
    final int pairs = column(BASE.replace("DISTINCT ", ""), Map.of(), "v").size();
    for (final String call : List.of("randomUUID()", "apoc.create.uuid()", "create.uuidBase64()",
        "text.random(8)", "sql.uuid()", "sql.date()", "date.currentTimestamp()")) {
      final String query = BASE.replace("AS v", "AS v, " + call + " AS x");
      assertThat(explain(query, Map.of())).as(call).doesNotContain("SEMI JOIN");
      // A clock read in milliseconds can repeat across rows, a random value cannot
      if (!call.contains("date"))
        assertThat(column(query, Map.of(), "v")).as(call).hasSize(pairs);
    }
  }

  /**
   * The allow-list of deterministic functions names only functions a query can call, and none that varies. A name not
   * on it is read as varying, so a function registered later costs the optimization, never a row.
   */
  @Test
  void theDeterministicFunctionsAreRegisteredAndNoneVaries() {
    final Set<String> registered = new TreeSet<>();
    for (final String name : FunctionValidator.getKnownFunctionNames())
      registered.add(name.toLowerCase(Locale.ROOT));
    for (final String name : CypherFunctionRegistry.getFunctionNames())
      registered.add(name.toLowerCase(Locale.ROOT));
    assertThat(registered).containsAll(DistinctSemiJoin.DETERMINISTIC_FUNCTIONS);

    for (final String name : List.of("abs", "toLower", "apoc.coll.sort", "coll.sort", "date", "datetime", "text.join"))
      assertThat(DistinctSemiJoin.isVaryingFunction(name)).as(name).isFalse();
    for (final String name : List.of("rand", "randomUUID", "timestamp", "file", "linenumber", "text.random", "create.uuid",
        "apoc.create.uuid", "create.uuidBase64", "create.vNode", "create.vRelationship", "date.currentTimestamp",
        "datetime.realtime", "sql.date", "sql.uuid", "sql.abs", "my.userFunction", "notRegisteredAnywhere"))
      assertThat(DistinctSemiJoin.isVaryingFunction(name)).as(name).isTrue();
  }

  /**
   * The allow-list is a review of the built-in: a function registered over an allowed name at run time is not known
   * to be pure, so the name stops keeping the semi-join until the built-in is back.
   */
  @Test
  void aFunctionRegisteredOverAnAllowedNameIsReadAsVarying() {
    final StatelessFunction original = CypherFunctionRegistry.get("math.sigmoidPrime");
    assertThat(original).isNotNull();
    assertThat(DistinctSemiJoin.isVaryingFunction("math.sigmoidPrime")).isFalse();
    try {
      CypherFunctionRegistry.registerOrReplace(new StatelessFunction() {
        @Override
        public String getName() {
          return "math.sigmoidPrime";
        }

        @Override
        public Object execute(final Object[] args, final CommandContext context) {
          return Math.random();
        }
      });
      assertThat(DistinctSemiJoin.isVaryingFunction("math.sigmoidPrime")).isTrue();
      assertThat(DistinctSemiJoin.isVaryingFunction("apoc.math.sigmoidPrime")).isTrue();
    } finally {
      CypherFunctionRegistry.registerOrReplace(original);
    }
    assertThat(DistinctSemiJoin.isVaryingFunction("math.sigmoidPrime")).isFalse();
  }

  /**
   * A temporal constructor given only a timezone answers from the statement clock, like its no-argument form: the same
   * instant on every row, so it keeps the semi-join and keeps one row per host.
   */
  @Test
  void aTimezoneTemporalConstructorAnswersTheStatementClock() {
    for (final String call : List.of("datetime('UTC')", "date('UTC')", "time('UTC')", "localdatetime('UTC')",
        "localtime('UTC')")) {
      final String perRow = "UNWIND range(1, 500) AS i RETURN count(DISTINCT toString(" + call + ")) AS n";
      assertThat(column(perRow, Map.of(), "n")).as(call).containsExactly("1");
    }
    assertThat(column("RETURN datetime('UTC').epochMillis = datetime().epochMillis AS t", Map.of(), "t")).containsExactly(
        "true");

    final String query = BASE.replace("AS v", "AS v, datetime('Europe/Rome') AS t");
    assertThat(explain(query, Map.of())).contains("SEMI JOIN");
    assertThat(column(query, Map.of(), "v")).isEqualTo(
        new ArrayList<>(new LinkedHashSet<>(column(BASE.replace("DISTINCT ", ""), Map.of(), "v"))));
  }

  @Test
  void aTwoClauseQueryHasNoChainToRun() {
    // MATCH ... RETURN DISTINCT has no clause between the binding and the RETURN: nothing to test per row
    assertThat(explain("MATCH (h:Host) RETURN DISTINCT h.name AS v", Map.of())).doesNotContain("SEMI JOIN");
  }

  @Test
  void anUncorrelatedChainRunsOnce() {
    // The chain reads nothing of its input row: its answer is the same for every row, and it is asked once
    final String query = "MATCH (h:Host) WITH h MATCH (x:Source) RETURN DISTINCT h.name AS name ORDER BY name";
    assertThat(explain(query, Map.of())).contains("remembered per query");
    assertThat(column(query, Map.of(), "name")).hasSize(HOSTS + 2);
    assertThat(chainRuns(profile(query, Map.of()))).isEqualTo(1);

    final String none = "MATCH (h:Host) WITH h MATCH (x:Source) WHERE x.missing = 1 RETURN DISTINCT h.name AS name";
    assertThat(column(none, Map.of(), "name")).isEmpty();
    assertThat(chainRuns(profile(none, Map.of()))).isEqualTo(1);
  }

  @Test
  void followsAVariableRenamedBeforeTheChain() {
    final String query = "MATCH (h:Host) WITH h AS x MATCH (x)-[:HAS]-(p:Port) RETURN DISTINCT x.name AS name ORDER BY name";
    assertThat(explain(query, Map.of())).contains("SEMI JOIN");
    assertThat(column(query, Map.of(), "name")).isEqualTo(
        sortedDistinct(column("MATCH (h:Host)-[:HAS]-(:Port) RETURN h.name AS name", Map.of(), "name")));
  }

  /**
   * Each query differs from {@link #BASE}, which takes the semi-join, by one thing that makes the later rows count.
   */
  @Test
  void notUsedWhenTheResultNeedsTheLaterRows() {
    assertThat(explain(BASE, Map.of())).contains("SEMI JOIN");

    final String[] queries = {
        // RETURN reads a variable the later MATCH binds
        "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) RETURN DISTINCT p.public AS v",
        // not DISTINCT: every row counts
        "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) RETURN h.name AS v",
        // an aggregation counts the rows
        "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) RETURN DISTINCT h.name AS v, count(p) AS c",
        // a RETURN item that can answer differently for every row
        "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) RETURN DISTINCT h.name AS v, rand() AS x",
        // a WITH that looks across rows
        "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) WITH h, p LIMIT 3 RETURN DISTINCT h.name AS v",
        "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) WITH DISTINCT h, p RETURN DISTINCT h.name AS v",
        "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) WITH h, p ORDER BY p.public RETURN DISTINCT h.name AS v",
        // a WITH that rebinds the returned name
        "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) WITH p AS h RETURN DISTINCT h.public AS v",
        // RETURN *
        "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) RETURN DISTINCT *" };
    for (final String query : queries)
      assertThat(explain(query, Map.of())).as(query).doesNotContain("SEMI JOIN");

    // The one with rand() keeps a row per (host, port)
    assertThat(column(queries[3], Map.of(), "v")).hasSize(
        column("MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) RETURN h.name AS v", Map.of(), "v").size());
  }

  private static final String BASE = "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) RETURN DISTINCT h.name AS v";

  @Test
  void writeQueriesAreNotAffected() {
    database.transaction(() -> {
      final String write = "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) SET h.seen = true RETURN DISTINCT h.name AS v";
      assertThat(column(write, Map.of(), "v")).hasSize(
          sortedDistinct(column(BASE.replace("DISTINCT ", ""), Map.of(), "v")).size());
    });
    assertThat(column("MATCH (h:Host {seen: true}) RETURN h.name AS v", Map.of(), "v")).hasSize(
        sortedDistinct(column(BASE.replace("DISTINCT ", ""), Map.of(), "v")).size());
  }

  private static List<String> sortedDistinct(final List<String> values) {
    return new ArrayList<>(new TreeSet<>(values));
  }

  private static long chainRuns(final String profile) {
    return counter(profile, "chain runs");
  }

  private static long qualifiedSkips(final String profile) {
    return counter(profile, "already qualified");
  }

  private static long counter(final String profile, final String name) {
    final Matcher matcher = Pattern.compile(name + ": (\\d+)").matcher(profile);
    assertThat(matcher.find()).as(profile).isTrue();
    return Long.parseLong(matcher.group(1));
  }

  private String explain(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, params)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private String profile(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, params)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private List<String> column(final String query, final Map<String, Object> params, final String name) {
    final List<String> values = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", query, params)) {
      rs.forEachRemaining(r -> values.add(String.valueOf((Object) r.getProperty(name))));
    }
    return values;
  }
}
