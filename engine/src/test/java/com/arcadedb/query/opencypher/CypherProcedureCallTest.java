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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

/**
 * CALL of procedures: write classification, YIELD * scope, read-only relationships and algo aggregates.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherProcedureCallTest {
  /**
   * Regression tests for issue #6094: a bare top-level {@code CALL} to a registered write
   * {@code CypherProcedure} - with no CREATE/SET/MERGE/DELETE/REMOVE/FOREACH clause anywhere in the statement -
   * was classified {@code isReadOnly() == true}, because {@code SimpleCypherStatement} only inspected
   * {@code CALL { ... }} subquery blocks and never the statement's own {@code callClauses}.
   * <p>
   * That flag is the single discriminator behind three behaviours:
   * <ul>
   *   <li>{@code OpenCypherQueryEngine.executionDatabase()} - a read-only statement is planned against the raw
   *       database instance, a writing one against {@code getWrappedDatabaseInstance()}. On HA only the wrapped
   *       (Raft-aware) instance may {@code begin()}/{@code commit()}; the raw one applies pages locally without
   *       proposing them to Raft (#5492, #5655). Since #6073's fix, {@code CallStep} does auto-commit, so the
   *       misclassification became a live Raft bypass.</li>
   *   <li>{@code analyze().isIdempotent()} - {@code RaftReplicatedDatabase.command()} forwards a non-idempotent
   *       command to the leader; a misclassified one runs entirely locally on a follower.</li>
   *   <li>{@code OpenCypherQueryEngine.query()}'s idempotency gate, which is what keeps {@code Database.query()}
   *       reserved for reads.</li>
   * </ul>
   * The routing test below is deliberately not vacuous: off HA {@code getWrappedDatabaseInstance()} returns the
   * database itself, so the test installs a distinguishable wrapper first via
   * {@code LocalDatabase.setWrappedDatabaseInstance}, exactly as the HA server does.
   */
  @Nested
  class Issue6094WriteProcedureCallClassification {
    private Database database;

    /**
     * A write procedure that mutates nothing: it only records which {@link Database} instance the plan handed it,
     * which is precisely what {@code executionDatabase()} decides. Registered per test and unregistered afterwards.
     */
    private static class DatabaseProbeProcedure implements CypherProcedure {
      private final String  name;
      private final boolean write;
      private Database      seenDatabase;

      private DatabaseProbeProcedure(final String name, final boolean write) {
        this.name = name;
        this.write = write;
      }

      @Override
      public String getName() {
        return name;
      }

      @Override
      public int getMinArgs() {
        return 0;
      }

      @Override
      public int getMaxArgs() {
        return 0;
      }

      @Override
      public String getDescription() {
        return "Test probe capturing the database instance the plan executes against";
      }

      @Override
      public List<String> getYieldFields() {
        return List.of("ok");
      }

      @Override
      public boolean isWriteProcedure() {
        return write;
      }

      @Override
      public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
        validateArgs(args);
        seenDatabase = context.getDatabase();
        final ResultInternal row = new ResultInternal();
        row.setProperty("ok", true);
        return Stream.of(row);
      }
    }

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6094");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Person");
    }

    @AfterEach
    void teardown() {
      CypherProcedureRegistry.unregister("test.probeWrite");
      CypherProcedureRegistry.unregister("test.probeRead");
      if (database != null) {
        // Undo any wrapper installed by a routing test before dropping, so drop() runs on the real instance.
        final LocalDatabase local = (LocalDatabase) database;
        local.setWrappedDatabaseInstance(local);
        database.drop();
      }
    }

    private boolean isIdempotent(final String query) {
      return database.getQueryEngine("opencypher").analyze(query).isIdempotent();
    }

    // ---------------------------------------------------------------------------------------------------------
    // Classification
    // ---------------------------------------------------------------------------------------------------------

    @Test
    void bareCallToWriteProcedureIsNotIdempotent() {
      assertThat(isIdempotent("CALL merge.node(['Person'], {name:'X'}, {}) YIELD node RETURN node")).isFalse();
      assertThat(isIdempotent("MATCH (a:Person), (b:Person) CALL merge.relationship(a, 'KNOWS', {}, {}, b) YIELD rel RETURN rel"))
          .isFalse();
      assertThat(isIdempotent("MATCH (a:Person), (b:Person) CALL apoc.refactor.mergeNodes([a,b], {}) YIELD node RETURN node"))
          .isFalse();
      assertThat(isIdempotent("MATCH (a:Person) CALL apoc.refactor.cloneNodesWithRelationships([a], {}) YIELD output RETURN output"))
          .isFalse();
    }

    @Test
    void bareCallToReadOnlyProcedureStaysIdempotent() {
      assertThat(isIdempotent("CALL meta.stats() YIELD value RETURN value")).isTrue();
      assertThat(isIdempotent("MATCH (n:Person) RETURN n")).isTrue();
    }

    /**
     * The "apoc." prefix is stripped by the registry, so both spellings of the same procedure must classify
     * identically - otherwise the alias would be a silent bypass of the whole fix.
     */
    @Test
    void apocPrefixedSpellingClassifiesTheSameWay() {
      assertThat(isIdempotent("CALL apoc.merge.node(['Person'], {name:'X'}, {}) YIELD node RETURN node")).isFalse();
    }

    /**
     * An unregistered CALL target (resolved later as a SQL function, or rejected at execution) must not be
     * guessed at: classification stays on the clauses actually present, as before this fix.
     */
    @Test
    void unknownProcedureCallIsUnaffected() {
      assertThat(isIdempotent("CALL db.labels() YIELD label RETURN label")).isTrue();
    }

    /**
     * {@code anyWriteSubquery} recurses through the inner statement's own {@code isReadOnly()}, so correcting the
     * top-level classification transitively covers a write-procedure CALL nested in a {@code CALL { ... }} block.
     */
    @Test
    void writeProcedureCallInsideCallSubqueryIsNotIdempotent() {
      assertThat(isIdempotent(
          "CALL { CALL merge.node(['Person'], {name:'X'}, {}) YIELD node RETURN node } RETURN node")).isFalse();
    }

    // ---------------------------------------------------------------------------------------------------------
    // do.when: per-call refinement
    // ---------------------------------------------------------------------------------------------------------

    /**
     * {@code DoWhen.isWriteProcedure()} is unconditionally true - at registration time it cannot know what the
     * caller's branch strings do. Given the literal branch strings of one particular call site it can, and a call
     * whose branches are both statically read-only stays idempotent, so the long-standing
     * {@code database.query("...CALL apoc.do.when(...)...")} usage keeps working.
     */
    @Test
    void doWhenWithReadOnlyLiteralBranchesStaysIdempotent() {
      assertThat(isIdempotent("CALL apoc.do.when(true, 'RETURN 1 AS x', 'RETURN 2 AS x', {}) YIELD value RETURN value"))
          .isTrue();
      assertThat(isIdempotent("CALL apoc.do.when(false, 'RETURN 1 AS x', '', {}) YIELD value RETURN value")).isTrue();
    }

    @Test
    void doWhenWithAWritingLiteralBranchIsNotIdempotent() {
      assertThat(isIdempotent(
          "CALL apoc.do.when(true, \"CREATE (n:Person {name: 'Bob'}) RETURN n\", '', {}) YIELD value RETURN value")).isFalse();
      // The else branch counts too: the condition is not statically known in general.
      assertThat(isIdempotent(
          "CALL apoc.do.when(false, 'RETURN 1 AS x', \"CREATE (n:Person) RETURN n\", {}) YIELD value RETURN value")).isFalse();
    }

    /**
     * A branch supplied as a parameter cannot be classified at parse time, so it falls back to the conservative
     * "this writes" answer rather than being assumed read-only.
     */
    @Test
    void doWhenWithDynamicBranchArgumentIsConservativelyAWrite() {
      assertThat(isIdempotent("CALL apoc.do.when(true, $q, '', {}) YIELD value RETURN value")).isFalse();
    }

    /**
     * A branch string that does not parse as written stays a write - a bare parse failing is not proof that
     * nothing runs, since the engine strips an {@code EXPLAIN}/{@code PROFILE} prefix before parsing and
     * {@code PROFILE} does execute. What must not happen is the classification throwing while the <em>outer</em>
     * query is being parsed.
     */
    @Test
    void doWhenWithUnparseableBranchIsConservativelyAWriteAndDoesNotThrowAtParseTime() {
      assertThat(isIdempotent("CALL apoc.do.when(true, 'NOT CYPHER AT ALL', '', {}) YIELD value RETURN value")).isFalse();
    }

    /**
     * A call the procedure's own argument checks reject cannot reach a branch, so it cannot write, and
     * classifying it a write would replace its actionable error with {@code QueryNotIdempotentException} for
     * every caller on {@code Database.query()}. These two shapes are what {@code DoWhenTest} pins the error type
     * of; this test pins the classification that lets those errors through.
     */
    @Test
    void doWhenCallsThatCannotRunAtAllAreNotClassifiedAsWrites() {
      // Wrong argument count: validateArgs() rejects it before any branch runs.
      assertThat(isIdempotent("CALL apoc.do.when(true, 'RETURN 1') YIELD value RETURN value")).isTrue();
      // Branch argument is a literal of the wrong type: extractString() rejects it before any branch runs.
      assertThat(isIdempotent("CALL apoc.do.when(true, 1, '', {}) YIELD value RETURN value")).isTrue();
      assertThat(isIdempotent("CALL apoc.do.when(false, 'RETURN 1 AS x', 1, {}) YIELD value RETURN value")).isTrue();
    }

    /**
     * A branch string may itself be a {@code do.when} call, so classification recurses: parse the branch, which
     * builds a statement, which classifies its own CALL, which parses that call's branches. It terminates on the
     * nesting actually written in the text, and the verdict must still travel all the way out - a write buried two
     * levels down is a write.
     */
    @Test
    void doWhenNestedInsideADoWhenBranchIsClassifiedThroughBothLevels() {
      assertThat(isIdempotent(
          "CALL apoc.do.when(true, \"CALL apoc.do.when(true, 'RETURN 1 AS x', '', {}) YIELD value RETURN value\", '', {}) "
              + "YIELD value RETURN value")).isTrue();
      assertThat(isIdempotent(
          "CALL apoc.do.when(true, \"CALL apoc.do.when(true, 'CREATE (n:Person) RETURN n', '', {}) YIELD value RETURN value\", "
              + "'', {}) YIELD value RETURN value")).isFalse();
    }

    // ---------------------------------------------------------------------------------------------------------
    // Consequences
    // ---------------------------------------------------------------------------------------------------------

    /**
     * {@code Database.query()} is reserved for idempotent statements. Before the fix it silently accepted - and
     * executed - a bare CALL to a write procedure.
     */
    @Test
    void queryRejectsBareCallToWriteProcedure() {
      assertThatThrownBy(() -> database.query("opencypher",
          "CALL merge.node(['Person'], {name:'X'}, {}) YIELD node RETURN node"))
          .isInstanceOf(QueryNotIdempotentException.class);
    }

    @Test
    void commandStillExecutesBareCallToWriteProcedure() {
      try (final ResultSet rs = database.command("opencypher",
          "CALL merge.node(['Person'], {name:'John'}, {age: 30}) YIELD node RETURN node")) {
        assertThat(rs.next().getVertex().get().get("age")).isEqualTo(30L);
      }
      try (final ResultSet check = database.query("sql", "SELECT FROM Person WHERE name = 'John'")) {
        assertThat(check.hasNext()).isTrue();
      }
    }

    // ---------------------------------------------------------------------------------------------------------
    // HA routing: executionDatabase() must resolve the wrapped (Raft-aware) instance
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Installs a distinguishable {@link DatabaseInternal} wrapper, the way the HA server installs
     * {@code RaftReplicatedDatabase}, so that "raw instance" and "wrapped instance" are two different objects and
     * the routing assertion can tell them apart.
     */
    private DatabaseInternal installDistinguishableWrapper() {
      final LocalDatabase real = (LocalDatabase) database;
      final DatabaseInternal wrapper = (DatabaseInternal) Proxy.newProxyInstance(
          DatabaseInternal.class.getClassLoader(), new Class<?>[] { DatabaseInternal.class },
          (proxy, method, args) -> {
            // Mirrors RaftReplicatedDatabase: the wrapper is its own wrapped instance.
            if ("getWrappedDatabaseInstance".equals(method.getName()) && method.getParameterCount() == 0)
              return proxy;
            try {
              return method.invoke(real, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          });
      real.setWrappedDatabaseInstance(wrapper);
      return wrapper;
    }

    @Test
    void bareCallToWriteProcedureIsPlannedAgainstTheWrappedInstance() {
      final DatabaseProbeProcedure probe = new DatabaseProbeProcedure("test.probeWrite", true);
      CypherProcedureRegistry.registerOrReplace(probe);

      final DatabaseInternal wrapper = installDistinguishableWrapper();

      try (final ResultSet rs = database.command("opencypher", "CALL test.probeWrite() YIELD ok RETURN ok")) {
        assertThat(rs.next().<Boolean>getProperty("ok")).isTrue();
      }

      assertThat(probe.seenDatabase).isSameAs(wrapper);
      assertThat(probe.seenDatabase).isNotSameAs(database);
    }

    /**
     * The counterpart that proves the routing assertion above discriminates: a read-only CALL keeps the raw
     * instance, so a read-only statement does not acquire the wrapper's read barrier.
     */
    @Test
    void bareCallToReadOnlyProcedureIsPlannedAgainstTheRawInstance() {
      final DatabaseProbeProcedure probe = new DatabaseProbeProcedure("test.probeRead", false);
      CypherProcedureRegistry.registerOrReplace(probe);

      final DatabaseInternal wrapper = installDistinguishableWrapper();

      try (final ResultSet rs = database.command("opencypher", "CALL test.probeRead() YIELD ok RETURN ok")) {
        assertThat(rs.next().<Boolean>getProperty("ok")).isTrue();
      }

      assertThat(probe.seenDatabase).isSameAs(database);
      assertThat(probe.seenDatabase).isNotSameAs(wrapper);
    }

    /**
     * The security/permission axis: a statement whose only write is a procedure CALL must not be reported as a
     * pure read by {@code analyze().getOperationTypes()} either, or a read-only principal would be allowed to run
     * it.
     */
    @Test
    void operationTypesOfABareWriteProcedureCallAreNotReadOnly() {
      assertThat(database.getQueryEngine("opencypher")
          .analyze("CALL merge.node(['Person'], {name:'X'}, {}) YIELD node RETURN node")
          .getOperationTypes())
          .doesNotContain(OperationType.READ);
    }
  }

  /**
   * Issue #6148: {@code CALL proc() YIELD *} never put any of the procedure's yielded output names into scope, so a
   * named reference to one of them - or a following {@code RETURN *} - failed even though the executor projects the
   * fields correctly once the query is allowed to run at all.
   * <p>
   * Root cause: {@code CypherSemanticValidator}'s {@code buildVarTypes}, {@code validateVariableScope} and
   * {@code statementDeclaresAnyVariable} (added for issue #6135 / PR #6138) all iterated {@code callClause.getYieldItems()}
   * to learn what a {@code CALL ... YIELD} exports, but that list is deliberately empty for {@code YIELD *}
   * ({@code isYieldAll()} is the only signal). Neo4j (the openCypher reference implementation) treats {@code YIELD *}
   * as an ordinary {@code YIELD} of every field the procedure declares.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue6148YieldStarProcedureScope extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("opencypher", "CREATE (:P {name: 'a'})-[:KNOWS]->(:P {name: 'b'})");
      // db.propertyKeys() reads the schema's declared properties, not the schemaless documents themselves
      database.command("sql", "CREATE PROPERTY P.name STRING");
    }

    @Test
    void yieldStarBindsANamedBuiltInProcedureOutput() {
      try (final ResultSet rs = database.query("opencypher", "CALL db.labels() YIELD * RETURN label")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.getPropertyNames()).containsExactly("label");
        assertThat(row.<String>getProperty("label")).isEqualTo("P");
      }
    }

    @Test
    void yieldStarLetsReturnStarSeeTheBuiltInProcedureOutput() {
      try (final ResultSet rs = database.query("opencypher", "CALL db.labels() YIELD * RETURN *")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().getPropertyNames()).containsExactly("label");
      }
    }

    @Test
    void yieldStarBindsDbRelationshipTypesOutput() {
      try (final ResultSet rs = database.query("opencypher", "CALL db.relationshiptypes() YIELD * RETURN relationshipType")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("relationshipType")).isEqualTo("KNOWS");
      }
    }

    @Test
    void yieldStarBindsDbPropertyKeysOutput() {
      try (final ResultSet rs = database.query("opencypher", "CALL db.propertykeys() YIELD * RETURN propertyKey")) {
        assertThat(rs.hasNext()).isTrue();
      }
    }

    @Test
    void yieldStarBindsARegisteredProcedureOutput() {
      try (final ResultSet rs = database.query("opencypher", "CALL meta.stats() YIELD * RETURN value")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Object>getProperty("value")).isNotNull();
      }
    }

    @Test
    void yieldStarInsideACallSubqueryStillBindsTheOutput() {
      try (final ResultSet rs = database.query("opencypher", "CALL { CALL db.labels() YIELD * RETURN label } RETURN label")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("label")).isEqualTo("P");
      }
    }

    @Test
    void yieldStarBindsDbSchemaOutputUnderEitherRegisteredName() {
      try (final ResultSet rs = database.query("opencypher", "CALL db.schema() YIELD * RETURN name, type, properties")) {
        assertThat(rs.hasNext()).isTrue();
      }
      try (final ResultSet rs = database.query("opencypher", "CALL db.schema.visualization() YIELD * RETURN name, type, properties")) {
        assertThat(rs.hasNext()).isTrue();
      }
    }

    /**
     * Routing the hardcoded {@code db.*} built-ins through the normal {@code CypherProcedure} dispatch (issue #6148's
     * fix) means they now go through {@code CypherProcedure#validateArgs} like every other procedure - previously the
     * hardcoded {@code switch} in {@code CallStep} ignored {@code args} entirely, so {@code CALL db.labels(1)} silently
     * succeeded. Pinning this down so a future refactor cannot silently reintroduce the old, more permissive behavior.
     */
    @Test
    void dbLabelsNowRejectsAnUnexpectedArgument() {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("opencypher", "CALL db.labels(1) YIELD label RETURN label")) {
          while (rs.hasNext())
            rs.next();
        }
      }).isInstanceOf(CommandSemanticException.class);
    }

    /**
     * A custom {@code DEFINE FUNCTION} has no statically declared output signature - unlike a registered
     * {@link com.arcadedb.query.opencypher.procedures.CypherProcedure}, {@code CypherProcedureRegistry} does not know
     * it at all, so {@code resolveYieldAllFieldNames} returns {@code null} for it. This pins down that an unresolvable
     * {@code YIELD *} keeps exactly today's (pre-fix) behavior instead of the fix guessing at a shape it cannot know.
     */
    @Test
    void yieldStarOnAnUnresolvableProcedureNameKeepsPriorBehavior() {
      database.command("sql", "DEFINE FUNCTION math.double \"SELECT :x * 2\" PARAMETERS [x] LANGUAGE sql");

      assertThatThrownBy(() -> database.query("opencypher", "CALL math.double(4) YIELD * RETURN value").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable");

      assertThatThrownBy(() -> database.query("opencypher", "CALL math.double(4) YIELD * RETURN *").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("NoVariablesInScope");
    }
  }

  /**
   * Issue #9453: a global aggregate over a column yielded by an {@code algo.*} procedure answered 0 / null while a Graph
   * Analytical View served the procedure, because the CALL step's count-only fast path replaced the procedure rows with
   * empty ones. The fast path also read the procedure's row-count hint from a query-wide variable, so a chained CALL
   * counted only the rows of the LAST input row, and a later CALL could count the rows of an EARLIER procedure.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9453AlgoAggregatesUnderView extends TestHelper {

    private static final String PAGERANK = "CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 10, tolerance: 0.0, direction: 'BOTH'}) "
        + "YIELD node, score ";

    @Override
    protected void beginTest() {
      // The reporter's graph: 1->2->3 plus two isolated nodes, so three components
      database.command("sql", "CREATE VERTEX TYPE Node");
      database.command("sql", "CREATE EDGE TYPE EDGE");
      // the weight algo.dijkstra.singleSource reads, declared so a view can materialize it
      database.command("sql", "CREATE PROPERTY EDGE.w DOUBLE");
      database.transaction(() -> {
        for (int i = 1; i <= 5; i++)
          database.command("sql", "CREATE VERTEX Node SET id = " + i);
        database.command("sql", "CREATE EDGE EDGE FROM (SELECT FROM Node WHERE id = 1) TO (SELECT FROM Node WHERE id = 2) SET w = 1");
        database.command("sql", "CREATE EDGE EDGE FROM (SELECT FROM Node WHERE id = 2) TO (SELECT FROM Node WHERE id = 3) SET w = 2");
      });
    }

    private GraphAnalyticalView createView() throws Exception {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW benchmark VERTEX TYPES (Node) EDGE TYPES (EDGE)");
      database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW benchmark");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "benchmark");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      return view;
    }

    private void dropView() {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW benchmark");
    }

    private Result single(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).as(query).isTrue();
        final Result row = rs.next();
        assertThat(rs.hasNext()).as(query).isFalse();
        return row;
      }
    }

    private long count(final String query) {
      return single(query).<Number>getProperty("n").longValue();
    }

    @Test
    void wccGlobalAggregatesOverAYieldedColumnUnderAView() throws Exception {
      final String q = "CALL algo.wcc() YIELD node, componentId RETURN count(*) AS n, count(DISTINCT componentId) AS d, max(componentId) AS m";
      final String withQ = "CALL algo.wcc() YIELD node, componentId WITH node, componentId "
          + "RETURN count(*) AS n, count(DISTINCT componentId) AS d, max(componentId) AS m";

      final Result before = single(q);
      assertThat(before.<Number>getProperty("n").longValue()).isEqualTo(5);
      assertThat(before.<Number>getProperty("d").longValue()).isEqualTo(3);
      final long maxBefore = before.<Number>getProperty("m").longValue();

      createView();
      try {
        for (final String query : new String[] { q, withQ }) {
          final Result row = single(query);
          assertThat(row.<Number>getProperty("n").longValue()).as(query).isEqualTo(5);
          assertThat(row.<Number>getProperty("d").longValue()).as(query).isEqualTo(3);
          assertThat(row.<Number>getProperty("m")).as(query).isNotNull();
          assertThat(row.<Number>getProperty("m").longValue()).as(query).isEqualTo(maxBefore);
        }
        // count(*) alone keeps the fast path and must still be exact
        assertThat(count("CALL algo.wcc() YIELD node, componentId RETURN count(*) AS n")).isEqualTo(5);
        assertThat(count("CALL algo.wcc() YIELD node, componentId RETURN count(*) AS n, count(*) AS n2")).isEqualTo(5);
      } finally {
        dropView();
      }
    }

    @Test
    void pagerankGlobalAggregatesOverAYieldedColumnUnderAView() throws Exception {
      final Result before = single(PAGERANK + "RETURN count(*) AS n, sum(score) AS s, max(score) AS m");
      final double sumBefore = before.<Number>getProperty("s").doubleValue();
      final double maxBefore = before.<Number>getProperty("m").doubleValue();
      assertThat(sumBefore).isCloseTo(1.0, offset(1e-9));

      createView();
      try {
        for (final String query : new String[] { PAGERANK + "RETURN count(*) AS n, sum(score) AS s, max(score) AS m",
            PAGERANK + "WITH node, score RETURN count(*) AS n, sum(score) AS s, max(score) AS m" }) {
          final Result row = single(query);
          assertThat(row.<Number>getProperty("n").longValue()).as(query).isEqualTo(5);
          assertThat(row.<Number>getProperty("s").doubleValue()).as(query).isCloseTo(sumBefore, offset(1e-9));
          assertThat(row.<Number>getProperty("m")).as(query).isNotNull();
          assertThat(row.<Number>getProperty("m").doubleValue()).as(query).isCloseTo(maxBefore, offset(1e-9));
        }
        assertThat(count(PAGERANK + "RETURN count(score) AS n")).isEqualTo(5);
        assertThat(single(PAGERANK + "RETURN avg(score) AS a").<Number>getProperty("a").doubleValue()).isCloseTo(sumBefore / 5, offset(1e-9));
      } finally {
        dropView();
      }
    }

    @Test
    void chainedCountOnlyCallSumsTheRowsOfEveryInputRow() throws Exception {
      final String[] queries = {
          // the procedure runs once per input row: every run contributes its own rows
          "UNWIND [1, 2, 3] AS x CALL algo.wcc() YIELD node RETURN count(*) AS n",
          "MATCH (s:Node) CALL algo.bfs(s) YIELD node RETURN count(*) AS n",
          // a CALL that follows an algo.* CALL must not count the earlier procedure's rows
          "CALL algo.wcc() YIELD node WITH count(*) AS c CALL db.labels() YIELD label RETURN count(*) AS n",
          "MATCH (s:Node) WITH s LIMIT 1 CALL algo.wcc() YIELD node WITH count(*) AS c MATCH (s:Node {id: 4}) "
              + "CALL algo.bfs(s) YIELD node RETURN count(*) AS n",
          // YIELD WHERE drops rows the procedure's size still counts
          "CALL algo.wcc() YIELD node, componentId WHERE node.id > 2 RETURN count(*) AS n",
          "UNWIND [1, 2] AS x CALL algo.wcc() YIELD node, componentId WHERE node.id > 2 RETURN count(*) AS n",
          // an empty OPTIONAL CALL still yields its one null row
          "MATCH (s:Node {id: 4}) OPTIONAL CALL algo.bfs(s) YIELD node RETURN count(*) AS n",
          "MATCH (s:Node) WHERE s.id >= 3 OPTIONAL CALL algo.bfs(s) YIELD node RETURN count(*) AS n",
          // a procedure that is not an algorithm takes the same count-only path
          "CALL db.labels() YIELD label RETURN count(*) AS n",
          "UNWIND [1, 2, 3] AS x CALL db.labels() YIELD label RETURN count(*) AS n" };

      final long[] expected = new long[queries.length];
      for (int i = 0; i < queries.length; i++)
        expected[i] = count(queries[i]);
      assertThat(expected[0]).isEqualTo(15);
      // reachable over BOTH directions: 2 for each node of {1,2,3}, 0 for the isolated ones
      assertThat(expected[1]).isEqualTo(6);
      assertThat(expected[3]).isEqualTo(0);
      assertThat(expected[4]).isEqualTo(3);
      assertThat(expected[5]).isEqualTo(6);
      assertThat(expected[6]).isEqualTo(1);
      // node 3 reaches 2 nodes, nodes 4 and 5 none: one null row each
      assertThat(expected[7]).isEqualTo(4);
      assertThat(expected[8]).isEqualTo(1);
      assertThat(expected[9]).isEqualTo(3);

      createView();
      try {
        for (int i = 0; i < queries.length; i++)
          assertThat(count(queries[i])).as(queries[i]).isEqualTo(expected[i]);
      } finally {
        dropView();
      }
    }

    /**
     * The boundary of the count-only path: a RETURN that also reads a yielded column (a grouping key) must see the real
     * rows, and a write procedure must run its writes whatever the RETURN reads.
     */
    @Test
    void countOnlyPathBoundary() throws Exception {
      createView();
      try {
        final String q = "CALL algo.wcc() YIELD node, componentId RETURN componentId, count(*) AS c ORDER BY c DESC, componentId";
        try (final ResultSet rs = database.query("opencypher", q)) {
          final StringBuilder counts = new StringBuilder();
          while (rs.hasNext()) {
            final Result row = rs.next();
            assertThat(row.<Object>getProperty("componentId")).as(q).isNotNull();
            counts.append(row.<Number>getProperty("c").longValue()).append(' ');
          }
          assertThat(counts.toString().trim()).as(q).isEqualTo("3 1 1");
        }
        assertThat(count("CALL algo.wcc() YIELD node, componentId RETURN count(*) AS n, collect(node.id)[0] IS NOT NULL AS x")).isEqualTo(5);
      } finally {
        dropView();
      }

      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher",
            "UNWIND [1, 2, 3] AS k CALL merge.node(['Tmp9453'], {k: k}, {}) YIELD node RETURN count(*) AS n")) {
          assertThat(rs.next().<Number>getProperty("n").longValue()).isEqualTo(3);
        }
      });
      assertThat(count("MATCH (t:Tmp9453) RETURN count(t) AS n")).isEqualTo(3);
    }

    /**
     * The count-only fast path relies on each procedure's stream knowing its exact size. Nothing functional fails when a
     * stream loses it (a {@code filter()} added to one of them, say) - the count just goes back to building a row per
     * node - so this pins it, on both the OLTP and the view-backed path.
     */
    @Test
    void algoStreamsKnowTheirExactSize() throws Exception {
      assertAlgoStreamsAreSized(false);

      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW sized VERTEX TYPES (Node) EDGE TYPES (EDGE) EDGE PROPERTIES (w)");
      assertThat(GraphAnalyticalViewRegistry.get(database, "sized").awaitReady(60, TimeUnit.SECONDS)).isTrue();
      try {
        assertAlgoStreamsAreSized(true);
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW sized");
      }
    }

    private void assertAlgoStreamsAreSized(final boolean viaView) {
      final Vertex start;
      try (final ResultSet rs = database.query("sql", "SELECT FROM Node WHERE id = 1")) {
        start = rs.next().getVertex().orElseThrow();
      }
      final Object[][] calls = { { "algo.wcc", new Object[0] }, { "algo.pagerank", new Object[0] },
          { "algo.labelpropagation", new Object[0] }, { "algo.localClusteringCoefficient", new Object[0] },
          { "algo.bfs", new Object[] { start } }, { "algo.dijkstra.singleSource", new Object[] { start, "EDGE", "w" } } };

      for (final Object[] call : calls) {
        final String name = (String) call[0];
        final CypherProcedure procedure = CypherProcedureRegistry.get(name);
        final long size;
        final BasicCommandContext context = newContext();
        try (final Stream<Result> rows = procedure.execute((Object[]) call[1], null, context)) {
          size = rows.spliterator().getExactSizeIfKnown();
        }
        assertThat(Boolean.TRUE.equals(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))).as(name).isEqualTo(viaView);
        long traversed = 0;
        try (final Stream<Result> rows = procedure.execute((Object[]) call[1], null, newContext())) {
          for (final Iterator<Result> it = rows.iterator(); it.hasNext(); it.next())
            ++traversed;
        }
        assertThat(traversed).as(name).isGreaterThan(0);
        assertThat(size).as(name).isEqualTo(traversed);
      }
    }

    private BasicCommandContext newContext() {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      return context;
    }

    /**
     * Found while fixing #9453: an OPTIONAL CALL whose procedure yielded no row for an input row dropped that row instead
     * of answering it with nulls, as OPTIONAL MATCH does and as Neo4j does for OPTIONAL CALL.
     */
    @Test
    void optionalCallWithNoRowsAnswersANullRow() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (s:Node) WHERE s.id >= 3 OPTIONAL CALL algo.bfs(s) YIELD node, depth RETURN s.id AS id, node.id AS reached, depth "
              + "ORDER BY id, reached")) {
        final StringBuilder rows = new StringBuilder();
        while (rs.hasNext()) {
          final Result row = rs.next();
          rows.append(row.<Object>getProperty("id")).append(':').append(row.<Object>getProperty("reached")).append(':')
              .append(row.<Object>getProperty("depth")).append(' ');
        }
        assertThat(rows.toString().trim()).isEqualTo("3:1:2 3:2:1 4:null:null 5:null:null");
      }

      // every row filtered out by YIELD WHERE is no row either
      final Result row = single("MATCH (s:Node {id: 1}) OPTIONAL CALL algo.bfs(s) YIELD node WHERE node.id > 100 RETURN s.id AS id, node");
      assertThat(row.<Integer>getProperty("id")).isEqualTo(1);
      assertThat(row.<Object>getProperty("node")).isNull();

      // YIELD * names no field: the null row binds every field the procedure declares
      try (final ResultSet rs = database.query("opencypher", "MATCH (s:Node {id: 4}) OPTIONAL CALL algo.bfs(s) YIELD * RETURN *")) {
        final Result star = rs.next();
        assertThat(star.getPropertyNames()).contains("s", "node", "depth");
        assertThat(star.<Object>getProperty("node")).isNull();
        assertThat(star.<Object>getProperty("depth")).isNull();
        assertThat(rs.hasNext()).isFalse();
      }

      // a plain CALL still drops the input row
      try (final ResultSet rs = database.query("opencypher", "MATCH (s:Node {id: 4}) CALL algo.bfs(s) YIELD node RETURN s.id AS id, node")) {
        assertThat(rs.hasNext()).isFalse();
      }
    }
  }

  /**
   * Issue #11008: a relationship variable read only by a procedure {@code CALL} - in its arguments, or in its
   * {@code YIELD ... WHERE} - was bound to {@code null}. A relationship variable nothing reads is walked as an anonymous
   * hop, and the check deciding whether anything reads it ({@code CypherVariableUsage.isEdgeVariableReferenced})
   * enumerated clause types by hand and had no arm for {@code CALL}, nor for {@code LOAD CSV}, whose URL is an
   * expression too. Neo4j binds the relationship in every one of these spellings, and so does the same query with a
   * {@code WITH a, b, r} in front of the call, which is how the gap showed.
   * <p>
   * The {@code YIELD ... WHERE} spelling hid a second bug behind the first: the predicate was evaluated against the
   * yielded fields only, so it could not read any outer variable, bound or not. In Neo4j it sees the whole scope.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue11008RelationshipReadOnlyByCall {
    private static final String DB_PATH = "./target/databases/issue-11008";

    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.transaction(() -> database.command("opencypher",
          "CREATE (:CallArgBugNode {marker: 'a'})-[:CALL_ARG_BUG_EDGE {weight: 7}]->(:CallArgBugNode {marker: 'b'})"));
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void relationshipPassedOnlyAsProcedureArgumentIsBound() {
      final Result row = single("""
          MATCH (a:CallArgBugNode {marker: 'a'})-[r:CALL_ARG_BUG_EDGE]->(b:CallArgBugNode {marker: 'b'})
          CALL do.when(true, 'RETURN $r AS r', null, {r: r}) YIELD value
          RETURN value.r AS captured, value.r IS NULL AS isNull""");

      assertThat(row.<Object>getProperty("captured")).isInstanceOf(Edge.class);
      assertThat(((Edge) row.getProperty("captured")).getTypeName()).isEqualTo("CALL_ARG_BUG_EDGE");
      assertThat(row.<Boolean>getProperty("isNull")).isFalse();
    }

    @Test
    void projectingTheRelationshipFirstStillAnswersTheSame() {
      final Result row = single("""
          MATCH (a:CallArgBugNode {marker: 'a'})-[r:CALL_ARG_BUG_EDGE]->(b:CallArgBugNode {marker: 'b'})
          WITH a, b, r
          CALL do.when(true, 'RETURN $r AS r', null, {r: r}) YIELD value
          RETURN value.r AS captured, value.r IS NULL AS isNull""");

      assertThat(row.<Object>getProperty("captured")).isInstanceOf(Edge.class);
      assertThat(row.<Boolean>getProperty("isNull")).isFalse();
    }

    @Test
    void relationshipPropertyReadOnlyByProcedureArgumentIsBound() {
      final Result row = single("""
          MATCH (:CallArgBugNode {marker: 'a'})-[r:CALL_ARG_BUG_EDGE]->(:CallArgBugNode {marker: 'b'})
          CALL do.when(true, 'RETURN $w AS w', null, {w: r.weight}) YIELD value
          RETURN value.w AS weight""");

      assertThat(row.<Number>getProperty("weight").intValue()).isEqualTo(7);
    }

    @Test
    void relationshipReadOnlyByYieldWhereIsBound() {
      final Result row = single("""
          MATCH (:CallArgBugNode {marker: 'a'})-[r:CALL_ARG_BUG_EDGE]->(:CallArgBugNode {marker: 'b'})
          CALL do.when(true, 'RETURN 1 AS one', null, {}) YIELD value WHERE r.weight = 7
          RETURN count(*) AS matched""");

      assertThat(row.<Number>getProperty("matched").longValue()).isEqualTo(1L);
    }

    /**
     * The second half of the YIELD WHERE case, which no binding fix reaches: the predicate was evaluated against the
     * yielded fields alone, so any outer variable - a projected value, not only a relationship - read as null there.
     */
    @Test
    void yieldWhereSeesTheOuterRow() {
      final Result row = single("""
          UNWIND [1, 7, 9] AS w
          CALL do.when(true, 'RETURN $x AS x', null, {x: w * 2}) YIELD value WHERE w = 7 AND value.x = 14
          RETURN collect(w) AS kept""");

      assertThat(row.<List<Object>>getProperty("kept")).hasSize(1);
      assertThat(((Number) row.<List<Object>>getProperty("kept").getFirst()).intValue()).isEqualTo(7);
    }

    @Test
    void relationshipReadOnlyByLoadCsvUrlIsBound() throws IOException {
      final File csv = new File(DB_PATH + "-csv/rows.csv");
      csv.getParentFile().mkdirs();
      try (final PrintWriter writer = new PrintWriter(csv, "UTF-8")) {
        writer.println("id");
        writer.println("1");
        writer.println("2");
      }
      try {
        database.transaction(() -> database.command("opencypher", "MATCH ()-[r:CALL_ARG_BUG_EDGE]->() SET r.url = $url",
            Map.of("url", "file://" + csv.getAbsolutePath())));

        final Result row = single("""
            MATCH (:CallArgBugNode {marker: 'a'})-[r:CALL_ARG_BUG_EDGE]->(:CallArgBugNode {marker: 'b'})
            LOAD CSV WITH HEADERS FROM r.url AS line
            RETURN count(line) AS loaded""");

        assertThat(row.<Number>getProperty("loaded").longValue()).isEqualTo(2L);
      } finally {
        csv.delete();
        csv.getParentFile().delete();
      }
    }

    private Result single(final String query) {
      // do.when may run a write statement, so it is classified as a write procedure and needs command().
      try (final ResultSet rs = database.command("opencypher", query)) {
        final List<Result> rows = rs.stream().toList();
        assertThat(rows).hasSize(1);
        return rows.getFirst();
      }
    }
  }
}
