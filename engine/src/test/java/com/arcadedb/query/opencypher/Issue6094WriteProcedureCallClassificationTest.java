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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue6094WriteProcedureCallClassificationTest {
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
