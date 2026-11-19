/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.database.QueryMetricsRecorder;
import com.arcadedb.database.QueryTracer;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests, one nested class per reported case.
 */
class SqlQueryDiagnosticsTest {
  /**
   * Regression for #7141.
   * <p>
   * Regression tests for issue #7141.
   * <p>
   * Two of the three defects the ticket reports are behavioural and are covered here: {@code ALTER DATABASE}
   * used to throw {@code "Error on saving database configuration"} with the {@link IOException} neither chained
   * nor logged, so a read-only filesystem, a permission problem and a full volume were indistinguishable; and
   * the HA single-bucket warning advised {@code CREATE VERTEX TYPE <name> BUCKETS 16} on types that by
   * definition already exist, which cannot work. The third (two log messages printing {@code null} where the
   * offending value belonged) is guarded by {@code LogMessageArityCensusTest} (census module).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Diagnostics extends TestHelper {

    /**
     * The reason the configuration could not be written must survive as the cause and the message must name
     * both the setting and the database, so the operator can tell the failure modes apart.
     */
    @Test
    void alterDatabaseReportsWhySavingTheConfigurationFailed() {
      final File configuration = new File(((DatabaseInternal) database).getDatabasePath() + File.separator
          + "configuration.json");

      // Turning the configuration file into a directory makes the FileOutputStream behind saveConfiguration()
      // fail with an IOException on every platform and for every user, root included.
      assertThat(configuration.delete() || !configuration.exists()).isTrue();
      assertThat(configuration.mkdir()).isTrue();
      try {
        assertThatThrownBy(() -> database.command("sql", "alter database `arcadedb.asyncWorkerThreads` 3"))
            .isInstanceOf(CommandExecutionException.class)
            .hasMessageContaining("arcadedb.asyncWorkerThreads")
            .hasMessageContaining(database.getName())
            .hasCauseInstanceOf(IOException.class);
      } finally {
        assertThat(configuration.delete()).isTrue();
      }
    }

    /**
     * The advice the HA plugin now gives has to work on a type that already exists - which every type it warns
     * about does, since it warns while wrapping an existing database for HA.
     */
    @Test
    void theBucketAdviceGivenForSingleBucketTypesActuallyRuns() {
      database.command("sql", "create vertex type Contended buckets 1");
      assertThat(database.getSchema().getType("Contended").getBuckets(false)).hasSize(1);

      // What the warning used to advise: impossible, the type already exists.
      assertThatThrownBy(() -> database.command("sql", "create vertex type Contended buckets 16"))
          .isInstanceOf(AlreadyExistsException.class)
          .hasMessageContaining("already exists");

      // What it advises now.
      database.command("sql", "alter type Contended bucket +Contended_1");
      database.command("sql", "alter type Contended bucket +Contended_2");
      database.command("sql", "alter type Contended bucketselectionstrategy `thread`");

      assertThat(database.getSchema().getType("Contended").getBuckets(false)).hasSize(3);
      assertThat(database.getSchema().getType("Contended").getBucketSelectionStrategy().getName()).isEqualTo("thread");
    }
  }

  /**
   * Regression for #7418.
   * <p>
   * Issue #7418: {@code SQLQueryEngine.analyze(query).execute(...)} - the entry point VectorSearch, HybridSearch and
   * the full-text SEARCH_INDEX function use to run their generated SQL - ran outside {@link QueryMetricsRecorder} and
   * {@link QueryTracer}, unlike {@code database.query()}/{@code command()}. So vector, hybrid and full-text search
   * traffic was invisible in {@code arcadedb.query.duration} and in traces, on every wire protocol.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AnalyzedQueryMetrics extends TestHelper {

    @AfterEach
    void reset() {
      QueryMetricsRecorder.Holder.register(null);
      QueryTracer.Holder.register(null);
      ProtocolContext.clear();
    }

    @Test
    void idempotentStatementIsRecordedAsQuery() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Doc");
        database.command("sql", "INSERT INTO Doc SET name = 'a'");
      });

      final AtomicReference<String> recorded = new AtomicReference<>();
      final AtomicInteger spansOpened = new AtomicInteger();
      QueryMetricsRecorder.Holder.register((protocol, database, language, type, nanos) -> recorded.set(protocol + "|" + language + "|" + type));
      QueryTracer.Holder.register((protocol, database, language, type, query) -> {
        spansOpened.incrementAndGet();
        return QueryTracer.Span.NO_OP;
      });

      database.transaction(() -> {
        ProtocolContext.set("test");
        final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("SELECT FROM Doc");
        assertThat(analyzed.isIdempotent()).isTrue();
        try (final ResultSet resultSet = analyzed.execute(Map.of())) {
          assertThat(resultSet.hasNext()).isTrue();
        }
      });

      assertThat(recorded.get()).isEqualTo("test|sql|query");
      assertThat(spansOpened.get()).isEqualTo(1);
    }

    @Test
    void nonIdempotentStatementIsRecordedAsCommand() {
      database.transaction(() -> database.command("sql", "CREATE DOCUMENT TYPE Doc2"));

      final AtomicReference<String> recorded = new AtomicReference<>();
      QueryMetricsRecorder.Holder.register((protocol, database, language, type, nanos) -> recorded.set(protocol + "|" + language + "|" + type));

      database.transaction(() -> {
        ProtocolContext.set("test");
        final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("INSERT INTO Doc2 SET name = 'a'");
        assertThat(analyzed.isIdempotent()).isFalse();
        try (final ResultSet resultSet = analyzed.execute(Map.of())) {
          assertThat(resultSet.hasNext()).isTrue();
        }
      });

      assertThat(recorded.get()).isEqualTo("test|sql|command");
    }
  }

  /**
   * Regression for #9313.
   * <p>
   * Issue #9313: {@code PROFILE <statement>} answered {@code isIdempotent()==true} and {@code [READ]} whatever it wrapped,
   * yet executes the wrapped statement. So {@code PROFILE INSERT} passed every read-only gate ({@code query()}, streaming,
   * MCP, HA follower routing). The classification must follow the wrapped statement; {@code EXPLAIN} never executes, so it
   * stays idempotent while still reporting the wrapped operation types.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ProfileClassification extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> database.getSchema().createDocumentType("Person"));
    }

    @Test
    void profileOfAWriteIsNotIdempotent() {
      final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("PROFILE INSERT INTO Person SET name = 'a'");
      assertThat(analyzed.isIdempotent()).isFalse();
      assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE).doesNotContain(OperationType.READ);
    }

    @Test
    void profileOfUpdateAndDeleteIsNotIdempotent() {
      assertThat(database.getQueryEngine("sql").analyze("PROFILE UPDATE Person SET name = 'b'").isIdempotent()).isFalse();
      assertThat(database.getQueryEngine("sql").analyze("PROFILE DELETE FROM Person").getOperationTypes()).contains(OperationType.DELETE);
    }

    @Test
    void profileOfASelectStaysIdempotent() {
      final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("PROFILE SELECT FROM Person");
      assertThat(analyzed.isIdempotent()).isTrue();
      assertThat(analyzed.getOperationTypes()).containsOnly(OperationType.READ);
    }

    @Test
    void profileOfDdlIsDdl() {
      final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("PROFILE CREATE DOCUMENT TYPE Other");
      assertThat(analyzed.isDDL()).isTrue();
      assertThat(analyzed.isIdempotent()).isFalse();
    }

    @Test
    void explainStaysIdempotentButReportsTheWrappedOperation() {
      final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("EXPLAIN INSERT INTO Person SET name = 'a'");
      assertThat(analyzed.isIdempotent()).isTrue();
      assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE);
    }

    @Test
    void profileInsertIsRefusedByQuery() {
      assertThatThrownBy(() -> database.query("sql", "PROFILE INSERT INTO Person SET name = 'x'"))
          .isInstanceOf(QueryNotIdempotentException.class);
      assertThat(database.countType("Person", false)).isZero();
    }

    @Test
    void profileSelectStillWorksOnQuery() {
      database.transaction(() -> database.command("sql", "INSERT INTO Person SET name = 'a'"));
      try (final ResultSet rs = database.query("sql", "PROFILE SELECT FROM Person")) {
        assertThat(rs.hasNext()).isTrue();
      }
    }

    @Test
    void cypherProfileOfAWriteIsRefusedByQuery() {
      assertThatThrownBy(() -> database.query("opencypher", "PROFILE CREATE (:Person {name: 'x'})"))
          .isInstanceOf(QueryNotIdempotentException.class);
      assertThat(database.countType("Person", false)).isZero();
    }

    @Test
    void cypherProfileOfAWriteIsClassifiedAsAWrite() {
      final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("opencypher").analyze("PROFILE CREATE (:Person {name: 'x'})");
      assertThat(analyzed.isIdempotent()).isFalse();
    }

    @Test
    void cypherProfileOfAReadIsAllowedOnQuery() {
      try (final ResultSet rs = database.query("opencypher", "PROFILE MATCH (p:Person) RETURN p")) {
        rs.stream().count();
      }
    }

    /** A separator other than a single space is not a PROFILE prefix on any path, so the write can never slip through as idempotent. */
    @Test
    void cypherProfileWithANewlineSeparatorNeverWritesThroughQuery() {
      assertThatThrownBy(() -> database.query("opencypher", "PROFILE\nCREATE (:Person {name: 'x'})")).isInstanceOf(Exception.class);
      assertThat(database.countType("Person", false)).isZero();
    }
  }

  /**
   * Regression for #9628.
   * <p>
   * Issue #9628: {@code isIdempotent()}, {@code isDDL()} and {@code getOperationTypes()} were answered by each statement node
   * from its own class and never from the statements nested in it. A write in a {@code LET}, in a SELECT's {@code LET}, in
   * a projection or in a FROM target was reported read-only ({@code [READ]}) and executed through the read-only
   * {@code query()} door; a DDL inside {@code IF}/{@code FOREACH}/{@code WHILE} was reported as a data write, and a read
   * inside {@code FOREACH}/{@code WHILE} as a write.
   */
  @Nested
  class NestedStatementClassification extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Victim");
        database.getSchema().createVertexType("V1");
      });
    }

    private QueryEngine.AnalyzedQuery sql(final String command) {
      return database.getQueryEngine("sql").analyze(command);
    }

    private QueryEngine.AnalyzedQuery script(final String command) {
      return database.getQueryEngine("sqlscript").analyze(command);
    }

    // ---------------------------------------------------------------- nested data writes: classification

    @ParameterizedTest
    @ValueSource(strings = {
        "LET $x = (INSERT INTO Victim SET a = 1)",
        "SELECT FROM Victim LET $y = (INSERT INTO Victim SET a = 2)",
        "SELECT $y FROM Victim LET $y = (INSERT INTO Victim SET a = 3)",
        "SELECT (INSERT INTO Victim SET a = 5) as p FROM Victim",
        "SELECT FROM (INSERT INTO Victim SET a = 6)",
        "SELECT FROM Victim LET $y = (INSERT INTO Victim SET a = 2) limit 20001",
        "PROFILE SELECT FROM (INSERT INTO Victim SET a = 6)",
        "LET $x = (PROFILE INSERT INTO Victim SET a = 7)",
        "SELECT CASE WHEN 1 = 1 THEN (INSERT INTO Victim SET a = 8) END as p FROM Victim",
        "RETURN (INSERT INTO Victim SET a = 9)" })
    void aNestedInsertIsAWrite(final String command) {
      final QueryEngine.AnalyzedQuery analyzed = sql(command);
      assertThat(analyzed.isIdempotent()).isFalse();
      assertThat(analyzed.isDDL()).isFalse();
      assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE);
    }

    @Test
    void aNestedDeleteAndUpdateAreWrites() {
      assertThat(sql("LET $x = (DELETE FROM Victim)").isIdempotent()).isFalse();
      assertThat(sql("LET $x = (DELETE FROM Victim)").getOperationTypes()).contains(OperationType.DELETE);
      assertThat(sql("LET $x = (UPDATE Victim SET a = 9)").isIdempotent()).isFalse();
      assertThat(sql("LET $x = (UPDATE Victim SET a = 9)").getOperationTypes()).contains(OperationType.UPDATE);
      assertThat(sql("SELECT FROM Victim LET $y = (DELETE FROM Victim)").getOperationTypes()).contains(OperationType.DELETE);
    }

    @Test
    void aNestedSchemaChangeIsDdl() {
      final QueryEngine.AnalyzedQuery let = sql("LET $x = (CREATE DOCUMENT TYPE Sneak1)");
      assertThat(let.isIdempotent()).isFalse();
      assertThat(let.isDDL()).isTrue();
      assertThat(let.getOperationTypes()).containsExactly(OperationType.SCHEMA);

      final QueryEngine.AnalyzedQuery select = sql("SELECT FROM Victim LET $z = (CREATE DOCUMENT TYPE Sneak2)");
      assertThat(select.isIdempotent()).isFalse();
      assertThat(select.isDDL()).isTrue();
      assertThat(select.getOperationTypes()).contains(OperationType.SCHEMA);
    }

    /** A write nested in another write adds what it does: an INSERT that runs a CREATE TYPE is also a schema change. */
    @Test
    void aWriteNestedInAWriteContributesItsOperation() {
      final QueryEngine.AnalyzedQuery analyzed = sql("INSERT INTO Victim SET a = (CREATE DOCUMENT TYPE Sneak3)");
      assertThat(analyzed.isDDL()).isTrue();
      assertThat(analyzed.getOperationTypes()).containsExactlyInAnyOrder(OperationType.CREATE, OperationType.SCHEMA);
    }

    // ---------------------------------------------------------------- the read-only door refuses them and writes nothing

    @ParameterizedTest
    @ValueSource(strings = {
        "LET $x = (INSERT INTO Victim SET a = 1)",
        "SELECT FROM Victim LET $y = (INSERT INTO Victim SET a = 2)",
        "SELECT $y FROM Victim LET $y = (INSERT INTO Victim SET a = 3)",
        "SELECT (INSERT INTO Victim SET a = 5) as p FROM Victim",
        "SELECT FROM (INSERT INTO Victim SET a = 6)" })
    void queryRefusesANestedInsert(final String command) {
      database.transaction(() -> database.command("sql", "INSERT INTO Victim SET a = 0"));
      assertThatThrownBy(() -> database.query("sql", command)).isInstanceOf(QueryNotIdempotentException.class);
      assertThat(database.countType("Victim", false)).isEqualTo(1);
    }

    @Test
    void queryRefusesANestedDeleteAndUpdate() {
      database.transaction(() -> database.command("sql", "INSERT INTO Victim SET a = 0"));
      assertThatThrownBy(() -> database.query("sql", "LET $x = (DELETE FROM Victim)")).isInstanceOf(QueryNotIdempotentException.class);
      assertThatThrownBy(() -> database.query("sql", "LET $x = (UPDATE Victim SET a = 9)")).isInstanceOf(QueryNotIdempotentException.class);
      assertThat(database.countType("Victim", false)).isEqualTo(1);
      try (final ResultSet rs = database.query("sql", "SELECT a FROM Victim")) {
        assertThat(rs.next().<Integer>getProperty("a")).isZero();
      }
    }

    @Test
    void queryRefusesANestedSchemaChange() {
      assertThatThrownBy(() -> database.query("sql", "LET $x = (CREATE DOCUMENT TYPE Sneak1)")).isInstanceOf(QueryNotIdempotentException.class);
      assertThatThrownBy(() -> database.query("sql", "SELECT FROM Victim LET $z = (CREATE DOCUMENT TYPE Sneak2)"))
          .isInstanceOf(QueryNotIdempotentException.class);
      assertThat(database.getSchema().existsType("Sneak1")).isFalse();
      assertThat(database.getSchema().existsType("Sneak2")).isFalse();
    }

    @Test
    void sqlScriptQueryRefusesANestedInsert() {
      assertThatThrownBy(() -> database.query("sqlscript", "SELECT FROM Victim LET $y = (INSERT INTO Victim SET a = 2);"))
          .isInstanceOf(QueryNotIdempotentException.class);
      assertThat(database.countType("Victim", false)).isZero();
    }

    /** command() is the door for writes: the same statements still run there. */
    @Test
    void commandStillRunsANestedWrite() {
      database.transaction(() -> database.command("sql", "LET $x = (INSERT INTO Victim SET a = 1)"));
      assertThat(database.countType("Victim", false)).isEqualTo(1);
    }

    // ---------------------------------------------------------------- nested reads stay reads

    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT FROM Victim",
        "LET $x = 1",
        "LET $x = (SELECT FROM Victim)",
        "SELECT FROM Victim LET $y = (SELECT FROM Victim)",
        "SELECT (SELECT count(*) FROM Victim) as p FROM Victim",
        "SELECT FROM (SELECT FROM Victim)",
        "SELECT FROM Victim WHERE a IN (SELECT a FROM Victim)",
        "MATCH {type: Victim, as: v} RETURN v",
        "TRAVERSE out() FROM (SELECT FROM V1)",
        "SELECT FROM Victim LET $x = (EXPLAIN INSERT INTO Victim SET a = 1)",
        "EXPLAIN SELECT FROM (INSERT INTO Victim SET a = 6)" })
    void aNestedReadStaysARead(final String command) {
      final QueryEngine.AnalyzedQuery analyzed = sql(command);
      assertThat(analyzed.isIdempotent()).isTrue();
      assertThat(analyzed.isDDL()).isFalse();
      if (!command.startsWith("EXPLAIN"))
        assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.READ);
      try (final ResultSet rs = database.query("sql", command)) {
        rs.stream().count();
      }
    }

    /** A write that reads to find what it writes is still classified by its write alone, as it always was. */
    @Test
    void aWriteWithANestedReadKeepsItsOwnOperations() {
      assertThat(sql("INSERT INTO Victim FROM (SELECT FROM Victim)").getOperationTypes()).containsExactly(OperationType.CREATE);
      assertThat(sql("DELETE FROM Victim WHERE a IN (SELECT a FROM Victim)").getOperationTypes()).containsExactly(OperationType.DELETE);
      assertThat(sql("UPDATE Victim SET a = (SELECT count(*) FROM Victim)").getOperationTypes()).containsExactly(OperationType.UPDATE);
    }

    /** A DDL stores the statements it holds (a view's query, a trigger's body) and runs none of them now. */
    @Test
    void aDdlIsClassifiedByItselfNotByTheStatementItDefines() {
      final QueryEngine.AnalyzedQuery view = sql("CREATE MATERIALIZED VIEW SneakView AS SELECT FROM Victim");
      assertThat(view.isDDL()).isTrue();
      assertThat(view.getOperationTypes()).containsExactly(OperationType.SCHEMA);
    }

    /** An expression a DDL evaluates while it runs is executed now, so a write in it is reported with the schema change. */
    @Test
    void aDdlEvaluatingANestedWriteReportsTheWrite() {
      final QueryEngine.AnalyzedQuery analyzed = sql("ALTER TYPE Victim CUSTOM sneak = (INSERT INTO Victim SET a = 1)");
      assertThat(analyzed.isDDL()).isTrue();
      assertThat(analyzed.isIdempotent()).isFalse();
      assertThat(analyzed.getOperationTypes()).containsExactlyInAnyOrder(OperationType.SCHEMA, OperationType.CREATE);
    }

    // ---------------------------------------------------------------- IF / FOREACH / WHILE follow their body

    @ParameterizedTest
    @ValueSource(strings = {
        "IF (1=1) { CREATE VERTEX TYPE Pwned; }",
        "FOREACH ($i IN [1]) { CREATE VERTEX TYPE Pwned; }",
        "WHILE ($x < 1) { CREATE VERTEX TYPE Pwned; }",
        "IF (1=1) { DROP TYPE Victim; }" })
    void aBlockWithADdlBodyIsDdl(final String command) {
      final QueryEngine.AnalyzedQuery analyzed = script(command);
      assertThat(analyzed.isIdempotent()).isFalse();
      assertThat(analyzed.isDDL()).isTrue();
      assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.SCHEMA);
    }

    @Test
    void ifInPlainSqlWithADdlBodyIsDdl() {
      final QueryEngine.AnalyzedQuery analyzed = sql("IF (1=1) { CREATE VERTEX TYPE Pwned; }");
      assertThat(analyzed.isDDL()).isTrue();
      assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.SCHEMA);
    }

    @Test
    void ifAroundBackupKeepsBackupsOperationTypes() {
      final QueryEngine.AnalyzedQuery analyzed = script("IF (1=1) { BACKUP DATABASE; }");
      assertThat(analyzed.isIdempotent()).isTrue();
      assertThat(analyzed.getOperationTypes()).containsExactlyInAnyOrder(OperationType.READ, OperationType.CREATE);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "FOREACH ($i IN [1]) { SELECT FROM Victim; }",
        "WHILE ($x < 1) { SELECT FROM Victim; }",
        "IF (1=1) { SELECT FROM Victim; }" })
    void aBlockWithAReadOnlyBodyIsARead(final String command) {
      final QueryEngine.AnalyzedQuery analyzed = script(command);
      assertThat(analyzed.isIdempotent()).isTrue();
      assertThat(analyzed.isDDL()).isFalse();
      assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.READ);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "FOREACH ($i IN [1]) { INSERT INTO Victim SET a = $i; }",
        "WHILE ($x < 1) { INSERT INTO Victim SET a = 1; }",
        "IF (1=1) { INSERT INTO Victim SET a = 1; }",
        "IF (1=1) { SELECT FROM (INSERT INTO Victim SET a = 1); }" })
    void aBlockWithAWriteBodyIsAWrite(final String command) {
      final QueryEngine.AnalyzedQuery analyzed = script(command);
      assertThat(analyzed.isIdempotent()).isFalse();
      assertThat(analyzed.isDDL()).isFalse();
      assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE).doesNotContain(OperationType.SCHEMA);
    }

    @Test
    void aBlockThatWritesStillExecutesThroughCommand() {
      database.transaction(() -> database.command("sqlscript", "FOREACH ($i IN [1, 2]) { INSERT INTO Victim SET a = $i; }"));
      assertThat(database.countType("Victim", false)).isEqualTo(2);
      database.transaction(() -> database.command("sqlscript", "IF (1=1) { SELECT FROM (INSERT INTO Victim SET a = 3); }"));
      assertThat(database.countType("Victim", false)).isEqualTo(3);
    }

    @Test
    void aReadOnlyForEachRunsThroughQuery() {
      database.transaction(() -> database.command("sql", "INSERT INTO Victim SET a = 1"));
      try (final ResultSet rs = database.query("sqlscript", "FOREACH ($i IN [1]) { SELECT FROM Victim; }")) {
        rs.stream().count();
      }
    }
  }
}
