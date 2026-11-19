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
package com.arcadedb.exception;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.function.FunctionLibraryFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression tests for how the engine's exceptions map to an {@link ErrorCategory}, one nested class per fixed report.
 */
class ExceptionCategoryTest {
  /**
   * Regression for #10153: creating a schema object under a taken name was a server fault on every wire - HTTP 500, Postgres {@code XX000},
   * Bolt's generic DatabaseError - or, for the {@link SchemaException} ones, the opposite of what happened: the {@code SCHEMA}
   * category ("the schema does not define this"), Postgres {@code 42P01} undefined_table. The refusals raised two bare types with no
   * conflict arm anywhere. They raise {@link AlreadyExistsException} now, through SQL, Cypher and the Java API alike, and
   * {@link ErrorCategory} has a category for it every wire maps to its own conflict code.
   * <p>
   * The per-statement tests below pin today's sites, and {@code TakenNameRefusalCensusTest} (census module) pins the next one, over
   * the compiled classes, which is how three fixes (#10073, #10074, #10075) each landed one name and left these behind.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AlreadyExistsCategory extends TestHelper {

    @Test
    void everySqlCreateOnATakenNameIsAnAlreadyExistsConflict() {
      database.command("sql", "create vertex type V1");
      assertTaken(() -> database.command("sql", "create vertex type V1"), AlreadyExistsException.Kind.TYPE);
      // a name held by a type of another kind is the same conflict
      assertTaken(() -> database.command("sql", "create edge type V1"), AlreadyExistsException.Kind.TYPE);
      assertTaken(() -> database.command("sql", "create document type V1"), AlreadyExistsException.Kind.TYPE);

      database.command("sql", "create property V1.p1 string");
      assertTaken(() -> database.command("sql", "create property V1.p1 string"), AlreadyExistsException.Kind.PROPERTY);

      database.command("sql", "create index on V1 (p1) unique");
      assertTaken(() -> database.command("sql", "create index on V1 (p1) unique"), AlreadyExistsException.Kind.INDEX);

      database.command("sql", "create bucket b1");
      assertTaken(() -> database.command("sql", "create bucket b1"), AlreadyExistsException.Kind.BUCKET);

      database.command("sql", "create trigger t1 before create on type V1 execute sql 'select 1'");
      assertTaken(() -> database.command("sql", "create trigger t1 before create on type V1 execute sql 'select 1'"),
          AlreadyExistsException.Kind.TRIGGER);

      database.command("sql", "create materialized view mv1 as select from V1");
      assertTaken(() -> database.command("sql", "create materialized view mv1 as select from V1"),
          AlreadyExistsException.Kind.MATERIALIZED_VIEW);
      // a view whose backing type would take a type's name
      assertTaken(() -> database.command("sql", "create materialized view V1 as select from V1"), AlreadyExistsException.Kind.TYPE);

      database.command("sql", "create timeseries type Sensor timestamp ts fields (value double)");
      assertTaken(() -> database.command("sql", "create timeseries type Sensor timestamp ts fields (value double)"),
          AlreadyExistsException.Kind.TYPE);

      final String aggregate = " as select ts.timeBucket('1h', ts) as hour, avg(value) as v from Sensor group by hour";
      database.command("sql", "create continuous aggregate ca1" + aggregate);
      assertTaken(() -> database.command("sql", "create continuous aggregate ca1" + aggregate),
          AlreadyExistsException.Kind.CONTINUOUS_AGGREGATE);
      assertTaken(() -> database.command("sql", "create continuous aggregate V1" + aggregate), AlreadyExistsException.Kind.TYPE);
    }

    @Test
    void aRenameOrACopyOntoATakenNameIsAnAlreadyExistsConflict() {
      database.command("sql", "create document type D1");
      database.command("sql", "create document type D2");
      // it used to be an IllegalArgumentException, a 400 rather than the 409 of the create
      assertTaken(() -> database.command("sql", "alter type D2 name D1"), AlreadyExistsException.Kind.TYPE);
      assertTaken(() -> database.getSchema().copyType("D2", "D1", null, 1, 4096, 1000), AlreadyExistsException.Kind.TYPE);

      database.command("sql", "create property D1.a string");
      database.command("sql", "create property D1.b string");
      assertTaken(() -> database.getSchema().getType("D1").renameProperty("b", "a"), AlreadyExistsException.Kind.PROPERTY);
    }

    /**
     * Issue #10796: the rename of a TIMESERIES type moves the buckets of its shards to names derived from the new one (#10749), so a
     * free type name can still be refused because one of those names is taken. A bucket holds it - one created under that name, or the
     * bucket of a document type whose name the derived one extends - and that is the conflict a taken name is on every wire. A sealed
     * file left in the directory is not a schema object, and stays a plain refusal.
     */
    @Test
    void aTimeSeriesRenameOntoATakenBucketNameIsAnAlreadyExistsConflict() throws IOException {
      database.command("sql", "create timeseries type Ts10796 timestamp ts fields (value double) shards 1");
      database.command("sql", "insert into Ts10796 set ts = 1000, value = 1.5");

      database.command("sql", "create bucket Taken10796_shard_0");
      assertTaken(() -> database.command("sql", "alter type Ts10796 name Taken10796"), AlreadyExistsException.Kind.BUCKET);

      // the buckets of a document type named Doc10796_shard are Doc10796_shard_0, ...
      database.command("sql", "create document type Doc10796_shard buckets 1");
      final String docBucket = database.getSchema().getType("Doc10796_shard").getBuckets(false).getFirst().getName();
      assertThat(docBucket).isEqualTo("Doc10796_shard_0");
      assertTaken(() -> database.command("sql", "alter type Ts10796 name Doc10796"), AlreadyExistsException.Kind.BUCKET);

      final Path stray = Path.of(database.getDatabasePath(), "Stray10796_shard_0.ts.sealed");
      Files.write(stray, new byte[] { 1, 2, 3 });
      try {
        final Throwable refused = catchThrowable(() -> database.command("sql", "alter type Ts10796 name Stray10796"));
        assertThat(CauseChain.find(refused, SchemaException.class)).as("got %s", refused).isNotNull();
        assertThat(CauseChain.find(refused, AlreadyExistsException.class)).isNull();
        assertThat(ErrorCategory.of(refused)).isEqualTo(ErrorCategory.SCHEMA);
      } finally {
        Files.delete(stray);
      }

      // nothing was changed by any of the three
      assertThat(database.getSchema().existsType("Ts10796")).isTrue();
      // the sample itself, read back: count(*) answers a cached counter, which a sample no longer readable would not change
      final List<Result> samples = database.query("sql", "select ts, value from Ts10796").stream().toList();
      assertThat(samples).hasSize(1);
      assertThat(DateUtils.toEpochMillis(samples.getFirst().getProperty("ts"))).isEqualTo(1000L);
      assertThat(samples.getFirst().<Double>getProperty("value")).isEqualTo(1.5);
    }

    @Test
    void aPropertyNameASuperTypeHoldsIsTheSameConflict() {
      database.command("sql", "create document type Parent10153");
      database.command("sql", "create property Parent10153.inherited string");
      database.command("sql", "create document type Child10153 extends Parent10153");
      database.command("sql", "create property Child10153.own string");

      assertTaken(() -> database.getSchema().getType("Child10153").createProperty("inherited", Type.STRING),
          AlreadyExistsException.Kind.PROPERTY);
      assertTaken(() -> database.getSchema().getType("Child10153").renameProperty("own", "inherited"),
          AlreadyExistsException.Kind.PROPERTY);
    }

    @Test
    void theJavaApiRaisesTheSameTypeAsTheStatements() {
      database.getSchema().createDocumentType("Api");
      assertTaken(() -> database.getSchema().createDocumentType("Api"), AlreadyExistsException.Kind.TYPE);
      // TypeAlreadyExistsException (#10067) is one of them
      assertThat(catchThrowable(() -> database.getSchema().createVertexType("Api"))).isInstanceOf(TypeAlreadyExistsException.class);

      database.getSchema().getType("Api").createProperty("p", Type.STRING);
      assertTaken(() -> database.getSchema().getType("Api").createProperty("p", Type.STRING), AlreadyExistsException.Kind.PROPERTY);

      database.getSchema().createBucket("apiBucket");
      assertTaken(() -> database.getSchema().createBucket("apiBucket"), AlreadyExistsException.Kind.BUCKET);
    }

    @Test
    void aFunctionOrALibraryDefinedTwiceIsAnAlreadyExistsConflict() {
      database.command("sql", "define function lib10153.twice 'return x * 2' parameters [x] language js");
      assertTaken(() -> database.command("sql", "define function lib10153.twice 'return x * 3' parameters [x] language js"),
          AlreadyExistsException.Kind.FUNCTION);

      assertTaken(() -> ((LocalSchema) database.getSchema()).registerFunctionLibrary(
          FunctionLibraryFactory.createLibrary(database, "lib10153", "js")), AlreadyExistsException.Kind.FUNCTION_LIBRARY);
    }

    @Test
    void aCypherConstraintOrIndexOnATakenNameIsAnAlreadyExistsConflict() {
      database.command("opencypher", "CREATE CONSTRAINT c10153 FOR (n:P) REQUIRE n.name IS UNIQUE");
      assertTaken(() -> database.command("opencypher", "CREATE CONSTRAINT c10153 FOR (n:P) REQUIRE n.code IS UNIQUE"),
          AlreadyExistsException.Kind.CONSTRAINT);
      // a constraint and an index share one namespace, as in Neo4j; the kind is the one that HOLDS the name
      assertTaken(() -> database.command("opencypher", "CREATE INDEX c10153 FOR (n:P) ON (n.other)"),
          AlreadyExistsException.Kind.CONSTRAINT);

      database.command("opencypher", "CREATE INDEX i10153 FOR (n:P) ON (n.age)");
      assertTaken(() -> database.command("opencypher", "CREATE INDEX i10153 FOR (n:P) ON (n.zip)"), AlreadyExistsException.Kind.INDEX);
      assertTaken(() -> database.command("opencypher", "CREATE CONSTRAINT i10153 FOR (n:P) REQUIRE n.zip IS UNIQUE"),
          AlreadyExistsException.Kind.INDEX);
    }

    @Test
    void theCategoryIsDecidedThroughAWrapperAndBeforeSchema() {
      final AlreadyExistsException taken = new AlreadyExistsException(AlreadyExistsException.Kind.TYPE, "Type 'X' already exists");
      assertThat(ErrorCategory.of(taken)).isEqualTo(ErrorCategory.ALREADY_EXISTS);
      assertThat(ErrorCategory.of(new CommandExecutionException("Error on executing the script", taken)))
          .isEqualTo(ErrorCategory.ALREADY_EXISTS);
      // a plain SchemaException keeps its own category: the name is missing there, not held
      assertThat(ErrorCategory.of(new SchemaException("Type 'X' was not found"))).isEqualTo(ErrorCategory.SCHEMA);
      assertThat(taken.getLabel()).isEqualTo("Type already exists");
      assertThat(new AlreadyExistsException(null, "x").getLabel()).isEqualTo("Schema object already exists");
      assertThat(AlreadyExistsException.Kind.fromName("NOT_A_KIND")).isNull();
      assertThat(AlreadyExistsException.Kind.fromName("MATERIALIZED_VIEW")).isEqualTo(AlreadyExistsException.Kind.MATERIALIZED_VIEW);
    }

    private static void assertTaken(final Executable executable, final AlreadyExistsException.Kind kind) {
      final Throwable thrown = catchThrowable(executable::execute);
      final AlreadyExistsException taken = CauseChain.find(thrown, AlreadyExistsException.class);
      assertThat(taken).as("expected a taken-name refusal, got %s", thrown).isNotNull();
      assertThat(taken.getKind()).isEqualTo(kind);
      assertThat(taken).isInstanceOf(SchemaException.class);
      assertThat(ErrorCategory.of(thrown)).isEqualTo(ErrorCategory.ALREADY_EXISTS);
    }
  }

  /**
   * Regression for #10692: a database closed, dropped or restored under a handle a client had already resolved is something the operator did and
   * undoes, not a server fault. {@link DatabaseIsClosedException} fell through every rule of {@link ErrorCategory} to
   * {@link ErrorCategory#SERVER}, so Postgres answered {@code XX000} internal_error for an operator's {@code close database}, while Bolt
   * answered {@code DatabaseUnavailable} for the same handle.
   * <p>
   * The census: every type a closed handle or the refused resolution of a held name raises maps to a category that is not
   * {@link ErrorCategory#SERVER}, however it is wrapped on the way out.
   */
  @Nested
  class ClosedDatabaseCategory {
    private static final String PATH = "target/databases/ClosedDatabaseCategory";

    @BeforeEach
    @AfterEach
    void clean() {
      FileUtils.deleteRecursively(new File(PATH));
    }

    @Test
    void whatAClosedHandleRaisesIsUnavailable() {
      final Database database;
      try (final DatabaseFactory factory = new DatabaseFactory(PATH)) {
        database = factory.create();
        database.getSchema().createDocumentType("Doc");
        database.close();
      }

      // the exception the engine actually raises, not one built by hand
      final Throwable onQuery = catchThrowable(() -> database.query("sql", "select from Doc"));
      final Throwable onBegin = catchThrowable(database::begin);

      for (final Throwable raised : new Throwable[] { onQuery, onBegin }) {
        assertThat(raised).isInstanceOf(DatabaseIsClosedException.class);
        assertThat(ErrorCategory.of(raised)).isEqualTo(ErrorCategory.UNAVAILABLE);
        assertThat(ErrorCategory.classifiedCause(raised)).isSameAs(raised);
      }
    }

    @Test
    void noTypeOfTheClosedOrHeldFamilyIsAServerFault() {
      final Throwable[] family = {
          new DatabaseIsClosedException("db"),
          new DatabaseIsReopeningException("db"),
          new DatabaseNotAvailableException("Database 'db' is not available: it was closed by a 'close database' command") };

      for (final Throwable error : family) {
        assertThat(ErrorCategory.of(error)).as(error.getClass().getSimpleName()).isNotEqualTo(ErrorCategory.SERVER);
        // as the auto-commit wrapper and the query engines deliver it
        assertThat(ErrorCategory.of(new TransactionException("Error on commit", error))).as(error.getClass().getSimpleName())
            .isNotEqualTo(ErrorCategory.SERVER);
        assertThat(ErrorCategory.of(new CommandExecutionException("Error on executing the query", error)))
            .as(error.getClass().getSimpleName()).isNotEqualTo(ErrorCategory.SERVER);
      }
    }

    @Test
    void aClosedHandleIsUnavailableEvenUnderAWrapperThatBlamesTheCaller() {
      // GraphQL and the query engines wrap execution failures in a CommandParsingException
      assertThat(ErrorCategory.of(new CommandParsingException("failed", new DatabaseIsClosedException("db"))))
          .isEqualTo(ErrorCategory.UNAVAILABLE);
    }

    @Test
    void theCloseForReopenStaysRetryable() {
      assertThat(ErrorCategory.of(new DatabaseIsReopeningException("db"))).isEqualTo(ErrorCategory.RETRY);
    }
  }

  /**
   * Regression for #10795: a name that is no database ({@link DatabaseNotFoundException}, raised by the open of a directory that is not there)
   * classified as {@link ErrorCategory#SERVER}, so every protocol that asks {@link ErrorCategory} answered a mistyped database name
   * as a server fault. It is {@link ErrorCategory#DATABASE_NOT_FOUND}: the caller's to correct, and neither the transient
   * {@link ErrorCategory#UNAVAILABLE} of the type it extends nor the missing RECORD of {@link ErrorCategory#NOT_FOUND}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DatabaseNotFoundCategory {

    @Test
    void theOpenOfANameThatIsNoDatabaseIsDatabaseNotFound() {
      final File missing = new File("target/databases/issue10795-never-created");
      FileUtils.deleteRecursively(missing);

      try (final DatabaseFactory factory = new DatabaseFactory(missing.getPath())) {
        assertThatThrownBy(() -> factory.open(ComponentFile.MODE.READ_WRITE)).isInstanceOfSatisfying(DatabaseNotFoundException.class, e -> {
          assertThat(ErrorCategory.of(e)).isEqualTo(ErrorCategory.DATABASE_NOT_FOUND);
          assertThat(ErrorCategory.classifiedCause(e)).isSameAs(e);
        });
      } finally {
        FileUtils.deleteRecursively(missing);
      }
      assertThat(missing).doesNotExist();
    }

    @Test
    void noWrapperRelabelsANameThatIsNoDatabase() {
      final DatabaseNotFoundException missing = new DatabaseNotFoundException("Database 'nope' does not exist");

      // A parser or a validator that wraps the failure (GraphQL, the query engines) must not turn it into the caller's syntax error
      assertThat(ErrorCategory.of(new CommandParsingException("failed", missing))).isEqualTo(ErrorCategory.DATABASE_NOT_FOUND);
      assertThat(ErrorCategory.of(new ValidationException("failed", missing))).isEqualTo(ErrorCategory.DATABASE_NOT_FOUND);
      // nor the transient verdict of the type it extends, when both ride the same chain
      assertThat(ErrorCategory.of(new DatabaseNotAvailableException("held").initCause(missing)))
          .isEqualTo(ErrorCategory.DATABASE_NOT_FOUND);
      assertThat(ErrorCategory.classifiedCause(new TransactionException("commit", missing))).isSameAs(missing);
    }

    @Test
    void aHeldDatabaseKeepsItsTransientVerdict() {
      assertThat(ErrorCategory.of(new DatabaseNotAvailableException("held"))).isEqualTo(ErrorCategory.UNAVAILABLE);
      assertThat(ErrorCategory.of(new TransactionException("commit", new DatabaseNotAvailableException("held"))))
          .isEqualTo(ErrorCategory.UNAVAILABLE);
    }

    @Test
    void aConflictOrAStopOnRequestStillWins() {
      final DatabaseNotFoundException missing = new DatabaseNotFoundException("Database 'nope' does not exist");

      assertThat(ErrorCategory.of(new ConcurrentModificationException("conflict", missing))).isEqualTo(ErrorCategory.RETRY);
      assertThat(ErrorCategory.of(new QueryTerminatedException("stopped").initCause(missing))).isEqualTo(ErrorCategory.TERMINATED);
    }
  }

  /**
   * Regression for #11314: every refusal of a declared property constraint was the same bare {@link ValidationException}, and a misspelled SQL
   * function a plain {@link CommandExecutionException}, which no {@link ErrorCategory} rule classifies, so the Postgres wire
   * answered {@code 22023} for a NOT NULL violation and a value over a maximum length alike, and {@code XX000} (a server fault)
   * for a call of a function that does not exist. The refusal now carries the constraint it violates
   * ({@link ValidationException#getKind()}), a number the target type cannot hold is a {@link ValueOutOfRangeException}, and an
   * unknown function an {@link UnknownFunctionException} classified {@link ErrorCategory#PARSING}, in SQL and openCypher alike.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ConstraintKind extends TestHelper {

    @BeforeEach
    void createSchema() {
      database.command("sql", "CREATE DOCUMENT TYPE Ty");
      database.command("sql", "CREATE PROPERTY Ty.i INTEGER (mandatory true, min 1, max 10)");
      database.command("sql", "CREATE PROPERTY Ty.nn STRING (notnull true)");
      database.command("sql", "CREATE PROPERTY Ty.rx STRING (regexp '[a-z]+')");
      database.command("sql", "CREATE PROPERTY Ty.mx STRING (max 3)");
      database.command("sql", "CREATE PROPERTY Ty.mn STRING (min 2)");
      database.command("sql", "CREATE PROPERTY Ty.bn BINARY (max 2)");
      database.command("sql", "CREATE PROPERTY Ty.ls LIST (max 2)");
      database.command("sql", "CREATE PROPERTY Ty.ro STRING (readonly true)");
      database.command("sql", "CREATE PROPERTY Ty.d DATE");
      database.command("sql", "CREATE DOCUMENT TYPE Other");
      database.command("sql", "CREATE PROPERTY Ty.lk LINK OF Ty");
    }

    @Test
    void everyDeclaredConstraintNamesItsKind() {
      assertKind(() -> insert("i = 1, nn = null"), ValidationException.Kind.NOT_NULL);
      assertKind(() -> insert("nn = 'a'"), ValidationException.Kind.MANDATORY);
      assertKind(() -> insert("i = 11, nn = 'a'"), ValidationException.Kind.MAX);
      assertKind(() -> insert("i = 0, nn = 'a'"), ValidationException.Kind.MIN);
      assertKind(() -> insert("i = 1, nn = 'a', rx = 'ABC'"), ValidationException.Kind.REGEXP);
      assertKind(() -> insert("i = 1, nn = 'a', mx = 'abcdef'"), ValidationException.Kind.MAX_LENGTH);
      // a length under the MIN is a rule on the value like any other bound, not the truncation a varchar(n) refuses
      assertKind(() -> insert("i = 1, nn = 'a', mn = 'a'"), ValidationException.Kind.MIN);
      // the items of a collection are counted, not truncated: a bound like a number's
      assertKind(() -> insert("i = 1, nn = 'a', ls = [1, 2, 3]"), ValidationException.Kind.MAX);

      assertKind(() -> database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Ty").set("i", 1).set("nn", "a").set("bn", new byte[] { 1, 2, 3 });
        doc.save();
      }), ValidationException.Kind.MAX_LENGTH);

      assertKind(() -> database.transaction(() -> {
        final MutableDocument other = database.newDocument("Other");
        other.save();
        database.newDocument("Ty").set("i", 1).set("nn", "a").set("lk", other.getIdentity()).save();
      }), ValidationException.Kind.TYPE);
    }

    @Test
    void aChangeToAReadonlyPropertyIsAReadonlyViolation() {
      database.transaction(() -> insert("i = 1, nn = 'a', ro = 'first'"));
      assertKind(() -> database.transaction(() -> database.command("sql", "UPDATE Ty SET ro = 'second'")), ValidationException.Kind.READONLY);
    }

    @Test
    void aNumberTheTypeCannotHoldIsOutOfRangeAndTextThatIsNoNumberIsNot() {
      final Throwable overflow = catchThrowable(() -> database.transaction(() -> insert("i = 99999999999, nn = 'a'")));
      assertThat(CauseChain.find(overflow, ValueOutOfRangeException.class)).as("got %s", overflow).isNotNull();
      assertThat(ErrorCategory.of(overflow)).isEqualTo(ErrorCategory.VALIDATION);

      final Throwable notANumber = catchThrowable(() -> database.transaction(() -> insert("i = 'abc', nn = 'a'")));
      assertThat(CauseChain.find(notANumber, ValueOutOfRangeException.class)).as("got %s", notANumber).isNull();
      assertThat(CauseChain.find(notANumber, NumberFormatException.class)).as("got %s", notANumber).isNotNull();
      assertThat(ErrorCategory.of(notANumber)).isEqualTo(ErrorCategory.VALIDATION);

      final Throwable notADate = catchThrowable(() -> database.transaction(() -> insert("i = 1, nn = 'a', d = 'notadate'")));
      assertThat(CauseChain.find(notADate, DateTimeException.class)).as("got %s", notADate).isNotNull();
      assertThat(ErrorCategory.of(notADate)).isEqualTo(ErrorCategory.VALIDATION);
    }

    @Test
    void aNumberOutOfRangeIsStillAnIllegalArgumentAndIsKeptByTheLenientConversion() {
      final DocumentType type = database.getSchema().getType("Ty");
      final Throwable refused = catchThrowable(() -> Type.convert(database, 99999999999L, Integer.class, type.getProperty("i")));
      assertThat(refused).isInstanceOf(ValueOutOfRangeException.class).isInstanceOf(IllegalArgumentException.class);

      // a reader with no schema to blame keeps what arrived, as it keeps every other value the declared type cannot take
      assertThat(Type.convertOrKeep(database, 99999999999L, Integer.class, type.getProperty("i"))).isEqualTo(99999999999L);
    }

    @Test
    void anUnknownSqlFunctionIsTheCallersMistake() {
      for (final String query : new String[] { "SELECT foo(1)", "SELECT now()" }) {
        final Throwable refused = catchThrowable(() -> database.query("sql", query).stream().count());
        final UnknownFunctionException unknown = CauseChain.find(refused, UnknownFunctionException.class);
        assertThat(unknown).as("%s answered %s", query, refused).isNotNull();
        assertThat(unknown.getMessage()).startsWith("Unknown function name '");
        assertThat(ErrorCategory.of(refused)).as(query).isEqualTo(ErrorCategory.PARSING);
      }
      final UnknownFunctionException named = CauseChain.find(catchThrowable(() -> database.query("sql", "SELECT foo(1)").stream().count()),
          UnknownFunctionException.class);
      assertThat(named.getFunctionName()).isEqualTo("foo");
      // still the CommandExecutionException every caller of a function lookup catches
      assertThat(named).isInstanceOf(CommandExecutionException.class);
    }

    @Test
    void anUnknownCypherFunctionIsTheSameMistakeAndStillASyntaxError() {
      final Throwable refused = catchThrowable(() -> database.query("opencypher", "RETURN noSuchFunction(1) AS x").stream().count());
      assertThat(refused).isInstanceOf(CommandParsingException.class).isNotInstanceOf(CommandSemanticException.class);
      assertThat(refused.getMessage()).contains("Unknown function 'noSuchFunction'");
      assertThat(CauseChain.find(refused, UnknownFunctionException.class)).as("got %s", refused).isNotNull();
      assertThat(ErrorCategory.of(refused)).isEqualTo(ErrorCategory.PARSING);
    }

    private void insert(final String assignments) {
      database.command("sql", "INSERT INTO Ty SET " + assignments);
    }

    private void assertKind(final Executable write, final ValidationException.Kind expected) {
      final Throwable refused = catchThrowable(() -> {
        try {
          database.transaction(() -> {
            try {
              write.execute();
            } catch (final RuntimeException e) {
              throw e;
            } catch (final Throwable e) {
              throw new RuntimeException(e);
            }
          });
        } finally {
          if (database.isTransactionActive())
            database.rollback();
        }
      });
      final ValidationException validation = CauseChain.find(refused, ValidationException.class);
      assertThat(validation).as("expected a %s violation, got %s", expected, refused).isNotNull();
      assertThat(validation.getKind()).as(validation.getMessage()).isEqualTo(expected);
      assertThat(ErrorCategory.of(refused)).isEqualTo(ErrorCategory.VALIDATION);
    }
  }

  /**
   * Regression for #11384: about sixteen SQL and Cypher statements raised a plain {@link CommandExecutionException} for a named object that
   * does not exist, which no {@link ErrorCategory} rule classifies, so the Postgres wire answered {@code XX000} internal_error and
   * Bolt {@code UnknownError} for what their sibling statements, which go through the schema, answered as the caller's mistake.
   * Every statement of the report, and of its "read only, not reached" list where a statement reaches it, must now be classified
   * as what it is: a missing schema object, a method or a procedure nothing answers, a setting no one declared.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MissingObjectCategory extends TestHelper {

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("VV");
      database.getSchema().createDocumentType("DD");
    }

    @Test
    void everyMissingSchemaObjectIsASchemaFailure() {
      final List<String> statements = List.of(//
          "DROP INDEX nosuchindex",//
          "REBUILD INDEX nosuchindex",//
          "DROP TRIGGER nosuchtrg",//
          "DROP MATERIALIZED VIEW nosuchmv",//
          "REFRESH MATERIALIZED VIEW nosuchmv",// the sibling that already answered SCHEMA
          "DROP CONTINUOUS AGGREGATE nosuchca",//
          "DROP GRAPH ANALYTICAL VIEW nosuchgav",//
          "REBUILD GRAPH ANALYTICAL VIEW nosuchgav",//
          "ALTER GRAPH ANALYTICAL VIEW nosuchgav UPDATE MODE OFF",//
          "TRUNCATE BUCKET nosuchbucket",//
          "DROP BUCKET nosuchbucket",// the sibling that already answered SCHEMA
          "DROP PROPERTY VV.nosuch",//
          "ALTER PROPERTY VV.nosuch NAME x",// the sibling that already answered SCHEMA
          "CREATE PROPERTY nosuchtype.p STRING",//
          "CREATE VERTEX TYPE VX EXTENDS nosuchtype",//
          "CREATE TRIGGER t11384 AFTER CREATE ON TYPE nosuchtype EXECUTE SQL \"SELECT 1\"",//
          "DROP TYPE nosuchtype",//
          "ALTER TYPE nosuchtype CUSTOM a = 1",//
          "ALTER TYPE VV SUPERTYPE +nosuchtype",//
          "REBUILD TYPE nosuchtype",//
          "TRUNCATE TYPE nosuchtype",//
          "SELECT FROM bucket:nosuch",//
          "SELECT FROM bucket:[nosuch]",//
          "SELECT FROM schema:nosuch",//
          "SELECT FROM index:nosuch",//
          "DELETE FROM index:nosuch",//
          "SELECT FROM nosuchtype",//
          "SELECT count(*) FROM nosuchtype",//
          "MATCH {type: nosuchtype, as: n} RETURN n",//
          "INSERT INTO bucket:nosuch SET a = 1");

      final List<String> wrong = new ArrayList<>();
      for (final String statement : statements) {
        final Throwable error = run("sql", statement);
        if (ErrorCategory.of(error) != ErrorCategory.SCHEMA)
          wrong.add(statement + " -> " + ErrorCategory.of(error) + " (" + describe(error) + ")");
      }
      assertThat(wrong).as("a missing schema object is the caller's mistake, never a server fault").isEmpty();
    }

    @Test
    void aMethodNothingAnswersIsAnUnknownFunction() {
      database.transaction(() -> database.newDocument("DD").set("name", "x").save());
      for (final String statement : List.of("SELECT 'a'.nosuch()", "SELECT 'a'.nosuchmethod(1)", "SELECT FROM DD WHERE name.nosuch() = 1")) {
        final Throwable error = run("sql", statement);
        assertThat(ErrorCategory.of(error)).as(statement + ": " + describe(error)).isEqualTo(ErrorCategory.PARSING);
        assertThat(CauseChain.find(error, UnknownFunctionException.class)).as(statement).isNotNull();
      }
    }

    @Test
    void aProcedureNothingAnswersIsAnUnknownProcedureOptionalOrNot() {
      for (final String statement : List.of("CALL db.nosuch()", "CALL nosuch.proc(1) YIELD x RETURN x", "OPTIONAL CALL db.nosuch() RETURN 1")) {
        final Throwable error = run("opencypher", statement);
        assertThat(ErrorCategory.of(error)).as(statement + ": " + describe(error)).isEqualTo(ErrorCategory.PARSING);
        assertThat(CauseChain.find(error, UnknownProcedureException.class)).as(statement + ": " + describe(error)).isNotNull();
      }
    }

    @Test
    void aCustomFunctionAnsweringNullIsCalledNotRefusedAsUnknown() {
      database.command("sql", "DEFINE FUNCTION lib11384.nothing 'SELECT null AS result' LANGUAGE sql");
      // a CALL of a function that exists runs it, whatever it answers: a null answer used to fall through to the SQL functions
      try (final ResultSet rs = database.command("opencypher", "CALL lib11384.nothing()")) {
        rs.stream().count();
      }
    }

    @Test
    void aSettingNoOneDeclaredIsAValidationFailure() {
      for (final String statement : List.of("ALTER DATABASE `nosuchsetting` 1", "ALTER DATABASE `arcadedb.server.name` 'x'")) {
        final Throwable error = run("sql", statement);
        assertThat(ErrorCategory.of(error)).as(statement + ": " + describe(error)).isEqualTo(ErrorCategory.VALIDATION);
      }
    }

    private Throwable run(final String language, final String statement) {
      final Throwable error = catchThrowable(() -> {
        try (final ResultSet rs = database.command(language, statement)) {
          rs.stream().count();
        }
      });
      assertThat(error).as(statement + " must be refused").isNotNull();
      return error;
    }

    private static String describe(final Throwable error) {
      final StringBuilder text = new StringBuilder();
      for (Throwable t = error; t != null && text.length() < 400; t = t.getCause() == t ? null : t.getCause())
        text.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage()).append(" <- ");
      return text.toString();
    }
  }

  /**
   * Regression tests for #8199: a database closed out from under a request was retryable over HTTP (503) and a server
   * fault on every other wire protocol, and over HTTP it was retryable for every close, permanent ones included. The close
   * an HA snapshot reinstall makes - the one that is transient by construction - now closes the handle FOR REOPEN, which
   * raises {@link DatabaseIsReopeningException}; {@link ErrorCategory} reports only that one as {@link ErrorCategory#RETRY}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DatabaseClosedForReopen {
    private static final String DB_PATH = "./target/databases/DatabaseClosedForReopen";

    private DatabaseFactory factory;

    @BeforeEach
    void setUp() {
      factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
    }

    @AfterEach
    void tearDown() {
      if (factory.exists())
        factory.open().drop();
      factory.close();
    }

    @Test
    void onlyAHandleClosedForReopenIsRetryable() {
      assertThat(ErrorCategory.of(new DatabaseIsReopeningException("db"))).isEqualTo(ErrorCategory.RETRY);
      // as the auto-commit wrapper delivers it
      assertThat(ErrorCategory.of(new TransactionException("Error on commit", new DatabaseIsReopeningException("db"))))
          .isEqualTo(ErrorCategory.RETRY);

      // a DROP, a CLOSE DATABASE, a restore or a shutdown: not a transaction to re-run, but not a server fault either - the
      // operator took the database away and gives it back (issue #10692)
      assertThat(ErrorCategory.of(new DatabaseIsClosedException("db"))).isEqualTo(ErrorCategory.UNAVAILABLE);
    }

    @Test
    void aHandleClosedForReopenRefusesWithTheRetryableSubtype() {
      final Database database = factory.create();
      database.getSchema().createDocumentType("Doc");

      ((DatabaseInternal) database).getEmbedded().closeForReopen();

      assertThatThrownBy(database::begin).isInstanceOf(DatabaseIsReopeningException.class);
      assertThatThrownBy(() -> database.query("sql", "select from Doc")).isInstanceOf(DatabaseIsReopeningException.class);

      // the database itself reopens normally under its name
      try (final Database reopened = factory.open()) {
        assertThat(reopened.getSchema().existsType("Doc")).isTrue();
      }
    }

    @Test
    void aHandleClosedForGoodRefusesWithThePlainException() {
      final Database database = factory.create();
      database.close();

      assertThatThrownBy(() -> database.query("sql", "select from schema:types"))
          .isInstanceOf(DatabaseIsClosedException.class)
          .isNotInstanceOf(DatabaseIsReopeningException.class);
    }
  }
}
