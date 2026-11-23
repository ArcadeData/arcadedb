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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.DatabaseIsReopeningException;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.ObjectInUseException;
import com.arcadedb.exception.RecordVetoedException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.exception.UnknownFunctionException;
import com.arcadedb.exception.UnknownProcedureException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;
import io.undertow.server.HttpServerExchange;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The HTTP status a statement failure is answered with, end to end on a server: the caller's mistake is a 4xx, a
 * server fault keeps the 500.
 */
class HttpErrorStatusServerTest extends BaseGraphServerTest {
  /*
   * Issue #11314: the HTTP arms added for a refused value and an unknown function, next to the arms around them. A
   * {@link ValidationException} answered 500 "Internal error" because no arm named it; it is a 400 now, with the kind it carries in
   * {@code exceptionArgs}. Its subtype {@link RecordVetoedException}, a listener's veto, is the same refusal of the request and
   * gets the same 400 with no kind, and the {@link DuplicatedKeyException} arm, which does not extend it, keeps its 409.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #11314 a refused value is a 400 with its kind, the neighbouring arms keep their status
  @Test
  void aRefusedValueIs400WithItsKindAndTheNeighbouringArmsKeepTheirStatus() {
    final ClassifyingHandler handler = new ClassifyingHandler(getServer(0).getHttpServer());

    final AbstractServerHttpHandler.ErrorClassification notNull = handler.classifyError(
        new CommandExecutionException("Error on executing", new ValidationException(ValidationException.Kind.NOT_NULL, "cannot be null")));
    assertThat(notNull.status()).isEqualTo(400);
    assertThat(notNull.message()).isEqualTo("Constraint violated (NOT_NULL)");
    assertThat(notNull.exceptionArgs()).isEqualTo("NOT_NULL");
    assertThat(notNull.reported()).isInstanceOf(ValidationException.class);

    final AbstractServerHttpHandler.ErrorClassification vetoed = handler.classifyError(RecordVetoedException.ofCreate("V"));
    assertThat(vetoed.status()).isEqualTo(400);
    assertThat(vetoed.message()).isEqualTo("Validation error");
    assertThat(vetoed.exceptionArgs()).isNull();
    assertThat(vetoed.reported()).isInstanceOf(RecordVetoedException.class);

    final AbstractServerHttpHandler.ErrorClassification unknown = handler.classifyError(
        new UnknownFunctionException("foo", "Unknown function name 'foo'"));
    assertThat(unknown.status()).isEqualTo(400);
    assertThat(unknown.reported()).isInstanceOf(UnknownFunctionException.class);

    assertThat(handler.classifyError(new DuplicatedKeyException("idx", "[1]", new RID(1, 1))).status()).isEqualTo(409);
    // a failure nothing identifies as the caller's keeps the 500
    assertThat(handler.classifyError(new CommandExecutionException("Backup failed")).status()).isEqualTo(500);
  }

  private static final class ClassifyingHandler extends AbstractServerHttpHandler {
    private ClassifyingHandler(final HttpServer httpServer) {
      super(httpServer);
    }

    @Override
    protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
      throw new UnsupportedOperationException();
    }
  }

  /*
   * Issue #11384 over HTTP: a statement naming an index, a trigger, a view, a bucket, a property or a type the schema does not define
   * answered 500 "Internal error" with a stack trace in the server log, whether it raised a {@link CommandExecutionException} (the
   * statements that checked the name themselves) or a {@link SchemaException} (the ones that went through the schema). Both raise a
   * {@link SchemaException} now, which HTTP answers 400 as every other protocol answers the caller's mistake; a
   * {@link SchemaException} carrying a failure under it is a server fault and keeps the 500.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #11384 a statement naming a missing schema object is a 400, not a 500
  @Test
  void aMissingSchemaObjectIs400AndASchemaFailureWithACauseKeepsThe500() {
    final ClassifyingHandler handler = new ClassifyingHandler(getServer(0).getHttpServer());
    final Database database = getServerDatabase(0, getDatabaseName());

    for (final String statement : List.of("DROP INDEX nosuchindex", "DROP TRIGGER nosuchtrg", "TRUNCATE BUCKET nosuchbucket",
        "SELECT FROM bucket:nosuch", "DROP BUCKET nosuchbucket", "REFRESH MATERIALIZED VIEW nosuchmv")) {
      final Throwable error = catchThrowable(() -> {
        try (final ResultSet rs = database.command("sql", statement)) {
          rs.stream().count();
        }
      });
      final AbstractServerHttpHandler.ErrorClassification classified = handler.classifyError(error);
      assertThat(classified.status()).as(statement).isEqualTo(400);
      assertThat(classified.message()).as(statement).isEqualTo("Schema error");
      assertThat(classified.reported()).as(statement).isInstanceOf(SchemaException.class);
    }

    // wrapped by the engine, the same answer
    assertThat(handler.classifyError(new CommandExecutionException("Error", new SchemaException("Type 'X' not found"))).status())
        .isEqualTo(400);
    // a procedure nothing answers is the caller's too
    assertThat(handler.classifyError(new UnknownProcedureException("db.nosuch", "Unknown procedure")).status()).isEqualTo(400);
    // the neighbouring arm keeps its 409
    assertThat(handler.classifyError(new AlreadyExistsException(AlreadyExistsException.Kind.TYPE, "Type 'X' already exists")).status())
        .isEqualTo(409);
    // a schema failure that carries what broke under it is the server's
    assertThat(handler.classifyError(new SchemaException("Error on adding new item to the dictionary", new IOException("disk full")))
        .status()).isEqualTo(500);
  }

  // Regression: #10872 a handle closed under an in-flight request answers 503 while the database is there
  /**
   * Issue #10872, the in-flight half: a {@code close database} that closes the handle a request had already resolved answers that
   * request what the resolver answers the next one, 503, while the database is still there; and 404 only once nothing of it is
   * left. Before, the in-flight request was told 404 and the next one 503 for the same closed database.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ClosedUnderARequest {
    private static final String NAME = "Issue10872ClosedUnder";

    @AfterEach
    void dropDatabase() {
      final ArcadeDBServer server = getServer(0);
      server.unmarkClosedByCommand(NAME);
      if (server.existsDatabase(NAME))
        server.dropLocalDatabase(server.getDatabaseForOperator(NAME).getEmbedded(), NAME);
      FileUtils.deleteRecursively(new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
    }

    @Test
    void aHandleClosedUnderARequestIsUnavailableWhileTheDatabaseIsThere() {
      final ArcadeDBServer server = getServer(0);
      server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
      final ClassifyingHandler handler = new ClassifyingHandler(server.getHttpServer());

      // closed by command: the database is there, and the next request is told 503 by the resolver
      server.getDatabaseForOperator(NAME).getEmbedded().close();
      server.removeDatabase(NAME);
      server.markClosedByCommand(NAME);
      final AbstractServerHttpHandler.ErrorClassification closed = handler.classifyError(DatabaseIsClosedException.of(NAME));
      assertThat(closed.status()).isEqualTo(503);
      assertThat(closed.message()).isEqualTo("Database is closed");
      // typed as the resolver's refusal, so the remote client rebuilds it and does not resend the command
      assertThat(closed.reported()).isInstanceOf(DatabaseNotAvailableException.class);
      assertThat(closed.exceptionArgs()).isEqualTo("CLOSED");

      // what the engine raises carries the name
      assertThat(DatabaseIsClosedException.of(NAME).getDatabaseName()).isEqualTo(NAME);
      assertThat(new DatabaseIsReopeningException(NAME).getDatabaseName()).isEqualTo(NAME);

      // dropped: nothing of it is left, 404
      server.unmarkClosedByCommand(NAME);
      FileUtils.deleteRecursively(new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
      assertThat(handler.classifyError(DatabaseIsClosedException.of(NAME + "Gone")).status()).isEqualTo(404);
      // a thrower that did not say which database keeps the answer it always had
      assertThat(handler.classifyError(new DatabaseIsClosedException("a closed handle")).status()).isEqualTo(404);
    }

    private static final class ClassifyingHandler extends AbstractServerHttpHandler {
      private ClassifyingHandler(final HttpServer httpServer) {
        super(httpServer);
      }

      @Override
      protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
        throw new UnsupportedOperationException();
      }
    }
  }

  // Regression: #11803 SQL statements failing on something the caller can fix are a 4xx, not a 500
  /**
   * Issue #11803: SQL statements that fail on something the caller can fix answered HTTP 500 "Internal error", because the engine
   * raised a plain {@code CommandExecutionException} for them. Each one is answered as the caller's mistake now: 409 for a statement
   * refused for what the database holds (a type that still has records, an indexed property), 400 for the others.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SqlUserErrors {
    private static final String V = "Issue11803V";
    private static final String E = "Issue11803E";

    @BeforeEach
    void createSchema() {
      final Database database = getServerDatabase(0, getDatabaseName());
      for (final String statement : new String[] { "CREATE VERTEX TYPE " + V, "CREATE PROPERTY " + V + ".id INTEGER",
          "CREATE PROPERTY " + V + ".s STRING", "CREATE INDEX ON " + V + " (id) UNIQUE", "CREATE EDGE TYPE " + E })
        database.command("sql", statement);
      database.transaction(() -> {
        database.command("sql", "INSERT INTO " + V + " SET id = 1");
        database.command("sql", "INSERT INTO " + V + " SET id = 2");
        database.command("sql", "CREATE EDGE " + E + " FROM (SELECT FROM " + V + " WHERE id = 1) TO (SELECT FROM " + V + " WHERE id = 2)");
      });
    }

    @Test
    void aStatementRefusedForWhatTheDatabaseHoldsIs409() throws Exception {
      assertAnswer("TRUNCATE TYPE " + V, 409, ObjectInUseException.class, "UNSAFE");
      assertAnswer("TRUNCATE TYPE " + E, 409, ObjectInUseException.class, "UNSAFE");
      for (final Bucket bucket : getServerDatabase(0, getDatabaseName()).getSchema().getType(V).getBuckets(false))
        if (bucket.count() > 0)
          assertAnswer("TRUNCATE BUCKET " + bucket.getName(), 409, ObjectInUseException.class, "UNSAFE");
      assertAnswer("DROP TYPE " + V, 409, ObjectInUseException.class, "contains Vertices");
      assertAnswer("DROP TYPE " + E, 409, ObjectInUseException.class, "contains Edges");
      assertAnswer("DROP PROPERTY " + V + ".id", 409, ObjectInUseException.class, "FORCE");

      // nothing was changed by any of them
      final Database database = getServerDatabase(0, getDatabaseName());
      assertThat(database.countType(V, false)).isEqualTo(2);
      assertThat(database.countType(E, false)).isEqualTo(1);
      assertThat(database.getSchema().getType(V).existsProperty("id")).isTrue();
    }

    @Test
    void alterPropertyTypeIsRefusedWithoutAdviceThatAlsoFails() throws Exception {
      // the bare word used to be refused for its value, with the advice to quote it, and the quoted form then for the setting
      final JSONObject bare = assertAnswer("ALTER PROPERTY " + V + ".s TYPE LONG", 400, CommandSQLParsingException.class,
          "cannot be changed");
      assertThat(bare.getString("detail")).doesNotContain("Quote it");
      final JSONObject quoted = assertAnswer("ALTER PROPERTY " + V + ".s TYPE 'LONG'", 400, CommandSQLParsingException.class,
          "cannot be changed");
      assertThat(quoted.getString("detail")).contains("UPDATE " + V);

      // any other setting ALTER PROPERTY does not have names the ones it does
      assertAnswer("ALTER PROPERTY " + V + ".s NOSUCH 1", 400, CommandSQLParsingException.class, "MANDATORY");
      assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getType(V).getProperty("s").getType().name()).isEqualTo("STRING");
    }

    @Test
    void aTargetOfTheWrongKindIs400WithAWellFormedName() throws Exception {
      final JSONObject edge = assertAnswer(
          "CREATE EDGE " + V + " FROM (SELECT FROM " + V + " WHERE id = 1) TO (SELECT FROM " + V + " WHERE id = 2)", 400,
          SchemaException.class, "Type '" + V + "' is not an edge type");
      assertThat(edge.getString("detail")).doesNotContain("' '");
      assertAnswer("CREATE VERTEX " + E, 400, SchemaException.class, "Type '" + E + "' is not a vertex type");
    }

    @Test
    void aLimitOrSkipThatIsNoNumberIs400() throws Exception {
      assertAnswer("SELECT FROM " + V + " LIMIT 'a'", 400, TypeMismatchException.class, "Invalid value for LIMIT: a");
      assertAnswer("SELECT FROM " + V + " SKIP 'a'", 400, TypeMismatchException.class, "Invalid value for SKIP: a");
    }

    @Test
    void rebuildingAnIndexThatDoesNotExistIs400() throws Exception {
      assertAnswer("REBUILD INDEX Issue11803Nope", 400, SchemaException.class, "Issue11803Nope");
    }

    @Test
    void importingASourceThatDoesNotExistIs400() throws Exception {
      final File missing = new File(System.getProperty("java.io.tmpdir"), "issue11803-" + System.nanoTime() + ".jsonl");
      assertThat(missing).doesNotExist();
      assertAnswer("IMPORT DATABASE file://" + missing.getAbsolutePath(), 400, IllegalArgumentException.class, "was not found");
    }

    @Test
    void theNeighboursKeepTheirAnswer() throws Exception {
      assertAnswer("DROP TYPE Issue11803Nope", 400, SchemaException.class, "Issue11803Nope");
      assertAnswer("TRUNCATE TYPE Issue11803Nope", 400, SchemaException.class, "Issue11803Nope");
      assertAnswer("DROP PROPERTY " + V + ".nope", 400, SchemaException.class, "nope");
      // and UNSAFE / FORCE still do what the refusals advise
      assertThat(post("DROP PROPERTY " + V + ".id FORCE").getResponseCode()).isEqualTo(200);
      assertThat(post("TRUNCATE TYPE " + E + " UNSAFE").getResponseCode()).isEqualTo(200);
    }

    private JSONObject assertAnswer(final String statement, final int status, final Class<? extends Throwable> exception,
        final String detail) throws Exception {
      final HttpURLConnection connection = post(statement);
      try {
        assertThat(connection.getResponseCode()).as(statement).isEqualTo(status);
        final String body;
        try (final InputStream in = connection.getErrorStream()) {
          body = FileUtils.readStreamAsString(in, "UTF-8");
        }
        final JSONObject answer = new JSONObject(body);
        assertThat(answer.getString("exception", "")).as(statement).isEqualTo(exception.getName());
        assertThat(answer.getString("detail", "")).as(statement).contains(detail);
        return answer;
      } finally {
        connection.disconnect();
      }
    }

    private HttpURLConnection post(final String statement) throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) new URI(
          getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())).toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", statement, null, Map.of());
      connection.connect();
      return connection;
    }
  }

  // Regression: #11973 a Cypher unique constraint over duplicates is a 409, not a 500
  /**
   * Issue #11973: an openCypher unique constraint built over values that are not unique fails with a {@link DuplicatedKeyException}
   * under two index build wrappers and the statement's own, and the HTTP classifier looked at the first two links only, so the
   * caller's data conflict answered 500. It is the 409 every other duplicated key answers.
   */
  @Nested
  class UniqueConstraintOverDuplicates {
    @BeforeEach
    void createDuplicates() {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.transaction(() -> database.command("opencypher", "CREATE (:Issue11973D {x: 1}), (:Issue11973D {x: 1})"));
    }

    @Test
    void aCypherUniqueConstraintOverDuplicatesIs409() throws Exception {
      assertConflict("opencypher", "CREATE CONSTRAINT c11973 FOR (n:Issue11973D) REQUIRE n.x IS UNIQUE");
    }

    private void assertConflict(final String language, final String statement) throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) new URI(
          getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())).toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, language, statement, null, Map.of());
      connection.connect();
      try {
        assertThat(connection.getResponseCode()).as(statement).isEqualTo(409);
        final String body;
        try (final InputStream in = connection.getErrorStream()) {
          body = FileUtils.readStreamAsString(in, "UTF-8");
        }
        final JSONObject answer = new JSONObject(body);
        assertThat(answer.getString("exception", "")).as(statement).isEqualTo(DuplicatedKeyException.class.getName());
        assertThat(answer.getString("detail", "")).as(statement).contains("Duplicated key");
      } finally {
        connection.disconnect();
      }
      // nothing of the refused index is left
      assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getType("Issue11973D").getAllIndexes(false)).isEmpty();
    }
  }

  // Regression: #12037 a statement refused for the data already stored is a 4xx, not a 500
  /**
   * Issue #12037: a declaration refused because of the stored data answered 500 for MANDATORY and NOTNULL (in CREATE PROPERTY and in
   * ALTER PROPERTY) and for the change of a partition key, while the same refusal for MIN, MAX and REGEXP answered 400, and a UNIQUE
   * index built over duplicate keys is the 409 of every duplicated key. Each is the caller's request refused for a reason that holds
   * on a retry, and each leaves the schema and the record as they were.
   */
  @Nested
  class RefusedForTheStoredData {
    private static final String D = "Issue12037D";
    private static final String P = "Issue12037P";
    private static final String G = "Issue12037G";

    @Test
    void anExistenceConstraintTheStoredRecordsViolateIs400WithItsKind() throws Exception {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.command("sql", "CREATE DOCUMENT TYPE " + D);
      database.command("sql", "CREATE PROPERTY " + D + ".p INTEGER");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO " + D + " SET q = 1");
        database.command("sql", "INSERT INTO " + D + " SET p = null, n = null");
      });

      assertRefused("sql", "CREATE PROPERTY " + D + ".m INTEGER (MANDATORY true)", ValidationException.Kind.MANDATORY, "mandatory");
      assertRefused("sql", "CREATE PROPERTY " + D + ".n INTEGER (NOTNULL true)", ValidationException.Kind.NOT_NULL, "cannot be null");
      assertRefused("sql", "ALTER PROPERTY " + D + ".p MANDATORY true", ValidationException.Kind.MANDATORY, "Cannot set MANDATORY");
      assertRefused("sql", "ALTER PROPERTY " + D + ".p NOTNULL true", ValidationException.Kind.NOT_NULL, "Cannot set NOTNULL");

      // the control: the value constraints, which answered 400 before
      database.transaction(() -> database.command("sql", "INSERT INTO " + D + " SET p = 5"));
      assertRefused("sql", "ALTER PROPERTY " + D + ".p MIN 10", ValidationException.Kind.MIN, "less than 10");

      // and nothing was declared
      assertThat(database.getSchema().getType(D).existsProperty("m")).isFalse();
      assertThat(database.getSchema().getType(D).existsProperty("n")).isFalse();
      assertThat(database.getSchema().getType(D).getProperty("p").isMandatory()).isFalse();
      assertThat(database.getSchema().getType(D).getProperty("p").isNotNull()).isFalse();
      assertThat(database.getSchema().getType(D).getProperty("p").getMin()).isNull();
    }

    @Test
    void aUniqueIndexOverDuplicateKeysIs409() throws Exception {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.command("sql", "CREATE DOCUMENT TYPE " + G);
      database.command("sql", "CREATE PROPERTY " + G + ".p INTEGER");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO " + G + " SET p = 1");
        database.command("sql", "INSERT INTO " + G + " SET p = 1");
      });

      final JSONObject answer = post("sql", "CREATE INDEX ON " + G + " (p) UNIQUE", 409);
      assertThat(answer.getString("exception", "")).isEqualTo(DuplicatedKeyException.class.getName());
      assertThat(answer.getString("detail", "")).contains("Duplicated key [1]");

      // nothing of the refused index is left
      assertThat(database.getSchema().getType(G).getAllIndexes(false)).isEmpty();
    }

    @Test
    void changingThePartitionKeyIs400FromSqlAndOpenCypher() throws Exception {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.command("sql", "CREATE VERTEX TYPE " + P + " BUCKETS 4");
      database.command("sql", "CREATE PROPERTY " + P + ".k INTEGER");
      database.command("sql", "CREATE INDEX ON " + P + " (k) UNIQUE");
      database.command("sql", "ALTER TYPE " + P + " BUCKETSELECTIONSTRATEGY `partitioned('k')`");
      database.transaction(() -> database.command("sql", "INSERT INTO " + P + " SET k = 1, n = 1"));

      final String refusal = "Cannot modify the partition key";
      final JSONObject sql = post("sql", "UPDATE " + P + " SET k = 100 WHERE n = 1", 400);
      assertThat(sql.getString("exception", "")).isEqualTo(ValidationException.class.getName());
      assertThat(sql.getString("detail", "")).contains(refusal);
      final JSONObject cypher = post("opencypher", "MATCH (t:" + P + " {n: 1}) SET t.k = 100 RETURN t.k", 400);
      assertThat(cypher.getString("exception", "")).isEqualTo(ValidationException.class.getName());
      assertThat(cypher.getString("detail", "")).contains(refusal);

      // the record is unchanged
      assertThat(database.query("sql", "SELECT k FROM " + P + " WHERE n = 1").next().<Integer>getProperty("k")).isEqualTo(1);
    }

    private void assertRefused(final String language, final String statement, final ValidationException.Kind kind, final String detail)
        throws Exception {
      final JSONObject answer = post(language, statement, 400);
      assertThat(answer.getString("exception", "")).as(statement).isEqualTo(ValidationException.class.getName());
      assertThat(answer.getString("exceptionArgs", "")).as(statement).isEqualTo(kind.name());
      assertThat(answer.getString("detail", "")).as(statement).contains(detail);
    }

    private JSONObject post(final String language, final String statement, final int status) throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) new URI(
          getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())).toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, language, statement, null, Map.of());
      connection.connect();
      try {
        assertThat(connection.getResponseCode()).as(statement).isEqualTo(status);
        try (final InputStream in = connection.getErrorStream()) {
          return new JSONObject(FileUtils.readStreamAsString(in, "UTF-8"));
        }
      } finally {
        connection.disconnect();
      }
    }
  }
}
