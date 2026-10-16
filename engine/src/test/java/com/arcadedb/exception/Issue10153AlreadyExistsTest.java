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
import com.arcadedb.function.FunctionLibraryFactory;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #10153: creating a schema object under a taken name was a server fault on every wire - HTTP 500, Postgres {@code XX000},
 * Bolt's generic DatabaseError - or, for the {@link SchemaException} ones, the opposite of what happened: the {@code SCHEMA}
 * category ("the schema does not define this"), Postgres {@code 42P01} undefined_table. The refusals raised two bare types with no
 * conflict arm anywhere. They raise {@link AlreadyExistsException} now, through SQL, Cypher and the Java API alike, and
 * {@link ErrorCategory} has a category for it every wire maps to its own conflict code.
 * <p>
 * The guarantee is {@link #noSourceRefusesATakenNameWithAnotherType}: the per-statement tests below pin today's sites, the scan
 * pins the next one, which is how three fixes (#10073, #10074, #10075) each landed one name and left these behind.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10153AlreadyExistsTest extends TestHelper {

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

  /**
   * Pattern of a {@code throw new X(...)} statement whose arguments word a taken name, up to the {@code ;} that ends it. The
   * arguments may span lines.
   */
  private static final Pattern TAKEN_NAME_THROW = Pattern.compile(
      "throw\\s+new\\s+([A-Za-z0-9_.]+)\\s*\\(([^;]*?(?:already exists|already registered|already defined)[^;]*?)\\)\\s*;",
      Pattern.DOTALL);

  /**
   * The refusals that word a taken name but are not a schema object's, each for the reason given: a test that allow-lists a new
   * site has to say why it is not a conflict on a schema object name.
   */
  private static final Set<String> NOT_A_SCHEMA_OBJECT_NAME = Set.of(
      // the embedded DatabaseFactory.create() on a path that holds a database: the server turns it into its own
      // DatabaseAlreadyExistsException (#10074) before any wire sees it
      "database/LocalDatabase.java:DatabaseOperationException:Database '",
      // internal invariants on the file registry, never a name a caller chose
      "schema/LocalSchema.java:SchemaException:File with id '",
      "engine/FileManager.java:SchemaException:File id ",
      // a bucket index named after the bucket and System.nanoTime(): an internal invariant, not a caller's name
      "schema/LocalSchema.java:DatabaseMetadataException:Cannot create index '",
      // IF NOT EXISTS answered by an index of another definition: a 400 by design (#6921, #7228), not a taken name
      "query/sql/parser/CreateIndexStatement.java:IllegalArgumentException:Cannot create the index '",
      // a Cypher variable bound twice, or with two types, in one statement: a parse error, not a schema object name
      "query/opencypher/parser/CypherSemanticValidator.java:CommandParsingException:Variable");

  @Test
  void noSourceRefusesATakenNameWithAnotherType() throws IOException {
    final Path root = Path.of("src/main/java/com/arcadedb");
    assertThat(root).isDirectory();

    final List<String> offenders = new ArrayList<>();
    int checked = 0;
    try (final Stream<Path> files = Files.walk(root)) {
      for (final Path file : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".java"))::iterator) {
        final String source = Files.readString(file);
        final Matcher matcher = TAKEN_NAME_THROW.matcher(source);
        while (matcher.find()) {
          ++checked;
          final String exception = matcher.group(1);
          if (exception.endsWith("AlreadyExistsException"))
            continue;
          final String arguments = matcher.group(2).strip();
          final String firstLiteral = arguments.contains("\"") ? arguments.substring(arguments.indexOf('"') + 1) : arguments;
          final String key = root.relativize(file).toString().replace('\\', '/') + ":" + exception + ":";
          if (NOT_A_SCHEMA_OBJECT_NAME.stream().anyMatch(allowed -> allowed.startsWith(key) && firstLiteral.startsWith(
              allowed.substring(key.length()))))
            continue;
          offenders.add(key + " " + arguments.replaceAll("\\s+", " "));
        }
      }
    }

    // not vacuous: the scan has to see the sites this issue moved to the new type
    assertThat(checked).as("taken-name refusals found by the scan").isGreaterThan(25);
    assertThat(offenders).as("a taken schema object name must raise AlreadyExistsException, which every wire answers as a conflict")
        .isEmpty();
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
