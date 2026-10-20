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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TypeAlreadyExistsException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.CreateVertexTypeStatement;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.Identifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #10147: a {@code CREATE ... TYPE} statement is ONE schema publication, whatever it declares.
 * <p>
 * Under Raft every outermost {@code recordFileChanges} frame publishes its own {@code SCHEMA_ENTRY}, and the statement ran one frame
 * for the create and one more for each super type it linked (and the index of a UNIQUE edge type, and the CUSTOM values). A follower
 * deciding whether a transaction that had already written can be carried past the type it asked the leader for
 * ({@code RaftReplicatedDatabase.transactionStaysValidPastTheType}) counts the entries applied since, expects exactly one, and
 * refused every composite openCypher label ({@code CREATE (n:A:B)} renders {@code CREATE VERTEX TYPE `A~B` EXTENDS A, B}) with
 * a message blaming a schema change nobody made. It also saw the type appear without its super types and then gain them.
 * <p>
 * The guarantee is the invariant the counting rule depends on, over every shape the statement can take: it opens one outermost
 * session (the number of Raft entries the same code publishes, see {@link SchemaSessionCounter}) and advances
 * {@code LocalSchema.getVersion()} - which moves once per schema publication - by exactly one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10147CreateTypeOneSchemaEntryTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE VERTEX TYPE B");
    database.command("sql", "CREATE EDGE TYPE BaseRel");
    database.command("sql", "CREATE DOCUMENT TYPE BaseDoc");
    // A super type that declares an index: linking a sub type propagates it over the sub type's buckets, in transactions of their own
    database.command("sql", "CREATE VERTEX TYPE Indexed");
    database.command("sql", "CREATE PROPERTY Indexed.key STRING");
    database.command("sql", "CREATE INDEX ON Indexed (key) UNIQUE");
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {
      "CREATE VERTEX TYPE Plain",
      "CREATE DOCUMENT TYPE PlainDoc",
      "CREATE EDGE TYPE Rel",
      "CREATE VERTEX TYPE SubOne EXTENDS A",
      "CREATE VERTEX TYPE `A~B` IF NOT EXISTS EXTENDS A, B",
      "CREATE VERTEX TYPE ThreeSupers EXTENDS A, B, Indexed",
      "CREATE VERTEX TYPE SubIndexed EXTENDS Indexed",
      "CREATE EDGE TYPE SubRel EXTENDS BaseRel",
      "CREATE DOCUMENT TYPE SubDoc EXTENDS BaseDoc",
      "CREATE EDGE TYPE RelUnique UNIQUE",
      "CREATE EDGE TYPE SubRelUnique EXTENDS BaseRel UNIQUE",
      "CREATE EDGE TYPE RelLight LIGHTWEIGHT",
      "CREATE VERTEX TYPE WithBuckets BUCKETS 4",
      "CREATE VERTEX TYPE WithBucketsAndSupers EXTENDS A, B BUCKETS 2",
      "CREATE VERTEX TYPE WithPageSize PAGESIZE 131072",
      "CREATE VERTEX TYPE WithCustom CUSTOM a = 1, b = 'x'",
      "CREATE VERTEX TYPE WithSupersAndCustom EXTENDS A, B CUSTOM a = 1" })
  void oneStatementIsOneSchemaEntryWhateverItDeclares(final String statement) {
    final long versionBefore = schema().getVersion();

    final int sessions = SchemaSessionCounter.count((LocalDatabase) database, () -> database.command("sql", statement).close());

    assertThat(sessions).as("outermost schema sessions, i.e. SCHEMA_ENTRY under Raft, of: %s", statement).isEqualTo(1);
    assertThat(schema().getVersion() - versionBefore).as("schema versions published by: %s", statement).isEqualTo(1);
  }

  /** The same statement under a name that is a variable, which is resolved when it runs. */
  @Test
  void theStatementWithAVariableNameIsOneEntryToo() {
    final long versionBefore = schema().getVersion();

    final int sessions = SchemaSessionCounter.count((LocalDatabase) database,
        () -> database.command("sqlscript", "LET name = 'Dynamic'; CREATE VERTEX TYPE $name EXTENDS A, B;").close());

    assertThat(sessions).isEqualTo(1);
    assertThat(schema().getVersion() - versionBefore).isEqualTo(1);
    assertThat(database.getSchema().getType("Dynamic").getSuperTypes()).extracting(DocumentType::getName)
        .containsExactlyInAnyOrder("A", "B");
  }

  /** One entry is useless if it carries less than the three of them did: the type, both links and the custom values are all there. */
  @Test
  void theSingleEntryCarriesTheTypeItsSuperTypesAndItsCustomValues() {
    database.command("sql", "CREATE VERTEX TYPE `A~B` EXTENDS A, B CUSTOM origin = 'cypher'").close();

    assertComposite();

    reopenDatabase();

    assertComposite();
    assertThat(schema().getEmbedded().toJSON().toString()).contains("A~B");
  }

  private void assertComposite() {
    final DocumentType composite = database.getSchema().getType("A~B");
    assertThat(composite.getSuperTypes()).extracting(DocumentType::getName).containsExactlyInAnyOrder("A", "B");
    assertThat(composite.getCustomValue("origin")).isEqualTo("cypher");
    assertThat(database.getSchema().getType("A").getSubTypes()).extracting(DocumentType::getName).contains("A~B");
    assertThat(database.getSchema().getType("B").getSubTypes()).extracting(DocumentType::getName).contains("A~B");
  }

  /** The inherited index is propagated by the same statement: a record of the composite type is in the super type's unique index. */
  @Test
  void theIndexOfASuperTypeCoversTheNewSubTypeAfterTheSingleEntry() {
    database.command("sql", "CREATE VERTEX TYPE SubIndexed EXTENDS Indexed").close();

    database.transaction(() -> database.newVertex("SubIndexed").set("key", "k1").save());

    assertThat(database.query("sql", "SELECT FROM Indexed WHERE key = 'k1'").stream().count()).isEqualTo(1L);
    assertThatThrownBy(() -> database.transaction(() -> database.newVertex("Indexed").set("key", "k1").save()))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  /** {@code IF NOT EXISTS} on a type that is there asks nothing of the schema: no entry, and nothing bumps the version. */
  @Test
  void ifNotExistsOnAnExistingTypePublishesNothing() {
    final long versionBefore = schema().getVersion();

    final int sessions = SchemaSessionCounter.count((LocalDatabase) database,
        () -> database.command("sql", "CREATE VERTEX TYPE A IF NOT EXISTS EXTENDS B").close());

    assertThat(sessions).isZero();
    assertThat(schema().getVersion()).isEqualTo(versionBefore);
    assertThat(database.getSchema().getType("A").getSuperTypes()).isEmpty();
  }

  /** A statement refused before it creates anything opens no session at all. */
  @Test
  void aStatementRefusedBeforeItCreatesAnythingPublishesNothing() {
    database.command("sql", "CREATE TIMESERIES TYPE Metrics TIMESTAMP ts TAGS (sensor STRING) FIELDS (temperature DOUBLE)").close();
    final long versionBefore = schema().getVersion();

    final SchemaSessionCounter.Count seen = new SchemaSessionCounter.Count();
    assertThatThrownBy(() -> SchemaSessionCounter.count((LocalDatabase) database, seen,
        () -> database.command("sql", "CREATE VERTEX TYPE Child EXTENDS Metrics").close())).isInstanceOf(SchemaException.class);

    assertThat(seen.sessions()).isZero();
    assertThat(schema().getVersion()).isEqualTo(versionBefore);
    assertThat(database.getSchema().existsType("Child")).isFalse();
  }

  /**
   * The take-back of a refused link is a drop that can fail too: the statement still ends with the refusal that made it necessary, the
   * failure of the drop is kept on it as suppressed, and the schema is still usable. The type the statement creates is one the schema
   * never held, so that linking it is refused and dropping it fails.
   */
  @Test
  void aFailingTakeBackSurfacesTheRefusalAndKeepsTheDropFailureAsSuppressed() {
    final SchemaException refusal = new SchemaException("the link is refused");
    final CreateVertexTypeStatement statement = new CreateVertexTypeStatement() {
      @Override
      protected DocumentType createType(final Schema schema, final String typeName) {
        return (DocumentType) Proxy.newProxyInstance(DocumentType.class.getClassLoader(), new Class<?>[] { DocumentType.class },
            (proxy, method, args) -> {
              if (method.getName().equals("addSuperType"))
                throw refusal;
              if (method.getName().equals("getBuckets"))
                return List.of();
              return null;
            });
      }
    };
    statement.name = new Identifier("Ghosty");
    statement.supertypes = List.of(new Identifier("A"));
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final Throwable thrown = catchThrowable(() -> statement.executeDDL(context));

    assertThat(thrown).isSameAs(refusal);
    assertThat(thrown.getSuppressed()).as("the failure of the take-back").hasSize(1);
    assertThat(database.getSchema().existsType("Ghosty")).isFalse();

    // Nothing was left locked or half published: the next statement is one entry as usual
    final long versionBefore = schema().getVersion();
    assertThat(SchemaSessionCounter.count((LocalDatabase) database, () -> database.command("sql", "CREATE VERTEX TYPE Fine EXTENDS A").close()))
        .isEqualTo(1);
    assertThat(schema().getVersion() - versionBefore).isEqualTo(1);
  }

  /**
   * A value that is refused once the type exists (setting it is a schema change of its own) takes the type back like a refused link: the
   * statement ends with the refusal, nothing of the type is left, and the take-back, which succeeds here, adds nothing suppressed.
   */
  @Test
  void aRefusedCustomValueTakesTheTypeBack() {
    final SchemaException refusal = new SchemaException("the value is refused");
    final CreateVertexTypeStatement parsed = (CreateVertexTypeStatement) ((DatabaseInternal) database).getStatementCache()
        .get("CREATE VERTEX TYPE Valued CUSTOM k = 1");
    final CreateVertexTypeStatement statement = new CreateVertexTypeStatement() {
      @Override
      protected DocumentType createType(final Schema schema, final String typeName) {
        // A real type, so that the take-back has something to take back, which refuses the one step after it
        final DocumentType real = schema.createVertexType(typeName);
        return (DocumentType) Proxy.newProxyInstance(DocumentType.class.getClassLoader(), new Class<?>[] { DocumentType.class },
            (proxy, method, args) -> {
              if (method.getName().equals("setCustomValue"))
                throw refusal;
              try {
                return method.invoke(real, args);
              } catch (final InvocationTargetException e) {
                throw e.getCause();
              }
            });
      }
    };
    statement.name = parsed.name;
    statement.customProperties.putAll(parsed.customProperties);
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final Throwable thrown = catchThrowable(() -> statement.executeDDL(context));

    assertThat(thrown).isSameAs(refusal);
    assertThat(thrown.getSuppressed()).as("the take-back succeeded").isEmpty();
    assertThat(database.getSchema().existsType("Valued")).isFalse();
    assertThat(database.getSchema().getBuckets()).extracting(bucket -> bucket.getName()).noneMatch(name -> name.startsWith("Valued_"));
  }

  /**
   * A value that cannot be computed refuses a statement for a type that is still to be created, and leaves nothing behind. A statement
   * that lost the race for an {@code IF NOT EXISTS} type does not fail on it: it answers that the type is there, which is what makes the
   * statement safe to repeat. The race is made at the one point it can happen, between the first look at the schema and the lock, and the
   * value is computed before the lock because a user expression must not hold every writer of the database.
   */
  @Test
  void aCustomValueThatCannotBeComputedRefusesTheStatementUnlessItLostTheRaceForAnIfNotExistsType() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final Throwable alone = catchThrowable(() -> statementWithAFailingCustomValue("Unvalued", false, false).executeDDL(context));
    assertThat(alone).isInstanceOf(CommandExecutionException.class).hasMessageContaining("the value cannot be computed");
    assertThat(database.getSchema().existsType("Unvalued")).as("a refused statement leaves nothing behind").isFalse();

    try (final ResultSet answer = statementWithAFailingCustomValue("Raced", true, true).executeDDL(context)) {
      assertThat(answer.next().<Boolean>getProperty("created")).as("the type is there, made by the statement that won").isFalse();
    }
    assertThat(database.getSchema().existsType("Raced")).isTrue();

    // Without IF NOT EXISTS the loser is still told the type is there, not that a value failed
    assertThat(catchThrowable(() -> statementWithAFailingCustomValue("RacedAgain", false, true).executeDDL(context)))
        .isInstanceOf(TypeAlreadyExistsException.class);
  }

  /**
   * A {@code CREATE VERTEX TYPE} whose one {@code CUSTOM} value throws when it is computed. With {@code losesTheRace} another statement
   * creates the type right after the statement first looked at the schema.
   */
  private CreateVertexTypeStatement statementWithAFailingCustomValue(final String typeName, final boolean ifNotExists,
      final boolean losesTheRace) {
    final CreateVertexTypeStatement statement = new CreateVertexTypeStatement() {
      @Override
      protected void checkSuperTypes(final Schema schema, final CommandContext context) {
        super.checkSuperTypes(schema, context);
        if (losesTheRace)
          schema.createVertexType(typeName);
      }
    };
    statement.name = new Identifier(typeName);
    statement.ifNotExists = ifNotExists;
    statement.customProperties.put(new Identifier("k"), new Expression() {
      @Override
      public Object execute(final Identifiable currentRecord, final CommandContext context) {
        throw new CommandExecutionException("the value cannot be computed");
      }
    });
    return statement;
  }

  private LocalSchema schema() {
    return ((LocalDatabase) database).getSchema().getEmbedded();
  }
}
