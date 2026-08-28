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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6134. A property DEFAULT is stored verbatim by {@code setDefaultValue()} and evaluated as an SQL expression by
 * {@code getDefaultValue()}. Nothing used to validate it in between and the evaluation swallowed every exception, so a
 * `DEFAULT this is (not parseable` silently populated every record of the type with its own source text, a bare
 * `DEFAULT active` silently evaluated to null against the (always null) current record, and the expression was
 * re-parsed on every single record create. The expression is now compiled and validated once, at DDL time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PropertyDefaultValueTest extends TestHelper {

  @Test
  void unparseableDefaultIsRejectedAtDdlTime() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      final Property p = type.createProperty("broken", Type.STRING);

      assertThatThrownBy(() -> p.setDefaultValue("this is (not parseable")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Probe.broken").hasMessageContaining("this is (not parseable");

      // The rejected default must not have been applied.
      assertThat(type.getPolymorphicPropertiesWithDefaultDefined()).doesNotContain("broken");

      final MutableDocument doc = database.newDocument("Probe").save();
      assertThat(doc.has("broken")).isFalse();
    });
  }

  @Test
  void bareIdentifierDefaultIsRejectedAtDdlTime() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      final Property p = type.createProperty("bare", Type.STRING);

      assertThatThrownBy(() -> p.setDefaultValue("active")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Probe.bare").hasMessageContaining("active");

      assertThat(type.getPolymorphicPropertiesWithDefaultDefined()).doesNotContain("bare");
    });
  }

  @Test
  void bareIdentifierDefaultIsRejectedThroughSql() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE ProbeSql");
      database.command("sql", "CREATE PROPERTY ProbeSql.bare STRING");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY ProbeSql.bare DEFAULT active")).isInstanceOf(
          SchemaException.class).hasMessageContaining("field reference");
      // CREATE PROPERTY wraps it, so the reason has to survive into the wrapper's own message - that is what a client
      // sees over HTTP.
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY ProbeSql.bare2 STRING (DEFAULT active)"))
          .hasMessageContaining("field reference");
    });
  }

  @Test
  void recordScopedDefaultFailsLoudlyInsteadOfStoringItsOwnSourceText() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      // `@rid` parses, so it is not caught by the DDL-time syntax check, but it can only be resolved against a
      // current record and the default expression is always evaluated against a null one. It used to NPE inside
      // getDefaultValue(), get swallowed, and write the literal string "@rid" on every record.
      type.createProperty("ridDefault", Type.STRING).setDefaultValue("@rid");

      assertThatThrownBy(() -> database.newDocument("Probe").save()).hasMessageContaining("Probe.ridDefault")
          .hasMessageContaining("@rid");
    });
  }

  @Test
  void validDefaultsKeepWorking() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      type.createProperty("quoted", Type.STRING).setDefaultValue("'ok'");
      type.createProperty("number", Type.INTEGER).setDefaultValue("7");
      type.createProperty("expression", Type.INTEGER).setDefaultValue("3 + 4");
      type.createProperty("method", Type.STRING).setDefaultValue("'a'.append('b')");

      final MutableDocument doc = database.newDocument("Probe").save();
      assertThat(doc.getString("quoted")).isEqualTo("ok");
      assertThat(doc.getInteger("number")).isEqualTo(7);
      assertThat(doc.getInteger("expression")).isEqualTo(7);
      assertThat(doc.getString("method")).isEqualTo("ab");
    });
  }

  @Test
  void nonStringDefaultsAreNotEvaluatedAsExpressions() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      type.createProperty("num", Type.INTEGER).setDefaultValue(42);

      assertThat(type.getProperty("num").getDefaultValue()).isEqualTo(42);
      assertThat(database.newDocument("Probe").save().getInteger("num")).isEqualTo(42);
    });
  }

  /**
   * The compiled expression is cached, the evaluated result is not: a {@code sysdate()} default must still produce a
   * fresh value on every record.
   */
  @Test
  void perRecordDefaultsAreStillReEvaluated() throws Exception {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      type.createProperty("createdOn", Type.DATETIME_MICROS).setDefaultValue("sysdate()");
    });

    final AtomicReference<LocalDateTime> first = new AtomicReference<>();
    final AtomicReference<LocalDateTime> second = new AtomicReference<>();

    database.transaction(() -> first.set((LocalDateTime) database.newDocument("Probe").save().get("createdOn")));
    Thread.sleep(50);
    database.transaction(() -> second.set((LocalDateTime) database.newDocument("Probe").save().get("createdOn")));

    assertThat(second.get()).isAfter(first.get());
  }

  /**
   * The expression must be parsed once at DDL time, not on every {@code getDefaultValue()} call. There is no public
   * hook for "how many times did you parse", so this asserts the observable consequence: the same compiled
   * {@link Expression} instance backs every evaluation.
   */
  @Test
  void defaultExpressionIsCompiledOnlyOnce() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      final AbstractProperty p = (AbstractProperty) type.createProperty("quoted", Type.STRING).setDefaultValue("'ok'");

      final Expression compiled = p.getDefaultValueExpression();
      assertThat(compiled).isNotNull();

      for (int i = 0; i < 100; i++) {
        assertThat(p.getDefaultValue()).isEqualTo("ok");
        assertThat(p.getDefaultValueExpression()).isSameAs(compiled);
      }
    });
  }

  /**
   * One compiled expression now backs every record create of the type, so the same AST instance is executed by every
   * thread inserting concurrently - where before #6134 each call parsed its own throwaway copy. Executing a parsed tree
   * concurrently is already what the whole engine does ({@code StatementCache} hands one {@code Statement} to every
   * caller of a given SQL string and it is executed without copying), but the defaults path is the one place where the
   * sharing is per-schema-object rather than per-query, so it is pinned here: a constant, a method call and
   * {@code sysdate()} hammered from several threads must all produce correct values and raise nothing.
   */
  @Test
  void aCompiledDefaultIsSafeToEvaluateConcurrently() throws Exception {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      type.createProperty("constant", Type.STRING).setDefaultValue("'ok'");
      type.createProperty("method", Type.STRING).setDefaultValue("'a'.append('b')");
      type.createProperty("createdOn", Type.DATETIME_MICROS).setDefaultValue("sysdate()");
    });

    final DocumentType type = database.getSchema().getType("Probe");
    final int threads = 8, iterations = 500;
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);
    final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

    for (int t = 0; t < threads; t++) {
      new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < iterations; i++) {
            assertThat(type.getProperty("constant").getDefaultValue()).isEqualTo("ok");
            assertThat(type.getProperty("method").getDefaultValue()).isEqualTo("ab");
            assertThat(type.getProperty("createdOn").getDefaultValue()).isInstanceOf(LocalDateTime.class);
          }
        } catch (Throwable e) {
          failures.add(e);
        } finally {
          done.countDown();
        }
      }).start();
    }

    start.countDown();
    assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
    assertThat(failures).isEmpty();
  }

  /**
   * A default that evaluates to null fills in nothing: the property stays absent instead of being explicitly set to
   * null. That also keeps the NOTNULL check pointing at the real gap - {@code DocumentValidator} only raises "cannot
   * be null" when {@code has()} is true.
   */
  @Test
  void nullEvaluatingDefaultLeavesThePropertyAbsent() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      type.createProperty("nullDefault", Type.STRING).setDefaultValue("null");
      type.createProperty("other", Type.STRING).setDefaultValue("'ok'");

      final MutableDocument doc = database.newDocument("Probe").save();
      assertThat(doc.has("nullDefault")).isFalse();
      assertThat(doc.getString("other")).isEqualTo("ok");
      assertThat(doc.toJSON().has("nullDefault")).isFalse();
    });
  }

  /**
   * A default is inherited, and now so is its compiled form. Re-setting it on the supertype must be visible through
   * the subtype at once - there is one {@link AbstractProperty} behind both views, so a stale compiled expression
   * would be a per-subtype cache that does not exist. Covers the "polymorphic" half of
   * {@code getPolymorphicPropertiesWithDefaultDefined()}.
   */
  @Test
  void changingASupertypeDefaultIsVisibleThroughTheSubtypeImmediately() {
    database.command("sql", "CREATE DOCUMENT TYPE Base");
    database.command("sql", "CREATE PROPERTY Base.status STRING (DEFAULT 'first')");
    database.command("sql", "CREATE DOCUMENT TYPE Derived EXTENDS Base");

    final DocumentType derived = database.getSchema().getType("Derived");
    assertThat(derived.getPolymorphicPropertiesWithDefaultDefined()).contains("status");
    assertThat(derived.getPolymorphicProperty("status").getDefaultValue()).isEqualTo("first");
    database.transaction(() -> assertThat(database.newDocument("Derived").save().getString("status")).isEqualTo("first"));

    database.command("sql", "ALTER PROPERTY Base.status DEFAULT 'second'");

    assertThat(derived.getPolymorphicProperty("status").getDefaultValue()).isEqualTo("second");
    database.transaction(() -> assertThat(database.newDocument("Derived").save().getString("status")).isEqualTo("second"));
  }

  /**
   * The null rule has to survive a round trip through schema.json. Every SQL path stores a default as the expression's
   * source text - {@code DEFAULT null} is the four-character string, not a Java null - so reloading recompiles it to
   * the same null-literal expression and the property stays absent rather than reappearing as present-with-null.
   */
  @Test
  void aNullEvaluatingDefaultSurvivesAReopen() {
    database.command("sql", "CREATE DOCUMENT TYPE Probe");
    database.command("sql", "CREATE PROPERTY Probe.nullDefault STRING (DEFAULT null)");
    database.command("sql", "CREATE PROPERTY Probe.other STRING (DEFAULT 'ok')");

    reopenDatabase();

    final DocumentType type = database.getSchema().getType("Probe");
    assertThat(type.getPolymorphicPropertiesWithDefaultDefined()).contains("nullDefault", "other");
    assertThat(type.getProperty("nullDefault").getDefaultValue()).isNull();

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Probe").save();
      assertThat(doc.has("nullDefault")).isFalse();
      assertThat(doc.getString("other")).isEqualTo("ok");
    });
  }

  /**
   * The SQL insert path (which runs {@code ApplyDefaultsStep}) and the engine path (which runs
   * {@code LocalDatabase.setDefaultValues}) must agree on the null rule.
   */
  @Test
  void sqlInsertAgreesWithTheEnginePathOnNullDefaults() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Probe");
      database.command("sql", "CREATE PROPERTY Probe.nullDefault STRING (DEFAULT null)");
      database.command("sql", "CREATE PROPERTY Probe.other STRING (DEFAULT 'ok')");

      database.command("sql", "INSERT INTO Probe SET marker = 1");

      final ResultSet rs = database.query("sql", "SELECT FROM Probe");
      final MutableDocument doc = rs.next().getElement().get().asDocument().modify();
      assertThat(doc.has("nullDefault")).isFalse();
      assertThat(doc.getString("other")).isEqualTo("ok");
    });
  }

  @Test
  void updateApplyDefaultsAgreesOnNullDefaults() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Probe");
      database.command("sql", "CREATE PROPERTY Probe.marker INTEGER");
      database.command("sql", "INSERT INTO Probe SET marker = 1");

      database.command("sql", "CREATE PROPERTY Probe.nullDefault STRING (DEFAULT null)");
      database.command("sql", "CREATE PROPERTY Probe.other STRING (DEFAULT 'ok')");

      database.command("sql", "UPDATE Probe SET marker = 2 APPLY DEFAULTS");

      final ResultSet rs = database.query("sql", "SELECT FROM Probe");
      final MutableDocument doc = rs.next().getElement().get().asDocument().modify();
      assertThat(doc.has("nullDefault")).isFalse();
      assertThat(doc.getString("other")).isEqualTo("ok");
    });
  }

  /**
   * A NOTNULL property whose default cannot fill in a value now fails as "mandatory, but not found", pointing at the
   * schema gap, rather than as "cannot be null", which used to point at the caller's data.
   */
  @Test
  void notNullPropertyWithNullDefaultIsNotReportedAsANullViolation() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      type.createProperty("status", Type.STRING).setDefaultValue("null").setNotNull(true).setMandatory(true);

      assertThatThrownBy(() -> database.newDocument("Probe").save()).hasMessageContaining("mandatory");
    });
  }

  /**
   * Schema metadata reports the DEFINITION of a default, never an evaluation of it. Two things used to be wrong here:
   * the {@code DEFAULT_NOT_SET} sentinel leaked out of {@code getDefaultValue()}, so every plain property came back
   * with {@code "default": "<DEFAULT_NOT_SET>"}, and a {@code sysdate()} default was reported as a snapshot taken when
   * the row was fetched rather than as the expression that was declared.
   */
  @Test
  void schemaMetadataReportsTheDefinitionAndNotTheSentinel() {
    database.command("sql", "CREATE DOCUMENT TYPE Probe");
    database.command("sql", "CREATE PROPERTY Probe.plain STRING");
    database.command("sql", "CREATE PROPERTY Probe.withDefault STRING (DEFAULT 'ok')");
    database.command("sql", "CREATE PROPERTY Probe.createdOn DATETIME (DEFAULT sysdate())");

    final DocumentType type = database.getSchema().getType("Probe");
    assertThat(type.getProperty("plain").getDefaultValue()).isNull();
    assertThat(type.getProperty("plain").getDefaultValueDefinition()).isNull();
    assertThat(type.getProperty("withDefault").getDefaultValueDefinition()).isEqualTo("'ok'");
    assertThat(type.getProperty("createdOn").getDefaultValueDefinition()).isEqualTo("sysdate()");

    final String json = database.query("sql", "SELECT properties FROM schema:types WHERE name = 'Probe'").next().toJSON()
        .toString();
    assertThat(json).doesNotContain("DEFAULT_NOT_SET");
    assertThat(json).contains("sysdate()");
  }

  /**
   * An existing database whose schema.json already holds an invalid default must still open: the load path logs a
   * warning and keeps the pre-#6134 behaviour for that property rather than refusing to hydrate the schema. And the one
   * statement that can repair it, ALTER PROPERTY, must work on it - which means not evaluating the outgoing default.
   */
  @Test
  void aLegacyInvalidDefaultDoesNotBlockTheSchemaLoadAndCanBeRepaired() throws Exception {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      type.createProperty("broken", Type.STRING).setDefaultValue("'placeholder'");
    });

    reopenWithPersistedDefault("broken", "this is (not parseable");

    // The behaviour an earlier release had is preserved for an already-persisted invalid default.
    assertThat(database.getSchema().getType("Probe").getProperty("broken").getDefaultValue()).isEqualTo(
        "this is (not parseable");

    // Setting it again, to the same text, is still reported: the load path accepted it without validating, so "the
    // value did not change" must not be mistaken for "the value was already validated".
    assertThatThrownBy(
        () -> database.getSchema().getType("Probe").getProperty("broken").setDefaultValue("this is (not parseable"))
        .isInstanceOf(SchemaException.class);

    database.command("sql", "ALTER PROPERTY Probe.broken DEFAULT 'fixed'");
    assertThat(database.getSchema().getType("Probe").getProperty("broken").getDefaultValue()).isEqualTo("fixed");
  }

  /**
   * The other half of the previous test. A persisted default is rejected on load for one of two reasons, and they
   * leave {@code compileDefaultValue} by different exits: an unparseable one has no compiled expression to keep and
   * falls back to its source text, while a bare identifier compiles fine and keeps evaluating to null. Both have to
   * survive the load; this covers the second.
   */
  @Test
  void aLegacyBareIdentifierDefaultLoadsAndKeepsEvaluatingToNull() throws Exception {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      type.createProperty("bare", Type.STRING).setDefaultValue("'placeholder'");
    });

    reopenWithPersistedDefault("bare", "active");

    final DocumentType type = database.getSchema().getType("Probe");
    assertThat(type.getProperty("bare").getDefaultValue()).isNull();
    assertThat(type.getProperty("bare").getDefaultValueDefinition()).isEqualTo("active");

    // And the null rule applies to it like any other null-evaluating default.
    database.transaction(() -> assertThat(database.newDocument("Probe").save().has("bare")).isFalse());

    database.command("sql", "ALTER PROPERTY Probe.bare DEFAULT 'fixed'");
    assertThat(database.getSchema().getType("Probe").getProperty("bare").getDefaultValue()).isEqualTo("fixed");
  }

  /**
   * Issue #6799. The exact reproduction from the report: an edge property that had a DEFAULT, dropped, then an edge
   * created. The name used to stay behind in the type's default-property set, so the next record create looked it up
   * with {@code getPolymorphicProperty()} and blew up with "Cannot find property 'obsolete' in type 'TestEdge'".
   */
  @Test
  void droppingAPropertyWithADefaultDoesNotBreakTheNextRecordCreate() {
    database.command("sql", "CREATE VERTEX TYPE TestVertex");
    database.command("sql", "CREATE PROPERTY TestVertex.id STRING");
    database.command("sql", "CREATE INDEX ON TestVertex (id) UNIQUE");

    database.command("sql", "CREATE EDGE TYPE TestEdge");
    database.command("sql", "CREATE PROPERTY TestEdge.obsolete STRING");
    database.command("sql", "ALTER PROPERTY TestEdge.obsolete DEFAULT 'legacy'");
    database.command("sql", "DROP PROPERTY TestEdge.obsolete");

    assertThat(database.getSchema().getType("TestEdge").getPolymorphicPropertiesWithDefaultDefined()).doesNotContain(
        "obsolete");

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TestVertex SET id = 'source'");
      database.command("sql", "CREATE VERTEX TestVertex SET id = 'target'");

      final ResultSet rs = database.command("sql", "CREATE EDGE TestEdge FROM (SELECT FROM TestVertex WHERE id = 'source') "
          + "TO (SELECT FROM TestVertex WHERE id = 'target')");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().getElement().get().asEdge().has("obsolete")).isFalse();
    });
  }

  /**
   * The same invariant through the engine API, on a document type, and with a second defaulted property left in place:
   * dropping one must not disturb the other.
   */
  @Test
  void aDroppedDefaultLeavesTheSurvivingDefaultsAlone() {
    final DocumentType type = database.getSchema().createDocumentType("Probe");
    type.createProperty("gone", Type.STRING).setDefaultValue("'legacy'");
    type.createProperty("kept", Type.STRING).setDefaultValue("'ok'");

    type.dropProperty("gone");

    assertThat(type.getPolymorphicPropertiesWithDefaultDefined()).containsExactly("kept");
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Probe").save();
      assertThat(doc.has("gone")).isFalse();
      assertThat(doc.getString("kept")).isEqualTo("ok");
    });
  }

  /**
   * The set is per-declaring-type and read polymorphically, so dropping a defaulted property on a supertype has to
   * clear it for every subtype too.
   */
  @Test
  void droppingADefaultOnASupertypeClearsItForTheSubtypes() {
    database.command("sql", "CREATE DOCUMENT TYPE Base");
    database.command("sql", "CREATE PROPERTY Base.status STRING (DEFAULT 'first')");
    database.command("sql", "CREATE DOCUMENT TYPE Derived EXTENDS Base");

    final DocumentType derived = database.getSchema().getType("Derived");
    assertThat(derived.getPolymorphicPropertiesWithDefaultDefined()).contains("status");

    database.command("sql", "DROP PROPERTY Base.status");

    assertThat(derived.getPolymorphicPropertiesWithDefaultDefined()).doesNotContain("status");
    database.transaction(() -> assertThat(database.newDocument("Derived").save().has("status")).isFalse());
  }

  /**
   * {@code getOrCreateProperty()} with a different type drops and recreates the property internally. The recreated one
   * has no default, so the name must not survive from the dropped one.
   */
  @Test
  void retypingAPropertyWithGetOrCreateDropsItsDefault() {
    final DocumentType type = database.getSchema().createDocumentType("Probe");
    type.createProperty("counter", Type.STRING).setDefaultValue("'legacy'");

    type.getOrCreateProperty("counter", Type.INTEGER);

    assertThat(type.getPolymorphicPropertiesWithDefaultDefined()).doesNotContain("counter");
    database.transaction(() -> assertThat(database.newDocument("Probe").save().has("counter")).isFalse());
  }

  /**
   * The default-property cache is updated read-copy-write. Setting a default on N properties of the same type
   * concurrently used to let two threads copy the same snapshot and publish sets that each held only their own name,
   * silently losing the other defaults - a record created afterwards would come out missing them. The update is a CAS
   * now, so every name survives whatever the interleaving was.
   */
  @Test
  @Timeout(60)
  void concurrentDefaultUpdatesOnTheSameTypeDoNotLoseEachOther() throws Exception {
    final int properties = 16;

    final DocumentType type = database.getSchema().createDocumentType("Probe");
    for (int i = 0; i < properties; i++)
      type.createProperty("p" + i, Type.STRING);

    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(properties);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final List<Thread> threads = new ArrayList<>(properties);

    for (int i = 0; i < properties; i++) {
      final int index = i;
      final Thread t = new Thread(() -> {
        try {
          start.await();
          type.getProperty("p" + index).setDefaultValue("'v" + index + "'");
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        } finally {
          done.countDown();
        }
      });
      threads.add(t);
      t.start();
    }

    start.countDown();
    done.await();
    for (final Thread t : threads)
      t.join();
    assertThat(failure.get()).isNull();

    final List<String> expected = new ArrayList<>();
    for (int i = 0; i < properties; i++)
      expected.add("p" + i);
    assertThat(type.getPolymorphicPropertiesWithDefaultDefined()).containsExactlyInAnyOrderElementsOf(expected);

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Probe").save();
      for (int i = 0; i < properties; i++)
        assertThat(doc.getString("p" + i)).isEqualTo("v" + i);
    });
  }

  /**
   * The other half of the same race: a DROP PROPERTY landing while other properties of the type are having their
   * defaults set. The dropped name must be gone and none of the others may have been dropped with it.
   */
  @Test
  @Timeout(60)
  void aConcurrentDropDoesNotResurrectOrLoseOtherDefaults() throws Exception {
    final int properties = 12;

    final DocumentType type = database.getSchema().createDocumentType("Probe");
    type.createProperty("doomed", Type.STRING).setDefaultValue("'legacy'");
    for (int i = 0; i < properties; i++)
      type.createProperty("p" + i, Type.STRING);

    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(properties + 1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final List<Thread> threads = new ArrayList<>(properties + 1);

    for (int i = 0; i <= properties; i++) {
      final int index = i;
      final Thread t = new Thread(() -> {
        try {
          start.await();
          if (index == properties)
            type.dropProperty("doomed");
          else
            type.getProperty("p" + index).setDefaultValue("'v" + index + "'");
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        } finally {
          done.countDown();
        }
      });
      threads.add(t);
      t.start();
    }

    start.countDown();
    done.await();
    for (final Thread t : threads)
      t.join();
    assertThat(failure.get()).isNull();

    final List<String> expected = new ArrayList<>();
    for (int i = 0; i < properties; i++)
      expected.add("p" + i);
    assertThat(type.getPolymorphicPropertiesWithDefaultDefined()).containsExactlyInAnyOrderElementsOf(expected);

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Probe").save();
      assertThat(doc.has("doomed")).isFalse();
      for (int i = 0; i < properties; i++)
        assertThat(doc.getString("p" + i)).isEqualTo("v" + i);
    });
  }

  /**
   * #6799 reached from the other side: the property object outlives the DROP, so a caller holding one can still call
   * {@code setDefaultValue()} on it. Writing that default through would put the dropped name back into the cache and
   * break the next record create all over again, so the publication rejects a detached handle instead - identity
   * against the type's own declaration, which also covers a name that was dropped and recreated in between.
   */
  @Test
  void aDroppedPropertyHandleCannotWriteItsDefaultBack() {
    final DocumentType type = database.getSchema().createDocumentType("Probe");
    final Property stale = type.createProperty("obsolete", Type.STRING);
    stale.setDefaultValue("'legacy'");

    type.dropProperty("obsolete");

    assertThatThrownBy(() -> stale.setDefaultValue("'resurrected'")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Probe.obsolete");
    assertThat(type.getPolymorphicPropertiesWithDefaultDefined()).doesNotContain("obsolete");

    // And the same for a namesake recreated in the meantime: the stale handle must not write through to it.
    type.createProperty("obsolete", Type.INTEGER);
    assertThatThrownBy(() -> stale.setDefaultValue("'resurrected'")).isInstanceOf(SchemaException.class);
    assertThat(type.getPolymorphicPropertiesWithDefaultDefined()).doesNotContain("obsolete");

    database.transaction(() -> assertThat(database.newDocument("Probe").save().has("obsolete")).isFalse());
  }

  /**
   * Closes the database, rewrites one property's persisted default in schema.json to something this release would
   * reject at DDL time, and reopens - standing in for a database created by an earlier release.
   */
  private void reopenWithPersistedDefault(final String propertyName, final String persistedDefault) throws IOException {
    database.close();

    final File schemaFile = new File(getDatabasePath() + File.separator + LocalSchema.SCHEMA_FILE_NAME);
    final JSONObject schema = new JSONObject(FileUtils.readFileAsString(schemaFile));
    schema.getJSONObject("types").getJSONObject("Probe").getJSONObject("properties").getJSONObject(propertyName)
        .put("default", persistedDefault);
    Files.write(schemaFile.toPath(), schema.toString().getBytes(StandardCharsets.UTF_8));

    database = factory.open();
  }
}
