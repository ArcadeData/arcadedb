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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Schema regression tests: lightweight edge constraint.
 *
 * <p>One nested class per fixed issue, each keeping its own fixture.</p>
 */
class LightweightEdgeConstraintTest {
  /**
   * Regression for #9846: lightweight subtype inherits constraint.
   *
   * Regression for issue #9846: a MANDATORY or default-valued property of an edge type is silently unenforced on a
   * LIGHTWEIGHT subtype. A lightweight edge allocates no record, so {@code DocumentValidator} never runs for it and the
   * inherited definitions are never applied; yet the type's own {@code createProperty} was the only place that refused
   * the combination, and only for properties DECLARED on the lightweight type itself. {@code addSuperType}, the
   * {@code setMandatory}/{@code setDefaultValue} of a property of a super type and {@code setLightweight} accepted it
   * from the other side.
   * <p>
   * The rule is the one {@code GraphBatch.typeAppliesSchemaToEmptyEdge} already states for the legacy per-batch override
   * (#9019), read polymorphically: a property that applies to an edge WITHOUT properties - MANDATORY, or with a default -
   * cannot live under a type whose edges are stored as pairs of pointers. An optional property is harmless and stays legal.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LightweightSubtypeInheritsConstraintTest extends TestHelper {

    private static final String VERTEX = "Issue9846Person";
    private static final String PARENT = "Issue9846Follows";
    private static final String CHILD  = "Issue9846CloseFollows";

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType(VERTEX);
    }

    @Test
    void aLightweightTypeCannotBeCreatedUnderASuperTypeWithAMandatoryProperty() {
      database.command("sql", "CREATE EDGE TYPE " + PARENT).close();
      database.command("sql", "CREATE PROPERTY " + PARENT + ".since DATETIME (MANDATORY TRUE)").close();

      final AtomicInteger sessions = new AtomicInteger();
      final Callable<Void> counter = () -> {
        sessions.incrementAndGet();
        return null;
      };
      final DatabaseInternal internal = (DatabaseInternal) database;
      internal.registerCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, counter);
      try {
        assertThatThrownBy(() -> database.command("sql", "CREATE EDGE TYPE " + CHILD + " EXTENDS " + PARENT + " LIGHTWEIGHT").close())
            .hasMessageContaining("since")
            .hasMessageContaining("LIGHTWEIGHT");
      } finally {
        internal.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, counter);
      }

      assertThat(database.getSchema().existsType(CHILD)).as("a refused CREATE leaves no half-built type behind").isFalse();
      assertThat(sessions.get()).as("refused before anything was created: no type registered, then dropped again").isZero();
    }

    /** The first super type is acceptable and the second is not: the statement is refused as a whole, before anything is linked. */
    @Test
    void oneRefusedSuperTypeAmongSeveralRefusesTheWholeCreate() {
      database.getSchema().createEdgeType("Issue9846Plain");
      database.getSchema().createEdgeType(PARENT).createProperty("since", Type.DATETIME).setMandatory(true);

      assertThatThrownBy(
          () -> database.command("sql", "CREATE EDGE TYPE " + CHILD + " EXTENDS Issue9846Plain, " + PARENT + " LIGHTWEIGHT").close())
          .hasMessageContaining("since");

      assertThat(database.getSchema().existsType(CHILD)).isFalse();
      assertThat(database.getSchema().getType("Issue9846Plain").getSubTypes()).isEmpty();

      // and nothing reached the schema file either
      reopenDatabase();
      assertThat(database.getSchema().existsType(CHILD)).isFalse();
      assertThat(database.getSchema().getType("Issue9846Plain").getSubTypes()).isEmpty();
    }

    @Test
    void aLightweightTypeCannotBeCreatedUnderASuperTypeWithADefaultValuedProperty() {
      final EdgeType parent = database.getSchema().createEdgeType(PARENT);
      parent.createProperty("weight", Type.INTEGER).setDefaultValue(1);

      assertThatThrownBy(() -> database.getSchema().buildEdgeType().withName(CHILD).withSuperType(PARENT).withLightweight(true).create())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("weight");

      assertThat(database.getSchema().existsType(CHILD)).isFalse();
    }

    @Test
    void theConstraintIsFoundThroughAnIntermediateSuperType() {
      final EdgeType root = database.getSchema().createEdgeType(PARENT);
      root.createProperty("since", Type.DATETIME).setMandatory(true);
      database.getSchema().createEdgeType("Issue9846Middle").addSuperType(PARENT);

      assertThatThrownBy(
          () -> database.getSchema().buildEdgeType().withName(CHILD).withSuperType("Issue9846Middle").withLightweight(true).create())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("since");
    }

    @Test
    void anExistingLightweightTypeCannotBeLinkedUnderASuperTypeWithAMandatoryProperty() {
      final EdgeType parent = database.getSchema().createEdgeType(PARENT);
      parent.createProperty("since", Type.DATETIME).setMandatory(true);
      final EdgeType child = database.getSchema().buildEdgeType().withName(CHILD).withLightweight(true).create();

      assertThatThrownBy(() -> child.addSuperType(PARENT)).isInstanceOf(SchemaException.class).hasMessageContaining("since");
      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE " + CHILD + " SUPERTYPE +" + PARENT).close())
          .hasMessageContaining("since");

      assertThat(child.getSuperTypes()).as("the refusal leaves the type unlinked").isEmpty();
    }

    @Test
    void aSuperTypeLinkedAboveALightweightDescendantIsCheckedForTheWholeSubtree() {
      final EdgeType parent = database.getSchema().createEdgeType(PARENT);
      parent.createProperty("since", Type.DATETIME).setMandatory(true);
      final EdgeType unrelated = database.getSchema().createEdgeType("Issue9846Unrelated");
      final EdgeType lightweight = database.getSchema().buildEdgeType().withName(CHILD).withLightweight(true).create();
      lightweight.addSuperType(unrelated);

      // 'unrelated' is not lightweight itself, but its subtype is, and it would inherit PARENT's mandatory property
      assertThatThrownBy(() -> unrelated.addSuperType(PARENT)).isInstanceOf(SchemaException.class).hasMessageContaining("since");
      assertThat(unrelated.getSuperTypes()).isEmpty();
    }

    @Test
    void aMandatoryOrDefaultedPropertyCannotBeDeclaredOnASuperTypeOfALightweightType() {
      final EdgeType parent = database.getSchema().createEdgeType(PARENT);
      database.getSchema().buildEdgeType().withName(CHILD).withSuperType(PARENT).withLightweight(true).create();
      final Property since = parent.createProperty("since", Type.DATETIME);
      final Property weight = parent.createProperty("weight", Type.INTEGER);

      assertThatThrownBy(() -> since.setMandatory(true)).isInstanceOf(SchemaException.class).hasMessageContaining(CHILD);
      assertThatThrownBy(() -> weight.setDefaultValue(1)).isInstanceOf(SchemaException.class).hasMessageContaining(CHILD);
      assertThatThrownBy(
          () -> database.command("sql", "CREATE PROPERTY " + PARENT + ".stamp DATETIME (MANDATORY TRUE)").close())
          .hasMessageContaining(CHILD);

      // a default given as an EXPRESSION is a default too, and so is the same change made with ALTER PROPERTY
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY " + PARENT + ".created DATETIME (DEFAULT sysdate())").close())
          .hasMessageContaining(CHILD);
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY " + PARENT + ".since MANDATORY true").close())
          .hasMessageContaining(CHILD);
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY " + PARENT + ".weight DEFAULT 5").close())
          .hasMessageContaining(CHILD);

      assertThat(since.isMandatory()).isFalse();
      assertThat(weight.getDefaultValueDefinition()).isNull();
      assertThat(parent.existsProperty("stamp")).as("CREATE PROPERTY takes the property back when its attributes are refused").isFalse();
      assertThat(parent.existsProperty("created")).isFalse();
    }

    /** A null default fills in nothing, so it is not refused: only a default that would apply to a lightweight edge is. */
    @Test
    void aNullDefaultIsStillAcceptedUnderALightweightSubtype() {
      final EdgeType parent = database.getSchema().createEdgeType(PARENT);
      final Property weight = parent.createProperty("weight", Type.INTEGER);
      database.getSchema().buildEdgeType().withName(CHILD).withSuperType(PARENT).withLightweight(true).create();

      assertThatThrownBy(() -> weight.setDefaultValue(2)).isInstanceOf(SchemaException.class).hasMessageContaining(CHILD);
      assertThatCode(() -> weight.setDefaultValue(null)).doesNotThrowAnyException();
      assertThat(weight.getDefaultValueDefinition()).isNull();
    }

    @Test
    void aLightweightDescendantFurtherDownIsFoundToo() {
      final EdgeType parent = database.getSchema().createEdgeType(PARENT);
      database.getSchema().createEdgeType("Issue9846Middle").addSuperType(PARENT);
      database.getSchema().buildEdgeType().withName(CHILD).withSuperType("Issue9846Middle").withLightweight(true).create();
      final Property since = parent.createProperty("since", Type.DATETIME);

      assertThatThrownBy(() -> since.setMandatory(true)).isInstanceOf(SchemaException.class).hasMessageContaining(CHILD);
    }

    /**
     * A diamond reaches its bottom type through two paths: the walk visits it once, and finds the lightweight type below it
     * whichever path it takes. Without a lightweight type below, the same diamond accepts the constraint.
     */
    @Test
    void aLightweightTypeBelowADiamondIsFound() {
      final EdgeType top = database.getSchema().createEdgeType(PARENT);
      database.getSchema().createEdgeType("Issue9846Left").addSuperType(PARENT);
      database.getSchema().createEdgeType("Issue9846Right").addSuperType(PARENT);
      final Property since = top.createProperty("since", Type.DATETIME);

      // no lightweight type under the diamond yet: the constraint is accepted, and taken back so the next step starts clean
      since.setMandatory(true);
      since.setMandatory(false);

      database.getSchema().buildEdgeType().withName(CHILD).withSuperType("Issue9846Left").withSuperType("Issue9846Right")
          .withLightweight(true).create();

      assertThatThrownBy(() -> since.setMandatory(true)).isInstanceOf(SchemaException.class).hasMessageContaining(CHILD);
      assertThat(since.isMandatory()).isFalse();
    }

    @Test
    void aTypeThatInheritsOrDeclaresAConstrainedPropertyCannotBecomeLightweight() {
      final EdgeType parent = database.getSchema().createEdgeType(PARENT);
      parent.createProperty("since", Type.DATETIME).setMandatory(true);
      database.getSchema().createEdgeType(CHILD).addSuperType(PARENT);

      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE " + CHILD + " WITH lightweight = true").close())
          .hasMessageContaining("since");
      assertThat(((LocalEdgeType) database.getSchema().getType(CHILD)).isLightweight()).isFalse();

      // and the type's OWN constrained property: the creation path refuses a property on a lightweight type, the flip must too
      final EdgeType own = database.getSchema().createEdgeType("Issue9846Own");
      own.createProperty("weight", Type.INTEGER).setDefaultValue(1);
      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Issue9846Own WITH lightweight = true").close())
          .hasMessageContaining("weight");
      assertThat(((LocalEdgeType) own).isLightweight()).isFalse();
    }

    /**
     * A database written before the check may hold the combination, and it must still open: the check is on the DDL that
     * creates it, not on loading what was accepted before. The file is edited by hand to put a lightweight type under a super
     * type with a MANDATORY property, which is the state the DDL used to be able to produce.
     */
    @Test
    void aSchemaThatAlreadyHoldsTheCombinationStillOpens() throws Exception {
      final EdgeType parent = database.getSchema().createEdgeType(PARENT);
      parent.createProperty("since", Type.DATETIME).setMandatory(true);
      database.getSchema().createEdgeType(CHILD).addSuperType(PARENT);

      database.close();

      final Path schemaFile = Path.of(getDatabasePath(), "schema.json");
      final JSONObject schemaJson = new JSONObject(Files.readString(schemaFile));
      schemaJson.getJSONObject("types").getJSONObject(CHILD).put("lightweight", true);
      Files.writeString(schemaFile, schemaJson.toString());

      database = factory.open();

      final LocalEdgeType child = (LocalEdgeType) database.getSchema().getType(CHILD);
      assertThat(child.isLightweight()).isTrue();
      assertThat(child.getSuperTypes()).extracting(DocumentType::getName).containsExactly(PARENT);
      assertThat(child.getPolymorphicProperty("since").isMandatory()).isTrue();

      // and it is the DDL that is refused, not the type that is broken: the property can still be loosened
      database.getSchema().getType(PARENT).getProperty("since").setMandatory(false);
      assertThat(database.getSchema().getType(PARENT).getProperty("since").isMandatory()).isFalse();
    }

    // CONTROLS: what stays legal

    @Test
    void anOptionalInheritedPropertyStaysLegalOnALightweightSubtype() {
      final EdgeType parent = database.getSchema().createEdgeType(PARENT);
      parent.createProperty("note", Type.STRING);
      parent.createProperty("score", Type.INTEGER).setNotNull(true);

      final EdgeType child = database.getSchema().buildEdgeType().withName(CHILD).withSuperType(PARENT).withLightweight(true).create();

      assertThat(((LocalEdgeType) child).isLightweight()).isTrue();
      assertThat(child.getSuperTypes()).extracting(DocumentType::getName).containsExactly(PARENT);
      // and the optional definitions can still be tightened on a super type that has NO lightweight descendant
      final EdgeType other = database.getSchema().createEdgeType("Issue9846Other");
      other.createProperty("since", Type.DATETIME).setMandatory(true);
      assertThat(other.getProperty("since").isMandatory()).isTrue();
    }

    @Test
    void aRegularSubtypeStillInheritsAndEnforcesTheMandatoryProperty() {
      final EdgeType parent = database.getSchema().createEdgeType(PARENT);
      parent.createProperty("since", Type.DATETIME).setMandatory(true);
      database.getSchema().createEdgeType(CHILD).addSuperType(PARENT);

      database.transaction(() -> {
        final MutableVertex a = database.newVertex(VERTEX).save();
        final MutableVertex b = database.newVertex(VERTEX).save();
        assertThatThrownBy(() -> a.newEdge(CHILD, b)).isInstanceOf(ValidationException.class);
      });
    }

    @Test
    void theConstraintIsStillEnforcedOnTheParentItself() {
      database.command("sql", "CREATE EDGE TYPE " + PARENT).close();
      database.command("sql", "CREATE PROPERTY " + PARENT + ".since DATETIME (MANDATORY TRUE)").close();

      database.transaction(() -> {
        final MutableVertex a = database.newVertex(VERTEX).save();
        final MutableVertex b = database.newVertex(VERTEX).save();
        assertThatThrownBy(() -> a.newEdge(PARENT, b)).isInstanceOf(ValidationException.class);
      });

      try (final ResultSet rs = database.query("sql", "select count(*) as c from " + PARENT)) {
        final Result row = rs.next();
        assertThat(row.<Long>getProperty("c")).isZero();
      }
    }

    @Test
    void aLightweightTypeUnderAParentWithoutConstraintsKeepsWorking() {
      database.getSchema().createEdgeType(PARENT);
      database.command("sql", "CREATE EDGE TYPE " + CHILD + " EXTENDS " + PARENT + " LIGHTWEIGHT").close();

      database.transaction(() -> {
        final MutableVertex a = database.newVertex(VERTEX).save();
        final MutableVertex b = database.newVertex(VERTEX).save();
        a.newEdge(CHILD, b);
        // a lightweight edge has no record, so the type counter cannot see it: ask the vertex
        assertThat(a.countEdges(Vertex.DIRECTION.OUT, CHILD)).isEqualTo(1);
      });
    }
  }

  /**
   * Regression for #9848: edge unique declaration atomicity.
   *
   * Regression for issue #9848: {@code ALTER TYPE <edgeType> WITH unique = ...} flipped the flag in one critical section
   * and materialised (or dropped) its backing index in another, and {@code DROP INDEX} judged whether the index backs a
   * constraint BEFORE it took the write lock that drops it. Between the two a concurrent {@code DROP INDEX} of the index
   * the alter was about to create passed the guard (the flag was still {@code false}, the index absent), then dropped the
   * index the alter built, leaving the type advertising UNIQUE with nothing enforcing it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EdgeUniqueDeclarationAtomicityTest extends TestHelper {

    private static final String VERTEX     = "Issue9848Person";
    private static final String EDGE       = "Issue9848Follows";
    private static final String INDEX_NAME = LocalEdgeType.uniqueIndexName(EDGE);

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType(VERTEX);
      database.getSchema().createEdgeType(EDGE);
    }

    private LocalEdgeType edgeType() {
      return (LocalEdgeType) database.getSchema().getType(EDGE);
    }

    /**
     * The race, made deterministic. A helper thread holds the database write lock and will run the ALTER under it once
     * released to do so; meanwhile a second thread asks to drop the index the ALTER is about to create. Before the fix the
     * drop judged the constraint against the not-yet-flipped flag (and an absent index) and only then waited for the lock,
     * so it dropped the index the ALTER had built in the meantime. After it, the judgement waits for the lock and sees the
     * constraint.
     */
    @Test
    void dropIndexJudgesTheConstraintUnderTheLockThatDropsTheIndex() throws Exception {
      final LocalDatabase embedded = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      final CountDownLatch lockHeld = new CountDownLatch(1);
      final CountDownLatch runAlter = new CountDownLatch(1);
      final ExecutorService threads = Executors.newFixedThreadPool(2);

      try {
        final Future<?> alter = threads.submit(() -> embedded.executeInWriteLock(() -> {
          lockHeld.countDown();
          assertThat(runAlter.await(30, TimeUnit.SECONDS)).isTrue();
          database.command("sql", "ALTER TYPE " + EDGE + " WITH unique = true").close();
          return null;
        }));
        assertThat(lockHeld.await(30, TimeUnit.SECONDS)).as("the helper thread took the database write lock").isTrue();

        final AtomicReference<Throwable> dropOutcome = new AtomicReference<>();
        final Future<?> drop = threads.submit(() -> {
          try {
            database.getSchema().dropIndex(INDEX_NAME);
          } catch (final Throwable t) {
            dropOutcome.set(t);
          }
        });

        // A short wait EXPECTED TO TIME OUT: the drop is queued behind the write lock whatever it has decided so far, so
        // no elapsed-time claim is made and a stalled JVM can only make this more true.
        assertThatThrownBy(() -> drop.get(2, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);

        runAlter.countDown();
        alter.get(60, TimeUnit.SECONDS);
        drop.get(60, TimeUnit.SECONDS);

        assertThat(dropOutcome.get()).as("the drop must see the constraint the alter declared, and refuse")
            .isInstanceOf(SchemaException.class);
        assertThat(dropOutcome.get().getMessage()).contains(INDEX_NAME).contains("UNIQUE");
      } finally {
        runAlter.countDown();
        threads.shutdownNow();
        assertThat(threads.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
      }

      assertThat(edgeType().isUnique()).isTrue();
      assertThat(database.getSchema().existsIndex(INDEX_NAME)).as("the index that enforces the constraint is still there").isTrue();
    }

    /**
     * The contract the issue asks for, observed from outside: the flag and its index change in ONE schema recording
     * session. A session announces its end through {@code SCHEMA_AFTER_FILE_CHANGES}, which a nested frame does not fire,
     * so two sessions are two critical sections and a window in which another session sees the flag without the index.
     */
    @Test
    void alterUniqueRunsAsASingleSchemaSession() {
      final AtomicInteger sessions = new AtomicInteger();
      final Callable<Void> counter = () -> {
        sessions.incrementAndGet();
        return null;
      };
      final DatabaseInternal internal = (DatabaseInternal) database;
      internal.registerCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, counter);
      try {
        database.command("sql", "ALTER TYPE " + EDGE + " WITH unique = true").close();
        assertThat(sessions.get()).as("declaring UNIQUE and building its index is one session").isEqualTo(1);

        sessions.set(0);
        database.command("sql", "ALTER TYPE " + EDGE + " WITH unique = false").close();
        assertThat(sessions.get()).as("withdrawing UNIQUE and dropping its index is one session").isEqualTo(1);
      } finally {
        internal.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, counter);
      }
    }

    /**
     * An index that cannot be built (the type already holds a duplicate pair) must not leave the type declaring UNIQUE
     * with no index behind it: the flag flip and the index are one change, so a refusal takes both back.
     */
    @Test
    void aRefusedIndexBuildLeavesTheFlagAsItWas() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex(VERTEX).save();
        final MutableVertex b = database.newVertex(VERTEX).save();
        a.newEdge(EDGE, b);
        a.newEdge(EDGE, b);
      });

      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE " + EDGE + " WITH unique = true").close()).isNotNull();

      assertThat(edgeType().isUnique()).as("the declaration was not applied, so the type must not advertise it").isFalse();
      assertThat(edgeType().isLightweight()).isFalse();
      assertThat(database.getSchema().existsIndex(INDEX_NAME)).isFalse();
      assertThat(edgeType().getAllIndexes(true)).as("the refused build left no index, not even a partial one").isEmpty();
    }

    /**
     * What a refused ALTER leaves behind has to hold on disk too, not only in memory: the schema file is rewritten by the
     * session that failed, after the flags went back, so a reopen reads the type as it was.
     */
    @Test
    void aRefusedAlterLeavesTheSchemaFileAsItWas() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex(VERTEX).save();
        final MutableVertex b = database.newVertex(VERTEX).save();
        a.newEdge(EDGE, b);
        a.newEdge(EDGE, b);
      });

      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE " + EDGE + " WITH unique = true").close()).isNotNull();
      reopenDatabase();

      assertThat(edgeType().isUnique()).isFalse();
      assertThat(database.getSchema().existsIndex(INDEX_NAME)).isFalse();
    }

    /**
     * Both settings of one statement are one change: the storage shape is refused first (the type already holds edge records),
     * and the UNIQUE request that came with it is not applied on its own, in memory or on disk.
     */
    @Test
    void aRefusedStorageShapeTakesTheUniqueRequestOfTheSameStatementWithIt() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex(VERTEX).save();
        final MutableVertex b = database.newVertex(VERTEX).save();
        a.newEdge(EDGE, b);
      });

      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE " + EDGE + " WITH lightweight = true, unique = true").close())
          .hasMessageContaining("LIGHTWEIGHT");
      reopenDatabase();

      assertThat(edgeType().isLightweight()).isFalse();
      assertThat(edgeType().isUnique()).isFalse();
      assertThat(database.getSchema().existsIndex(INDEX_NAME)).isFalse();
    }

    @Test
    void declaringAndWithdrawingStillMaterialisesAndDropsTheIndex() {
      database.command("sql", "ALTER TYPE " + EDGE + " WITH unique = true").close();
      assertThat(edgeType().isUnique()).isTrue();
      assertThat(database.getSchema().existsIndex(INDEX_NAME)).isTrue();

      assertThatThrownBy(() -> database.getSchema().dropIndex(INDEX_NAME))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("UNIQUE");
      assertThat(database.getSchema().existsIndex(INDEX_NAME)).isTrue();

      database.command("sql", "ALTER TYPE " + EDGE + " WITH unique = false").close();
      assertThat(edgeType().isUnique()).isFalse();
      assertThat(database.getSchema().existsIndex(INDEX_NAME)).isFalse();
    }

    @Test
    void lightweightAndUniqueTogetherStillSettleTheStorageShapeFirst() {
      database.command("sql", "ALTER TYPE " + EDGE + " WITH lightweight = true, unique = true").close();

      assertThat(edgeType().isLightweight()).isTrue();
      assertThat(edgeType().isUnique()).isTrue();
      assertThat(database.getSchema().existsIndex(INDEX_NAME)).as("a lightweight type builds no index").isFalse();
    }
  }

  /**
   * Regression for #9975: lightweight unenforced constraint warning.
   *
   * Regression for issue #9975: a database written before #9846 may pair a LIGHTWEIGHT edge type with a MANDATORY or
   * default-valued property it inherits. It must still open - refusing would be worse than the defect - but the constraint is
   * declared and never applied, so the load logs a WARNING naming the type, the property and the two ways out, and
   * {@code CHECK DATABASE} reports the same finding.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LightweightUnenforcedConstraintWarningTest extends TestHelper {

    private static final String PARENT = "Issue9975Follows";
    private static final String CHILD  = "Issue9975CloseFollows";

    @Test
    void openingASchemaThatHoldsTheCombinationLogsAWarning() throws IOException {
      writeLegacySchema();

      final List<String> warnings = new ArrayList<>();
      final Logger previous = LogManager.instance().getLogger();
      LogManager.instance().setLogger(new CapturingLogger(warnings));
      try {
        database = factory.open();
      } finally {
        LogManager.instance().setLogger(previous);
      }

      assertThat(((LocalEdgeType) database.getSchema().getType(CHILD)).isLightweight()).isTrue();
      assertThat(warnings).filteredOn(w -> w.contains(CHILD)).singleElement().satisfies(w -> assertThat(w)//
          .contains("LIGHTWEIGHT").contains("MANDATORY").contains(PARENT + ".since").contains("not enforced"));

      // the way out the warning names, so the integrity check that closes the test finds nothing
      database.getSchema().getType(PARENT).getProperty("since").setMandatory(false);
    }

    @Test
    void checkDatabaseReportsTheUnenforcedConstraint() throws IOException {
      writeLegacySchema();
      database = factory.open();

      try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
        final Result result = rs.next();
        final Collection<String> warnings = result.getProperty("warnings");
        assertThat(warnings).filteredOn(w -> w.contains(CHILD)).singleElement()
            .satisfies(w -> assertThat(w).contains(PARENT + ".since").contains("MANDATORY"));
      }

      // once the property is loosened there is nothing left to report
      database.getSchema().getType(PARENT).getProperty("since").setMandatory(false);
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
        final Collection<String> warnings = rs.next().getProperty("warnings");
        assertThat(warnings).noneMatch(w -> w.contains(CHILD));
      }
    }

    @Test
    void everyUnenforcedPropertyIsNamedWithItsOwnWayOut() throws IOException {
      database.getSchema().createEdgeType(PARENT);
      database.command("sql", "CREATE PROPERTY " + PARENT + ".since DATETIME (MANDATORY TRUE)").close();
      database.command("sql", "CREATE PROPERTY " + PARENT + ".weight INTEGER (DEFAULT 5)").close();
      database.getSchema().createEdgeType(CHILD).addSuperType(PARENT);
      database.close();
      markChildLightweight();
      database = factory.open();

      final String warning = LocalEdgeType.unenforcedConstraintWarning(database.getSchema().getType(CHILD));
      assertThat(warning).contains("MANDATORY property '" + PARENT + ".since' (inherited: make the property optional)")
          .contains("default-valued property '" + PARENT + ".weight' (inherited: remove the property's default value (DEFAULT null))")
          .contains("declare the type without LIGHTWEIGHT");

      // each named way out clears its own part of the finding
      database.command("sql", "ALTER PROPERTY " + PARENT + ".since MANDATORY false").close();
      assertThat(LocalEdgeType.unenforcedConstraintWarning(database.getSchema().getType(CHILD))).doesNotContain(".since")
          .contains(".weight");
      database.command("sql", "ALTER PROPERTY " + PARENT + ".weight DEFAULT null").close();
      assertThat(LocalEdgeType.unenforcedConstraintWarning(database.getSchema().getType(CHILD))).isNull();
    }

    /**
     * {@code DEFAULT null} is kept as the text "null" and fills in nothing, so it is not a default a lightweight edge fails to
     * apply: it was refused as one, which left the SQL way out of the finding refused too.
     */
    @Test
    void aSqlNullDefaultIsNotADefaultUnderALightweightType() {
      database.getSchema().createEdgeType(PARENT);
      database.command("sql", "CREATE PROPERTY " + PARENT + ".weight INTEGER (DEFAULT null)").close();
      database.command("sql", "CREATE EDGE TYPE " + CHILD + " EXTENDS " + PARENT + " LIGHTWEIGHT").close();

      database.command("sql", "ALTER PROPERTY " + PARENT + ".weight DEFAULT null").close();
      database.command("sql", "CREATE PROPERTY " + PARENT + ".other STRING (DEFAULT null)").close();
      assertThat(LocalEdgeType.unenforcedConstraintWarning(database.getSchema().getType(CHILD))).isNull();

      // the STRING 'null' is a value like any other, and still refused
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY " + PARENT + ".text STRING (DEFAULT 'null')").close())
          .hasMessageContaining(CHILD).hasMessageContaining("keep the property without a default");
      assertThat(Property.fillsInAValue("'null'")).isTrue();
      assertThat(Property.fillsInAValue(" NULL ")).isFalse();
    }

    @Test
    void aSchemaWithoutTheCombinationLogsNothing() {
      database.getSchema().createEdgeType(PARENT).createProperty("since", Type.DATETIME);
      database.getSchema().buildEdgeType().withName(CHILD).withSuperType(PARENT).withLightweight(true).create();
      database.close();

      final List<String> warnings = new ArrayList<>();
      final Logger previous = LogManager.instance().getLogger();
      LogManager.instance().setLogger(new CapturingLogger(warnings));
      try {
        database = factory.open();
      } finally {
        LogManager.instance().setLogger(previous);
      }
      assertThat(warnings).noneMatch(w -> w.contains(CHILD));
    }

    /**
     * The state the DDL could produce before #9846: a lightweight type under a super type with a MANDATORY property, written by
     * hand because every DDL path now refuses it.
     */
    private void writeLegacySchema() throws IOException {
      final EdgeType parent = database.getSchema().createEdgeType(PARENT);
      parent.createProperty("since", Type.DATETIME).setMandatory(true);
      database.getSchema().createEdgeType(CHILD).addSuperType(PARENT);
      database.close();
      markChildLightweight();
    }

    private void markChildLightweight() throws IOException {
      final Path schemaFile = Path.of(getDatabasePath(), "schema.json");
      final JSONObject schemaJson = new JSONObject(Files.readString(schemaFile));
      schemaJson.getJSONObject("types").getJSONObject(CHILD).put("lightweight", true);
      Files.writeString(schemaFile, schemaJson.toString());
    }

    private static final class CapturingLogger implements Logger {
      private final List<String> warnings;

      private CapturingLogger(final List<String> warnings) {
        this.warnings = warnings;
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
          final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7,
          final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12, final Object arg13,
          final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
        log(requester, level, message, exception, context,
            new Object[] { arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17 });
      }

      @Override
      public synchronized void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        if (level == Level.WARNING && message != null)
          warnings.add(args == null || args.length == 0 ? message : String.format(message, args));
      }

      @Override
      public void flush() {
      }
    }
  }
}
