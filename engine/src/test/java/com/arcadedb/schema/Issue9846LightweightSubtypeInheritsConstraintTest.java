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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
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
class Issue9846LightweightSubtypeInheritsConstraintTest extends TestHelper {

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
