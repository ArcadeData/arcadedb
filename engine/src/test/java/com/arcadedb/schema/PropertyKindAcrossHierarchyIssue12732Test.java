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
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #12732: one property name must not mean two kinds of value in one type hierarchy. A declaration of a name a
 * type below already declares with another type (or another OF type) was accepted by {@code CREATE PROPERTY}, {@code ALTER PROPERTY
 * ... NAME}, {@code ALTER PROPERTY ... OF} and {@code ALTER TYPE ... SUPERTYPE +}, while the other order was refused. An index on the
 * super type's declaration then keyed the sub type's rows by the wrong type: lookups missed them, and incompatible encodings
 * corrupted the index file. A same-kind overlap stays accepted.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PropertyKindAcrossHierarchyIssue12732Test extends TestHelper {

  @Test
  void createPropertyOnASuperTypeIsRefusedWhenASubTypeDeclaresTheNameWithAnotherType() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE VERTEX TYPE B EXTENDS A");
    database.command("sql", "CREATE PROPERTY B.p DATETIME");

    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY A.p FLOAT"))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("'B'")
        .hasMessageContaining("DATETIME");
    assertThat(database.getSchema().getType("A").existsProperty("p")).isFalse();

    // the reported sequence, with the refused statement left out: the index is keyed by B's own declaration
    database.transaction(() -> database.command("sql", "INSERT INTO B SET p = '2019-04-18 00:00:00'"));
    assertThat(count("SELECT FROM B WHERE p = '2019-04-18 00:00:00'")).isEqualTo(1);
    database.command("sql", "CREATE INDEX ON B (p) NOTUNIQUE");
    assertThat(count("SELECT FROM B WHERE p = '2019-04-18 00:00:00'")).isEqualTo(1);
    assertThat(count("SELECT FROM B WHERE p > '2019-04-01 00:00:00'")).isEqualTo(1);
  }

  @Test
  void theOtherOrderIsStillRefused() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE VERTEX TYPE B EXTENDS A");
    database.command("sql", "CREATE PROPERTY A.p FLOAT");

    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY B.p DATETIME"))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("already defined in a super type");
  }

  @Test
  void aSameKindOverlapIsStillAcceptedAndIndexed() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE VERTEX TYPE B EXTENDS A");
    database.command("sql", "CREATE PROPERTY B.q STRING");
    database.command("sql", "CREATE PROPERTY A.q STRING");

    database.transaction(() -> database.command("sql", "INSERT INTO B SET q = 'x'"));
    database.command("sql", "CREATE INDEX ON A (q) NOTUNIQUE");
    assertThat(count("SELECT FROM B WHERE q = 'x'")).isEqualTo(1);
    assertThat(count("SELECT FROM A WHERE q = 'x'")).isEqualTo(1);
  }

  @Test
  void aDeclarationDeeperInTheSubtreeRefusesToo() {
    database.command("sql", "CREATE DOCUMENT TYPE A");
    database.command("sql", "CREATE DOCUMENT TYPE B EXTENDS A");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS B");
    database.command("sql", "CREATE PROPERTY C.p DATETIME");

    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY A.p FLOAT")).isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("'C'");
    assertThat(database.getSchema().getType("A").existsProperty("p")).isFalse();
  }

  @Test
  void aDeclarationOfAnotherSuperTypeOfASubTypeRefusesToo() {
    // C sees what A and X declare: an A.p BINARY next to X.p LONG would give C's records two meanings for p
    database.command("sql", "CREATE DOCUMENT TYPE A");
    database.command("sql", "CREATE DOCUMENT TYPE X");
    database.command("sql", "CREATE PROPERTY X.p LONG");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS A, X");

    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY A.p BINARY")).isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("'X'");
    assertThat(database.getSchema().getType("A").existsProperty("p")).isFalse();

    // the same type is one meaning
    database.command("sql", "CREATE PROPERTY A.p LONG");
    assertThat(database.getSchema().getType("A").getProperty("p").getType()).isEqualTo(Type.LONG);
  }

  @Test
  void anotherOfTypeIsAnotherKind() {
    database.command("sql", "CREATE DOCUMENT TYPE A");
    database.command("sql", "CREATE DOCUMENT TYPE B EXTENDS A");
    database.command("sql", "CREATE PROPERTY B.l LIST OF STRING");

    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY A.l LIST OF INTEGER")).isInstanceOf(AlreadyExistsException.class);
    assertThat(database.getSchema().getType("A").existsProperty("l")).isFalse();

    database.command("sql", "CREATE PROPERTY A.l LIST OF STRING");
    // and the OF type cannot drift apart afterwards, from either side
    assertThatThrownBy(() -> database.getSchema().getType("B").getProperty("l").setOfType("INTEGER")).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> database.getSchema().getType("A").getProperty("l").setOfType("INTEGER")).isInstanceOf(SchemaException.class);
    assertThat(database.getSchema().getType("A").getProperty("l").getOfType()).isEqualTo("STRING");
    assertThat(database.getSchema().getType("B").getProperty("l").getOfType()).isEqualTo("STRING");
  }

  @Test
  void renamingToANameASubTypeDeclaresWithAnotherTypeIsRefused() {
    database.command("sql", "CREATE DOCUMENT TYPE A");
    database.command("sql", "CREATE DOCUMENT TYPE B EXTENDS A");
    database.command("sql", "CREATE PROPERTY A.q FLOAT");
    database.command("sql", "CREATE PROPERTY B.p DATETIME");

    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY A.q NAME p")).isInstanceOf(AlreadyExistsException.class);
    assertThat(database.getSchema().getType("A").existsProperty("q")).isTrue();
    assertThat(database.getSchema().getType("A").existsProperty("p")).isFalse();
  }

  @Test
  void addingASuperTypeThatDeclaresTheNameWithAnotherTypeIsRefusedBeforeTheIndexIsCorrupted() {
    database.command("sql", "CREATE DOCUMENT TYPE A");
    database.command("sql", "CREATE PROPERTY A.p BINARY");
    database.command("sql", "CREATE INDEX ON A (p) NOTUNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE B");
    database.command("sql", "CREATE PROPERTY B.p LONG");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE B SUPERTYPE +A")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("'p'");
    assertThat(database.getSchema().getType("B").getSuperTypes()).isEmpty();
    assertThat(database.getSchema().getType("A").getSubTypes()).isEmpty();

    database.transaction(() -> {
      database.command("sql", "INSERT INTO B SET p = 5");
      database.command("sql", "INSERT INTO B SET p = 6");
    });
    assertThat(database.getSchema().getIndexByName("A[p]").countEntries()).isZero();
    assertThat(count("SELECT FROM B WHERE p = 5")).isEqualTo(1);
  }

  @Test
  void aDeclarationBelowTheJoiningTypeRefusesTheLinkToo() {
    database.command("sql", "CREATE DOCUMENT TYPE A");
    database.command("sql", "CREATE PROPERTY A.p BINARY");
    database.command("sql", "CREATE DOCUMENT TYPE B");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS B");
    database.command("sql", "CREATE PROPERTY C.p LONG");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE B SUPERTYPE +A")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("'C'");
    assertThat(database.getSchema().getType("B").getSuperTypes()).isEmpty();
  }

  @Test
  void anIndexKindTheJoiningDeclarationCannotHoldIsRefusedAsASchemaError() {
    // BY KEY over a MAP, the joining type declares a STRING and holds a record: was a 500 IndexException out of the propagation
    database.command("sql", "CREATE DOCUMENT TYPE T1");
    database.command("sql", "CREATE PROPERTY T1.m MAP");
    database.command("sql", "CREATE INDEX ON T1 (m BY KEY) NOTUNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE Q");
    database.command("sql", "CREATE PROPERTY Q.m STRING");
    database.transaction(() -> database.command("sql", "INSERT INTO Q SET m = 'x'"));

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Q SUPERTYPE +T1")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("'m'");
    assertThat(database.getSchema().getType("Q").getSuperTypes()).isEmpty();

    // GEOSPATIAL over a STRING, the joining type declares an INTEGER: was accepted, then every insert answered a 500
    database.command("sql", "CREATE DOCUMENT TYPE G1");
    database.command("sql", "CREATE PROPERTY G1.g STRING");
    database.command("sql", "CREATE INDEX ON G1 (g) GEOSPATIAL");
    database.command("sql", "CREATE DOCUMENT TYPE G2");
    database.command("sql", "CREATE PROPERTY G2.g INTEGER");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE G2 SUPERTYPE +G1")).isInstanceOf(SchemaException.class);
    database.transaction(() -> database.command("sql", "INSERT INTO G2 SET g = 5"));
    assertThat(count("SELECT FROM G2")).isEqualTo(1);
  }

  @Test
  void addingASuperTypeThatDeclaresTheNameWithTheSameTypeIsStillAccepted() {
    database.command("sql", "CREATE DOCUMENT TYPE A");
    database.command("sql", "CREATE PROPERTY A.p LONG");
    database.command("sql", "CREATE INDEX ON A (p) NOTUNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE B");
    database.command("sql", "CREATE PROPERTY B.p LONG");
    database.transaction(() -> database.command("sql", "INSERT INTO B SET p = 5"));

    database.command("sql", "ALTER TYPE B SUPERTYPE +A");
    assertThat(database.getSchema().getType("B").isSubTypeOf("A")).isTrue();
    database.transaction(() -> database.command("sql", "INSERT INTO B SET p = 6"));
    assertThat(database.getSchema().getIndexByName("A[p]").countEntries()).isEqualTo(2);
    assertThat(count("SELECT FROM A WHERE p = 5")).isEqualTo(1);
  }

  @Test
  void aNewTypeCannotExtendTwoTypesThatDeclareTheNameWithDifferentTypes() {
    database.command("sql", "CREATE DOCUMENT TYPE A");
    database.command("sql", "CREATE PROPERTY A.p BINARY");
    database.command("sql", "CREATE DOCUMENT TYPE X");
    database.command("sql", "CREATE PROPERTY X.p LONG");

    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS X, A")).isInstanceOf(SchemaException.class);
    assertThat(database.getSchema().existsType("C")).isFalse();
    assertThat(database.getSchema().getType("A").getSubTypes()).isEmpty();
    assertThat(database.getSchema().getType("X").getSubTypes()).isEmpty();
  }

  @Test
  void droppingAMiddleTypeOfAnOverlapThatAlreadyHoldsIsNotRefused() throws Exception {
    // B -> M -> A with B.p and A.p of the same type, then the file is edited to make them disagree: a database written before the
    // check. Dropping M links B to A directly, which adds nothing B did not already inherit, so it is no new overlap
    database.command("sql", "CREATE DOCUMENT TYPE A");
    database.command("sql", "CREATE DOCUMENT TYPE M EXTENDS A");
    database.command("sql", "CREATE DOCUMENT TYPE B EXTENDS M");
    database.command("sql", "CREATE PROPERTY B.p LONG");
    database.command("sql", "CREATE PROPERTY A.p LONG");
    database.close();

    editSchema(root -> root.getJSONObject("types").getJSONObject("B").getJSONObject("properties").getJSONObject("p").put("type",
        "STRING"));

    database = factory.open();
    assertThat(database.getSchema().getType("B").getProperty("p").getType()).isEqualTo(Type.STRING);
    assertThat(database.getSchema().getType("A").getProperty("p").getType()).isEqualTo(Type.LONG);

    database.command("sql", "DROP TYPE M");
    assertThat(database.getSchema().getType("B").isSubTypeOf("A")).isTrue();
  }

  @Test
  void aRefusedCreatePropertyIsUndoneDespiteAnIndexOfASubTypeOnAnotherDeclaration() {
    // T4 sees p0 through X as well: once T3.p0 is created and the stored records refuse it, the undo must not be refused by T4's
    // index, which stands on X's declaration and keeps it whatever happens to T3.p0
    database.command("sql", "CREATE DOCUMENT TYPE T3");
    database.command("sql", "CREATE DOCUMENT TYPE X");
    database.command("sql", "CREATE PROPERTY X.p0 DATE");
    database.command("sql", "CREATE DOCUMENT TYPE T4 EXTENDS T3, X");
    database.command("sql", "CREATE INDEX ON T4 (p0) UNIQUE_HASH");
    database.transaction(() -> database.command("sql", "INSERT INTO T4 SET other = 1"));

    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T3.p0 DATE (mandatory true)"))
        .isInstanceOf(ValidationException.class)
        .satisfies(e -> assertThat(e.getSuppressed()).isEmpty());
    assertThat(database.getSchema().getType("T3").existsProperty("p0")).isFalse();
    assertThat(database.getSchema().existsIndex("T4[p0]")).isTrue();

    // the same index does not stop the property from being dropped, renamed or dropped with FORCE: T4 keeps X's declaration
    database.command("sql", "CREATE PROPERTY T3.p0 DATE");
    database.command("sql", "ALTER PROPERTY T3.p0 NAME p1");
    database.command("sql", "ALTER PROPERTY T3.p1 NAME p0");
    database.command("sql", "DROP PROPERTY T3.p0 FORCE");
    assertThat(database.getSchema().getType("T3").existsProperty("p0")).isFalse();
    assertThat(database.getSchema().existsIndex("T4[p0]")).isTrue();
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.stream().count();
    }
  }

  private interface SchemaEdit {
    void apply(JSONObject root);
  }

  private void editSchema(final SchemaEdit edit) throws Exception {
    final File schemaFile = new File(getDatabasePath() + File.separator + LocalSchema.SCHEMA_FILE_NAME);
    final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath()));
    edit.apply(root);
    Files.writeString(schemaFile.toPath(), root.toString());
    // the fallback copy must not stand in for the edited primary
    final File prev = new File(getDatabasePath() + File.separator + "schema.prev.json");
    if (prev.exists())
      Files.writeString(prev.toPath(), root.toString());
  }
}
