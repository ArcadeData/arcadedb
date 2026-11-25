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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Schema regression tests: what a type may be linked under as a super type.
 *
 * <p>One nested class per fixed issue, each keeping its own fixture.</p>
 */
class SuperTypeAdmissionTest {

  /**
   * Regression for issue #12028: {@code ALTER TYPE A SUPERTYPE +C}, with {@code C} already below {@code A}, was not refused as a cycle.
   * It failed with a {@code StackOverflowError} after linking, and the link stayed in memory: {@code SELECT FROM A} and every later
   * schema change on the type overflowed the stack too.
   */
  @Nested
  class SuperTypeCycle extends TestHelper {

    @Test
    void aSuperTypeThatIsAlreadyASubTypeIsRefusedAndNothingChanges() {
      database.command("sql", "CREATE VERTEX TYPE A");
      database.command("sql", "CREATE VERTEX TYPE B EXTENDS A");
      database.command("sql", "CREATE VERTEX TYPE C EXTENDS B");
      database.transaction(() -> database.command("sql", "INSERT INTO C SET x = 1"));

      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE A SUPERTYPE +C"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("'C' is already a sub type of 'A'");
      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE A SUPERTYPE +B"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("'B' is already a sub type of 'A'");

      assertThat(database.getSchema().getType("A").getSuperTypes()).isEmpty();
      assertThat(database.getSchema().getType("C").getSuperTypes()).extracting(DocumentType::getName).containsExactly("B");
      try (final ResultSet rs = database.query("sql", "SELECT FROM A")) {
        assertThat(rs.stream().count()).isEqualTo(1);
      }
      assertThatCode(() -> database.command("sql", "CREATE PROPERTY A.y INTEGER")).doesNotThrowAnyException();
    }

    @Test
    void theJavaApiAndTheTypeBuilderAreRefusedToo() {
      final DocumentType a = database.getSchema().createDocumentType("A");
      database.getSchema().createDocumentType("B").addSuperType("A");

      assertThatThrownBy(() -> a.addSuperType("B")).isInstanceOf(SchemaException.class).hasMessageContaining("cannot have a cycle");
      // the builder on an existing type links the super types it lists
      assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("A").withSuperType("B").withIgnoreIfExists(true).create())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("cannot have a cycle");
      assertThat(a.getSuperTypes()).isEmpty();
    }

    /**
     * A schema file that already holds a cycle, as the next schema save after the refused statement used to write, opens: the link that
     * would close the cycle is left out.
     */
    @Test
    void aSchemaFileWithACycleStillOpens() throws Exception {
      database.getSchema().createVertexType("A");
      database.getSchema().createVertexType("B").addSuperType("A");
      database.getSchema().createVertexType("C").addSuperType("B");
      database.transaction(() -> database.newVertex("C").set("x", 1).save());

      database.close();

      final Path schemaFile = Path.of(getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
      final JSONObject schemaJson = new JSONObject(Files.readString(schemaFile));
      schemaJson.getJSONObject("types").getJSONObject("A").put("parents", new JSONArray().put("C"));
      Files.writeString(schemaFile, schemaJson.toString());

      database = factory.open();

      final List<DocumentType> types = List.of(database.getSchema().getType("A"), database.getSchema().getType("B"),
          database.getSchema().getType("C"));
      // the three links form a cycle: one of them is left out, whichever the load met last, and the two left are a chain, so exactly
      // one of the three types has no super type
      assertThat(types.stream().mapToInt(t -> t.getSuperTypes().size()).sum()).isEqualTo(2);
      final List<DocumentType> roots = types.stream().filter(t -> t.getSuperTypes().isEmpty()).toList();
      assertThat(roots).hasSize(1);
      // every polymorphic walk ends, and the root of the chain reaches the record of C: the scans, and a schema change that saves the
      // schema again
      final String root = roots.getFirst().getName();
      assertThat(database.countType(root, true)).isEqualTo(1L);
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + root)) {
        assertThat(rs.stream().count()).isEqualTo(1);
      }
      try (final ResultSet rs = database.query("sql", "SELECT FROM C")) {
        assertThat(rs.stream().count()).isEqualTo(1);
      }
      assertThatCode(() -> database.command("sql", "CREATE PROPERTY A.y INTEGER")).doesNotThrowAnyException();
    }

    /**
     * Two types whose names differ only by case are two types (the schema keys them by the exact name), so linking one under the other
     * is no link of a type to itself, and the link the other way is a real cycle. The check used to compare the names ignoring the case.
     */
    @Test
    void typesWhoseNamesDifferOnlyByCaseAreNotTakenForOneAnother() {
      final DocumentType upper = database.getSchema().createDocumentType("Foo");
      final DocumentType lower = documentTypeOnItsOwnBucket("foo", "FooLowerBucket");

      assertThatCode(() -> lower.addSuperType(upper)).doesNotThrowAnyException();
      assertThat(lower.getSuperTypes()).extracting(DocumentType::getName).containsExactly("Foo");

      assertThatThrownBy(() -> upper.addSuperType(lower)).isInstanceOf(SchemaException.class)
          .hasMessageContaining("'foo' is already a sub type of 'Foo'");
      assertThat(upper.getSuperTypes()).isEmpty();
    }

    /**
     * The same link, in a schema file written by a database that accepted it, is not mistaken for a cycle by the load either: the load
     * used to leave it out with a warning, and the next schema save wrote the loss.
     */
    @Test
    void aStoredLinkBetweenTypesNamedAlikeIsNotDroppedByTheLoad() throws Exception {
      database.getSchema().createDocumentType("Foo");
      documentTypeOnItsOwnBucket("foo", "FooLowerBucket");

      database.close();

      final Path schemaFile = Path.of(getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
      final JSONObject schemaJson = new JSONObject(Files.readString(schemaFile));
      schemaJson.getJSONObject("types").getJSONObject("foo").put("parents", new JSONArray().put("Foo"));
      Files.writeString(schemaFile, schemaJson.toString());

      database = factory.open();

      assertThat(database.getSchema().getType("foo").getSuperTypes()).extracting(DocumentType::getName).containsExactly("Foo");
      assertThat(database.getSchema().getType("Foo").getSuperTypes()).isEmpty();
    }

    /** A bucket of its own, so that the files of two types named alike do not meet on a file system that ignores the case. */
    private DocumentType documentTypeOnItsOwnBucket(final String typeName, final String bucketName) {
      final Bucket bucket = database.getSchema().createBucket(bucketName);
      return database.getSchema().buildDocumentType().withName(typeName).withBuckets(List.of(bucket)).create();
    }
  }

  /**
   * Regression for issue #12029: a type could be made a sub type of a type of another record kind, by ALTER TYPE ... SUPERTYPE + and by
   * CREATE ... TYPE ... EXTENDS. The records then showed up under the super type's label, and openCypher failed with a ClassCastException
   * on the first one it met. A document super type stays legal for a vertex or an edge type: schemas use one to share properties.
   */
  @Nested
  class SuperTypeOfAnotherRecordKind extends TestHelper {

    @Test
    void alterTypeRefusesASuperTypeOfAnotherKind() {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE EDGE TYPE E");
      database.command("sql", "CREATE DOCUMENT TYPE D");
      database.command("sql", "CREATE VERTEX TYPE W");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO V SET n = 1");
        database.command("sql", "INSERT INTO D SET n = 2");
      });

      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE D SUPERTYPE +V"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("Cannot make the document type 'D' a sub type of the vertex type 'V'");
      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE W SUPERTYPE +E"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("Cannot make the vertex type 'W' a sub type of the edge type 'E'");
      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE E SUPERTYPE +V"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("Cannot make the edge type 'E' a sub type of the vertex type 'V'");
      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE E SUPERTYPE +W"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("Cannot make the edge type 'E' a sub type of the vertex type 'W'");

      assertThat(database.getSchema().getType("D").getSuperTypes()).isEmpty();
      assertThat(database.getSchema().getType("W").getSuperTypes()).isEmpty();
      try (final ResultSet rs = database.query("opencypher", "MATCH (v:V) RETURN v.n AS n")) {
        assertThat(rs.stream().map(r -> r.<Integer>getProperty("n")).toList()).containsExactly(1);
      }

      // the same kind, and a document super type for a graph type, stay legal
      assertThatCode(() -> database.command("sql", "ALTER TYPE W SUPERTYPE +V")).doesNotThrowAnyException();
      assertThat(database.getSchema().getType("W").getSuperTypes()).extracting(DocumentType::getName).containsExactly("V");
      assertThatCode(() -> database.command("sql", "ALTER TYPE V SUPERTYPE +D")).doesNotThrowAnyException();
      assertThatCode(() -> database.command("sql", "ALTER TYPE E SUPERTYPE +D")).doesNotThrowAnyException();
    }

    @Test
    void createTypeExtendsRefusesASuperTypeOfAnotherKindAndCreatesNothing() {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE EDGE TYPE E");
      database.command("sql", "CREATE DOCUMENT TYPE D");

      assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE D2 EXTENDS V"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("a type can only extend a document type or a type of its own kind");
      assertThatThrownBy(() -> database.command("sql", "CREATE EDGE TYPE E2 EXTENDS V"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("a type can only extend a document type or a type of its own kind");
      assertThatThrownBy(() -> database.command("sql", "CREATE VERTEX TYPE V2 EXTENDS E"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("a type can only extend a document type or a type of its own kind");
      assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("D4").withSuperType("V").create())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("a type can only extend a document type or a type of its own kind");

      for (final String name : new String[] { "D2", "E2", "V2", "D4" })
        assertThat(database.getSchema().existsType(name)).as(name).isFalse();
      // refused BEFORE the type is made, not made and taken back: a document type registers its name in the dictionary when it is made,
      // and a name that was registered stays there
      assertThat(database.getSchema().getDictionary().getIdByName("D2", false)).isEqualTo(-1);
      assertThat(database.getSchema().getDictionary().getIdByName("D4", false)).isEqualTo(-1);

      assertThatCode(() -> database.command("sql", "CREATE EDGE TYPE E5 EXTENDS E")).doesNotThrowAnyException();
      assertThatCode(() -> database.command("sql", "CREATE DOCUMENT TYPE D5 EXTENDS D")).doesNotThrowAnyException();
      assertThatCode(() -> database.command("sql", "CREATE VERTEX TYPE V5 EXTENDS D")).doesNotThrowAnyException();
      assertThatCode(() -> database.getSchema().buildEdgeType().withName("E6").withSuperType("D").create()).doesNotThrowAnyException();
    }

    /** A database written before the check may hold such a link, and it must still open: the check is on the DDL. */
    @Test
    void aSchemaThatAlreadyHoldsACrossKindLinkStillOpens() throws Exception {
      database.getSchema().createVertexType("V");
      database.getSchema().createDocumentType("D");

      database.close();

      final Path schemaFile = Path.of(getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
      final JSONObject schemaJson = new JSONObject(Files.readString(schemaFile));
      schemaJson.getJSONObject("types").getJSONObject("D").put("parents", new JSONArray().put("V"));
      Files.writeString(schemaFile, schemaJson.toString());

      database = factory.open();

      assertThat(database.getSchema().getType("D").getSuperTypes()).extracting(DocumentType::getName).containsExactly("V");
    }

    /**
     * A TIMESERIES type is a document type: under a graph type it is still refused for being a time series, with the message that says
     * so, and not for its kind.
     */
    @Test
    void aTimeSeriesTypeUnderAGraphTypeKeepsItsOwnRefusal() {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE TIMESERIES TYPE Reading TIMESTAMP ts FIELDS (v DOUBLE)");

      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Reading SUPERTYPE +V")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("TIMESERIES");
      assertThat(database.getSchema().getType("Reading").getSuperTypes()).isEmpty();
    }

    /**
     * DROP TYPE of the type in the middle links its sub types straight to its super types, which is a new link for them (issue #11272),
     * so the kind rule is asked of it: dropping {@code M} out of a pre-check {@code D -> M -> X}, with a document type under a vertex
     * type, would hang the document directly under {@code X}. The drop is refused before anything changes and names the link to take
     * out, after which it goes through.
     */
    @Test
    void dropTypeDoesNotReattachASubTypeUnderASuperTypeOfAnotherKind() {
      final LocalDocumentType x = (LocalDocumentType) database.getSchema().createVertexType("X");
      final LocalDocumentType m = (LocalDocumentType) database.getSchema().createVertexType("M");
      final LocalDocumentType d = (LocalDocumentType) database.getSchema().createDocumentType("D");
      m.addSuperType(x);
      // the re-establishing linkage of the schema load, which a database written before the check went through
      d.addSuperType(m, false);

      for (final String statement : List.of("DROP TYPE M", "DROP TYPE M IF EXISTS", "DROP TYPE M UNSAFE"))
        assertThatThrownBy(() -> database.command("sql", statement)).as(statement).isInstanceOf(SchemaException.class)
            .hasMessageContaining("Cannot drop the type 'M'").hasMessageContaining("'D'")
            .hasMessageContaining("ALTER TYPE `D` SUPERTYPE -`M`");

      // nothing changed
      assertThat(database.getSchema().existsType("M")).isTrue();
      assertThat(d.getSuperTypes()).extracting(DocumentType::getName).containsExactly("M");
      assertThat(m.getSuperTypes()).extracting(DocumentType::getName).containsExactly("X");

      database.command("sql", "ALTER TYPE `D` SUPERTYPE -`M`");
      database.command("sql", "DROP TYPE M");
      assertThat(database.getSchema().existsType("M")).isFalse();
      assertThat(x.getSubTypes()).isEmpty();
      assertThat(d.getSuperTypes()).isEmpty();
    }

    /** The rule is transitive, so a hierarchy that satisfies it is still dropped from the middle: a vertex type lands under a document type. */
    @Test
    void dropTypeOfAMiddleTypeOfAHierarchyThatFollowsTheRuleIsUntouched() {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE VERTEX TYPE Mid EXTENDS Doc");
      database.command("sql", "CREATE VERTEX TYPE Leaf EXTENDS Mid");

      database.command("sql", "DROP TYPE Mid");

      assertThat(database.getSchema().getType("Leaf").getSuperTypes()).extracting(DocumentType::getName).containsExactly("Doc");
    }
  }
}
