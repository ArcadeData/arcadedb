package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Collection;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Cypher constraint DDL statements (CREATE CONSTRAINT, DROP CONSTRAINT).
 */
class OpenCypherConstraintTest {
  private Database database;
  private String databasePath;

  @BeforeEach
  void setUp(TestInfo testInfo) {
    databasePath = "./target/databases/testopencypher-constraint-" + testInfo.getTestMethod().get().getName();
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void createUniqueConstraint() {
    database.command("opencypher", "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE");

    // Verify a unique index was created on Person.id
    final Collection<TypeIndex> indexes = database.getSchema().getType("Person").getAllIndexes(false);
    assertThat(indexes.size()).isGreaterThanOrEqualTo(1);

    boolean found = false;
    for (final TypeIndex idx : indexes) {
      if (idx.isUnique() && idx.getPropertyNames().contains("id")) {
        found = true;
        break;
      }
    }
    assertThat(found).isTrue();
  }

  @Test
  void createUniqueConstraintWithName() {
    database.command("opencypher", "CREATE CONSTRAINT myConstraint FOR (p:Person) REQUIRE p.id IS UNIQUE");

    final Collection<TypeIndex> indexes = database.getSchema().getType("Person").getAllIndexes(false);
    boolean found = false;
    for (final TypeIndex idx : indexes) {
      if (idx.isUnique() && idx.getPropertyNames().contains("id")) {
        found = true;
        break;
      }
    }
    assertThat(found).isTrue();
  }

  @Test
  void createUniqueConstraintIfNotExists() {
    database.command("opencypher", "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE");
    // Run again - should not throw
    database.command("opencypher", "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE");

    final Collection<TypeIndex> indexes = database.getSchema().getType("Person").getAllIndexes(false);
    boolean found = false;
    for (final TypeIndex idx : indexes) {
      if (idx.isUnique() && idx.getPropertyNames().contains("id")) {
        found = true;
        break;
      }
    }
    assertThat(found).isTrue();
  }

  @Test
  void createNotNullConstraint() {
    database.getSchema().getType("Person").createProperty("name", Type.STRING);

    database.command("opencypher", "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.name IS NOT NULL");

    final Property property = database.getSchema().getType("Person").getPropertyIfExists("name");
    assertThat(property).isNotNull();
    assertThat(property.isMandatory()).isTrue();
  }

  @Test
  void createNodeKeyConstraint() {
    database.getSchema().getType("Person").createProperty("id", Type.STRING);

    database.command("opencypher", "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS NODE KEY");

    // Verify unique index exists
    final Collection<TypeIndex> indexes = database.getSchema().getType("Person").getAllIndexes(false);
    boolean foundUnique = false;
    for (final TypeIndex idx : indexes) {
      if (idx.isUnique() && idx.getPropertyNames().contains("id")) {
        foundUnique = true;
        break;
      }
    }
    assertThat(foundUnique).isTrue();

    // Verify property is mandatory
    final Property property = database.getSchema().getType("Person").getPropertyIfExists("id");
    assertThat(property).isNotNull();
    assertThat(property.isMandatory()).isTrue();
  }

  @Test
  void createUniqueConstraintEnforcesUniqueness() {
    database.command("opencypher", "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (p:Person {email: 'alice@test.com'})");
    });

    assertThatThrownBy(() -> database.transaction(() -> {
      database.command("opencypher", "CREATE (p:Person {email: 'alice@test.com'})");
    })).isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void createConstraintMultipleProperties() {
    database.command("opencypher", "CREATE CONSTRAINT FOR (p:Person) REQUIRE (p.first, p.last) IS UNIQUE");

    final Collection<TypeIndex> indexes = database.getSchema().getType("Person").getAllIndexes(false);
    boolean found = false;
    for (final TypeIndex idx : indexes) {
      if (idx.isUnique() && idx.getPropertyNames().contains("first") && idx.getPropertyNames().contains("last")) {
        found = true;
        break;
      }
    }
    assertThat(found).isTrue();
  }

  @Test
  void dropConstraint() {
    // First create property and index manually with a known name
    database.getSchema().getType("Person").createProperty("id", Type.STRING);
    database.getSchema().buildTypeIndex("Person", new String[] { "id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(true)
        .create();

    // Find the index name
    final Collection<TypeIndex> indexes = database.getSchema().getType("Person").getAllIndexes(false);
    assertThat(indexes.size()).isGreaterThanOrEqualTo(1);
    final String indexName = indexes.iterator().next().getName();

    database.command("opencypher", "DROP CONSTRAINT `" + indexName + "`");

    // Verify index was removed
    assertThat(database.getSchema().existsIndex(indexName)).isFalse();
  }

  @Test
  void dropConstraintIfExists() {
    // Should not throw even if constraint doesn't exist
    database.command("opencypher", "DROP CONSTRAINT nonExistentConstraint IF EXISTS");
  }

  @Test
  void createConstraintAutoCreatesVertexType() {
    // Issue #3760: CREATE CONSTRAINT should auto-create the type if it doesn't exist
    assertThat(database.getSchema().existsType("NewType")).isFalse();

    database.command("opencypher", "CREATE CONSTRAINT FOR (t:NewType) REQUIRE t.id IS UNIQUE");

    // Verify the type was auto-created as a vertex type
    assertThat(database.getSchema().existsType("NewType")).isTrue();
    assertThat(database.getSchema().getType("NewType")).isInstanceOf(VertexType.class);

    // Verify constraint was created
    final Collection<TypeIndex> indexes = database.getSchema().getType("NewType").getAllIndexes(false);
    boolean found = false;
    for (final TypeIndex idx : indexes) {
      if (idx.isUnique() && idx.getPropertyNames().contains("id")) {
        found = true;
        break;
      }
    }
    assertThat(found).isTrue();
  }

  @Test
  void createConstraintAutoCreatesEdgeType() {
    // Issue #3760: CREATE CONSTRAINT should auto-create edge types too
    assertThat(database.getSchema().existsType("LIKES")).isFalse();

    database.command("opencypher", "CREATE CONSTRAINT FOR ()-[r:LIKES]-() REQUIRE r.since IS NOT NULL");

    // Verify the type was auto-created as an edge type
    assertThat(database.getSchema().existsType("LIKES")).isTrue();
    assertThat(database.getSchema().getType("LIKES")).isInstanceOf(EdgeType.class);

    // Verify property is mandatory
    final Property property = database.getSchema().getType("LIKES").getPropertyIfExists("since");
    assertThat(property).isNotNull();
    assertThat(property.isMandatory()).isTrue();
  }

  @Test
  void createConstraintAutoCreatesTypeWithIfNotExists() {
    // Issue #3760: IF NOT EXISTS should also auto-create the type
    assertThat(database.getSchema().existsType("Widget")).isFalse();

    database.command("opencypher", "CREATE CONSTRAINT IF NOT EXISTS FOR (w:Widget) REQUIRE w.code IS UNIQUE");

    assertThat(database.getSchema().existsType("Widget")).isTrue();

    // Running again should not throw
    database.command("opencypher", "CREATE CONSTRAINT IF NOT EXISTS FOR (w:Widget) REQUIRE w.code IS UNIQUE");
  }

  @Test
  void createNodeKeyConstraintAutoCreatesType() {
    // Issue #3760: NODE KEY constraint should also auto-create the type
    assertThat(database.getSchema().existsType("Product")).isFalse();

    database.command("opencypher", "CREATE CONSTRAINT FOR (p:Product) REQUIRE p.sku IS NODE KEY");

    assertThat(database.getSchema().existsType("Product")).isTrue();

    // Verify unique index
    final Collection<TypeIndex> indexes = database.getSchema().getType("Product").getAllIndexes(false);
    boolean foundUnique = false;
    for (final TypeIndex idx : indexes) {
      if (idx.isUnique() && idx.getPropertyNames().contains("sku")) {
        foundUnique = true;
        break;
      }
    }
    assertThat(foundUnique).isTrue();

    // Verify property is mandatory
    final Property property = database.getSchema().getType("Product").getPropertyIfExists("sku");
    assertThat(property).isNotNull();
    assertThat(property.isMandatory()).isTrue();
  }

  @Test
  void createTypedConstraintString() {
    database.command("opencypher", "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS TYPED STRING");

    final Property property = database.getSchema().getType("Person").getPropertyIfExists("email");
    assertThat(property).isNotNull();
    assertThat(property.getType()).isEqualTo(Type.STRING);
  }

  @Test
  void createTypedConstraintInteger() {
    // Cypher INTEGER is 64-bit, maps to ArcadeDB LONG
    database.command("opencypher", "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.age IS TYPED INTEGER");

    final Property property = database.getSchema().getType("Person").getPropertyIfExists("age");
    assertThat(property).isNotNull();
    assertThat(property.getType()).isEqualTo(Type.LONG);
  }

  @Test
  void createTypedConstraintBoolean() {
    database.command("opencypher", "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.active IS TYPED BOOLEAN");

    final Property property = database.getSchema().getType("Person").getPropertyIfExists("active");
    assertThat(property).isNotNull();
    assertThat(property.getType()).isEqualTo(Type.BOOLEAN);
  }

  @Test
  void createTypedConstraintFloat() {
    // Cypher FLOAT is 64-bit (IEEE 754 double), maps to ArcadeDB DOUBLE
    database.command("opencypher", "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.score IS TYPED FLOAT");

    final Property property = database.getSchema().getType("Person").getPropertyIfExists("score");
    assertThat(property).isNotNull();
    assertThat(property.getType()).isEqualTo(Type.DOUBLE);
  }

  @Test
  void createTypedConstraintDate() {
    database.command("opencypher", "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.born IS TYPED DATE");

    final Property property = database.getSchema().getType("Person").getPropertyIfExists("born");
    assertThat(property).isNotNull();
    assertThat(property.getType()).isEqualTo(Type.DATE);
  }

  @Test
  void createTypedConstraintLocalDatetime() {
    database.command("opencypher", "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.created IS TYPED LOCAL DATETIME");

    final Property property = database.getSchema().getType("Person").getPropertyIfExists("created");
    assertThat(property).isNotNull();
    assertThat(property.getType()).isEqualTo(Type.DATETIME);
  }

  @Test
  void createTypedConstraintWithColonColon() {
    // Alternative syntax: REQUIRE p.email :: STRING
    database.command("opencypher", "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email :: STRING");

    final Property property = database.getSchema().getType("Person").getPropertyIfExists("email");
    assertThat(property).isNotNull();
    assertThat(property.getType()).isEqualTo(Type.STRING);
  }

  @Test
  void createTypedConstraintAutoCreatesType() {
    assertThat(database.getSchema().existsType("Animal")).isFalse();

    database.command("opencypher", "CREATE CONSTRAINT FOR (a:Animal) REQUIRE a.species IS TYPED STRING");

    assertThat(database.getSchema().existsType("Animal")).isTrue();
    assertThat(database.getSchema().getType("Animal")).isInstanceOf(VertexType.class);

    final Property property = database.getSchema().getType("Animal").getPropertyIfExists("species");
    assertThat(property).isNotNull();
    assertThat(property.getType()).isEqualTo(Type.STRING);
  }

  @Test
  void createTypedConstraintIfNotExists() {
    database.command("opencypher", "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.email IS TYPED STRING");
    // Run again - should not throw
    database.command("opencypher", "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.email IS TYPED STRING");

    final Property property = database.getSchema().getType("Person").getPropertyIfExists("email");
    assertThat(property).isNotNull();
    assertThat(property.getType()).isEqualTo(Type.STRING);
  }

  @Test
  void createTypedConstraintOnExistingPropertyChangesType() {
    database.getSchema().getType("Person").createProperty("code", Type.STRING);

    database.command("opencypher", "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.code IS TYPED INTEGER");

    final Property property = database.getSchema().getType("Person").getPropertyIfExists("code");
    assertThat(property).isNotNull();
    assertThat(property.getType()).isEqualTo(Type.LONG);
  }

  @Test
  void createTypedConstraintOnRelationship() {
    database.command("opencypher", "CREATE CONSTRAINT FOR ()-[r:KNOWS]-() REQUIRE r.since IS TYPED DATE");

    final Property property = database.getSchema().getType("KNOWS").getPropertyIfExists("since");
    assertThat(property).isNotNull();
    assertThat(property.getType()).isEqualTo(Type.DATE);
  }
}
