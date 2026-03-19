package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Collection;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

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
}
