package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import java.util.Collection;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Cypher index DDL statements (CREATE INDEX, DROP INDEX).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherIndexTest {
  private Database database;
  private String databasePath;

  @BeforeEach
  void setUp(TestInfo testInfo) {
    databasePath = "./target/databases/testopencypher-index-" + testInfo.getTestMethod().get().getName();
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.getSchema().createVertexType("Snapshot");
    database.getSchema().createEdgeType("FOLLOWS");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void createIndexSingleProperty() {
    database.command("opencypher", "CREATE INDEX FOR (sn:Snapshot) ON (sn.id)");

    final Collection<TypeIndex> indexes = database.getSchema().getType("Snapshot").getAllIndexes(false);
    assertThat(indexes).anyMatch(idx -> idx.getPropertyNames().contains("id"));
  }

  @Test
  void createIndexMultipleProperties() {
    database.command("opencypher", "CREATE INDEX FOR (sn:Snapshot) ON (sn.id, sn.event)");

    final Collection<TypeIndex> indexes = database.getSchema().getType("Snapshot").getAllIndexes(false);
    assertThat(indexes).anyMatch(idx -> idx.getPropertyNames().contains("id") && idx.getPropertyNames().contains("event"));
  }

  @Test
  void createIndexWithName() {
    database.command("opencypher", "CREATE INDEX snapshot_index FOR (sn:Snapshot) ON (sn.id)");

    final Collection<TypeIndex> indexes = database.getSchema().getType("Snapshot").getAllIndexes(false);
    assertThat(indexes).anyMatch(idx -> idx.getPropertyNames().contains("id"));
  }

  @Test
  void createIndexIfNotExists() {
    database.command("opencypher", "CREATE INDEX IF NOT EXISTS FOR (sn:Snapshot) ON (sn.id)");
    // Run again - should not throw
    database.command("opencypher", "CREATE INDEX IF NOT EXISTS FOR (sn:Snapshot) ON (sn.id)");

    final Collection<TypeIndex> indexes = database.getSchema().getType("Snapshot").getAllIndexes(false);
    assertThat(indexes).anyMatch(idx -> idx.getPropertyNames().contains("id"));
  }

  @Test
  void createIndexWithNameIfNotExists() {
    database.command("opencypher", "CREATE INDEX snapshot_index IF NOT EXISTS FOR (sn:Snapshot) ON (sn.id, sn.event)");
    // Run again - should not throw
    database.command("opencypher", "CREATE INDEX snapshot_index IF NOT EXISTS FOR (sn:Snapshot) ON (sn.id, sn.event)");

    final Collection<TypeIndex> indexes = database.getSchema().getType("Snapshot").getAllIndexes(false);
    assertThat(indexes).anyMatch(idx -> idx.getPropertyNames().contains("id") && idx.getPropertyNames().contains("event"));
  }

  @Test
  void createRangeIndex() {
    database.command("opencypher", "CREATE RANGE INDEX FOR (sn:Snapshot) ON (sn.id)");

    final Collection<TypeIndex> indexes = database.getSchema().getType("Snapshot").getAllIndexes(false);
    assertThat(indexes).anyMatch(idx -> idx.getPropertyNames().contains("id"));
  }

  @Test
  void createIndexForRelationship() {
    database.command("opencypher", "CREATE INDEX FOR ()-[r:FOLLOWS]-() ON (r.since)");

    final Collection<TypeIndex> indexes = database.getSchema().getType("FOLLOWS").getAllIndexes(false);
    assertThat(indexes).anyMatch(idx -> idx.getPropertyNames().contains("since"));
  }

  @Test
  void createIndexAutoCreatesVertexType() {
    assertThat(database.getSchema().existsType("NewType")).isFalse();

    database.command("opencypher", "CREATE INDEX FOR (n:NewType) ON (n.code)");

    assertThat(database.getSchema().existsType("NewType")).isTrue();
    assertThat(database.getSchema().getType("NewType")).isInstanceOf(VertexType.class);

    final Collection<TypeIndex> indexes = database.getSchema().getType("NewType").getAllIndexes(false);
    assertThat(indexes).anyMatch(idx -> idx.getPropertyNames().contains("code"));
  }

  @Test
  void createIndexAutoCreatesEdgeType() {
    assertThat(database.getSchema().existsType("LIKES")).isFalse();

    database.command("opencypher", "CREATE INDEX FOR ()-[r:LIKES]-() ON (r.weight)");

    assertThat(database.getSchema().existsType("LIKES")).isTrue();
    assertThat(database.getSchema().getType("LIKES")).isInstanceOf(EdgeType.class);

    final Collection<TypeIndex> indexes = database.getSchema().getType("LIKES").getAllIndexes(false);
    assertThat(indexes).anyMatch(idx -> idx.getPropertyNames().contains("weight"));
  }

  @Test
  void dropIndex() {
    // Create an index first
    database.getSchema().getType("Snapshot").createProperty("id", Type.STRING);
    database.getSchema().buildTypeIndex("Snapshot", new String[] { "id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .create();

    final Collection<TypeIndex> indexes = database.getSchema().getType("Snapshot").getAllIndexes(false);
    assertThat(indexes).isNotEmpty();
    final String indexName = indexes.iterator().next().getName();

    database.command("opencypher", "DROP INDEX `" + indexName + "`");

    assertThat(database.getSchema().existsIndex(indexName)).isFalse();
  }

  @Test
  void dropIndexIfExists() {
    // Should not throw even if index doesn't exist
    database.command("opencypher", "DROP INDEX nonExistentIndex IF EXISTS");
  }

  @Test
  void createIndexNotUnique() {
    // Indexes created via CREATE INDEX should not be unique (unlike constraints)
    database.command("opencypher", "CREATE INDEX FOR (sn:Snapshot) ON (sn.id)");

    final Collection<TypeIndex> indexes = database.getSchema().getType("Snapshot").getAllIndexes(false);
    assertThat(indexes).anyMatch(idx -> idx.getPropertyNames().contains("id") && !idx.isUnique());
  }

  // Issue #4222: integer property values must be preserved (not coerced to STRING) after CREATE INDEX inserts an implicit property declaration.
  @Test
  void integerPropertyValueIsPreservedAfterCreateIndex() {
    // PART 1 (control): no index. Integer round-trips correctly.
    database.command("opencypher", "MATCH (n) DETACH DELETE n");
    database.command("opencypher", "CREATE (a:Person {uuid: 1})");
    database.command("opencypher", "MATCH (n) DETACH DELETE n");
    database.command("opencypher", "CREATE (a:Person {uuid: 1})");

    assertIntegerLookupsAllReturnOne("Part 1 (no index)");

    // PART 2 (broken before the fix): same sequence, but a CREATE INDEX inserted in the middle.
    database.command("opencypher", "MATCH (n) DETACH DELETE n");
    database.command("opencypher", "CREATE (a:Person {uuid: 1})");
    database.command("opencypher", "CREATE INDEX IF NOT EXISTS FOR (n:Person) ON (n.uuid)");
    database.command("opencypher", "MATCH (n) DETACH DELETE n");
    database.command("opencypher", "CREATE (a:Person {uuid: 1})");

    assertIntegerLookupsAllReturnOne("Part 2 (with CREATE INDEX)");
  }

  // Issue #4222: CREATE INDEX on an empty type must pick a value type that does not coerce subsequent numeric inserts.
  @Test
  void integerPropertyValueIsPreservedWhenIndexIsCreatedOnEmptyType() {
    // No data when the index is created => fall back to a type that does not coerce numbers.
    database.command("opencypher", "CREATE INDEX IF NOT EXISTS FOR (n:Account) ON (n.id)");
    database.command("opencypher", "CREATE (a:Account {id: 42})");

    final ResultSet rs = database.query("opencypher", "MATCH (n:Account {id: 42}) RETURN n.id AS u");
    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    final Object value = r.getProperty("u");
    assertThat(value).isInstanceOf(Number.class);
    assertThat(((Number) value).longValue()).isEqualTo(42L);
  }

  private void assertIntegerLookupsAllReturnOne(final String description) {
    // 1. plain label match: returned uuid must still be a number (not "1").
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Person) RETURN n.uuid AS u")) {
      assertThat(rs.hasNext()).as(description + " - label match has row").isTrue();
      final Object value = rs.next().getProperty("u");
      assertThat(value).as(description + " - uuid retains numeric type").isInstanceOf(Number.class);
      assertThat(((Number) value).longValue()).as(description + " - uuid value preserved").isEqualTo(1L);
    }

    // 2. inline property filter: must find the node by integer literal.
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Person {uuid: 1}) RETURN n.uuid AS u")) {
      assertThat(rs.hasNext()).as(description + " - inline property filter finds node").isTrue();
      final Object value = rs.next().getProperty("u");
      assertThat(value).as(description + " - inline lookup uuid numeric").isInstanceOf(Number.class);
      assertThat(((Number) value).longValue()).isEqualTo(1L);
    }

    // 3. WHERE equality with integer literal: must find the node.
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Person) WHERE n.uuid = 1 RETURN n.uuid AS u")) {
      assertThat(rs.hasNext()).as(description + " - WHERE filter finds node").isTrue();
      final Object value = rs.next().getProperty("u");
      assertThat(value).as(description + " - WHERE lookup uuid numeric").isInstanceOf(Number.class);
      assertThat(((Number) value).longValue()).isEqualTo(1L);
    }
  }
}
