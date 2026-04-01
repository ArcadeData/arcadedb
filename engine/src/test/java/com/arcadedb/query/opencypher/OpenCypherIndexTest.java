package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.TypeIndex;
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
}
