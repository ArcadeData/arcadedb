package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for CREATE INDEX and REBUILD INDEX syntax fixes.
 */
class IndexSyntaxTest extends TestHelper {

  @Test
  void createIndexIfNotExists() {
    database.command("sql", "CREATE DOCUMENT TYPE CompactTest");
    database.command("sql", "CREATE PROPERTY CompactTest.vec ARRAY_OF_FLOATS");

    // Test IF NOT EXISTS with LSM_VECTOR and METADATA
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON CompactTest (vec) LSM_VECTOR METADATA {dimensions: 4, similarity: 'COSINE'}");

    var schema = database.getSchema();
    var indexes = schema.getIndexes();
    assertThat(indexes.length > 0).isTrue();
  }

  @Test
  void createIndexByItem() {
    database.command("sql", "CREATE DOCUMENT TYPE SimpleListDoc");
    database.command("sql", "CREATE PROPERTY SimpleListDoc.tags LIST OF STRING");

    // Test BY ITEM syntax
    database.command("sql", "CREATE INDEX ON SimpleListDoc (tags BY ITEM) NOTUNIQUE");

    var schema = database.getSchema();
    var indexes = schema.getIndexes();
    assertThat(indexes.length > 0).isTrue();
  }

  @Test
  void rebuildIndexWithBacktickName() {
    database.command("sql", "CREATE DOCUMENT TYPE Embedding");
    database.command("sql", "CREATE PROPERTY Embedding.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Embedding (vector) LSM_VECTOR METADATA {dimensions: 4}");

    // Test REBUILD INDEX with backtick-quoted identifier
    database.command("sql", "REBUILD INDEX `Embedding[vector]`");

    var schema = database.getSchema();
    var index = schema.getIndexByName("Embedding[vector]");
    assertThat(index).isNotNull();
  }

  @Test
  void rebuildAllIndexes() {
    database.command("sql", "CREATE DOCUMENT TYPE TestType");
    database.command("sql", "CREATE PROPERTY TestType.name STRING");
    database.command("sql", "CREATE INDEX ON TestType (name) NOTUNIQUE");

    // Test REBUILD INDEX * syntax
    database.command("sql", "REBUILD INDEX *");

    var schema = database.getSchema();
    assertThat(schema.getIndexes().length > 0).isTrue();
  }
}
