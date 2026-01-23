package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for CREATE INDEX and REBUILD INDEX syntax fixes.
 */
public class IndexSyntaxTest extends TestHelper {

  @Test
  public void testCreateIndexIfNotExists() {
    database.command("sql", "CREATE DOCUMENT TYPE CompactTest");
    database.command("sql", "CREATE PROPERTY CompactTest.vec ARRAY_OF_FLOATS");

    // Test IF NOT EXISTS with LSM_VECTOR and METADATA
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON CompactTest (vec) LSM_VECTOR METADATA {dimensions: 4, similarity: 'COSINE'}");

    var schema = database.getSchema();
    var indexes = schema.getIndexes();
    Assertions.assertTrue(indexes.length > 0);
  }

  @Test
  public void testCreateIndexByItem() {
    database.command("sql", "CREATE DOCUMENT TYPE SimpleListDoc");
    database.command("sql", "CREATE PROPERTY SimpleListDoc.tags LIST OF STRING");

    // Test BY ITEM syntax
    database.command("sql", "CREATE INDEX ON SimpleListDoc (tags BY ITEM) NOTUNIQUE");

    var schema = database.getSchema();
    var indexes = schema.getIndexes();
    Assertions.assertTrue(indexes.length > 0);
  }

  @Test
  public void testRebuildIndexWithBacktickName() {
    database.command("sql", "CREATE DOCUMENT TYPE Embedding");
    database.command("sql", "CREATE PROPERTY Embedding.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Embedding (vector) LSM_VECTOR METADATA {dimensions: 4}");

    // Test REBUILD INDEX with backtick-quoted identifier
    database.command("sql", "REBUILD INDEX `Embedding[vector]`");

    var schema = database.getSchema();
    var index = schema.getIndexByName("Embedding[vector]");
    Assertions.assertNotNull(index);
  }

  @Test
  public void testRebuildAllIndexes() {
    database.command("sql", "CREATE DOCUMENT TYPE TestType");
    database.command("sql", "CREATE PROPERTY TestType.name STRING");
    database.command("sql", "CREATE INDEX ON TestType (name) NOTUNIQUE");

    // Test REBUILD INDEX * syntax
    database.command("sql", "REBUILD INDEX *");

    var schema = database.getSchema();
    Assertions.assertTrue(schema.getIndexes().length > 0);
  }
}
