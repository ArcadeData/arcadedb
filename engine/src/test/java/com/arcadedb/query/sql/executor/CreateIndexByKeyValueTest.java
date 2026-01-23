package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.parser.CreateIndexStatement;
import com.arcadedb.query.sql.parser.StatementCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test CREATE INDEX with BY KEY and BY VALUE syntax parsing.
 * These tests verify that the parser correctly handles the BY KEY/VALUE syntax.
 */
public class CreateIndexByKeyValueTest {
  private Database database;
  private StatementCache cache;

  @BeforeEach
  public void setup() {
    database = new DatabaseFactory("./target/test-databases/CreateIndexByKeyValueTest").create();
    cache = new StatementCache(database, 100);
  }

  @AfterEach
  public void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
    TestHelper.checkActiveDatabases();
  }

  @Test
  public void testParseIndexByKey() {
    // Test that BY KEY syntax is parsed correctly
    String sql = "CREATE INDEX idx_tags_key ON V (tags BY KEY) NOTUNIQUE";

    CreateIndexStatement stmt = (CreateIndexStatement) cache.get(sql);
    assertThat(stmt.propertyList).hasSize(1);
    assertThat(stmt.propertyList.get(0).name.getStringValue()).isEqualTo("tags");
    assertThat(stmt.propertyList.get(0).byKey).isTrue();
    assertThat(stmt.propertyList.get(0).byValue).isFalse();
  }

  @Test
  public void testParseIndexByValue() {
    // Test that BY VALUE syntax is parsed correctly
    String sql = "CREATE INDEX idx_data_value ON V (data BY VALUE) NOTUNIQUE";

    CreateIndexStatement stmt = (CreateIndexStatement) cache.get(sql);
    assertThat(stmt.propertyList).hasSize(1);
    assertThat(stmt.propertyList.get(0).name.getStringValue()).isEqualTo("data");
    assertThat(stmt.propertyList.get(0).byKey).isFalse();
    assertThat(stmt.propertyList.get(0).byValue).isTrue();
  }

  @Test
  public void testParseIndexMultiplePropertiesWithBy() {
    // Test multiple properties with different BY modifiers
    String sql = "CREATE INDEX idx_multi ON V (tags BY KEY, data BY VALUE) NOTUNIQUE";

    CreateIndexStatement stmt = (CreateIndexStatement) cache.get(sql);
    assertThat(stmt.propertyList).hasSize(2);

    // First property: tags BY KEY
    assertThat(stmt.propertyList.get(0).name.getStringValue()).isEqualTo("tags");
    assertThat(stmt.propertyList.get(0).byKey).isTrue();
    assertThat(stmt.propertyList.get(0).byValue).isFalse();

    // Second property: data BY VALUE
    assertThat(stmt.propertyList.get(1).name.getStringValue()).isEqualTo("data");
    assertThat(stmt.propertyList.get(1).byKey).isFalse();
    assertThat(stmt.propertyList.get(1).byValue).isTrue();
  }

  @Test
  public void testParseIndexWithoutBy() {
    // Test that regular properties without BY still work
    String sql = "CREATE INDEX idx_name ON V (name) NOTUNIQUE";

    CreateIndexStatement stmt = (CreateIndexStatement) cache.get(sql);
    assertThat(stmt.propertyList).hasSize(1);
    assertThat(stmt.propertyList.get(0).name.getStringValue()).isEqualTo("name");
    assertThat(stmt.propertyList.get(0).byKey).isFalse();
    assertThat(stmt.propertyList.get(0).byValue).isFalse();
  }

  @Test
  public void testGetCompleteKeyWithByKey() {
    // Test that getCompleteKey() returns the correct field name with " by key"
    String sql = "CREATE INDEX idx ON V (field BY KEY) NOTUNIQUE";

    CreateIndexStatement stmt = (CreateIndexStatement) cache.get(sql);
    String completeKey = stmt.propertyList.get(0).getCompleteKey();
    assertThat(completeKey).isEqualTo("field by key");
  }

  @Test
  public void testGetCompleteKeyWithByValue() {
    // Test that getCompleteKey() returns the correct field name with " by value"
    String sql = "CREATE INDEX idx ON V (field BY VALUE) NOTUNIQUE";

    CreateIndexStatement stmt = (CreateIndexStatement) cache.get(sql);
    String completeKey = stmt.propertyList.get(0).getCompleteKey();
    assertThat(completeKey).isEqualTo("field by value");
  }
}
