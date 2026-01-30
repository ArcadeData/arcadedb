package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.parser.CheckDatabaseStatement;
import com.arcadedb.query.sql.parser.StatementCache;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test CHECK DATABASE extended syntax (FIX, COMPRESS, TYPE, BUCKET).
 */
class CheckDatabaseExtendedTest {
  private Database database;
  private StatementCache cache;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/test-databases/CheckDatabaseExtendedTest").create();
    cache = new StatementCache(database, 100);

    // Create some types for testing
    database.getSchema().createDocumentType("Customer");
    database.getSchema().createDocumentType("Order");
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
    TestHelper.checkActiveDatabases();
  }

  @Test
  void checkDatabaseBasic() {
    // Test basic CHECK DATABASE without any options
    String sql = "CHECK DATABASE";

    CheckDatabaseStatement stmt = (CheckDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.types).isEmpty();
    assertThat(stmt.buckets).isEmpty();
    assertThat(stmt.fix).isFalse();
    assertThat(stmt.compress).isFalse();
  }

  @Test
  void checkDatabaseWithFix() {
    // Test CHECK DATABASE with FIX flag
    String sql = "CHECK DATABASE FIX";

    CheckDatabaseStatement stmt = (CheckDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.fix).isTrue();
    assertThat(stmt.compress).isFalse();
  }

  @Test
  void checkDatabaseWithCompress() {
    // Test CHECK DATABASE with COMPRESS flag
    String sql = "CHECK DATABASE COMPRESS";

    CheckDatabaseStatement stmt = (CheckDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.fix).isFalse();
    assertThat(stmt.compress).isTrue();
  }

  @Test
  void checkDatabaseWithFixAndCompress() {
    // Test CHECK DATABASE with both FIX and COMPRESS
    String sql = "CHECK DATABASE FIX COMPRESS";

    CheckDatabaseStatement stmt = (CheckDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.fix).isTrue();
    assertThat(stmt.compress).isTrue();
  }

  @Test
  void checkDatabaseWithType() {
    // Test CHECK DATABASE TYPE with single type
    String sql = "CHECK DATABASE TYPE Customer";

    CheckDatabaseStatement stmt = (CheckDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.types).hasSize(1);
    assertThat(stmt.types.iterator().next().getStringValue()).isEqualTo("Customer");
  }

  @Test
  void checkDatabaseWithMultipleTypes() {
    // Test CHECK DATABASE TYPE with multiple types
    String sql = "CHECK DATABASE TYPE Customer, Order";

    CheckDatabaseStatement stmt = (CheckDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.types).hasSize(2);
  }

  @Test
  void checkDatabaseWithBucketNumber() {
    // Test CHECK DATABASE BUCKET with numeric bucket
    String sql = "CHECK DATABASE BUCKET 3";

    CheckDatabaseStatement stmt = (CheckDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.buckets).hasSize(1);
    // getValue() returns PInteger for numeric buckets, so check bucketId field
    assertThat(stmt.buckets.iterator().next().bucketId).isNotNull();
    assertThat(stmt.buckets.iterator().next().bucketId.getValue()).isEqualTo(3);
  }

  @Test
  void checkDatabaseWithBucketName() {
    // Test CHECK DATABASE BUCKET with bucket name
    String sql = "CHECK DATABASE BUCKET Customer";

    CheckDatabaseStatement stmt = (CheckDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.buckets).hasSize(1);
    assertThat(stmt.buckets.iterator().next().getValue()).isEqualTo("Customer");
  }

  @Test
  void checkDatabaseWithTypeAndFix() {
    // Test CHECK DATABASE TYPE with FIX flag
    String sql = "CHECK DATABASE TYPE Customer FIX";

    CheckDatabaseStatement stmt = (CheckDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.types).hasSize(1);
    assertThat(stmt.fix).isTrue();
  }

  @Test
  void checkDatabaseWithBucketAndFix() {
    // Test CHECK DATABASE BUCKET with FIX flag
    String sql = "CHECK DATABASE BUCKET 3 FIX";

    CheckDatabaseStatement stmt = (CheckDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.buckets).hasSize(1);
    assertThat(stmt.fix).isTrue();
  }

  @Test
  void checkDatabaseFullSyntax() {
    // Test all options together
    String sql = "CHECK DATABASE TYPE Customer BUCKET 3 FIX COMPRESS";

    CheckDatabaseStatement stmt = (CheckDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.types).hasSize(1);
    assertThat(stmt.buckets).hasSize(1);
    assertThat(stmt.fix).isTrue();
    assertThat(stmt.compress).isTrue();
  }

  @Test
  void checkDatabaseCaseInsensitive() {
    // Test case insensitivity
    String sql = "check database type Customer fix compress";

    CheckDatabaseStatement stmt = (CheckDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.types).hasSize(1);
    assertThat(stmt.fix).isTrue();
    assertThat(stmt.compress).isTrue();
  }
}
