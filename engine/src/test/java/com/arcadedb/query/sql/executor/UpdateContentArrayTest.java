package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #1315: SQL: JSON Array support for UPDATE
 * Tests that UPDATE statement supports arrays of JSON documents with CONTENT clause,
 * similar to INSERT and CREATE VERTEX statements.
 */
public class UpdateContentArrayTest extends TestHelper {

  public UpdateContentArrayTest() {
    autoStartTx = true;
  }

  @Test
  void insertVsUpdateWithJsonArrayComparison() {
    // This test shows that INSERT already supports JSON arrays
    // and validates that UPDATE should work the same way
    database.getSchema().createDocumentType("TestDoc");

    // INSERT with JSON array - this already works
    final ResultSet insertResult = database.command("sql",
        "INSERT INTO TestDoc CONTENT [{\"type\":\"insert\",\"value\":1},{\"type\":\"insert\",\"value\":2}]");

    int insertCount = 0;
    while (insertResult.hasNext()) {
      final Result item = insertResult.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("type")).isEqualTo("insert");
      insertCount++;
    }
    assertThat(insertCount).isEqualTo(2);

    // Verify 2 documents were created
    final ResultSet queryResult = database.query("sql", "SELECT FROM TestDoc");
    long totalCount = 0;
    while (queryResult.hasNext()) {
      queryResult.next();
      totalCount++;
    }
    assertThat(totalCount).isEqualTo(2);
  }

  @Test
  void updateWithContentJsonArrayWithoutUpsert() {
    // Create document type and some documents
    database.getSchema().createDocumentType("Person");

    database.transaction(() -> {
      final MutableDocument doc1 = database.newDocument("Person");
      doc1.set("name", "alice");
      doc1.set("age", 25);
      doc1.save();

      final MutableDocument doc2 = database.newDocument("Person");
      doc2.set("name", "bob");
      doc2.set("age", 30);
      doc2.save();

      final MutableDocument doc3 = database.newDocument("Person");
      doc3.set("name", "charlie");
      doc3.set("age", 35);
      doc3.save();
    });

    // Update existing documents with JSON array content
    final ResultSet result = database.command("sql",
        "UPDATE Person CONTENT [{\"name\":\"alice\",\"age\":26,\"city\":\"NYC\"},{\"name\":\"bob\",\"age\":31,\"city\":\"LA\"},{\"name\":\"charlie\",\"age\":36,\"city\":\"SF\"}] RETURN AFTER");

    // Should update 3 documents
    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isIn("alice", "bob", "charlie");
      assertThat(item.<String>getProperty("city")).isIn("NYC", "LA", "SF");
      count++;
    }

    assertThat(count).isEqualTo(3);

    // Verify updates
    final ResultSet verifyResult = database.query("sql", "SELECT FROM Person WHERE name = 'alice'");
    assertThat(verifyResult.hasNext()).isTrue();
    final Result alice = verifyResult.next();
    assertThat(alice.<Integer>getProperty("age")).isEqualTo(26);
    assertThat(alice.<String>getProperty("city")).isEqualTo("NYC");
  }

  @Test
  void updateWithContentJsonArrayWithWhere() {
    // Create document type and some documents
    database.getSchema().createDocumentType("Product");

    database.transaction(() -> {
      for (int i = 1; i <= 5; i++) {
        final MutableDocument doc = database.newDocument("Product");
        doc.set("id", i);
        doc.set("name", "product" + i);
        doc.set("price", i * 10.0);
        doc.save();
      }
    });

    // Update only products with id > 3 using JSON array
    final ResultSet result = database.command("sql",
        "UPDATE Product CONTENT [{\"id\":4,\"name\":\"updated4\",\"price\":100},{\"id\":5,\"name\":\"updated5\",\"price\":200}] RETURN AFTER WHERE id > 3");

    // Should update 2 documents
    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isIn("updated4", "updated5");
      count++;
    }

    assertThat(count).isEqualTo(2);

    // Verify that products 1-3 were not modified
    final ResultSet unchanged = database.query("sql", "SELECT FROM Product WHERE id <= 3 ORDER BY id");
    assertThat(unchanged.hasNext()).isTrue();
    assertThat(unchanged.next().<String>getProperty("name")).isEqualTo("product1");
    assertThat(unchanged.next().<String>getProperty("name")).isEqualTo("product2");
    assertThat(unchanged.next().<String>getProperty("name")).isEqualTo("product3");
  }

  @Test
  void updateWithContentJsonArrayParameter() {
    // Create document type and existing documents
    database.getSchema().createDocumentType("Employee");

    database.transaction(() -> {
      final MutableDocument doc1 = database.newDocument("Employee");
      doc1.set("name", "john");
      doc1.set("dept", "Sales");
      doc1.save();

      final MutableDocument doc2 = database.newDocument("Employee");
      doc2.set("name", "jane");
      doc2.set("dept", "Marketing");
      doc2.save();
    });

    // Test UPDATE with JSON array as parameter
    final Map<String, Object> params = new HashMap<>();
    final List<Map<String, Object>> contentArray = new ArrayList<>();

    final Map<String, Object> employee1 = new HashMap<>();
    employee1.put("name", "john");
    employee1.put("dept", "IT");
    contentArray.add(employee1);

    final Map<String, Object> employee2 = new HashMap<>();
    employee2.put("name", "jane");
    employee2.put("dept", "HR");
    contentArray.add(employee2);

    params.put("content", contentArray);

    final ResultSet result = database.command("sql",
        "UPDATE Employee CONTENT :content RETURN AFTER", params);

    // Should update 2 documents
    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isIn("john", "jane");
      assertThat(item.<String>getProperty("dept")).isIn("IT", "HR");
      count++;
    }

    assertThat(count).isEqualTo(2);
  }

  @Test
  void updateExistingDocumentsWithArray() {
    // Create document type
    database.getSchema().createDocumentType("TestDoc");

    // First, create some documents using INSERT
    database.command("sql",
        "INSERT INTO TestDoc CONTENT [{\"type\":\"original\",\"value\":1},{\"type\":\"original\",\"value\":2}]");

    // Verify we have 2 documents
    ResultSet queryResult = database.query("sql", "SELECT FROM TestDoc");
    long beforeCount = 0;
    while (queryResult.hasNext()) {
      queryResult.next();
      beforeCount++;
    }
    assertThat(beforeCount).isEqualTo(2);

    // Now UPDATE those documents with JSON array content
    final ResultSet updateResult = database.command("sql",
        "UPDATE TestDoc CONTENT [{\"type\":\"updated\",\"value\":3},{\"type\":\"updated\",\"value\":4}] RETURN AFTER");

    int updateCount = 0;
    while (updateResult.hasNext()) {
      final Result item = updateResult.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("type")).isEqualTo("updated");
      assertThat(item.<Integer>getProperty("value")).isIn(3, 4);
      updateCount++;
    }
    assertThat(updateCount).isEqualTo(2);

    // Verify still 2 documents (updated, not inserted new ones)
    final ResultSet afterQuery = database.query("sql", "SELECT FROM TestDoc");
    long afterCount = 0;
    while (afterQuery.hasNext()) {
      afterQuery.next();
      afterCount++;
    }
    assertThat(afterCount).isEqualTo(2);
  }

  // Issue #1315: UPDATE CONTENT with JSON array of documents updates all records and returns each after RETURN AFTER
  @Test
  void originalIssueExample() {
    database.command("sql", "CREATE DOCUMENT TYPE doc");

    // First, create 3 documents using INSERT (which already supported arrays)
    database.command("sql",
        "INSERT INTO doc CONTENT [{\"name\":\"tim\",\"status\":\"new\"},{\"name\":\"tom\",\"status\":\"new\"},{\"name\":\"jim\",\"status\":\"new\"}]");

    // Verify 3 documents were created
    ResultSet queryResult = database.query("sql", "SELECT COUNT(*) as count FROM doc");
    assertThat(queryResult.next().<Long>getProperty("count")).isEqualTo(3L);

    // Now use UPDATE with JSON array to modify all 3 documents
    final ResultSet result = database.command("sql",
        "UPDATE doc CONTENT [{\"name\":\"tim\",\"status\":\"updated\"},{\"name\":\"tom\",\"status\":\"updated\"},{\"name\":\"jim\",\"status\":\"updated\"}] RETURN AFTER");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isIn("tim", "tom", "jim");
      assertThat(item.<String>getProperty("status")).isEqualTo("updated");
      count++;
    }

    assertThat(count).isEqualTo(3);

    queryResult = database.query("sql", "SELECT FROM doc WHERE status = 'updated'");
    count = 0;
    while (queryResult.hasNext()) {
      queryResult.next();
      count++;
    }
    assertThat(count).isEqualTo(3);
  }

  // Issue #1315: UPDATE CONTENT with JSON array matches INSERT array behavior on existing records
  @Test
  void updateMatchesInsertBehavior() {
    database.command("sql", "CREATE DOCUMENT TYPE Person");

    // INSERT with JSON array (this already worked)
    database.command("sql",
        "INSERT INTO Person CONTENT [{\"name\":\"alice\",\"age\":25},{\"name\":\"bob\",\"age\":30}]");

    // Verify INSERT created 2 documents
    ResultSet queryResult = database.query("sql", "SELECT COUNT(*) as count FROM Person");
    assertThat(queryResult.next().<Long>getProperty("count")).isEqualTo(2L);

    // UPDATE with JSON array
    database.command("sql",
        "UPDATE Person CONTENT [{\"name\":\"alice\",\"age\":26},{\"name\":\"bob\",\"age\":31}]");

    queryResult = database.query("sql", "SELECT FROM Person WHERE name = 'alice'");
    assertThat(queryResult.hasNext()).isTrue();
    final Result alice = queryResult.next();
    assertThat(alice.<Integer>getProperty("age")).isEqualTo(26);

    queryResult = database.query("sql", "SELECT FROM Person WHERE name = 'bob'");
    assertThat(queryResult.hasNext()).isTrue();
    final Result bob = queryResult.next();
    assertThat(bob.<Integer>getProperty("age")).isEqualTo(31);
  }

  // Issue #1315: CREATE VERTEX CONTENT also accepts a JSON array creating one vertex per element
  @Test
  void createVertexAlsoSupportsArrays() {
    database.command("sql", "CREATE VERTEX TYPE Person");

    final ResultSet result = database.command("sql",
        "CREATE VERTEX Person CONTENT [{\"name\":\"alice\"},{\"name\":\"bob\"},{\"name\":\"charlie\"}]");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item).isNotNull();
      count++;
    }

    assertThat(count).isEqualTo(3);

    final ResultSet queryResult = database.query("sql", "SELECT COUNT(*) as count FROM Person");
    assertThat(queryResult.next().<Long>getProperty("count")).isEqualTo(3L);
  }
}
