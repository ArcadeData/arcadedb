package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #1315: SQL: JSON Array support for UPDATE
 *
 * This test demonstrates the exact use case from the GitHub issue:
 * "Currently INSERT and CREATE VERTEX support an array of JSON documents as CONTENT.
 *  It would be useful to have UPDATE also support arrays of JSON documents."
 *
 * Example from the issue:
 * CREATE DOCUMENT TYPE doc;
 * UPDATE doc CONTENT [{"name":"tim"},{"name":"tom"},{"name":"jim"}] UPSERT
 */
public class GitHubIssue1315Test extends TestHelper {

  public GitHubIssue1315Test() {
    autoStartTx = true;
  }

  @Test
  public void testOriginalIssueExample() {
    // Reproduce the example from GitHub issue #1315
    // The issue requests: UPDATE doc CONTENT [{"name":"tim"},{"name":"tom"},{"name":"jim"}] UPSERT
    //
    // The most practical use case is to first create documents with INSERT (which already supports arrays),
    // then use UPDATE with arrays to modify existing documents.

    database.command("sql", "CREATE DOCUMENT TYPE doc");

    // First, create 3 documents using INSERT (which already supported arrays)
    database.command("sql",
        "INSERT INTO doc CONTENT [{\"name\":\"tim\",\"status\":\"new\"},{\"name\":\"tom\",\"status\":\"new\"},{\"name\":\"jim\",\"status\":\"new\"}]");

    // Verify 3 documents were created
    ResultSet queryResult = database.query("sql", "SELECT COUNT(*) as count FROM doc");
    assertThat(queryResult.next().<Long>getProperty("count")).isEqualTo(3L);

    // Now use UPDATE with JSON array to modify all 3 documents
    // This is the NEW feature being implemented for issue #1315
    final ResultSet result = database.command("sql",
        "UPDATE doc CONTENT [{\"name\":\"tim\",\"status\":\"updated\"},{\"name\":\"tom\",\"status\":\"updated\"},{\"name\":\"jim\",\"status\":\"updated\"}] RETURN AFTER");

    // Verify 3 documents were updated
    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isIn("tim", "tom", "jim");
      assertThat(item.<String>getProperty("status")).isEqualTo("updated");
      count++;
    }

    assertThat(count).isEqualTo(3);

    // Verify all documents have updated status
    queryResult = database.query("sql", "SELECT FROM doc WHERE status = 'updated'");
    count = 0;
    while (queryResult.hasNext()) {
      queryResult.next();
      count++;
    }
    assertThat(count).isEqualTo(3);
  }

  @Test
  public void testUpdateMatchesInsertBehavior() {
    // Demonstrate that UPDATE now behaves like INSERT with array CONTENT
    database.command("sql", "CREATE DOCUMENT TYPE Person");

    // INSERT with JSON array (this already worked)
    database.command("sql",
        "INSERT INTO Person CONTENT [{\"name\":\"alice\",\"age\":25},{\"name\":\"bob\",\"age\":30}]");

    // Verify INSERT created 2 documents
    ResultSet queryResult = database.query("sql", "SELECT COUNT(*) as count FROM Person");
    assertThat(queryResult.next().<Long>getProperty("count")).isEqualTo(2L);

    // UPDATE with JSON array (this is the new feature)
    database.command("sql",
        "UPDATE Person CONTENT [{\"name\":\"alice\",\"age\":26},{\"name\":\"bob\",\"age\":31}]");

    // Verify UPDATE modified the existing 2 documents
    queryResult = database.query("sql", "SELECT FROM Person WHERE name = 'alice'");
    assertThat(queryResult.hasNext()).isTrue();
    final Result alice = queryResult.next();
    assertThat(alice.<Integer>getProperty("age")).isEqualTo(26);

    queryResult = database.query("sql", "SELECT FROM Person WHERE name = 'bob'");
    assertThat(queryResult.hasNext()).isTrue();
    final Result bob = queryResult.next();
    assertThat(bob.<Integer>getProperty("age")).isEqualTo(31);
  }

  @Test
  public void testCreateVertexAlsoSupportsArrays() {
    // Verify that CREATE VERTEX also supports arrays (as mentioned in the issue)
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

    // Verify all 3 vertices were created
    final ResultSet queryResult = database.query("sql", "SELECT COUNT(*) as count FROM Person");
    assertThat(queryResult.next().<Long>getProperty("count")).isEqualTo(3L);
  }
}
