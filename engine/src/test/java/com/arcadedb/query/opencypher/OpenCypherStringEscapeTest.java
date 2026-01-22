package com.arcadedb.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for string escape sequences in OpenCypher queries.
 * Issue #1649: Manually escape $ for cypher when there is a new line in this same string
 * https://github.com/ArcadeData/arcadedb/issues/1649
 */
public class OpenCypherStringEscapeTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-stringescape").create();
    database.getSchema().createVertexType("CHUNK");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Test: Dollar sign without newline - This should work according to the issue report.
   */
  @Test
  void testDollarSignWithoutNewline() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (n:CHUNK {name: 'document DrSamChepard.pdf, chunk 5', subtype: 'CHUNK', text: '$50,000 bail,'}) RETURN ID(n)");

      assertThat(result.hasNext()).isTrue();
      result.next();
    });

    // Verify the created vertex
    final ResultSet verify = database.query("opencypher", "MATCH (n:CHUNK) RETURN n.text AS text");
    assertThat(verify.hasNext()).isTrue();
    final Result r = verify.next();
    assertThat((String) r.getProperty("text")).isEqualTo("$50,000 bail,");
  }

  /**
   * Test: Newline without dollar sign - This should work according to the issue report.
   */
  @Test
  void testNewlineWithoutDollarSign() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (n:CHUNK {name: 'document DrSamChepard.pdf, chunk 5', subtype: 'CHUNK', text: '50,000 bail, \\nlol new line'}) RETURN ID(n)");

      assertThat(result.hasNext()).isTrue();
      result.next();
    });

    // Verify the created vertex - the text should contain an actual newline
    final ResultSet verify = database.query("opencypher", "MATCH (n:CHUNK) RETURN n.text AS text");
    assertThat(verify.hasNext()).isTrue();
    final Result r = verify.next();
    final String text = r.getProperty("text");
    assertThat(text).contains("\n");
    assertThat(text).isEqualTo("50,000 bail, \nlol new line");
  }

  /**
   * Test: Dollar sign AND newline in the same string - This fails according to the issue report.
   * This is the core test for Issue #1649.
   */
  @Test
  void testDollarSignWithNewline() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (n:CHUNK {name: 'document DrSamChepard.pdf, chunk 5', subtype: 'CHUNK', text: '$50,000 bail, \\nlol new line'}) RETURN ID(n)");

      assertThat(result.hasNext()).isTrue();
      result.next();
    });

    // Verify the created vertex
    final ResultSet verify = database.query("opencypher", "MATCH (n:CHUNK) RETURN n.text AS text");
    assertThat(verify.hasNext()).isTrue();
    final Result r = verify.next();
    final String text = r.getProperty("text");
    assertThat(text).isEqualTo("$50,000 bail, \nlol new line");
  }

  /**
   * Additional test: Various escape sequences.
   */
  @Test
  void testVariousEscapeSequences() {
    database.transaction(() -> {
      // Test tab escape
      database.command("opencypher", "CREATE (n:CHUNK {text: 'hello\\tworld'}) RETURN n");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:CHUNK) RETURN n.text AS text");
    assertThat(verify.hasNext()).isTrue();
    final Result r = verify.next();
    final String text = r.getProperty("text");
    assertThat(text).isEqualTo("hello\tworld");
  }

  /**
   * Test: Escaped single quote.
   */
  @Test
  void testEscapedSingleQuote() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:CHUNK {text: 'it\\'s working'}) RETURN n");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:CHUNK) RETURN n.text AS text");
    assertThat(verify.hasNext()).isTrue();
    final Result r = verify.next();
    final String text = r.getProperty("text");
    assertThat(text).isEqualTo("it's working");
  }

  /**
   * Test: Escaped backslash.
   */
  @Test
  void testEscapedBackslash() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:CHUNK {text: 'path\\\\to\\\\file'}) RETURN n");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:CHUNK) RETURN n.text AS text");
    assertThat(verify.hasNext()).isTrue();
    final Result r = verify.next();
    final String text = r.getProperty("text");
    assertThat(text).isEqualTo("path\\to\\file");
  }

  /**
   * Test: Carriage return escape.
   */
  @Test
  void testCarriageReturnEscape() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:CHUNK {text: 'line1\\rline2'}) RETURN n");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:CHUNK) RETURN n.text AS text");
    assertThat(verify.hasNext()).isTrue();
    final Result r = verify.next();
    final String text = r.getProperty("text");
    assertThat(text).isEqualTo("line1\rline2");
  }

  /**
   * Test: Double-quoted strings with escape sequences.
   */
  @Test
  void testDoubleQuotedStringWithEscapes() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:CHUNK {text: \"hello\\nworld\"}) RETURN n");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:CHUNK) RETURN n.text AS text");
    assertThat(verify.hasNext()).isTrue();
    final Result r = verify.next();
    final String text = r.getProperty("text");
    assertThat(text).isEqualTo("hello\nworld");
  }
}
