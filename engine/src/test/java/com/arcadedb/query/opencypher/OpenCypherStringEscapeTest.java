package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

;

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
  void dollarSignWithoutNewline() {
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
  void newlineWithoutDollarSign() {
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
  void dollarSignWithNewline() {
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
  void variousEscapeSequences() {
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
  void escapedSingleQuote() {
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
  void escapedBackslash() {
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
  void carriageReturnEscape() {
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
  void doubleQuotedStringWithEscapes() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:CHUNK {text: \"hello\\nworld\"}) RETURN n");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:CHUNK) RETURN n.text AS text");
    assertThat(verify.hasNext()).isTrue();
    final Result r = verify.next();
    final String text = r.getProperty("text");
    assertThat(text).isEqualTo("hello\nworld");
  }

  /** See issue #3333 */
  @Nested
  class StringMatchingInReturnRegression {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/test-issue3333").create();
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void stringMatchingInReturn() {
      // Exact scenario from issue #3333
      try (final ResultSet rs = database.query("opencypher",
          """
          WITH 'Hello World' AS txt \
          RETURN txt STARTS WITH 'He' AS a, \
          txt CONTAINS 'lo' AS b, \
          txt ENDS WITH 'rld' AS c""")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<Boolean>getProperty("a")).isTrue();
        assertThat(row.<Boolean>getProperty("b")).isTrue();
        assertThat(row.<Boolean>getProperty("c")).isTrue();
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void stringMatchingInReturnFalse() {
      // Test that false results are returned correctly too
      try (final ResultSet rs = database.query("opencypher",
          """
          WITH 'Hello World' AS txt \
          RETURN txt STARTS WITH 'Xyz' AS a, \
          txt CONTAINS 'xyz' AS b, \
          txt ENDS WITH 'xyz' AS c""")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<Boolean>getProperty("a")).isFalse();
        assertThat(row.<Boolean>getProperty("b")).isFalse();
        assertThat(row.<Boolean>getProperty("c")).isFalse();
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void stringMatchingWithPropertyInReturn() {
      // Test with node property access
      database.getSchema().createVertexType("Person");
      database.transaction(() -> {
        database.command("opencypher", "CREATE (:Person {name: 'Alice Johnson'})");
      });

      try (final ResultSet rs = database.query("opencypher",
          """
          MATCH (p:Person) \
          RETURN p.name STARTS WITH 'Ali' AS startsWithAli, \
          p.name CONTAINS 'John' AS containsJohn, \
          p.name ENDS WITH 'son' AS endsWithSon""")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<Boolean>getProperty("startsWithAli")).isTrue();
        assertThat(row.<Boolean>getProperty("containsJohn")).isTrue();
        assertThat(row.<Boolean>getProperty("endsWithSon")).isTrue();
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void regexInReturn() {
      // Test that regex (=~) also works in RETURN
      try (final ResultSet rs = database.query("opencypher",
          """
          WITH 'Hello World' AS txt \
          RETURN txt =~ 'Hello.*' AS matchesRegex""")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<Boolean>getProperty("matchesRegex")).isTrue();
        assertThat(rs.hasNext()).isFalse();
      }
    }
  }
}
