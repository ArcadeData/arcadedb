package com.arcadedb.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.opencypher.ast.CypherStatement;
import com.arcadedb.opencypher.ast.MatchClause;
import com.arcadedb.opencypher.parser.AntlrCypherParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Debug test to check if pattern parsing is working.
 */
public class ParserDebugTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/parser-debug-test").create();
    database.getSchema().createVertexType("Person");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testPatternParsing() {
    final AntlrCypherParser parser = new AntlrCypherParser((DatabaseInternal) database);
    final String query = "MATCH (n:Person) RETURN n";

    System.out.println("=== Parsing query: " + query);
    final CypherStatement stmt = parser.parse(query);

    System.out.println("=== Match clauses count: " + stmt.getMatchClauses().size());
    assertThat(stmt.getMatchClauses()).hasSize(1);

    final MatchClause matchClause = stmt.getMatchClauses().get(0);
    System.out.println("=== Has path patterns: " + matchClause.hasPathPatterns());
    System.out.println("=== Path patterns count: " + matchClause.getPathPatterns().size());

    assertThat(matchClause.hasPathPatterns()).isTrue();
    assertThat(matchClause.getPathPatterns()).hasSize(1);

    System.out.println("=== First pattern is single node: " + matchClause.getPathPatterns().get(0).isSingleNode());
    assertThat(matchClause.getPathPatterns().get(0).isSingleNode()).isTrue();
  }

  @Test
  void testQueryExecution() {
    database.transaction(() -> {
      database.newVertex("Person").set("name", "Alice").save();
      database.newVertex("Person").set("name", "Bob").save();
    });

    System.out.println("=== Executing query");
    final var result = database.query("opencypher", "MATCH (n:Person) RETURN n");

    System.out.println("=== Has next: " + result.hasNext());
    int count = 0;
    while (result.hasNext() && count < 10) {  // Safety limit
      System.out.println("=== Result " + count + ": " + result.next());
      count++;
    }
    System.out.println("=== Total results: " + count);

    assertThat(count).isEqualTo(2);
  }
}
