package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

public class DebugMatchTest extends TestHelper {
  public DebugMatchTest() {
    autoStartTx = true;
  }

  @Override
  public void beginTest() {
    // Use EXACT same setup as MatchStatementExecutionTest
    database.command("sql", "CREATE VERTEX type Person");
    database.command("sql", "CREATE EDGE type Friend");
    database.command("sql", "CREATE VERTEX Person set name = 'n1'");
    database.command("sql", "CREATE VERTEX Person set name = 'n2'");
    database.command("sql", "CREATE VERTEX Person set name = 'n3'");
    database.command("sql", "CREATE VERTEX Person set name = 'n4'");
    database.command("sql", "CREATE VERTEX Person set name = 'n5'");
    database.command("sql", "CREATE VERTEX Person set name = 'n6'");

    final String[][] friendList = new String[][] { { "n1", "n2" }, { "n1", "n3" }, { "n2", "n4" }, { "n4", "n5" }, { "n4", "n6" } };
    for (final String[] pair : friendList) {
      database.command("sql", "CREATE EDGE Friend from (select from Person where name = ?) to (select from Person where name = ?)",
          pair[0], pair[1]);
    }

    // Add MathOp vertices like MatchStatementExecutionTest does
    database.command("sql", "CREATE VERTEX type MathOp");
    database.command("sql", "CREATE VERTEX MathOp set a = 1, b = 3, c = 2");
    database.command("sql", "CREATE VERTEX MathOp set a = 5, b = 3, c = 2");

    database.commit();
    database.begin();
  }

  @Test
  void compareMatchAST() {
    // Compare ASTs from both parsers WITHOUT cache interference
    final String matchQuery = "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return friend";

    System.out.println("\n=== Comparing Match ASTs (no cache) ===");

    // Parse with JavaCC directly
    try {
      final com.arcadedb.query.sql.parser.SqlParser javaccParser = new com.arcadedb.query.sql.parser.SqlParser(database, matchQuery);
      final com.arcadedb.query.sql.parser.MatchStatement javaccStmt = (com.arcadedb.query.sql.parser.MatchStatement) javaccParser.Parse();
      printMatchStatement("JavaCC", javaccStmt);
      System.out.println("\n=== JavaCC AST as JSON ===");
      System.out.println(prettyPrintJSON(javaccStmt.toJSON(), 0));
    } catch (Exception e) {
      System.out.println("JavaCC parse error: " + e.getMessage());
      e.printStackTrace();
    }

    // Parse with ANTLR directly
    try {
      final com.arcadedb.query.sql.antlr.SQLAntlrParser antlrParser = new com.arcadedb.query.sql.antlr.SQLAntlrParser(database);
      final com.arcadedb.query.sql.parser.Statement antlrStmt = antlrParser.parse(matchQuery);
      if (antlrStmt instanceof com.arcadedb.query.sql.parser.MatchStatement) {
        printMatchStatement("ANTLR", (com.arcadedb.query.sql.parser.MatchStatement) antlrStmt);
        System.out.println("\n=== ANTLR AST as JSON ===");
        System.out.println(prettyPrintJSON(((com.arcadedb.query.sql.parser.MatchStatement) antlrStmt).toJSON(), 0));
      }
    } catch (Exception e) {
      System.out.println("ANTLR parse error: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Test
  void debugCommonFriends() {
    System.out.println("\n=== Testing MATCH for common friends ===");
    System.out.println("Graph structure:");
    System.out.println("  n1 --Friend--> n2 --Friend--> n4");
    System.out.println("  n1 --Friend--> n3");

    // Test the EXACT failing query from MatchStatementExecutionTest.commonFriends()
    final String wrappedQuery = "select friend.name as name from (match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return friend)";
    System.out.println("Expected: 1 result (n2)");

    System.out.println("\n=== Testing WRAPPED query (SELECT...FROM MATCH) ===");
    System.out.println("Query: " + wrappedQuery);
    System.out.println("Expected result: n2");

    // Test with JavaCC parser
    System.out.println("\n=== JavaCC Wrapped Results ===");
    database.getConfiguration().setValue(com.arcadedb.GlobalConfiguration.SQL_PARSER_IMPLEMENTATION, "javacc");
    ResultSet wrappedJavaccResult = database.query("sql", wrappedQuery);
    int wrappedJavaccCount = 0;
    while (wrappedJavaccResult.hasNext()) {
      var result = wrappedJavaccResult.next();
      wrappedJavaccCount++;
      System.out.println("  JavaCC Result " + wrappedJavaccCount + ": " + result.getProperty("name"));
    }
    System.out.println("JavaCC Total results: " + wrappedJavaccCount);
    wrappedJavaccResult.close();

    // Test with ANTLR parser
    System.out.println("\n=== ANTLR Wrapped Results ===");
    database.getConfiguration().setValue(com.arcadedb.GlobalConfiguration.SQL_PARSER_IMPLEMENTATION, "antlr");
    ResultSet wrappedAntlrResult = database.query("sql", wrappedQuery);
    int wrappedAntlrCount = 0;
    while (wrappedAntlrResult.hasNext()) {
      var result = wrappedAntlrResult.next();
      wrappedAntlrCount++;
      System.out.println("  ANTLR Result " + wrappedAntlrCount + ": " + result.getProperty("name"));
    }
    System.out.println("ANTLR Total results: " + wrappedAntlrCount);
    wrappedAntlrResult.close();

    // Now test the unwrapped MATCH query
    System.out.println("\n=== Testing UNWRAPPED MATCH query ===");
    final String query = "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return friend";

    System.out.println("\n=== JavaCC Parser AST ===");
    try {
      final com.arcadedb.query.sql.parser.SqlParser javaccParser = new com.arcadedb.query.sql.parser.SqlParser(database, query);
      final com.arcadedb.query.sql.parser.MatchStatement javaccStmt = (com.arcadedb.query.sql.parser.MatchStatement) javaccParser.Parse();
      printMatchStatement("JavaCC", javaccStmt);
    } catch (Exception e) {
      System.out.println("JavaCC parse error: " + e.getMessage());
    }

    System.out.println("\n=== ANTLR Parser AST ===");
    try {
      final com.arcadedb.query.sql.antlr.SQLAntlrParser antlrParser = new com.arcadedb.query.sql.antlr.SQLAntlrParser(database);
      final com.arcadedb.query.sql.parser.Statement antlrStmt = antlrParser.parse(query);
      if (antlrStmt instanceof com.arcadedb.query.sql.parser.MatchStatement) {
        printMatchStatement("ANTLR", (com.arcadedb.query.sql.parser.MatchStatement) antlrStmt);
      }
    } catch (Exception e) {
      System.out.println("ANTLR parse error: " + e.getMessage());
      e.printStackTrace();
    }

    // Test with JavaCC parser
    System.out.println("\n=== JavaCC Query Results ===");
    database.getConfiguration().setValue(com.arcadedb.GlobalConfiguration.SQL_PARSER_IMPLEMENTATION, "javacc");
    ResultSet qResult = database.query("sql", query);
    int javaccCount = 0;
    while (qResult.hasNext()) {
      var result = qResult.next();
      javaccCount++;
      if (result.getProperty("friend") instanceof com.arcadedb.graph.Vertex vertex) {
        System.out.println("  JavaCC Result " + javaccCount + ": " + vertex.get("name"));
      }
    }
    System.out.println("JavaCC Total results: " + javaccCount);
    qResult.close();

    // Test with ANTLR parser
    System.out.println("\n=== ANTLR Query Results ===");
    database.getConfiguration().setValue(com.arcadedb.GlobalConfiguration.SQL_PARSER_IMPLEMENTATION, "antlr");
    qResult = database.query("sql", query);

    int antlrCount = 0;
    while (qResult.hasNext()) {
      var result = qResult.next();
      antlrCount++;
      if (result.getProperty("friend") instanceof com.arcadedb.graph.Vertex vertex) {
        System.out.println("  ANTLR Result " + antlrCount + ": " + vertex.get("name"));
      }
    }
    System.out.println("ANTLR Total results: " + antlrCount);
    qResult.close();
  }

  private void printMatchStatement(String parserName, com.arcadedb.query.sql.parser.MatchStatement stmt) {
    System.out.println(parserName + " - Number of match expressions: " + stmt.getMatchExpressions().size());
    for (int i = 0; i < stmt.getMatchExpressions().size(); i++) {
      final com.arcadedb.query.sql.parser.MatchExpression expr = stmt.getMatchExpressions().get(i);
      System.out.println("\n" + parserName + " - Match Expression " + i + ":");

      // Print origin details
      if (expr.getOrigin() != null) {
        System.out.println("  Origin items=" + expr.getOrigin().items.size() + ", alias=" + expr.getOrigin().getAlias());
        for (int k = 0; k < expr.getOrigin().items.size(); k++) {
          final var originItem = expr.getOrigin().items.get(k);
          StringBuilder typeStr = new StringBuilder();
          if (originItem.typeName != null) originItem.typeName.toString(null, typeStr);
          StringBuilder whereStr = new StringBuilder();
          if (originItem.filter != null) originItem.filter.toString(null, whereStr);
          System.out.println("    Origin item " + k + ": typeName=" + typeStr + ", where=" + whereStr + ", alias=" + originItem.alias);
        }
      } else {
        System.out.println("  Origin: NULL");
      }

      System.out.println("  Path items: " + expr.getItems().size());
      for (int j = 0; j < expr.getItems().size(); j++) {
        final com.arcadedb.query.sql.parser.MatchPathItem item = expr.getItems().get(j);
        System.out.println("    Item " + j + ":");
        System.out.println("      Class: " + item.getClass().getSimpleName());
        System.out.println("      Method: " + (item.getMethod() != null ? item.getMethod().methodName : "null"));

        // Print method parameters
        if (item.getMethod() != null && item.getMethod().params != null && !item.getMethod().params.isEmpty()) {
          System.out.println("      Method params (" + item.getMethod().params.size() + "):");
          for (int p = 0; p < item.getMethod().params.size(); p++) {
            final var param = item.getMethod().params.get(p);
            StringBuilder paramStr = new StringBuilder();
            param.toString(null, paramStr);
            System.out.println("        Param " + p + ": " + paramStr);
          }
        }

        // Print filter details
        if (item.getFilter() != null) {
          System.out.println("      Filter items=" + item.getFilter().items.size() + ", alias=" + item.getFilter().getAlias());
          for (int f = 0; f < item.getFilter().items.size(); f++) {
            final var filterItem = item.getFilter().items.get(f);
            StringBuilder typeStr = new StringBuilder();
            if (filterItem.typeName != null) filterItem.typeName.toString(null, typeStr);
            StringBuilder whereStr = new StringBuilder();
            if (filterItem.filter != null) filterItem.filter.toString(null, whereStr);
            System.out.println("        Filter item " + f + ": typeName=" + typeStr + ", where=" + whereStr + ", alias=" + filterItem.alias);
          }
        } else {
          System.out.println("      Filter: null");
        }
      }
    }
  }

  /**
   * Pretty-prints a JSON object (Map/List) with proper indentation.
   */
  private String prettyPrintJSON(Object obj, int indent) {
    StringBuilder sb = new StringBuilder();
    String indentStr = "  ".repeat(indent);

    if (obj instanceof java.util.Map) {
      sb.append("{\n");
      java.util.Map<?, ?> map = (java.util.Map<?, ?>) obj;
      java.util.List<? extends java.util.Map.Entry<?, ?>> entries = new java.util.ArrayList<>(map.entrySet());
      for (int i = 0; i < entries.size(); i++) {
        java.util.Map.Entry<?, ?> entry = entries.get(i);
        sb.append(indentStr).append("  \"").append(entry.getKey()).append("\": ");
        sb.append(prettyPrintJSON(entry.getValue(), indent + 1));
        if (i < entries.size() - 1) {
          sb.append(",");
        }
        sb.append("\n");
      }
      sb.append(indentStr).append("}");
    } else if (obj instanceof java.util.List) {
      java.util.List<?> list = (java.util.List<?>) obj;
      if (list.isEmpty()) {
        sb.append("[]");
      } else if (list.get(0) instanceof String || list.get(0) instanceof Number || list.get(0) instanceof Boolean) {
        // Simple array of primitives
        sb.append("[");
        for (int i = 0; i < list.size(); i++) {
          sb.append(prettyPrintJSON(list.get(i), 0));
          if (i < list.size() - 1) sb.append(", ");
        }
        sb.append("]");
      } else {
        // Array of objects
        sb.append("[\n");
        for (int i = 0; i < list.size(); i++) {
          sb.append(indentStr).append("  ");
          sb.append(prettyPrintJSON(list.get(i), indent + 1));
          if (i < list.size() - 1) {
            sb.append(",");
          }
          sb.append("\n");
        }
        sb.append(indentStr).append("]");
      }
    } else if (obj instanceof String) {
      sb.append("\"").append(obj).append("\"");
    } else if (obj instanceof Number || obj instanceof Boolean) {
      sb.append(obj);
    } else if (obj == null) {
      sb.append("null");
    } else {
      sb.append("\"").append(obj.toString()).append("\"");
    }

    return sb.toString();
  }
}
