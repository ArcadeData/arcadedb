package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.parser.*;
import org.junit.jupiter.api.Test;

public class AggregateDebugTest extends TestHelper {

  @Test
  public void debugAggregateInCollection() {
    final String className = "testAggregateInCollection";
    database.getSchema().createDocumentType(className);

    System.out.println("\n=== Testing aggregate in collection ===");
    final String query = "select [max(a), max(b)] from " + className;
    System.out.println("Query: " + query);

    // Parse and inspect the AST
    final Statement stmt = ((DatabaseInternal) database).getStatementCache().get(query);
    if (stmt instanceof SelectStatement selectStmt) {
      final Projection proj = selectStmt.getProjection();
      if (proj != null && proj.getItems() != null && !proj.getItems().isEmpty()) {
        final ProjectionItem item = proj.getItems().get(0);
        System.out.println("Projection item: " + item);

        final Expression expr = item.getExpression();
        System.out.println("Expression: " + expr);
        System.out.println("Expression.mathExpression: " + (expr != null ? expr.mathExpression : "null"));

        if (expr != null && expr.mathExpression != null) {
          System.out.println("MathExpression class: " + expr.mathExpression.getClass().getName());

          if (expr.mathExpression instanceof BaseExpression baseExpr) {
            System.out.println("BaseExpression found");
            System.out.println("  identifier: " + baseExpr.identifier);
            System.out.println("  expression: " + baseExpr.expression);
          }
        }
      }
    }

    try {
      final ResultSet result = database.query("sql", query);
      System.out.println("Query executed successfully!");
      result.close();
    } catch (final Exception x) {
      System.out.println("Exception caught: " + x.getClass().getName());
      System.out.println("Message: " + x.getMessage());
      x.printStackTrace();
      throw x;
    }
  }
}
