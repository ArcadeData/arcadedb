package com.arcadedb.query.sql;

import com.arcadedb.query.sql.parser.InsertStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

public class ProjectionDebugTest extends TestHelper {

  @Test
  void testInsertReturnParsing() {
    SQLAntlrParser parser = new SQLAntlrParser(database);
    Statement stmt = parser.parse("INSERT INTO TestDoc SET name = 'test1' RETURN @this");

    System.out.println("Statement class: " + stmt.getClass().getName());

    if (stmt instanceof InsertStatement) {
      InsertStatement insertStmt = (InsertStatement) stmt;
      System.out.println("Return statement: " + insertStmt.returnStatement);
      System.out.println("Return statement is null: " + (insertStmt.returnStatement == null));

      if (insertStmt.returnStatement != null) {
        System.out.println("Items count: " + insertStmt.returnStatement.items.size());
        if (insertStmt.returnStatement.items.size() > 0) {
          var item = insertStmt.returnStatement.items.get(0);
          System.out.println("First item expression: " + item.expression);
          System.out.println("First item expression toString: " + item.expression.toString());
          System.out.println("First item expression class: " + item.expression.getClass().getName());
        }
      } else {
        System.out.println("ERROR: returnStatement is NULL!");
      }
    } else {
      System.out.println("ERROR: Statement is not InsertStatement!");
    }
  }
}
