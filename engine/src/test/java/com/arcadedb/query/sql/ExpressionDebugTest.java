package com.arcadedb.query.sql;

import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.TestHelper;
import com.arcadedb.schema.Property;
import org.junit.jupiter.api.Test;

public class ExpressionDebugTest extends TestHelper {

  @Test
  void testAtThisExpression() {
    SQLAntlrParser parser = new SQLAntlrParser(database);

    // Parse just the expression part
    org.antlr.v4.runtime.CharStream input = org.antlr.v4.runtime.CharStreams.fromString(Property.THIS_PROPERTY);
    com.arcadedb.query.sql.grammar.SQLLexer lexer = new com.arcadedb.query.sql.grammar.SQLLexer(input);
    org.antlr.v4.runtime.CommonTokenStream tokens = new org.antlr.v4.runtime.CommonTokenStream(lexer);
    com.arcadedb.query.sql.grammar.SQLParser sqlParser = new com.arcadedb.query.sql.grammar.SQLParser(tokens);

    var exprCtx = sqlParser.expression();
    //System.out.println("Expression context: " + exprCtx.getClass().getSimpleName());
    //System.out.println("Expression text: " + exprCtx.getText());
    //System.out.println("Expression children count: " + exprCtx.getChildCount());

    com.arcadedb.query.sql.antlr.SQLASTBuilder builder = new com.arcadedb.query.sql.antlr.SQLASTBuilder(database);
    Expression expr = (Expression) builder.visit(exprCtx);

    //System.out.println("Expression result: " + expr);
    //System.out.println("Expression mathExpression: " + expr.mathExpression);
    if (expr.mathExpression != null) {
      //System.out.println("MathExpression class: " + expr.mathExpression.getClass().getName());
    }
  }
}
