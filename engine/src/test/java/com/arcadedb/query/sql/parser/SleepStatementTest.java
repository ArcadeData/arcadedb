package com.arcadedb.query.sql.parser;

public class SleepStatementTest extends ParserTestAbstract {

  public void testPlain() {
    checkRightSyntax("SLEEP 100");

    checkWrongSyntax("SLEEP");
    checkWrongSyntax("SLEEP 1 3 5");
    checkWrongSyntax("SLEEP 1.5");
    checkWrongSyntax("SLEEP 1,5");
  }
}
