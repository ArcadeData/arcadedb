package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class BeginStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("BEGIN");
    checkRightSyntax("begin");

    checkWrongSyntax("BEGIN foo ");
  }
}
