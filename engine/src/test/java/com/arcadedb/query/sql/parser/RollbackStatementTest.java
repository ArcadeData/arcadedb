package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class RollbackStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("ROLLBACK");
    checkRightSyntax("rollback");

    checkWrongSyntax("ROLLBACK RETRY 10");
  }
}
