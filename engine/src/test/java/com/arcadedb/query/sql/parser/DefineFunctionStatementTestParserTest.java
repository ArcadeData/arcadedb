package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class DefineFunctionStatementTestParserTest extends AbstractParserTest {

  @Test
  public void testPlain() {
    checkRightSyntax("DEFINE FUNCTION test.print \"print('\\nTest!')\"");
    checkRightSyntax("DEFINE FUNCTION test.dummy \"return a + b;\" PARAMETERS [a,b]");
    checkRightSyntax("DEFINE FUNCTION users.allUsersButAdmin \"SELECT FROM ouser WHERE name <> 'admin'\" LANGUAGE SQL");
    checkRightSyntax("define function users.allUsersButAdmin \"SELECT FROM ouser WHERE name <> 'admin'\" parameters [a,b] language SQL");

    checkWrongSyntax("DEFINE FUNCTION test \"print('\\nTest!')\"");
  }
}
